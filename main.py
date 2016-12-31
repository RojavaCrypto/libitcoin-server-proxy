import logging
#logging.basicConfig(level=logging.DEBUG)

import random
import struct
import sys
import time
import traceback
import zmq
import zmq.asyncio
import asyncio

# Here is the config.
local_port = 8081
remotes = [
    ("tcp://163.172.84.141:9091", "tcp://163.172.84.141:9092"),
    ("tcp://163.172.84.141:10091", "tcp://163.172.84.141:10092")
]

loop = zmq.asyncio.ZMQEventLoop()
asyncio.set_event_loop(loop)

class Proxy:

    def __init__(self, local_url):
        self.local_url = local_url
        self.remote_nodes = []

        self._stopped = False

    def add_remote(self, query_url, heartbeat_url):
        self.remote_nodes += [RemoteNode(query_url, heartbeat_url)]

    def start(self):
        self._listen = Interface(self.local_url)

        context = zmq.asyncio.Context()
        self._listen.start(context)
        [remote.start(context) for remote in self.remote_nodes]

        self._poller = zmq.asyncio.Poller()
        self._poller.register(self._listen.socket, zmq.POLLIN)
        for remote in self.remote_nodes:
            self._poller.register(remote.socket, zmq.POLLIN)

    def stop(self):
        [remote.stop() for remote in self.remote_nodes]
        self._stopped = True

    async def run(self):
        # Listen for something from localhost
        # Send and route response from server
        while not self._stopped:
            events = await self._poller.poll(timeout=0.1)
            for socket, mask in events:
                await self._process(socket)

    async def _process(self, socket):
        if socket == self._listen.socket:
            await self._forward_request()
        else:
            match = [remote for remote in self.remote_nodes
                     if remote.socket == socket]
            if not match:
                return
            assert len(match) == 1
            remote = match[0]
            await self._return_response(remote)

    async def _forward_request(self):
        request = await self._listen.receive()

        if request is None:
            return

        if self.current_remote is None:
            print("Dropping:", request)
            return

        await self.current_remote.send(request)
        print("Using %s." % self.current_remote.url)
        print("Sent:", request)

    async def _return_response(self, remote):
        response = await remote.receive()

        if response is None:
            return

        await self._listen.send(response)

    @property
    def current_remote(self):
        if not self.remote_nodes:
            return None
        active_remotes = [remote for remote in self.remote_nodes
                          if remote.stats.interval < 10]
        if not active_remotes:
            return None
        remote = max(active_remotes, key=lambda remote: remote.stats.height)
        return remote

class Interface:

    def __init__(self, local_port):
        self._local_port = local_port

        self._table = {}

    def start(self, context):
        url = "tcp://*:%s" % self._local_port
        self.socket = context.socket(zmq.ROUTER)
        self.socket.bind(url)
        print("Listening:", url)

    async def receive(self):
        request = await self.socket.recv_multipart()
        if len(request) != 4:
            print("Error: invalid request:", request, file=sys.stderr)
            return

        stripped_request = self._record(request)
        print("Recorded:", request)
        return stripped_request

    def _record(self, request):
        ident, command, request_id, data = request

        if command not in self._table:
            self._table[command] = {}

        self._table[command][request_id] = ident

        return command, request_id, data

    async def send(self, response):
        command, response_id, data = response

        if command not in self._table:
            print("Error: unheard of command:", response, file=sys.stderr)
            return
        if response_id not in self._table[command]:
            print("Error: unknown response id:", response_id, file=sys.stderr)
            return

        ident = self._table[command][response_id]
        del self._table[command][response_id]

        forward_response = ident, command, response_id, data
        await self.socket.send_multipart(forward_response)

class RemoteNode:

    def __init__(self, query_url, heartbeat_url):
        self._url = query_url
        self.stats = NodeStats(query_url, heartbeat_url)

    def start(self, context):
        self.socket = context.socket(zmq.DEALER)
        self.socket.connect(self._url)
        print("Connected:", self._url)
        self.stats.start(context)

    def stop(self):
        self.stats.stop()

    @property
    def url(self):
        return self._url

    async def send(self, request):
        await self.socket.send_multipart(request)

    async def receive(self):
        response = await self.socket.recv_multipart()
        if len(response) != 3:
            print("Error: invalid response:", response, file=sys.stderr)
            return
        return response

class NodeStats:

    def __init__(self, query_url, heartbeat_url):
        self._query_url = query_url
        self._heartbeat_url = heartbeat_url
        self._last_time = time.time()
        self._height = 0
        self._stopped = False

    def start(self, context):
        loop = asyncio.get_event_loop()
        loop.create_task(self._monitor_heartbeat(context))
        loop.create_task(self._monitor_height(context))

    def stop(self):
        self._stopped = True

    async def _monitor_heartbeat(self, context):
        socket = context.socket(zmq.SUB)
        socket.connect(self._heartbeat_url)
        socket.setsockopt(zmq.SUBSCRIBE, b"")
        while not self._stopped:
            response = await socket.recv_multipart()
            self._last_time = time.time()

    @staticmethod
    def _create_random_id():
        MAX_UINT32 = 4294967295
        return random.randint(0, MAX_UINT32)

    async def _monitor_height(self, context):
        while not self._stopped:
            self._height = await self._get_height(context)
            print("Height %s: %s" % (self._height, self._query_url))
            await asyncio.sleep(20)

    async def _get_height(self, context):
        query_socket = context.socket(zmq.DEALER)
        query_socket.connect(self._query_url)

        command = b"blockchain.fetch_last_height"
        ident = self._create_random_id()
        data = b""

        request = [
            command,
            struct.pack('<I', ident),
            data
        ]
        await query_socket.send_multipart(request)

        response = await query_socket.recv_multipart()

        command, response_ident, data = response
        if struct.unpack('<I', response_ident)[0] != ident:
            print("Error: non-matching idents", file=sys.stderr)
            return
        error = struct.unpack('<I', data[:4])[0]
        if error:
            print("Error: returning height in stats", file=sys.stderr)
            return

        height = struct.unpack('<I', data[4:])[0]
        return height

    @property
    def height(self):
        return self._height

    @property
    def interval(self):
        delta = time.time() - self._last_time
        assert delta > 0
        return delta

async def main(local_port, remotes):
    try:
        proxy = Proxy(local_port)
        for query_url, heartbeat_url in remotes:
            proxy.add_remote(query_url, heartbeat_url)
        proxy.start()

        await proxy.run()
    except:
        traceback.print_exc()

if __name__ == '__main__':
    tasks = [
        main(local_port, remotes)
    ]
    #loop.set_debug(True)
    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()

