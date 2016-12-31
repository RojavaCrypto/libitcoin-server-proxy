import logging
logging.basicConfig(level=logging.DEBUG)

import sys
import traceback
import zmq
import zmq.asyncio
import asyncio

loop = zmq.asyncio.ZMQEventLoop()
asyncio.set_event_loop(loop)

class Proxy:

    def __init__(self, local_url):
        self.local_url = local_url
        self.remote_nodes = []

        self._stopped = False

    def add_remote(self, url):
        self.remote_nodes += [RemoteNode(url)]

    def start(self):
        self._listen = Interface(self.local_url)

        context = zmq.asyncio.Context()
        self._listen.start(context)
        [remote_url.start(context) for remote_url in self.remote_nodes]

        self._poller = zmq.asyncio.Poller()
        self._poller.register(self._listen.socket, zmq.POLLIN)
        for remote in self.remote_nodes:
            self._poller.register(remote.socket, zmq.POLLIN)

    def stop(self):
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

        await self.current_remote.send(request)
        print("Sent:", request)

    async def _return_response(self, remote):
        response = await remote.receive()

        if response is None:
            return

        await self._listen.send(response)

    @property
    def current_remote(self):
        return self.remote_nodes[0]

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

    def __init__(self, url):
        self._url = url

    def start(self, context):
        self.socket = context.socket(zmq.DEALER)
        self.socket.connect(self._url)
        print("Connected:", self._url)

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

async def main(local_port, remote_url):
    try:
        proxy = Proxy(local_port)
        proxy.add_remote(remote_url)
        proxy.start()

        await proxy.run()
    except:
        traceback.print_exc()

import libbitcoin.server
context = libbitcoin.server.Context()

async def fake_connect(port):
    url = "tcp://localhost:%s" % port
    print("Connecting:", url)

    client = context.Client(url)

    ec, height = await client.last_height()
    if ec:
        print("Couldn't fetch last_height:", ec, file=sys.stderr)
        context.stop()
        return
    print("Last height:", height)

    ec, total_connections = await client.total_connections()
    if ec:
        print("Couldn't fetch total_connections:", ec, file=sys.stderr)
        context.stop()
        return
    print("Total server connections:", total_connections)

    context.stop()

if __name__ == '__main__':
    local_port = 8081
    remote_url = "tcp://163.172.84.141:9091"

    tasks = [
        #fake_connect(local_port),
        main(local_port, remote_url)
    ]
    tasks.extend(context.tasks())
    loop.set_debug(True)
    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()

