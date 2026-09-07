"""Real WebSocket integration tests. Does not substitute for Godot RPC tests."""
import asyncio
import json
import unittest
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed
from relay import Relay, HEADER, PROTOCOL

class Integration(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.relay = Relay()
        self.server = await serve(self.relay.handler, '127.0.0.1', 0, compression=None)
        self.url = 'ws://127.0.0.1:' + str(self.server.sockets[0].getsockname()[1])
        self.sockets = []

    async def asyncTearDown(self):
        await asyncio.gather(*(ws.close() for ws in self.sockets))
        self.server.close()
        await self.server.wait_closed()

    async def enter(self, code=None, protocol=PROTOCOL):
        ws = await connect(self.url, compression=None)
        self.sockets.append(ws)
        await ws.send(json.dumps({'op': 'create' if code is None else 'join', 'code': code, 'protocol': protocol}))
        return ws, json.loads(await asyncio.wait_for(ws.recv(), 2))

    async def binary(self, ws):
        while True:
            value = await asyncio.wait_for(ws.recv(), 2)
            if isinstance(value, bytes):
                return value

    async def test_eight_players_and_private_routing(self):
        host, ready = await self.enter()
        clients = []
        for i in range(7):
            client, info = await self.enter(ready['code'])
            self.assertEqual(info['id'], i + 2)
            clients.append(client)
        _, error = await self.enter(ready['code'])
        self.assertEqual(error['op'], 'error')
        await clients[0].send(HEADER.pack(1, 1, 1) + b'input')
        self.assertEqual(await self.binary(host), HEADER.pack(2, 1, 1) + b'input')
        await host.send(HEADER.pack(0, 2, 1) + b'world')
        for client in clients:
            self.assertEqual(await self.binary(client), HEADER.pack(1, 2, 1) + b'world')
        await host.send(HEADER.pack(2, 3, 1) + b'private-goals')
        self.assertTrue((await self.binary(clients[0])).endswith(b'private-goals'))
        # Marker shows ordering: other peers must not see the private packet first.
        await host.send(HEADER.pack(0, 0, 2) + b'end')
        for client in clients:
            self.assertTrue((await self.binary(client)).endswith(b'end'))

    async def test_lock_unlock_and_cleanup(self):
        host, info = await self.enter()
        code = info['code']
        await host.send(json.dumps({'op': 'lock', 'locked': True}))
        # Wait for relay state, not an arbitrary sleep.
        async with asyncio.timeout(2):
            while not self.relay.rooms[code].locked:
                await asyncio.sleep(0)
        _, rejection = await self.enter(code)
        self.assertEqual(rejection['op'], 'error')
        await host.send(json.dumps({'op': 'lock', 'locked': False}))
        async with asyncio.timeout(2):
            while self.relay.rooms[code].locked:
                await asyncio.sleep(0)
        client, joined = await self.enter(code)
        self.assertEqual(joined['op'], 'ready')
        await host.close()
        await asyncio.wait_for(client.wait_closed(), 2)
        self.assertNotIn(code, self.relay.rooms)

    async def test_room_isolation_and_client_authority(self):
        host, info = await self.enter()
        other, _ = await self.enter()
        client, _ = await self.enter(info['code'])
        await client.send(HEADER.pack(0, 0, 2) + b'forged-authority')
        self.assertEqual(json.loads(await client.recv())['op'], 'error')
        await asyncio.wait_for(client.wait_closed(), 2)
        await other.send(HEADER.pack(0, 0, 2) + b'other-room')
        await host.send(HEADER.pack(0, 0, 2) + b'own-room')
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(other.recv(), 0.1)

    async def test_unknown_room_and_version(self):
        _, unknown = await self.enter('000000000000')
        self.assertEqual(unknown['op'], 'error')
        _, mismatch = await self.enter(protocol='old')
        self.assertEqual(mismatch['op'], 'error')

    async def test_kick_and_rejoin(self):
        host, info = await self.enter()
        client, ready = await self.enter(info['code'])
        await host.send(json.dumps({'op': 'kick', 'id': ready['id']}))
        await asyncio.wait_for(client.wait_closed(), 2)
        _, fresh = await self.enter(info['code'])
        self.assertGreater(fresh['id'], ready['id'])

if __name__ == '__main__':
    unittest.main()
