"""Ephemeral, host-authoritative room relay. No gameplay or persistent accounts."""
import asyncio
import http
import signal
import json
import os
import secrets
import struct
import time
from dataclasses import dataclass, field
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

PROTOCOL = 'dah-relay-1'
HEADER = struct.Struct('<iBB')  # target/sender, channel, transfer mode
MAX_PACKET = 65536
MAX_ROOMS = 100

@dataclass
class Room:
    peers: dict = field(default_factory=dict)
    next_id: int = 2
    locked: bool = False

class Relay:
    def __init__(self):
        self.rooms = {}

    async def send(self, ws, message):
        if ws.transport.get_write_buffer_size() > 1024 * 1024:
            await ws.close(1013, 'Slow receiver')
            return
        try:
            await asyncio.wait_for(ws.send(message if isinstance(message, bytes) else json.dumps(message)), 3)
        except (ConnectionClosed, asyncio.TimeoutError):
            await ws.close()

    async def handler(self, ws):
        code, ident, room = '', 0, None
        try:
            raw = await asyncio.wait_for(ws.recv(), 10)
            if not isinstance(raw, str) or len(raw) > 512:
                raise ValueError('Invalid handshake')
            hello = json.loads(raw)
            if not isinstance(hello, dict) or hello.get('protocol') != PROTOCOL:
                raise ValueError('Different game version')
            if hello.get('op') == 'create':
                if len(self.rooms) >= MAX_ROOMS:
                    raise ValueError('Server full')
                code = secrets.token_hex(6).upper()
                while code in self.rooms:
                    code = secrets.token_hex(6).upper()
                room = Room()
                self.rooms[code] = room
                ident = 1
            elif hello.get('op') == 'join':
                code = str(hello.get('code', '')).upper()
                room = self.rooms.get(code)
                if room is None:
                    raise ValueError('Room not found')
                if room.locked or len(room.peers) >= 8:
                    raise ValueError('Room started or full')
                ident = room.next_id
                room.next_id += 1
            else:
                raise ValueError('Invalid request')
            existing = list(room.peers)
            room.peers[ident] = ws
            await self.send(ws, {'op': 'ready', 'id': ident, 'code': code, 'peers': existing})
            for pid in existing:
                await self.send(room.peers.get(pid, ws), {'op': 'joined', 'id': ident})
            window, count, volume = time.monotonic(), 0, 0
            async for raw in ws:
                if time.monotonic() - window >= 1:
                    window, count, volume = time.monotonic(), 0, 0
                count += 1
                volume += len(raw)
                if count > 500 or volume > 4 * 1024 * 1024:
                    raise ValueError('Rate limit')
                if isinstance(raw, str):
                    if len(raw) > 512:
                        raise ValueError('Control too large')
                    msg = json.loads(raw)
                    if not isinstance(msg, dict) or ident != 1:
                        raise ValueError('Only host may control room')
                    if msg.get('op') == 'lock' and isinstance(msg.get('locked'), bool):
                        room.locked = msg['locked']
                    elif msg.get('op') == 'kick':
                        victim = room.peers.get(int(msg.get('id', 0)))
                        if victim is not None and victim is not ws:
                            await victim.close(1000, 'Removed by host')
                    else:
                        raise ValueError('Invalid control')
                    continue
                if not HEADER.size < len(raw) <= MAX_PACKET + HEADER.size:
                    raise ValueError('Invalid packet size')
                target, channel, mode = HEADER.unpack_from(raw)
                if channel > 3 or mode > 2:
                    raise ValueError('Invalid packet metadata')
                # Clients can send only to authority. Sender ID is always overwritten.
                if ident != 1 and target != 1:
                    raise ValueError('Client target must be host')
                packet = HEADER.pack(ident, channel, mode) + raw[HEADER.size:]
                recipients = [sock for pid, sock in room.peers.items()
                              if pid != ident and (target == 0 or pid == target or (target < 0 and pid != -target))]
                await asyncio.gather(*(self.send(sock, packet) for sock in recipients))
        except (ValueError, TypeError, KeyError, asyncio.TimeoutError) as exc:
            await self.send(ws, {'op': 'error', 'message': str(exc)})
            await ws.close(1008, 'Rejected')
        except ConnectionClosed:
            pass
        finally:
            if room is not None and room.peers.get(ident) is ws:
                room.peers.pop(ident, None)
                if ident == 1:
                    self.rooms.pop(code, None)
                    await asyncio.gather(*(sock.close(1000, 'Host left') for sock in list(room.peers.values())))
                else:
                    await asyncio.gather(*(self.send(sock, {'op': 'left', 'id': ident}) for sock in list(room.peers.values())))

def health_check(connection, request):
    if request.path == '/healthz':
        return connection.respond(http.HTTPStatus.OK, 'OK\n')

async def main():
    relay = Relay()
    stop = asyncio.get_running_loop().create_future()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, stop.set_result, None)
    async with serve(relay.handler, '0.0.0.0', int(os.getenv('PORT', '8080')),
                     process_request=health_check, max_size=MAX_PACKET + HEADER.size,
                     max_queue=16, compression=None, ping_interval=20, ping_timeout=20):
        print('Relay listening', flush=True)
        await stop

if __name__ == '__main__':
    asyncio.run(main())
