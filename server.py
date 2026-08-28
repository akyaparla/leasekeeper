import sqlite3
import asyncio
import functools
from asyncio import StreamReader, StreamWriter

from dispatch import dispatch_command, init_db

async def handle_client(db: sqlite3.Connection, reader: StreamReader, writer: StreamWriter):
    try:
        while True:
            try:
                data = await reader.readline()
                if not data:
                    break
                message = data.decode()
                parts = message.rstrip('\n').split()
                if len(parts) == 0:
                    raise ValueError
                response = await dispatch_command(db, parts[0], parts[1:])

                writer.write(response.encode())
            except (ValueError, UnicodeDecodeError):
                writer.write(b"ERR bad-request\n")
                continue
            finally:
                await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()

async def main():
    db = init_db()
    server = await asyncio.start_server(
        functools.partial(handle_client, db), '127.0.0.1', 6380
    )
    addr = server.sockets[0].getsockname()
    print(f'Serving on {addr}')

    async with server:
        await server.serve_forever()

asyncio.run(main())