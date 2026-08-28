import sqlite3
import asyncio
import functools
import itertools
import sys
from asyncio import StreamReader, StreamWriter

def get_port() -> int:
    if len(sys.argv) > 1:
        return int(sys.argv[1])
    return 6380

from dispatch import dispatch_command, init_db

_seq = itertools.count()

async def handle_client(queue: asyncio.PriorityQueue, reader: StreamReader, writer: StreamWriter):
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
                cmd, args = parts[0], parts[1:]

                fut = asyncio.get_running_loop().create_future()
                priority = 0 if cmd == "ACQUIRE" else 1
                await queue.put((priority, next(_seq), cmd, args, fut))
                response = await fut

                writer.write(response.encode())
            except (ValueError, UnicodeDecodeError):
                writer.write(b"ERR bad-request\n")
                continue
            finally:
                await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()

async def worker(db: sqlite3.Connection, queue: asyncio.PriorityQueue):
    while True:
        _, _, cmd, args, fut = await queue.get()
        try:
            result = await dispatch_command(db, cmd, args)
        except Exception as e:
            fut.set_exception(e)
        else:
            fut.set_result(result)

async def start(host: str, port: int, db_path: str = "leases.db"):
    db = init_db(db_path)
    queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
    worker_task = asyncio.create_task(worker(db, queue))
    server = await asyncio.start_server(
        functools.partial(handle_client, queue), host, port
    )
    return server, db, worker_task

async def main():
    server, db, worker_task = await start('127.0.0.1', get_port())
    addr = server.sockets[0].getsockname()
    print(f'Serving on {addr}')

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())