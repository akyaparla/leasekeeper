import sqlite3
import asyncio
import functools
import itertools
import sys
from asyncio import StreamReader, StreamWriter

def get_port() -> int:  # configurable port
    if len(sys.argv) > 1:
        return int(sys.argv[1])
    return 6380

def get_metrics_port(port: int) -> int:
    if len(sys.argv) > 2:
        return int(sys.argv[2])
    return port + 1

from dispatch import dispatch_command, init_db
from metrics import Metrics, handle_metrics_request

_seq = itertools.count()

async def handle_client(queue: asyncio.PriorityQueue, metrics: Metrics, reader: StreamReader, writer: StreamWriter):
    try:
        while True:
            try:
                data = await reader.readline()
                if not data:
                    break
                message = data.decode()
                # grab commands and args
                parts = message.rstrip('\n').split() 
                if len(parts) == 0:
                    raise ValueError
                cmd, args = parts[0], parts[1:]

                # set empty "fut" var that the worker will populate with an err or response
                fut = asyncio.get_running_loop().create_future()
                priority = 0 if cmd == "ACQUIRE" else 1
                await queue.put((priority, next(_seq), cmd, args, fut))
                response = await fut
                # fut populated, return to client
                writer.write(response.encode())
            except (ValueError, UnicodeDecodeError):
                # Malformed or invalid input
                metrics.errors += 1
                writer.write(b"ERR bad-request\n")
                continue
            finally:
                await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()

async def worker(db: sqlite3.Connection, queue: asyncio.PriorityQueue, metrics: Metrics):
    # worker waits for queue to be populated, then runs dispatch
    while True:
        _, _, cmd, args, fut = await queue.get()
        try:
            result = await dispatch_command(db, cmd, args)
        except Exception as e:
            fut.set_exception(e)
        else:
            if cmd == "ACQUIRE":
                metrics.acquires += 1
            elif cmd == "RENEW":
                metrics.renews += 1
            fut.set_result(result)  # populated, go back to main server loop

async def start(host: str, port: int, db_path: str = "leases.db", metrics_port: int = 0):
    db = init_db(db_path)
    queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
    metrics = Metrics()
    # start worker on queue
    worker_task = asyncio.create_task(worker(db, queue, metrics))
    # use functools to feed servers with required inputs
    server = await asyncio.start_server(
        functools.partial(handle_client, queue, metrics), host, port
    )
    metrics_server = await asyncio.start_server(
        functools.partial(handle_metrics_request, metrics), host, metrics_port
    )
    return server, db, worker_task, metrics_server, metrics

async def main():
    port = get_port()
    server, db, worker_task, metrics_server, metrics = await start(
        '127.0.0.1', port, metrics_port=get_metrics_port(port)
    )
    addr = server.sockets[0].getsockname()
    metrics_addr = metrics_server.sockets[0].getsockname()
    print(f'Serving on {addr}')
    print(f'Metrics on http://{metrics_addr[0]}:{metrics_addr[1]}/metrics')

    async with server, metrics_server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())