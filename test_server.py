import asyncio
import itertools
import socket
import threading

import pytest

import server
from conftest import Client


# --- protocol / connection handling -----------------------------------------

@pytest.mark.asyncio
async def test_multiple_sequential_commands_same_connection(client):
    resp1 = await client.send("ACQUIRE lock 5 alice")
    assert resp1 != "NULL\n"
    resp2 = await client.send("WHO lock")
    assert resp2 == "alice\n"
    resp3 = await client.send("TTL lock")
    assert resp3.strip().isdigit()


@pytest.mark.asyncio
async def test_malformed_input_returns_bad_request_and_connection_stays_open(client):
    resp1 = await client.send("GARBAGE")
    assert resp1 == "ERR bad-request\n"
    resp2 = await client.send("ACQUIRE lock 5")
    assert resp2 != "NULL\n"


@pytest.mark.asyncio
async def test_empty_line_returns_bad_request(client):
    assert await client.send("") == "ERR bad-request\n"


@pytest.mark.asyncio
async def test_unknown_command_returns_bad_request(client):
    assert await client.send("FOOBAR arg1 arg2") == "ERR bad-request\n"


@pytest.mark.asyncio
async def test_acquire_missing_ttl_returns_bad_request(client):
    assert await client.send("ACQUIRE lockonly") == "ERR bad-request\n"


@pytest.mark.asyncio
async def test_disconnect_mid_command_does_not_crash_server(running_server):
    reader, writer = await asyncio.open_connection("127.0.0.1", running_server)
    writer.write(b"ACQ")  # no trailing newline, then abrupt disconnect
    await writer.drain()
    writer.close()
    try:
        await writer.wait_closed()
    except Exception:
        pass

    # server must still be alive and correctly serving other connections
    c = await Client.connect(running_server)
    resp = await c.send("ACQUIRE stillworks 5")
    assert resp != "NULL\n"
    await c.close()


@pytest.mark.asyncio
async def test_list_reflects_active_leases(client, running_server):
    assert await client.send("LIST") == "\n"

    await client.send("ACQUIRE zeta 5 alice")
    await client.send("ACQUIRE alpha 5 bob")

    other = await Client.connect(running_server)
    resp = await other.send("LIST")
    await other.close()

    assert resp == "alpha zeta\n"


# --- concurrency: the spec's actual correctness requirement -----------------

@pytest.mark.asyncio
async def test_concurrent_acquire_same_name_only_one_wins(running_server):
    n = 20
    clients = [await Client.connect(running_server) for _ in range(n)]
    try:
        responses = await asyncio.gather(
            *(c.send(f"ACQUIRE racelock 5 client{i}") for i, c in enumerate(clients))
        )
    finally:
        for c in clients:
            await c.close()

    winners = [r for r in responses if r != "NULL\n"]
    losers = [r for r in responses if r == "NULL\n"]
    assert len(winners) == 1
    assert len(losers) == n - 1


# --- ACQUIRE prioritization --------------------------------------------------

@pytest.mark.asyncio
async def test_priority_queue_dequeues_acquire_first_regardless_of_arrival_order():
    """Direct test of the ordering primitive server.py relies on."""
    queue = asyncio.PriorityQueue()
    seq = itertools.count()
    order_in = ["WHO", "TTL", "RELEASE", "ACQUIRE", "RENEW", "ACQUIRE", "WHO"]
    for cmd in order_in:
        priority = 0 if cmd == "ACQUIRE" else 1
        await queue.put((priority, next(seq), cmd, [], None))

    order_out = []
    while not queue.empty():
        _, _, cmd, _, _ = queue.get_nowait()
        order_out.append(cmd)

    assert order_out == ["ACQUIRE", "ACQUIRE", "WHO", "TTL", "RELEASE", "RENEW", "WHO"]


@pytest.mark.asyncio
async def test_tcp_acquire_arriving_during_backlog_is_prioritized(monkeypatch):
    """A TCP ACQUIRE must preempt pending work after the active command finishes."""
    backlog_size = 100
    queue = asyncio.PriorityQueue()
    metrics = server.Metrics()
    db = server.init_db(":memory:")
    execution_order = []
    backlog_at_first_dispatch = []
    first_who_started = threading.Event()
    acquire_client_ready = threading.Event()
    acquire_sent = threading.Event()
    real_dispatch = server.dispatch_command

    async def record_dispatch(db_connection, cmd, args):
        execution_order.append(cmd)
        if cmd == "WHO" and not first_who_started.is_set():
            backlog_at_first_dispatch.append(queue.qsize())
            first_who_started.set()
            assert acquire_sent.wait(timeout=5)
        return await real_dispatch(db_connection, cmd, args)

    monkeypatch.setattr(server, "dispatch_command", record_dispatch)
    worker_task = asyncio.create_task(server.worker(db, queue, metrics))
    tcp_server = await asyncio.start_server(
        lambda reader, writer: server.handle_client(queue, metrics, reader, writer),
        "127.0.0.1",
        0,
    )
    port = tcp_server.sockets[0].getsockname()[1]

    def acquire_from_thread():
        with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
            acquire_client_ready.set()
            if not first_who_started.wait(timeout=5):
                raise TimeoutError("WHO backlog did not start")
            sock.sendall(b"ACQUIRE new-lock 30 owner\n")
            acquire_sent.set()
            with sock.makefile("rb") as response_file:
                return response_file.readline().decode()

    acquire_task = asyncio.create_task(asyncio.to_thread(acquire_from_thread))
    who_clients = []
    try:
        while not acquire_client_ready.is_set():
            await asyncio.sleep(0)

        who_clients = [await Client.connect(port) for _ in range(backlog_size)]
        who_tasks = [
            asyncio.create_task(client.send("WHO existing-lock"))
            for client in who_clients
        ]
        await asyncio.gather(*who_tasks)
        acquire_response = await acquire_task
    finally:
        for client in who_clients:
            await client.close()
        tcp_server.close()
        await tcp_server.wait_closed()
        worker_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker_task
        db.close()

    assert acquire_response != "NULL\n"
    assert backlog_at_first_dispatch[0] > 0
    assert execution_order.index("ACQUIRE") < 10


@pytest.mark.asyncio
async def test_acquire_jumps_ahead_of_a_backlog(monkeypatch, running_server):
    """End-to-end: with a real backlog queued through the live server, a late
    ACQUIRE still finishes well ahead of earlier-queued non-ACQUIRE commands."""
    real_dispatch = server.dispatch_command

    async def slow_dispatch(db, cmd, args):
        if cmd != "ACQUIRE":
            await asyncio.sleep(0.05)
        return await real_dispatch(db, cmd, args)

    monkeypatch.setattr(server, "dispatch_command", slow_dispatch)

    seed = await Client.connect(running_server)
    await seed.send("ACQUIRE seedlock 100 seedowner")

    who_clients = [await Client.connect(running_server) for _ in range(5)]
    who_tasks = [asyncio.create_task(c.send("WHO seedlock")) for c in who_clients]

    await asyncio.sleep(0.01)  # let the WHOs get enqueued (and the first one start)

    acquire_client = await Client.connect(running_server)
    acquire_task = asyncio.create_task(acquire_client.send("ACQUIRE newlock 5 x"))

    remaining = set(who_tasks) | {acquire_task}
    finish_order = []
    while remaining:
        done, remaining = await asyncio.wait(remaining, return_when=asyncio.FIRST_COMPLETED)
        finish_order.extend(done)

    acquire_rank = finish_order.index(acquire_task)
    assert acquire_rank <= 2  # finishes near the front, not after the whole backlog

    for c in who_clients:
        await c.close()
    await acquire_client.close()
    await seed.close()
