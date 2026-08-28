import asyncio
import itertools

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
