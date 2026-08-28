import asyncio

import pytest

from conftest import Client


async def http_get(metrics_port: int, path: str = "/metrics", method: str = "GET") -> str:
    reader, writer = await asyncio.open_connection("127.0.0.1", metrics_port)
    writer.write(f"{method} {path} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode())
    await writer.drain()
    response = await reader.read(-1)  # server closes after responding, so EOF ends the read
    writer.close()
    await writer.wait_closed()
    return response.decode()


@pytest.mark.asyncio
async def test_metrics_starts_at_zero(running_server_with_metrics):
    _port, metrics_port = running_server_with_metrics
    response = await http_get(metrics_port)

    assert response.startswith("HTTP/1.1 200 OK\r\n")
    body = response.split("\r\n\r\n", 1)[1]
    assert body == "acquires 0\nrenews 0\nerrors 0\n"


@pytest.mark.asyncio
async def test_metrics_counts_acquires_renews_and_errors(running_server_with_metrics):
    port, metrics_port = running_server_with_metrics
    c = await Client.connect(port)

    resp = await c.send("ACQUIRE lock 5 alice")
    token, _ = resp.strip().split()
    await c.send("ACQUIRE lock 5 alice")  # contested, still counted as an ACQUIRE
    await c.send(f"RENEW lock {token} 10")
    await c.send("GARBAGE")  # bad-request, counted as an error
    await c.send("ACQUIRE lock")  # bad arity, counted as an error

    await c.close()

    response = await http_get(metrics_port)
    body = response.split("\r\n\r\n", 1)[1]
    assert body == "acquires 2\nrenews 1\nerrors 2\n"


@pytest.mark.asyncio
async def test_metrics_unknown_path_returns_404(running_server_with_metrics):
    _port, metrics_port = running_server_with_metrics
    response = await http_get(metrics_port, path="/nope")
    assert response.startswith("HTTP/1.1 404 Not Found\r\n")
