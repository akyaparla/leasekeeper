import asyncio

import pytest
import pytest_asyncio

import dispatch
import server


@pytest.fixture
def db():
    """In-memory lease store for unit tests against dispatch.py directly."""
    conn = dispatch.init_db(":memory:")
    yield conn
    conn.close()


class FakeClock:
    """Deterministic stand-in for time.time(), advanced manually by tests."""

    def __init__(self, t: float = 1_000_000.0):
        self.t = t

    def time(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


@pytest.fixture
def clock(monkeypatch):
    fc = FakeClock()
    monkeypatch.setattr(dispatch.time, "time", fc.time)
    return fc


@pytest_asyncio.fixture
async def running_server():
    """A real server instance (server.py's actual start()) on an ephemeral port."""
    srv, db_conn, worker_task, metrics_srv, _metrics = await server.start("127.0.0.1", 0, db_path=":memory:")
    port = srv.sockets[0].getsockname()[1]
    try:
        yield port
    finally:
        worker_task.cancel()
        srv.close()
        metrics_srv.close()
        await srv.wait_closed()
        await metrics_srv.wait_closed()
        db_conn.close()


@pytest_asyncio.fixture
async def running_server_with_metrics():
    """Like running_server, but also exposes the /metrics HTTP port for metrics tests."""
    srv, db_conn, worker_task, metrics_srv, _metrics = await server.start("127.0.0.1", 0, db_path=":memory:")
    port = srv.sockets[0].getsockname()[1]
    metrics_port = metrics_srv.sockets[0].getsockname()[1]
    try:
        yield port, metrics_port
    finally:
        worker_task.cancel()
        srv.close()
        metrics_srv.close()
        await srv.wait_closed()
        await metrics_srv.wait_closed()
        db_conn.close()


class Client:
    """Small helper wrapping one TCP connection for protocol-level tests."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer

    @classmethod
    async def connect(cls, port: int) -> "Client":
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        return cls(reader, writer)

    async def send(self, line: str) -> str:
        self.writer.write((line + "\n").encode())
        await self.writer.drain()
        data = await self.reader.readline()
        return data.decode()

    async def close(self):
        self.writer.close()
        await self.writer.wait_closed()


@pytest_asyncio.fixture
async def client(running_server):
    c = await Client.connect(running_server)
    yield c
    await c.close()
