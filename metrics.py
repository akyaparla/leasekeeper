import asyncio

class Metrics:
    """Plain counters shared between the worker (writer) and the HTTP handler (reader).
    Safe without locking: asyncio is single-threaded, and increments/reads never await."""

    def __init__(self):
        self.acquires = 0
        self.renews = 0
        self.errors = 0

    def render(self) -> bytes:
        return (
            f"acquires {self.acquires}\n"
            f"renews {self.renews}\n"
            f"errors {self.errors}\n"
        ).encode()

async def handle_metrics_request(
    metrics: Metrics, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
):
    try:
        request_line = await reader.readline()
        while True:
            # drain and discard headers up to the blank line ending the request
            line = await reader.readline()
            if line in (b"\r\n", b"\n", b""):
                break

        try:
            method, path = request_line.decode().split()[:2]
        except (ValueError, UnicodeDecodeError):
            method, path = "", ""

        if method == "GET" and path == "/metrics":
            status, body = "200 OK", metrics.render()
        else:
            status, body = "404 Not Found", b"not found\n"

        headers = (
            f"HTTP/1.1 {status}\r\n"
            f"Content-Type: text/plain\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"Connection: close\r\n"
            f"\r\n"
        )
        writer.write(headers.encode() + body)
        await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()
