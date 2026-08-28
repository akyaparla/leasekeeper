# LeaseKeeper

A tiny TCP service for ephemeral, exclusive leases with TTLs. Clients acquire a named lease,
renew it before it expires, and release it when done. Leases that aren't renewed in time
expire automatically.

## Quickstart

```
python server.py [port] [metrics_port]   # port defaults to 6380, metrics_port to port+1
```

Listens on `127.0.0.1`. `GET http://127.0.0.1:<metrics_port>/metrics` returns plain-text counters
(`acquires`, `renews`, `errors`).

```
pip install pytest-asyncio
pytest
```

`test_dispatch.py` covers the lease-store handlers, `test_server.py` covers protocol/connection
behavior, `LIST`, and the concurrent-`ACQUIRE` race, `test_metrics.py` covers the `/metrics`
endpoint.

## Protocol

One ASCII command per line (`\n`-terminated), space-separated arguments. Responses are also
space-delimited words terminated by `\n`.

| Command | Args | Returns |
|---|---|---|
| `ACQUIRE` | `name ttl_secs [owner]` | `token version` on success, `NULL` if held by someone else |
| `RENEW` | `name token ttl_secs` | updated `version` on success, else `ERR not-found`/`wrong-token`/`expired` |
| `RELEASE` | `name token` | `OK` on success, else `ERR not-found`/`wrong-token`/`expired` |
| `WHO` | `name` | `owner` if held (non-expired), else `NULL` |
| `TTL` | `name` | remaining TTL if held, else appropriate response |
| `LIST` | *(none)* | space-separated names of all active leases, or an empty line if none |

## Design Notes

### Assumptions

- `version` only changes on `ACQUIRE`; `RENEW` extends `expires_at` but leaves it untouched.
- `ACQUIRE` succeeds for the current owner re-acquiring their own active lease. This rotates the token and bumps the version, same as a fresh
acquire. An owner-less lease has no identity to match against, so it stays locked until expiry even for a later anonymous `ACQUIRE`.
- `ttl_secs` must be a positive integer, or it's `ERR bad-request`.
- `RENEW`/`RELEASE` distinguish failure causes: `ERR not-found` (name never acquired), `ERR wrong-token` (name exists, token doesn't match), `ERR expired` (name and token match, but the
lease is expired or was released).
- Malformed input gets `ERR bad-request` but doesn't close the connection.
- Command names are case-sensitive; arguments split on whitespace with no quoting.
- `WHO` returns `-` for a held lease with no owner; `NULL` means not held.
- Expiry is inclusive: `expires_at <= now` counts as expired.
- Expired/released leases are tombstoned (`expires_at` set to now), not deleted, to preserve
  version continuity.

### Tradeoffs

- **Lazy expiry**: expiry is checked on read/write instead of proactively evicted, so dead rows accumulate indefinitely.
- **One queue does two jobs**: every command is enqueued on a shared `asyncio.PriorityQueue`, and a single `worker` task is the only thing that ever touches `db`. Because only one worker runs at a time, races are structurally impossible. Because it's a *priority* queue (`ACQUIRE` sorts first, FIFO otherwise), `ACQUIRE` always jumps ahead of everything else, no separate scheduler needed. **Cost**: every command, including reads, is serialized through that one worker. This is a throughput ceiling accepted per "correctness > throughput." Atomic `WHERE`-guarded SQL is kept as defense-in-depth on top.

Stretch goals implemented: `LIST`, and `/metrics` (a second, plain `asyncio` HTTP listener on
`port + 1` by default; counts every ACQUIRE/RENEW attempt and every error, not just successes).

## Known limitations

- The leases table is never garbage collected.
- All command execution is serialized through a single worker task.
