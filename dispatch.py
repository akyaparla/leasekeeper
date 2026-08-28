import sqlite3
import time
import uuid

def handle_acquire(db: sqlite3.Connection, name: str, ttl_secs: int, owner: str | None) -> str:
    now = time.time()
    token = uuid.uuid4().hex  # generates random 128-bit id
    expires_at = now + ttl_secs
    params = {
        "name": name, 
        "token": token, 
        "owner": owner, 
        "expires_at": expires_at, 
        "now": now
    }

    # Only update when owner of existing lease sends ACQUIRE or lease is expired
    cursor = db.execute(
        """
        INSERT INTO leases (name, token, owner, version, expires_at)
        VALUES (:name, :token, :owner, 1, :expires_at)
        ON CONFLICT(name) DO UPDATE SET
            token      = excluded.token,
            owner      = excluded.owner,
            version    = leases.version + 1,
            expires_at = excluded.expires_at
        WHERE leases.expires_at <= :now OR leases.owner = :owner
        RETURNING token, version
        """,
        params
    )
    row = cursor.fetchone()
    db.commit()

    if row is None:
        return "NULL\n"

    return_token, version = row
    return f"{return_token} {version}\n"

def handle_renew(db: sqlite3.Connection, name: str, token: str, ttl_secs: int) -> str:
    now = time.time()
    expires_at = now + ttl_secs
    params = {
        "name": name,
        "token": token,
        "expires_at": expires_at,
        "now": now
    }
    cursor = db.execute(
        """
        UPDATE leases
        SET expires_at = :expires_at
        WHERE name = :name AND token = :token AND expires_at > :now
        RETURNING version
        """,
        params
    )
    row = cursor.fetchone()
    db.commit()

    if row is None:
        existing = db.execute(
            "SELECT token FROM leases WHERE name = :name", {"name": name}
        ).fetchone()
        if existing is None:  # Name not found
            return "ERR not-found\n"
        if existing[0] != token:  # User provided wrong token
            return "ERR wrong-token\n"
        return "ERR expired\n"  # name and token both matched, so expires_at <= now

    version, = row
    return f"{version}\n"

def handle_release(db: sqlite3.Connection, name: str, token: str) -> str:
    now = time.time()
    params = {
        "name": name, 
        "token": token, 
        "now": now
    }
    cursor = db.execute(
        """
        UPDATE leases
        SET expires_at = :now
        WHERE name = :name AND token = :token AND expires_at > :now
        RETURNING version
        """,
        params,
    )
    row = cursor.fetchone()
    db.commit()

    if row is None:
        existing = db.execute(
            "SELECT token FROM leases WHERE name = :name", {"name": name}
        ).fetchone()
        if existing is None:
            return "ERR not-found\n"
        if existing[0] != token:
            return "ERR wrong-token\n"
        return "ERR expired\n"  # name and token both matched, so expires_at <= now

    return "OK\n"

def handle_who(db: sqlite3.Connection, name: str) -> str:
    now = time.time()
    row = db.execute(
        "SELECT owner, expires_at FROM leases WHERE name = :name",
        {"name": name},
    ).fetchone()

    if row is None:
        return "NULL\n"

    owner, expires_at = row
    if expires_at <= now:
        return "NULL\n"

    return f"{owner if owner is not None else '-'}\n"

def handle_ttl(db: sqlite3.Connection, name: str) -> str:
    now = time.time()
    row = db.execute(
        "SELECT expires_at FROM leases WHERE name = :name",
        {"name": name},
    ).fetchone()

    if row is None:
        return "NULL\n"

    expires_at, = row
    remaining = expires_at - now
    if remaining <= 0:
        return "NULL\n"

    return f"{int(remaining)}\n"

def handle_list(db: sqlite3.Connection) -> str:
    now = time.time()
    rows = db.execute(
        "SELECT name FROM leases WHERE expires_at > :now ORDER BY name",
        {"now": now},
    ).fetchall()

    names = [name for name, in rows]
    return " ".join(names) + "\n"

async def dispatch_command(db, cmd: str, args: list[str]) -> str:
    if cmd == "ACQUIRE":
        if len(args) not in (2, 3):
            raise ValueError
        name, ttl_str = args[0], args[1]
        ttl_secs = int(ttl_str)
        if ttl_secs <= 0:
            raise ValueError
        owner = args[2] if len(args) == 3 else None
        return handle_acquire(db, name, ttl_secs, owner)
    elif cmd == "RENEW":
        if len(args) != 3:
            raise ValueError
        name, token, ttl_str = args
        ttl_secs = int(ttl_str)
        if ttl_secs <= 0:
            raise ValueError
        return handle_renew(db, name, token, ttl_secs)
    elif cmd == "RELEASE":
        if len(args) != 2:
            raise ValueError
        name, token = args
        return handle_release(db, name, token)
    elif cmd == "WHO":
        if len(args) != 1:
            raise ValueError
        name = args[0]
        return handle_who(db, name)
    elif cmd == "TTL":
        if len(args) != 1:
            raise ValueError
        name = args[0]
        return handle_ttl(db, name)
    elif cmd == "LIST":
        if len(args) != 0:
            raise ValueError
        return handle_list(db)
    else:
        raise ValueError

def init_db(path: str = "leases.db") -> sqlite3.Connection:
    db = sqlite3.connect(path)
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS leases (
            name       TEXT PRIMARY KEY,
            token      TEXT NOT NULL,
            owner      TEXT,
            version    INTEGER NOT NULL,
            expires_at REAL NOT NULL
        )
        """
    )
    db.commit()
    return db