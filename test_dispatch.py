import pytest

import dispatch


# --- ACQUIRE ---------------------------------------------------------------

def test_acquire_fresh_lease_returns_token_and_version_1(db):
    resp = dispatch.handle_acquire(db, "lock", 5, "alice")
    token, version = resp.strip().split()
    assert version == "1"
    assert len(token) > 0


def test_acquire_contested_lease_returns_null(db):
    dispatch.handle_acquire(db, "lock", 5, "alice")
    resp = dispatch.handle_acquire(db, "lock", 5, "bob")
    assert resp == "NULL\n"


def test_acquire_after_release_increments_version_not_reset(db):
    resp1 = dispatch.handle_acquire(db, "lock", 5, "alice")
    token1, v1 = resp1.strip().split()
    assert v1 == "1"

    assert dispatch.handle_release(db, "lock", token1) == "OK\n"

    resp2 = dispatch.handle_acquire(db, "lock", 5, "bob")
    token2, v2 = resp2.strip().split()
    assert v2 == "2"
    assert token2 != token1


def test_acquire_after_expiry_increments_version_not_reset(db, clock):
    resp1 = dispatch.handle_acquire(db, "lock", 5, "alice")
    _, v1 = resp1.strip().split()
    assert v1 == "1"

    clock.advance(10)  # past the 5s TTL

    resp2 = dispatch.handle_acquire(db, "lock", 5, "bob")
    _, v2 = resp2.strip().split()
    assert v2 == "2"


def test_acquire_no_owner_stores_null_owner(db):
    dispatch.handle_acquire(db, "lock", 5, None)
    assert dispatch.handle_who(db, "lock") == "-\n"


def test_acquire_not_reentrant_for_current_owner(db):
    resp1 = dispatch.handle_acquire(db, "lock", 5, "alice")
    token1, _ = resp1.strip().split()
    # alice tries to re-acquire her own still-active lease
    resp2 = dispatch.handle_acquire(db, "lock", 5, "alice")
    assert resp2 == "NULL\n"


# --- RENEW -------------------------------------------------------------------

def test_renew_extends_expiry_and_returns_unchanged_version(db, clock):
    resp = dispatch.handle_acquire(db, "lock", 5, "alice")
    token, v1 = resp.strip().split()

    clock.advance(3)  # still within original TTL

    renew_resp = dispatch.handle_renew(db, "lock", token, 20)
    assert renew_resp.strip() == v1  # version unchanged by RENEW

    clock.advance(10)  # would have expired under the *original* TTL, not the renewed one
    assert dispatch.handle_who(db, "lock") == "alice\n"


def test_renew_wrong_token_fails(db):
    dispatch.handle_acquire(db, "lock", 5, "alice")
    resp = dispatch.handle_renew(db, "lock", "not-the-real-token", 10)
    assert resp == "ERR not-found\n"


def test_renew_expired_lease_fails_even_with_correct_token(db, clock):
    resp = dispatch.handle_acquire(db, "lock", 5, "alice")
    token, _ = resp.strip().split()

    clock.advance(10)  # past TTL

    renew_resp = dispatch.handle_renew(db, "lock", token, 10)
    assert renew_resp == "ERR not-found\n"


def test_renew_nonexistent_lease_fails(db):
    resp = dispatch.handle_renew(db, "no-such-lock", "sometoken", 10)
    assert resp == "ERR not-found\n"


# --- RELEASE -------------------------------------------------------------------

def test_release_success_frees_the_lease(db):
    resp = dispatch.handle_acquire(db, "lock", 5, "alice")
    token, _ = resp.strip().split()

    assert dispatch.handle_release(db, "lock", token) == "OK\n"
    assert dispatch.handle_who(db, "lock") == "NULL\n"


def test_release_wrong_token_fails(db):
    dispatch.handle_acquire(db, "lock", 5, "alice")
    resp = dispatch.handle_release(db, "lock", "not-the-real-token")
    assert resp == "ERR not-found\n"


def test_release_expired_lease_fails(db, clock):
    resp = dispatch.handle_acquire(db, "lock", 5, "alice")
    token, _ = resp.strip().split()

    clock.advance(10)

    assert dispatch.handle_release(db, "lock", token) == "ERR not-found\n"


def test_release_nonexistent_lease_fails(db):
    resp = dispatch.handle_release(db, "no-such-lock", "sometoken")
    assert resp == "ERR not-found\n"


def test_release_then_reacquire_gets_fresh_token(db):
    resp1 = dispatch.handle_acquire(db, "lock", 5, "alice")
    token1, _ = resp1.strip().split()
    dispatch.handle_release(db, "lock", token1)

    resp2 = dispatch.handle_acquire(db, "lock", 5, "bob")
    token2, _ = resp2.strip().split()
    assert token2 != token1

    # the old token must no longer work against the new holder's lease
    assert dispatch.handle_release(db, "lock", token1) == "ERR not-found\n"


# --- WHO -------------------------------------------------------------------

def test_who_nonexistent_lease_returns_null(db):
    assert dispatch.handle_who(db, "ghost") == "NULL\n"


def test_who_active_lease_returns_owner(db):
    dispatch.handle_acquire(db, "lock", 5, "alice")
    assert dispatch.handle_who(db, "lock") == "alice\n"


def test_who_expired_lease_returns_null(db, clock):
    dispatch.handle_acquire(db, "lock", 5, "alice")
    clock.advance(10)
    assert dispatch.handle_who(db, "lock") == "NULL\n"


# --- TTL -------------------------------------------------------------------

def test_ttl_nonexistent_lease_returns_null(db):
    assert dispatch.handle_ttl(db, "ghost") == "NULL\n"


def test_ttl_active_lease_returns_remaining_seconds(db, clock):
    dispatch.handle_acquire(db, "lock", 100, "alice")
    clock.advance(40)
    resp = dispatch.handle_ttl(db, "lock")
    remaining = int(resp.strip())
    assert 55 <= remaining <= 60  # ~60s left, small tolerance for arithmetic


def test_ttl_expired_lease_returns_null(db, clock):
    dispatch.handle_acquire(db, "lock", 5, "alice")
    clock.advance(10)
    assert dispatch.handle_ttl(db, "lock") == "NULL\n"


# --- dispatch_command validation --------------------------------------------

@pytest.mark.asyncio
async def test_dispatch_unknown_command_raises(db):
    with pytest.raises(ValueError):
        await dispatch.dispatch_command(db, "BOGUS", [])


@pytest.mark.parametrize("args", [[], ["lock"], ["lock", "5", "owner", "extra"]])
@pytest.mark.asyncio
async def test_dispatch_acquire_bad_arity_raises(db, args):
    with pytest.raises(ValueError):
        await dispatch.dispatch_command(db, "ACQUIRE", args)


@pytest.mark.parametrize("ttl", ["0", "-1", "notanumber", "1.5"])
@pytest.mark.asyncio
async def test_dispatch_acquire_bad_ttl_raises(db, ttl):
    with pytest.raises(ValueError):
        await dispatch.dispatch_command(db, "ACQUIRE", ["lock", ttl])


@pytest.mark.parametrize("args", [[], ["lock"], ["lock", "token"], ["lock", "token", "5", "extra"]])
@pytest.mark.asyncio
async def test_dispatch_renew_bad_arity_raises(db, args):
    with pytest.raises(ValueError):
        await dispatch.dispatch_command(db, "RENEW", args)


@pytest.mark.parametrize("args", [[], ["lock"], ["lock", "token", "extra"]])
@pytest.mark.asyncio
async def test_dispatch_release_bad_arity_raises(db, args):
    with pytest.raises(ValueError):
        await dispatch.dispatch_command(db, "RELEASE", args)


@pytest.mark.parametrize("cmd", ["WHO", "TTL"])
@pytest.mark.parametrize("args", [[], ["lock", "extra"]])
@pytest.mark.asyncio
async def test_dispatch_who_ttl_bad_arity_raises(db, cmd, args):
    with pytest.raises(ValueError):
        await dispatch.dispatch_command(db, cmd, args)


@pytest.mark.asyncio
async def test_dispatch_acquire_success_roundtrip(db):
    resp = await dispatch.dispatch_command(db, "ACQUIRE", ["lock", "5", "alice"])
    assert resp != "NULL\n"
    token, version = resp.strip().split()
    assert version == "1"
