"""Check the persisted ledger using controlled clocks and concurrent connections.

Boundary and reference-model assertions compare stored state, not just return
values, so expiry, crash policy and reconciliation errors remain observable.
"""

import random
import sqlite3
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

import pytest

from model_router.limiter import RateLimited, TokenLimiter


def test_exact_limit_tenants_and_exact_expiry(tmp_path):
    now = [100.0]
    limiter = TokenLimiter(tmp_path / "usage.db", clock=lambda: int(now[0] * 1000))
    limiter.initialize()
    limiter.reserve("a", 50000)
    with pytest.raises(RateLimited) as denied:
        limiter.reserve("a", 1)
    assert denied.value.retry_after == 60
    limiter.reserve("b", 50000)
    now[0] = 159.999
    with pytest.raises(RateLimited) as denied:
        limiter.reserve("a", 1)
    assert denied.value.retry_after == 1
    now[0] = 160
    limiter.reserve("a", 50000)


def test_retry_after_accounts_for_enough_expiring_usage(tmp_path):
    now = [100.0]
    limiter = TokenLimiter(tmp_path / "usage.db", clock=lambda: int(now[0] * 1000))
    limiter.initialize()
    limiter.reserve("a", 10000)
    now[0] = 110
    limiter.reserve("a", 40000)
    now[0] = 120
    with pytest.raises(RateLimited) as denied:
        limiter.reserve("a", 20000)
    assert denied.value.retry_after == 50


def test_reconcile_persist_and_clock_rollback(tmp_path):
    path = tmp_path / "usage.db"
    limiter = TokenLimiter(path, clock=lambda: 100000)
    limiter.initialize()
    reservation = limiter.reserve("a", 50000)
    limiter.settle(reservation, 1000)
    # Settlement is idempotent and must not rewrite an already settled charge.
    limiter.settle(reservation, 0)
    reopened = TokenLimiter(path, clock=lambda: 50000)
    reopened.initialize()
    reopened.reserve("a", 49000)
    with pytest.raises(RateLimited) as denied:
        reopened.reserve("a", 1)
    assert denied.value.retry_after == 60
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT sum(charged_tokens) FROM reservations").fetchone()[0] == 50000


def test_unsettled_reservations_survive_restart(tmp_path):
    path = tmp_path / "usage.db"
    first = TokenLimiter(path, clock=lambda: 100000)
    first.initialize()
    first.reserve("a", 50000)
    second = TokenLimiter(path, clock=lambda: 101000)
    second.initialize()
    with pytest.raises(RateLimited):
        second.reserve("a", 1)


def test_concurrent_independent_connections_cannot_oversubscribe(tmp_path):
    path = tmp_path / "usage.db"
    TokenLimiter(path).initialize()

    def attempt(_):
        limiter = TokenLimiter(path, clock=lambda: 100000)
        try:
            limiter.reserve("a", 5000)
            return True
        except RateLimited:
            return False

    with ThreadPoolExecutor(max_workers=16) as pool:
        assert sum(pool.map(attempt, range(40))) == 10


def test_settlement_cannot_increase_reserved_usage(tmp_path):
    limiter = TokenLimiter(tmp_path / "usage.db", clock=lambda: 100000)
    limiter.initialize()
    reservation = limiter.reserve("a", 10)
    with pytest.raises(ValueError):
        limiter.settle(reservation, 11)
    limiter.settle(reservation, 0)
    limiter.reserve("a", 50000)


@pytest.mark.parametrize("path", ["", ":memory:"])
def test_requires_on_disk_database(path):
    with pytest.raises(ValueError):
        TokenLimiter(path)


def process_reservation(path):
    try:
        TokenLimiter(path, clock=lambda: 100000).reserve("a", 5000)
        return True
    except RateLimited:
        return False


def test_multiple_processes_share_admission_state(tmp_path):
    path = tmp_path / "usage.db"
    TokenLimiter(path).initialize()
    with ProcessPoolExecutor(max_workers=4) as pool:
        assert sum(pool.map(process_reservation, [path] * 20)) == 10


def test_settlement_keeps_original_timestamp_and_does_not_resurrect_expired_rows(tmp_path):
    now = [100.0]
    limiter = TokenLimiter(tmp_path / "usage.db", clock=lambda: int(now[0] * 1000))
    limiter.initialize()
    identifier = limiter.reserve("a", 50000)
    now[0] = 159
    limiter.settle(identifier, 1000)
    now[0] = 160
    limiter.reserve("a", 50000)
    limiter.settle(identifier, 1000)
    with pytest.raises(RateLimited):
        limiter.reserve("a", 1)


def test_uncertain_usage_cannot_be_reduced_by_late_settlement(tmp_path):
    path = tmp_path / "usage.db"
    limiter = TokenLimiter(path, clock=lambda: 0)
    limiter.initialize()
    identifier = limiter.reserve("digest", 50000)
    limiter.mark_uncertain(identifier)
    limiter.settle(identifier, 0)
    reopened = TokenLimiter(path, clock=lambda: 59999)
    reopened.initialize()
    with pytest.raises(RateLimited):
        reopened.reserve("digest", 1)
    reopened.clock = lambda: 60000
    reopened.reserve("digest", 50000)
    reopened.clock = lambda: 60001
    with pytest.raises(RateLimited):
        reopened.reserve("digest", 1)


def test_legacy_database_is_rejected_without_deleting_records(tmp_path):
    path = tmp_path / "legacy.db"
    with sqlite3.connect(path) as db:
        db.execute("CREATE TABLE usage (tenant TEXT, tokens INTEGER)")
        db.execute("INSERT INTO usage VALUES ('legacy-tenant', 50000)")
    with pytest.raises(ValueError, match="Legacy"):
        TokenLimiter(path).initialize()
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT * FROM usage").fetchall() == [("legacy-tenant", 50000)]


def test_workers_cannot_use_different_window_or_limit(tmp_path):
    path = tmp_path / "usage.db"
    TokenLimiter(path).initialize()
    for settings in ({"limit": 50001}, {"window_ms": 59999}):
        with pytest.raises(ValueError, match="differs"):
            TokenLimiter(path, **settings).initialize()


@pytest.mark.parametrize("now,allowed", [(59999, False), (60000, True), (60001, True)])
def test_exact_millisecond_boundary_from_zero(tmp_path, now, allowed):
    clock = [0]
    limiter = TokenLimiter(tmp_path / "usage.db", clock=lambda: clock[0])
    limiter.initialize()
    limiter.reserve("digest", 50000)
    clock[0] = now
    if allowed:
        limiter.reserve("digest", 50000)
    else:
        with pytest.raises(RateLimited):
            limiter.reserve("digest", 1)


def test_seeded_ledger_operations_match_reference_state(tmp_path):
    randomizer = random.Random(427)
    now = [0]
    effective_time = 0
    expected = {}
    identifiers = []
    path = tmp_path / "usage.db"
    limiter = TokenLimiter(path, clock=lambda: now[0])
    limiter.initialize()
    for i in range(2000):
        operation = randomizer.choice(["reserve", "reserve", "settle", "uncertain"])
        now[0] = max(0, now[0] + randomizer.randint(-5000, 8000))
        if operation == "reserve":
            effective_time = max(now[0], effective_time)
            expected = {key: row for key, row in expected.items() if row[1] > effective_time - 60000}
            tenant = randomizer.choice(["a", "b", "c"])
            tokens = randomizer.randint(1, 30000)
            used = sum(row[3] for row in expected.values() if row[0] == tenant)
            identifier = f"reference-{i}"
            if used + tokens > 50000:
                with pytest.raises(RateLimited):
                    limiter.reserve(tenant, tokens, identifier)
            else:
                assert limiter.reserve(tenant, tokens, identifier) == identifier
                expected[identifier] = [tenant, effective_time, tokens, tokens, "pending"]
                identifiers.append(identifier)
        elif identifiers:
            identifier = randomizer.choice(identifiers)
            row = expected.get(identifier)
            if operation == "settle":
                actual = randomizer.randint(0, row[2]) if row else 0
                limiter.settle(identifier, actual)
                if row and row[4] == "pending":
                    row[3], row[4] = actual, "settled"
            else:
                limiter.mark_uncertain(identifier)
                if row and row[4] == "pending":
                    row[4] = "uncertain"
        # Compare complete persisted state, including stale IDs, rather than
        # merely checking that admission did not throw. This catches resurrection,
        # moved timestamps and reductions of uncertain charges across sequences.
        with sqlite3.connect(path) as db:
            actual = {row[0]: list(row[1:]) for row in db.execute(
                "SELECT request_id, tenant_key_digest, recorded_at_ms, reserved_tokens, charged_tokens, state FROM reservations"
            )}
        assert actual == expected
