"""Atomic on-disk rolling-window reservations, independent of provider I/O.

Admission serializes expiry, budget checking and insertion in one short write
transaction. Settlement preserves admission time; pending and uncertain work
survive crashes until that same window boundary. Callers schedule this blocking
SQLite work outside the event loop and never await providers in a transaction.
"""

import math
import sqlite3
import time
import uuid
from collections.abc import Callable
from contextlib import closing, contextmanager
from pathlib import Path


def wall_time_ms() -> int:
    return time.time_ns() // 1_000_000


class RateLimited(Exception):
    def __init__(self, retry_after: int):
        super().__init__("Tenant token limit exceeded")
        self.retry_after = retry_after


class TokenLimiter:
    def __init__(self, path: str | Path, *, limit: int = 50000, window_ms: int = 60000,
                 busy_timeout: float = 1, clock: Callable[[], int] = wall_time_ms):
        if not str(path) or str(path) == ":memory:":
            raise ValueError("An on-disk SQLite database is required")
        if type(limit) is not int or limit <= 0 or type(window_ms) is not int or window_ms <= 0:
            raise ValueError("Invalid token limit or window")
        if not math.isfinite(busy_timeout) or busy_timeout <= 0:
            raise ValueError("Invalid database timeout")
        self.path = Path(path).resolve()
        self.limit = limit
        self.window_ms = window_ms
        self.busy_timeout = busy_timeout
        self.clock = clock

    def _connect(self):
        return sqlite3.connect(self.path, timeout=self.busy_timeout, isolation_level=None)

    @contextmanager
    def _write(self):
        with closing(self._connect()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                yield connection
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as connection:
            if connection.execute("PRAGMA journal_mode=WAL").fetchone()[0] != "wal":
                raise ValueError("On-disk WAL mode is required")
        with self._write() as connection:
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            tables = connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            # Legacy identities were caller-chosen labels. Guessing a mapping
            # to API-key digests would silently reset or misattribute quotas.
            if version == 0 and tables:
                raise ValueError("Legacy database requires an explicit identity migration")
            if version not in (0, 1):
                raise ValueError("Unsupported router database version")
            if version == 1 and {row[0] for row in tables} != {"reservations", "clock_state"}:
                raise ValueError("Incomplete router database schema")
            connection.execute("""CREATE TABLE IF NOT EXISTS reservations (
                request_id TEXT PRIMARY KEY, tenant_key_digest TEXT NOT NULL,
                recorded_at_ms INTEGER NOT NULL,
                reserved_tokens INTEGER NOT NULL CHECK(reserved_tokens > 0),
                charged_tokens INTEGER NOT NULL CHECK(charged_tokens >= 0 AND charged_tokens <= reserved_tokens),
                state TEXT NOT NULL CHECK(state IN ('pending', 'settled', 'uncertain')))""")
            connection.execute("CREATE INDEX IF NOT EXISTS reservations_tenant_time ON reservations(tenant_key_digest, recorded_at_ms)")
            connection.execute("CREATE INDEX IF NOT EXISTS reservations_time ON reservations(recorded_at_ms)")
            connection.execute("""CREATE TABLE IF NOT EXISTS clock_state (
                id INTEGER PRIMARY KEY CHECK(id=1), last_seen_ms INTEGER NOT NULL,
                token_limit INTEGER NOT NULL, window_ms INTEGER NOT NULL)""")
            if version == 0:
                connection.execute("INSERT INTO clock_state VALUES (1, 0, ?, ?)", (self.limit, self.window_ms))
            settings = connection.execute("SELECT token_limit, window_ms FROM clock_state WHERE id=1").fetchone()
            # All workers must agree on expiry and capacity; otherwise one can
            # delete records that another worker still considers chargeable.
            if settings != (self.limit, self.window_ms):
                raise ValueError("Router database limit/window differs from configuration")
            connection.execute("PRAGMA user_version=1")

    def reserve(self, tenant: str, tokens: int, request_id: str | None = None) -> str:
        if type(tokens) is not int or not 0 < tokens <= self.limit:
            raise ValueError("Reservation exceeds token budget")
        with self._write() as connection:
            # Read time after acquiring the write lock: a busy wait must not
            # make a newly admitted request inherit an earlier timestamp.
            now = self.clock()
            if type(now) is not int or now < 0:
                raise ValueError("Invalid millisecond clock")
            previous = connection.execute("SELECT last_seen_ms FROM clock_state WHERE id=1").fetchone()[0]
            # Persist the high-water mark across workers/restarts: a backward
            # host-clock jump must not admit work against a different time window.
            now = max(now, previous)
            connection.execute("UPDATE clock_state SET last_seen_ms=? WHERE id=1", (now,))
            connection.execute("DELETE FROM reservations WHERE recorded_at_ms <= ?", (now - self.window_ms,))
            used = connection.execute("SELECT coalesce(sum(charged_tokens), 0) FROM reservations WHERE tenant_key_digest=?", (tenant,)).fetchone()[0]
            if used + tokens > self.limit:
                for created, charge in connection.execute(
                    "SELECT recorded_at_ms, charged_tokens FROM reservations WHERE tenant_key_digest=? ORDER BY recorded_at_ms, request_id", (tenant,),
                ):
                    used -= charge
                    if used + tokens <= self.limit:
                        # Persist the observed clock even for denied admissions.
                        connection.commit()
                        raise RateLimited(max(1, math.ceil((created + self.window_ms - now) / 1000)))
            identifier = request_id or uuid.uuid4().hex
            connection.execute("INSERT INTO reservations VALUES (?, ?, ?, ?, ?, 'pending')",
                               (identifier, tenant, now, tokens, tokens))
            return identifier

    def settle(self, identifier: str, actual_tokens: int) -> None:
        if type(actual_tokens) is not int or actual_tokens < 0:
            raise ValueError("Invalid actual usage")
        with self._write() as connection:
            row = connection.execute("SELECT reserved_tokens, state FROM reservations WHERE request_id=?", (identifier,)).fetchone()
            # Expired/pruned or already settled records must never be resurrected.
            if row is None or row[1] != "pending":
                return
            if actual_tokens > row[0]:
                raise ValueError("Actual usage exceeds reservation")
            connection.execute("UPDATE reservations SET charged_tokens=?, state='settled' WHERE request_id=?", (actual_tokens, identifier))

    def mark_uncertain(self, identifier: str) -> None:
        # A later backup success cannot establish how much work a timed-out
        # primary performed. Persist that uncertainty across process restarts.
        with self._write() as connection:
            connection.execute("UPDATE reservations SET state='uncertain' WHERE request_id=? AND state='pending'", (identifier,))