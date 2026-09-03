"""SQLite-backed store for API keys and usage events.

Why SQLite and not Delta
------------------------
The rest of Shiden stores analytics data in Delta, but credentials and
request logs are a different workload: single-row transactional reads on
every request, and high-frequency small appends.  Delta is the wrong
shape for that (no row-level reads, no per-request transactions, small
files pile up).  SQLite is transactional, needs no infrastructure, and
the schema below is plain SQL -- moving to Postgres later is a driver
swap plus a dump/restore, not a redesign.

Concurrency
-----------
FastAPI runs the sync route handlers in a threadpool, so the store is hit
from multiple threads.  ``sqlite3`` connections cannot be shared across
threads, so each operation opens its own short-lived connection with WAL
journaling and a busy timeout.  At API-key-lookup volumes this costs
microseconds and removes an entire class of threading bugs.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Sequence

from shiden.api.auth import keys as keyutil

ALL_MARKETS = "*"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS api_keys (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    key_hash           TEXT    NOT NULL UNIQUE,
    display            TEXT    NOT NULL,
    name               TEXT    NOT NULL,
    environment        TEXT    NOT NULL,
    markets            TEXT    NOT NULL,
    rate_limit_per_min INTEGER NOT NULL,
    created_at         TEXT    NOT NULL,
    revoked_at         TEXT,
    last_used_at       TEXT
);

CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys (key_hash);

CREATE TABLE IF NOT EXISTS usage_events (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    key_id        INTEGER REFERENCES api_keys (id),
    occurred_at   TEXT    NOT NULL,
    method        TEXT    NOT NULL,
    path          TEXT    NOT NULL,
    market_id     TEXT,
    status_code   INTEGER NOT NULL,
    duration_ms   REAL    NOT NULL,
    response_rows INTEGER
);

CREATE INDEX IF NOT EXISTS idx_usage_key_time
    ON usage_events (key_id, occurred_at);
CREATE INDEX IF NOT EXISTS idx_usage_time ON usage_events (occurred_at);
"""


@dataclass(frozen=True)
class ApiKeyRecord:
    """A stored key. Never carries the secret."""

    id: int
    key_hash: str
    display: str
    name: str
    environment: str
    markets: tuple[str, ...]
    rate_limit_per_min: int
    created_at: str
    revoked_at: str | None
    last_used_at: str | None

    @property
    def is_active(self) -> bool:
        return self.revoked_at is None

    def allows_market(self, market_id: str) -> bool:
        return ALL_MARKETS in self.markets or market_id in self.markets


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _row_to_record(row: sqlite3.Row) -> ApiKeyRecord:
    return ApiKeyRecord(
        id=row["id"],
        key_hash=row["key_hash"],
        display=row["display"],
        name=row["name"],
        environment=row["environment"],
        markets=tuple(json.loads(row["markets"])),
        rate_limit_per_min=row["rate_limit_per_min"],
        created_at=row["created_at"],
        revoked_at=row["revoked_at"],
        last_used_at=row["last_used_at"],
    )


class KeyStore:
    """All persistence for authentication and usage metering."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        if self.db_path.parent != Path(""):
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path, timeout=5.0)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute("PRAGMA foreign_keys=ON")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(_SCHEMA)

    # ---------------------------------------------------------------- keys

    def issue_key(
        self,
        name: str,
        markets: Sequence[str] = (ALL_MARKETS,),
        rate_limit_per_min: int = 60,
        environment: str = keyutil.LIVE,
    ) -> tuple[str, ApiKeyRecord]:
        """Mint and persist a key.

        Returns ``(secret, record)``.  The secret is not recoverable
        afterwards -- surface it to the caller now or lose it.
        """
        if not name.strip():
            raise ValueError("key name must not be empty")
        if rate_limit_per_min < 1:
            raise ValueError("rate_limit_per_min must be >= 1")
        markets = tuple(markets) or (ALL_MARKETS,)

        generated = keyutil.generate_key(environment)
        created_at = _utcnow()

        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO api_keys (
                    key_hash, display, name, environment, markets,
                    rate_limit_per_min, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    generated.key_hash,
                    generated.display,
                    name.strip(),
                    generated.environment,
                    json.dumps(list(markets)),
                    rate_limit_per_min,
                    created_at,
                ),
            )
            row = conn.execute(
                "SELECT * FROM api_keys WHERE id = ?", (cursor.lastrowid,)
            ).fetchone()

        return generated.secret, _row_to_record(row)

    def lookup(self, secret: str) -> ApiKeyRecord | None:
        """Return the record for a presented secret, or None."""
        if not keyutil.looks_like_key(secret):
            return None
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM api_keys WHERE key_hash = ?",
                (keyutil.hash_key(secret),),
            ).fetchone()
        return _row_to_record(row) if row else None

    def list_keys(self, include_revoked: bool = True) -> list[ApiKeyRecord]:
        sql = "SELECT * FROM api_keys"
        if not include_revoked:
            sql += " WHERE revoked_at IS NULL"
        sql += " ORDER BY id"
        with self._connect() as conn:
            rows = conn.execute(sql).fetchall()
        return [_row_to_record(row) for row in rows]

    def revoke(self, display_or_id: str) -> bool:
        """Revoke by numeric id or display prefix. Returns True if changed."""
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE api_keys SET revoked_at = ?
                WHERE revoked_at IS NULL
                  AND (CAST(id AS TEXT) = ? OR display = ?)
                """,
                (_utcnow(), display_or_id, display_or_id),
            )
            return cursor.rowcount > 0

    def touch(self, key_id: int) -> None:
        """Record that a key was just used (best-effort, never raises)."""
        try:
            with self._connect() as conn:
                conn.execute(
                    "UPDATE api_keys SET last_used_at = ? WHERE id = ?",
                    (_utcnow(), key_id),
                )
        except sqlite3.Error:
            pass

    # --------------------------------------------------------------- usage

    def record_usage(
        self,
        key_id: int | None,
        method: str,
        path: str,
        status_code: int,
        duration_ms: float,
        market_id: str | None = None,
        response_rows: int | None = None,
    ) -> None:
        """Append one usage event. Best-effort: metering must never 500 a request."""
        try:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO usage_events (
                        key_id, occurred_at, method, path, market_id,
                        status_code, duration_ms, response_rows
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        key_id,
                        _utcnow(),
                        method,
                        path,
                        market_id,
                        status_code,
                        round(duration_ms, 2),
                        response_rows,
                    ),
                )
        except sqlite3.Error:
            pass

    def usage_summary(self, since: str | None = None) -> list[dict[str, Any]]:
        """Per-key request counts and latency, for billing and capacity work."""
        sql = """
            SELECT
                COALESCE(k.display, '<unauthenticated>') AS key_display,
                COALESCE(k.name, '-')                    AS name,
                COUNT(*)                                 AS requests,
                SUM(CASE WHEN u.status_code >= 400 THEN 1 ELSE 0 END) AS errors,
                ROUND(AVG(u.duration_ms), 1)             AS avg_ms,
                MAX(u.occurred_at)                       AS last_seen
            FROM usage_events u
            LEFT JOIN api_keys k ON k.id = u.key_id
        """
        params: tuple[Any, ...] = ()
        if since:
            sql += " WHERE u.occurred_at >= ?"
            params = (since,)
        sql += " GROUP BY u.key_id ORDER BY requests DESC"

        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def count_requests_since(self, key_id: int, since_iso: str) -> int:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS n FROM usage_events
                WHERE key_id = ? AND occurred_at >= ?
                """,
                (key_id, since_iso),
            ).fetchone()
        return int(row["n"])
