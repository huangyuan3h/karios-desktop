from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from contextlib import contextmanager

import psycopg
from psycopg_pool import ConnectionPool

from data_sync_service.config import get_settings

logger = logging.getLogger(__name__)

# OPT-125: process-wide pool. min 2 warm / max 20 — never 100: leave room
# for pgadmin / migrate / ad-hoc psql against max_connections=100.
POOL_MIN_SIZE = 2
POOL_MAX_SIZE = 20
# Bound for waiting on a free slot; statement/lock/idle timeouts bound the
# query itself (dashboard stream steps can no longer hang a thread forever).
POOL_ACQUIRE_TIMEOUT = 10.0
# DB-layer transient retry: exactly once after 0.5s (acquisition only — never
# mid-transaction, and never stacked with the network _with_retry 3x).
ACQUIRE_RETRY_DELAY = 0.5


def _pool_options() -> str:
    # statement_timeout: pre-existing 120s guard for batch upserts.
    # lock_timeout: fail fast on lock waits instead of burning a slot.
    # idle_in_transaction: recycle leaked transactions (scheduler threads).
    return (
        "-c statement_timeout=120000"
        " -c lock_timeout=10000"
        " -c idle_in_transaction_session_timeout=60000"
    )


_POOL: ConnectionPool | None = None
_POOL_DSN: str | None = None


def get_pool() -> ConnectionPool:
    """Process-wide pool, rebuilt when DATABASE_URL changes.

    Lazy so `import db` never touches the network; rebuild-on-change covers
    runtime .env edits and lets tests monkeypatch
    `data_sync_service.config.get_settings`.
    """
    global _POOL, _POOL_DSN
    settings = get_settings()
    dsn = settings.database_url
    if not dsn:
        raise ValueError("DATABASE_URL is not configured.")
    if _POOL is None or _POOL_DSN != dsn:
        close_pool()
        _POOL = ConnectionPool(
            dsn,
            min_size=POOL_MIN_SIZE,
            max_size=POOL_MAX_SIZE,
            timeout=POOL_ACQUIRE_TIMEOUT,
            open=True,  # fail fast at first DB use (explicit: default flips in a future release)
            kwargs={"connect_timeout": 5, "options": _pool_options()},
        )
        _POOL_DSN = dsn
    return _POOL


def close_pool() -> None:
    """Drop the process-wide pool (shutdown / test helper)."""
    global _POOL, _POOL_DSN
    pool, _POOL = _POOL, None
    _POOL_DSN = None
    if pool is not None:
        try:
            pool.close()
        except Exception:  # noqa: BLE001
            logger.warning("db pool close failed", exc_info=True)


@contextmanager
def get_connection() -> Iterator[psycopg.Connection]:
    """Check out a pooled connection (use as `with get_connection() as conn`).

    Drop-in for the old per-call `psycopg.connect`: every existing caller
    already uses `with`, so the switch is transparent — exit returns the
    connection to the pool instead of closing the socket.
    """
    pool = get_pool()
    try:
        conn = pool.getconn()
    except psycopg.OperationalError:
        # Transient (restart / failover / herd spike): exactly one retry.
        time.sleep(ACQUIRE_RETRY_DELAY)
        conn = pool.getconn()
    try:
        yield conn
    finally:
        pool.putconn(conn)


def check_db() -> tuple[bool, str | None]:
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return True, None
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)
