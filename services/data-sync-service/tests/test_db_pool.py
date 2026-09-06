"""DB pool: checkout/return, timeouts, transient retry, rebuild (OPT-125).

Hermetic: psycopg_pool.ConnectionPool is stubbed at
`data_sync_service.db.ConnectionPool`. No real Postgres.
"""

from __future__ import annotations

import threading
from types import SimpleNamespace

import psycopg
import pytest

from data_sync_service import db as db_mod


class FakeConn:
    def __init__(self, pool: FakePool) -> None:
        self._pool = pool
        self.closed = False
        self.commits = 0
        self.rollbacks = 0

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def cursor(self):
        class _Cur:
            def execute(self, *a, **k):
                pass

            def fetchone(self):
                return (1,)

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        return _Cur()


class FakePool:
    """Stand-in for psycopg_pool.ConnectionPool with concurrency tracking."""

    instances: list[FakePool] = []

    def __init__(self, dsn, **kwargs) -> None:
        self.dsn = dsn
        self.kwargs = kwargs
        self.out = 0
        self.max_out = 0
        self.put_back = 0
        self.closed = False
        self.fail_next = False
        self._lock = threading.Lock()
        # Honor max_size like the real pool: beyond it, checkouts queue
        # instead of opening new sockets (the too-many-clients fix).
        self._slots = threading.Semaphore(kwargs.get("max_size", 20))
        FakePool.instances.append(self)

    def getconn(self):
        acquired = self._slots.acquire(timeout=10)
        if not acquired:
            raise psycopg.OperationalError("pool timeout")
        with self._lock:
            if self.fail_next:
                self.fail_next = False
                self._slots.release()
                raise psycopg.OperationalError("connection failed")
            self.out += 1
            self.max_out = max(self.max_out, self.out)
        return FakeConn(self)

    def putconn(self, conn) -> None:
        with self._lock:
            self.out -= 1
            self.put_back += 1
        self._slots.release()

    def close(self) -> None:
        self.closed = True


@pytest.fixture(autouse=True)
def _fake_pool_cls(monkeypatch):
    FakePool.instances.clear()
    monkeypatch.setattr(db_mod, "ConnectionPool", FakePool)
    monkeypatch.setattr(
        db_mod, "get_settings", lambda: SimpleNamespace(database_url="postgresql://test/db")
    )
    db_mod.close_pool()
    try:
        yield
    finally:
        db_mod.close_pool()


def test_pool_built_with_guardrails() -> None:
    pool = db_mod.get_pool()
    assert isinstance(pool, FakePool)
    assert pool.kwargs["min_size"] == 2 and pool.kwargs["max_size"] == 20
    assert pool.kwargs["timeout"] == 10.0
    assert pool.kwargs["kwargs"]["connect_timeout"] == 5
    opts = pool.kwargs["kwargs"]["options"]
    assert "statement_timeout=120000" in opts
    assert "lock_timeout=10000" in opts
    assert "idle_in_transaction_session_timeout=60000" in opts
    # Same DSN → same instance (no rebuild).
    assert db_mod.get_pool() is pool


def test_pool_max_never_100() -> None:
    assert db_mod.POOL_MAX_SIZE <= 20


def test_checkout_returns_to_pool() -> None:
    pool = db_mod.get_pool()
    with db_mod.get_connection() as conn:
        assert isinstance(conn, FakeConn)
    assert pool.out == 0 and pool.put_back == 1
    assert conn.commits == 1  # psycopg3 with-connect() semantics: clean exit commits


def test_transient_failure_retries_once(monkeypatch) -> None:
    sleeps: list[float] = []
    monkeypatch.setattr(db_mod.time, "sleep", lambda s: sleeps.append(s))
    pool = db_mod.get_pool()
    pool.fail_next = True
    with db_mod.get_connection():
        pass
    assert sleeps == [db_mod.ACQUIRE_RETRY_DELAY]
    assert pool.put_back == 1


def test_persistent_failure_raises_after_one_retry(monkeypatch) -> None:
    monkeypatch.setattr(db_mod.time, "sleep", lambda s: None)
    pool = db_mod.get_pool()
    calls = {"n": 0}

    def _always_fail():
        calls["n"] += 1
        raise psycopg.OperationalError("down")

    pool.getconn = _always_fail
    with pytest.raises(psycopg.OperationalError):
        with db_mod.get_connection():
            pass
    assert calls["n"] == 2  # initial + exactly one retry


def test_non_transient_error_not_retried() -> None:
    pool = db_mod.get_pool()

    def _programming_error():
        raise psycopg.ProgrammingError("bad sql")

    pool.getconn = _programming_error
    with pytest.raises(psycopg.ProgrammingError):
        with db_mod.get_connection():
            pass


def test_rebuild_on_dsn_change(monkeypatch) -> None:
    first = db_mod.get_pool()
    monkeypatch.setattr(
        db_mod, "get_settings", lambda: SimpleNamespace(database_url="postgresql://other/db")
    )
    second = db_mod.get_pool()
    assert second is not first and first.closed is True


def test_missing_dsn_raises(monkeypatch) -> None:
    monkeypatch.setattr(db_mod, "get_settings", lambda: SimpleNamespace(database_url=""))
    with pytest.raises(ValueError, match="DATABASE_URL"):
        db_mod.get_pool()


def test_check_db_true_and_false(monkeypatch) -> None:
    ok, err = db_mod.check_db()
    assert (ok, err) == (True, None)
    pool = db_mod.get_pool()
    pool.getconn = lambda: (_ for _ in ()).throw(psycopg.OperationalError("down"))
    monkeypatch.setattr(db_mod.time, "sleep", lambda s: None)
    ok, err = db_mod.check_db()
    assert ok is False and "down" in (err or "")


def test_concurrent_herd_bounded_by_pool() -> None:
    pool = db_mod.get_pool()
    errors: list[Exception] = []

    def _work():
        try:
            with db_mod.get_connection():
                pass
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=_work) for _ in range(30)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    # 30 threads through 20 slots: never more than max_size checked out,
    # everything returned — no new-socket-per-request herd.
    assert pool.max_out <= 20
    assert pool.out == 0 and pool.put_back == 30
