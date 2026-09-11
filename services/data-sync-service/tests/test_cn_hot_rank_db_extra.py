"""db/cn_hot_rank.py coverage (fake conn)."""

from __future__ import annotations

from data_sync_service.db import cn_hot_rank


class _Cur:
    def __init__(self):
        self._executemany_calls = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def executemany(self, sql, values):
        self._executemany_calls.append((sql, list(values)))
        return self

    def execute(self, sql, params=None):
        return self


class _Conn:
    def __init__(self):
        self.cursors = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def cursor(self):
        c = _Cur()
        self.cursors.append(c)
        return c

    def commit(self):
        pass


def _conn(monkeypatch):
    conn = _Conn()
    monkeypatch.setattr(cn_hot_rank, "get_connection", lambda: conn)
    monkeypatch.setattr(cn_hot_rank, "ensure_table", lambda: None)
    return conn


def test_upsert_rows_placeholders_match(monkeypatch) -> None:
    conn = _conn(monkeypatch)
    n = cn_hot_rank.upsert_rows([
        {"ts_code": "600000.SH", "trade_date": "2026-09-10", "rank": 2311.0,
         "new_fans": 0.30, "iron_fans": 0.70},
        {"ts_code": "", "trade_date": None},
        {"ts_code": "000001.SZ", "trade_date": "20260910", "rank": "abc",
         "new_fans": None, "iron_fans": float("nan")},
    ])
    assert n == 2
    sql, values = conn.cursors[-1]._executemany_calls[0]
    assert "ON CONFLICT (ts_code, trade_date)" in sql
    assert len(values) == 2
    assert all(len(v) == sql.count("%s") for v in values)
    # bad rank string -> None, NaN sanitized
    assert values[1][2] is None
    assert values[1][4] is None


def test_upsert_rows_empty(monkeypatch) -> None:
    _conn(monkeypatch)
    assert cn_hot_rank.upsert_rows([]) == 0


def test_create_sql_pk_and_indexes() -> None:
    ddl = cn_hot_rank.CREATE_SQL
    assert "CREATE TABLE IF NOT EXISTS cn_hot_rank" in ddl
    assert "PRIMARY KEY (ts_code, trade_date)" in ddl
    assert "idx_cn_hot_rank_date" in ddl
