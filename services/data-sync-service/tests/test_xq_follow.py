"""db/xq_follow.py + service mapping coverage (fake conn, no DB)."""

from __future__ import annotations

from data_sync_service.db import xq_follow
from data_sync_service.service.xq_follow import _to_ts_code


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
    monkeypatch.setattr(xq_follow, "get_connection", lambda: conn)
    monkeypatch.setattr(xq_follow, "ensure_table", lambda: None)
    return conn


def test_upsert_rows_placeholders_match(monkeypatch) -> None:
    conn = _conn(monkeypatch)
    n = xq_follow.upsert_rows(
        [
            {"ts_code": "600000.SH", "follow": 1234.0, "price": 10.5},
            {"ts_code": "", "follow": 1.0},
            {"ts_code": "000001.SZ", "follow": "3,718,364", "price": float("nan")},
        ],
        "2026-09-12",
    )
    assert n == 2
    sql, values = conn.cursors[-1]._executemany_calls[0]
    assert "ON CONFLICT (ts_code, trade_date)" in sql
    assert all(len(v) == sql.count("%s") for v in values)
    assert values[1][2] == 3718364.0  # thousands-separated string parsed
    assert values[1][3] is None  # NaN price sanitized


def test_upsert_rows_empty(monkeypatch) -> None:
    _conn(monkeypatch)
    assert xq_follow.upsert_rows([], "2026-09-12") == 0


def test_create_sql_pk_and_indexes() -> None:
    ddl = xq_follow.CREATE_SQL
    assert "CREATE TABLE IF NOT EXISTS cn_xq_follow" in ddl
    assert "PRIMARY KEY (ts_code, trade_date)" in ddl
    assert "idx_cn_xq_follow_date" in ddl


def test_to_ts_code_mapping_and_filter() -> None:
    assert _to_ts_code("SH600519") == "600519.SH"
    assert _to_ts_code("SZ000001") == "000001.SZ"
    assert _to_ts_code("sh600519") == "600519.SH"
    assert _to_ts_code("BJ430047") is None  # BJ excluded
    assert _to_ts_code("SH000001") is None  # index/5-prefix excluded
    assert _to_ts_code("bad") is None
