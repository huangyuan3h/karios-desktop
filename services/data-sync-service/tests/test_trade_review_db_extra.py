"""db/trade_review.py coverage: CRUD + row mapping with fake conn."""

from __future__ import annotations

import pytest

from data_sync_service.db import trade_review as tr

FULL_ROW = (
    "r1", "CN:600000", "平安银行", "2026-07-01", "2026-07-10", 9,
    100.0, 2.5, 0.1, 2.0, "green", "red", True, False, True,
    "notes", 20.0, 10.5, 9.8, 11.2, "目标达成", "exec", "good", "improve",
    '{"k": "v"}', "2026-07-10T00:00:00Z", "2026-07-10T00:00:00Z",
)


class _Cur:
    def __init__(self, fetch_seq=None, rowcount=1):
        self._seq = list(fetch_seq or [])
        self._idx = 0
        self._rowcount = rowcount
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        return self

    def fetchall(self):
        item = self._seq[self._idx] if self._idx < len(self._seq) else None
        if item and item[0] == "all":
            self._idx += 1
            return item[1]
        return []

    def fetchone(self):
        item = self._seq[self._idx] if self._idx < len(self._seq) else None
        if item and item[0] == "one":
            self._idx += 1
            return item[1]
        return None

    @property
    def rowcount(self):
        return self._rowcount


class _Conn:
    def __init__(self, seq_by_cursor=None, rowcount=1):
        self._seq_by_cursor = list(seq_by_cursor or [])
        self._rowcount = rowcount
        self.cursors = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def cursor(self):
        seq = self._seq_by_cursor[len(self.cursors)] if len(self.cursors) < len(self._seq_by_cursor) else []
        c = _Cur(seq, self._rowcount)
        self.cursors.append(c)
        return c

    def commit(self):
        pass


def _conn(monkeypatch, seq_by_cursor=None, rowcount=1):
    conn = _Conn([[]] + list(seq_by_cursor or []), rowcount)
    monkeypatch.setattr(tr, "get_connection", lambda: conn)
    return conn


class TestMappers:
    def test_to_float(self) -> None:
        assert tr._to_float(None) is None
        assert tr._to_float("1.5") == 1.5
        assert tr._to_float("x") is None

    def test_to_int(self) -> None:
        assert tr._to_int(None) is None
        assert tr._to_int("3") == 3
        assert tr._to_int("x") is None

    def test_to_bool(self) -> None:
        assert tr._to_bool(1) is True
        assert tr._to_bool(0) is False

    def test_to_date(self) -> None:
        assert tr._to_date(None) is None
        assert tr._to_date("2026-08-07") == "2026-08-07"
        assert tr._to_date("  ") is None

    def test_to_json_obj(self) -> None:
        assert tr._to_json_obj({"a": 1}) == {"a": 1}
        assert tr._to_json_obj(None) == {}
        assert tr._to_json_obj('{"b": 2}') == {"b": 2}
        assert tr._to_json_obj("[1]") == {}
        assert tr._to_json_obj("not json") == {}

    def test_row_to_dict(self) -> None:
        out = tr._row_to_dict(FULL_ROW)
        assert out["id"] == "r1"
        assert out["symbol"] == "CN:600000"
        assert out["pnlAmount"] == 100.0
        assert out["buyLogicFundResonance"] is True
        assert out["customPayload"] == {"k": "v"}


class TestCrud:
    def test_fetch_all_no_symbol(self, monkeypatch) -> None:
        conn = _conn(monkeypatch, seq_by_cursor=[[("one", "2"), ("all", [FULL_ROW])]])
        total, items = tr.fetch_all()
        assert total == 2 and items[0]["id"] == "r1"
        sqls = [e[0] for e in conn.cursors[1].executed]
        assert any("COUNT(*)" in s and "symbol" not in s for s in sqls)

    def test_fetch_all_with_symbol(self, monkeypatch) -> None:
        conn = _conn(monkeypatch, seq_by_cursor=[[("one", "1"), ("all", [FULL_ROW])]])
        total, items = tr.fetch_all(symbol="CN:600000", limit=500, offset=-1)
        assert total == 1
        sqls = [e[0] for e in conn.cursors[1].executed]
        assert any("WHERE symbol = %s" in s for s in sqls)

    def test_fetch_all_empty(self, monkeypatch) -> None:
        _conn(monkeypatch, seq_by_cursor=[[("one", "0"), ("all", [])]])
        total, items = tr.fetch_all()
        assert total == 0 and items == []

    def test_fetch_by_id(self, monkeypatch) -> None:
        _conn(monkeypatch, seq_by_cursor=[[("one", FULL_ROW)]])
        out = tr.fetch_by_id("r1")
        assert out["id"] == "r1"

    def test_fetch_by_id_missing(self, monkeypatch) -> None:
        _conn(monkeypatch, seq_by_cursor=[[]])
        assert tr.fetch_by_id("nope") is None

    def test_fetch_by_id_empty_string(self, monkeypatch) -> None:
        _conn(monkeypatch, seq_by_cursor=[[("one", FULL_ROW)]])
        assert tr.fetch_by_id("") is None

    def test_create_review(self, monkeypatch) -> None:
        conn = _conn(monkeypatch, seq_by_cursor=[[], [], [("one", FULL_ROW)]])
        out = tr.create_review(review_id="r1", payload={"symbol": "CN:600000", "customPayload": {"a": 1}}, created_at="t", updated_at="t")
        assert out["id"] == "r1"
        insert_sql, params = conn.cursors[1].executed[0]
        assert "INSERT INTO" in insert_sql
        assert params[24] == '{"a": 1}'

    def test_create_review_missing(self, monkeypatch) -> None:
        conn = _conn(monkeypatch, seq_by_cursor=[[], [], []], rowcount=0)
        out = tr.create_review(review_id="r9", payload={}, created_at="t", updated_at="t")
        assert out == {}

    def test_update_review(self, monkeypatch) -> None:
        conn = _conn(monkeypatch, seq_by_cursor=[[], [("one", FULL_ROW)], [], [], [("one", FULL_ROW)]])
        out = tr.update_review(review_id="r1", payload={"pnlPct": 3.5}, updated_at="t2")
        assert out["id"] == "r1"
        update_sql, params = conn.cursors[3].executed[0]
        assert "UPDATE" in update_sql

    def test_update_review_missing(self, monkeypatch) -> None:
        _conn(monkeypatch, seq_by_cursor=[[], []])
        assert tr.update_review(review_id="nope", payload={}, updated_at="t") is None

    def test_delete_review_ok(self, monkeypatch) -> None:
        conn = _conn(monkeypatch, rowcount=1)
        assert tr.delete_review("r1") is True
        assert "DELETE" in conn.cursors[-1].executed[0][0]

    def test_delete_review_missing(self, monkeypatch) -> None:
        _conn(monkeypatch, rowcount=0)
        assert tr.delete_review("nope") is False

    def test_delete_review_empty(self, monkeypatch) -> None:
        _conn(monkeypatch)
        assert tr.delete_review("  ") is False
