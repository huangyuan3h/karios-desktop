"""db/top_inst.py coverage: upserts + summary/seat queries."""

from __future__ import annotations

from datetime import date

import pytest

from data_sync_service.db import top_inst as ti


class _Col:
    def __init__(self, name):
        self.name = name


class _Cur:
    def __init__(self, rows=None, colnames=None):
        self._rows = rows or []
        self.executed = []
        self.description = [_Col(c) for c in (colnames or [])]
        self._executemany_calls = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        return self

    def executemany(self, sql, values):
        self._executemany_calls.append((sql, values))
        return self

    def fetchall(self):
        return self._rows


class _Conn:
    def __init__(self, rows=None, colnames=None):
        self._rows = rows or []
        self._colnames = colnames or []
        self.cursors = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def cursor(self):
        c = _Cur(self._rows, self._colnames)
        self.cursors.append(c)
        return c

    def commit(self):
        pass


def _conn(monkeypatch, rows=None, colnames=None):
    conn = _Conn(rows, colnames)
    monkeypatch.setattr(ti, "get_connection", lambda: conn)
    return conn


class TestUpserts:
    def test_upsert_daily_rows(self, monkeypatch) -> None:
        conn = _conn(monkeypatch)
        n = ti.upsert_daily_rows([
            {"trade_date": "2026-08-07", "ts_code": "600000.SH", "exalter": "拉萨", "buy": 1.0, "sell": 0.5, "net_buy": 0.5, "side": "B", "reason": "r"},
            {"trade_date": date(2026, 8, 7), "ts_code": "600000.SH", "exalter": "机构专用", "buy": 2.0},
            {"trade_date": None, "ts_code": "", "exalter": ""},
        ])
        assert n == 2
        sql, values = conn.cursors[-1]._executemany_calls[0]
        assert "ON CONFLICT" in sql and len(values) == 2

    def test_upsert_daily_rows_empty(self, monkeypatch) -> None:
        assert ti.upsert_daily_rows([]) == 0
        _conn(monkeypatch)
        assert ti.upsert_daily_rows([{"trade_date": None}]) == 0

    def test_upsert_summary_rows(self, monkeypatch) -> None:
        conn = _conn(monkeypatch)
        n = ti.upsert_summary_rows([
            {"trade_date": "2026-08-07", "ts_code": "600000.SH", "inst_net_buy": 1.0, "inst_net_buy_yi": 0.1, "seat_label": "机构", "lhasa_dominant": True, "on_board": True},
            {"trade_date": "bad", "ts_code": ""},
        ])
        assert n == 1
        sql, values = conn.cursors[-1]._executemany_calls[0]
        assert "lhasa_dominant = excluded.lhasa_dominant" in sql
        assert values[0][4] == "机构" and values[0][5] is True

    def test_upsert_summary_rows_empty(self, monkeypatch) -> None:
        assert ti.upsert_summary_rows([]) == 0


class TestSummaries:
    COLS = ["trade_date", "ts_code", "inst_net_buy", "inst_net_buy_yi", "seat_label", "lhasa_dominant", "on_board"]

    def test_fetch_summaries_for_codes_latest(self, monkeypatch) -> None:
        conn = _conn(monkeypatch, rows=[
            (date(2026, 8, 7), "600000.SH", 1.5, 0.15, "机构专用", True, True),
            (date(2026, 8, 7), "000001.SZ", None, None, None, False, False),
        ], colnames=self.COLS)
        out = ti.fetch_summaries_for_codes(["600000.SH", "000001.SZ", "  "])
        assert out["600000.SH"]["inst_net_buy"] == 1.5
        assert out["600000.SH"]["trade_date"] == "2026-08-07"
        assert out["600000.SH"]["lhasa_dominant"] is True
        assert "000001.SZ" in out
        assert "DISTINCT ON" in conn.cursors[0].executed[0][0]

    def test_fetch_summaries_for_codes_by_date(self, monkeypatch) -> None:
        conn = _conn(monkeypatch, rows=[(date(2026, 8, 7), "600000.SH", 1.5, 0.15, None, False, True)], colnames=self.COLS)
        out = ti.fetch_summaries_for_codes(["600000.SH"], trade_date="2026-08-07")
        assert out["600000.SH"]["on_board"] is True
        assert "trade_date = %s" in conn.cursors[0].executed[0][0]

    def test_fetch_summaries_empty(self, monkeypatch) -> None:
        assert ti.fetch_summaries_for_codes([]) == {}
        assert ti.fetch_summaries_for_codes([" "]) == {}
        _conn(monkeypatch, rows=[], colnames=self.COLS)
        assert ti.fetch_summaries_for_codes(["600000.SH"], trade_date="bad") == {}

    def test_fetch_summaries_bad_float(self, monkeypatch) -> None:
        conn = _conn(monkeypatch, rows=[(date(2026, 8, 7), "600000.SH", "abc", 0.1, None, False, True)], colnames=self.COLS)
        out = ti.fetch_summaries_for_codes(["600000.SH"])
        assert out["600000.SH"]["inst_net_buy"] == "abc"


class TestSeats:
    COLS = ["trade_date", "ts_code", "exalter", "buy", "sell", "net_buy", "side", "reason"]

    def test_fetch_daily_seats_batch(self, monkeypatch) -> None:
        conn = _conn(monkeypatch, rows=[
            (date(2026, 8, 7), "600000.SH", "拉萨", 1.0, 0.5, 0.5, "B", "r"),
        ], colnames=self.COLS)
        out = ti.fetch_daily_seats_batch([("600000.SH", "2026-08-07"), ("600000.SH", "2026-08-07"), ("", "bad")])
        assert ("600000.SH", "2026-08-07") in out
        assert out[("600000.SH", "2026-08-07")][0]["exalter"] == "拉萨"
        assert "unnest" in conn.cursors[0].executed[0][0]

    def test_fetch_daily_seats_batch_empty(self, monkeypatch) -> None:
        assert ti.fetch_daily_seats_batch([]) == {}
        assert ti.fetch_daily_seats_batch([("", None)]) == {}

    def test_fetch_daily_seats(self, monkeypatch) -> None:
        _conn(monkeypatch, rows=[(date(2026, 8, 7), "600000.SH", "拉萨", 1.0, 0.5, 0.5, "B", "r")], colnames=self.COLS)
        out = ti.fetch_daily_seats("600000.SH", "2026-08-07")
        assert out[0]["exalter"] == "拉萨"

    def test_fetch_daily_seats_bad_date(self, monkeypatch) -> None:
        _conn(monkeypatch)
        assert ti.fetch_daily_seats("600000.SH", None) == []

    def test_date_str(self) -> None:
        assert ti._date_str(None) is None
        assert ti._date_str("20260807") == "2026-08-07"
        assert ti._date_str(date(2026, 8, 7)) == "2026-08-07"
