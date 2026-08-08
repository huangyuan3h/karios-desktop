"""db/macro_daily.py coverage with fake connection."""

from __future__ import annotations

from datetime import date, datetime

import pandas as pd

from data_sync_service.db import macro_daily as md

COLS = ["series_id", "trade_date", "source", "underlying_ts_code",
        "open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount"]


class _Cur:
    def __init__(self, rows=None, description=None) -> None:
        self._rows = rows or []
        self._desc = description or [type("C", (), {"name": n})() for n in COLS]
        self.executed: list[tuple] = []
        self.last_execute: tuple | None = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def execute(self, sql, params=None):
        self.last_execute = (sql, params)
        self.executed.append((sql, params))
        return self

    def executemany(self, sql, rows):
        self.executed.append((sql, rows))
        return self

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return self._rows

    @property
    def description(self):
        return self._desc


class _Conn:
    def __init__(self, cur) -> None:
        self._cur = cur

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def cursor(self):
        return self._cur

    def commit(self) -> None:
        pass


def _patch(monkeypatch, rows=None):
    cur = _Cur(rows)
    monkeypatch.setattr(md, "ensure_table", lambda: None)
    monkeypatch.setattr(md, "get_connection", lambda: _Conn(cur))
    return cur


def test_get_last_trade_date(monkeypatch) -> None:
    cur = _patch(monkeypatch, [(date(2026, 8, 7),)])
    assert md.get_last_trade_date("SPX") == date(2026, 8, 7)
    cur2 = _patch(monkeypatch, [(None,)])
    assert md.get_last_trade_date("SPX") is None


def test_upsert_from_dataframe(monkeypatch) -> None:
    df = pd.DataFrame(
        [
            {"trade_date": "2026-08-07", "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5,
             "pre_close": 1.4, "change": 0.1, "pct_chg": 7.1, "vol": 100, "amount": 200},
            {"trade_date": "2026-08-06", "open": None, "high": None, "low": None, "close": None,
             "pre_close": None, "change": None, "pct_change": None, "vol": None, "amount": None},
            {"trade_date": "junk", "close": 1.0},
            {"trade_date": "", "close": 1.0},
        ]
    )
    cur = _patch(monkeypatch)
    n = md.upsert_from_dataframe(df, series_id="SPX", source="tushare", underlying_ts_code="000001.SH")
    assert n == 3  # "" skipped; "junk" kept (not None)
    rows = cur.executed[0][1]
    assert rows[0][0] == "SPX"
    assert rows[0][3] == "000001.SH"
    assert rows[0][7] == 1.5 and rows[0][10] == 7.1
    assert rows[1][0] == "SPX" and rows[1][7] is None
    assert rows[2][1] == "junk"


def test_upsert_uses_pct_change_and_settle(monkeypatch) -> None:
    df = pd.DataFrame(
        [{"trade_date": "2026-08-07", "pct_change": 2.5, "settle": 3.0}]
    )
    cur = _patch(monkeypatch)
    n = md.upsert_from_dataframe(df, series_id="SPX", source="yfinance")
    assert n == 1
    row = cur.executed[0][1][0]
    assert row[7] == 3.0 and row[10] == 2.5


def test_upsert_empty(monkeypatch) -> None:
    cur = _patch(monkeypatch)
    assert md.upsert_from_dataframe(pd.DataFrame({"trade_date": []}), series_id="X", source="s") == 0
    assert cur.executed == []


def test_fetch_macro_daily_all_filters(monkeypatch) -> None:
    rows = [
        ("SPX", datetime(2026, 8, 7), "tushare", "000001.SH", 1, 2, 3, 4, 5, 6, 7, 8, 9),
        ("SPX", datetime(2026, 8, 6), "tushare", None, None, None, None, None, None, None, None, None, None),
    ]
    cur = _patch(monkeypatch, rows)
    out = md.fetch_macro_daily(series_id="SPX", start_date="2026-08-01", end_date="2026-08-07", limit=10)
    assert len(out) == 2
    assert out[0]["trade_date"] == "2026-08-06"
    assert out[0]["close"] is None and out[0]["underlying_ts_code"] is None
    assert out[1]["trade_date"] == "2026-08-07"
    assert out[1]["close"] == 4.0 and out[1]["source"] == "tushare"
    assert cur.last_execute[0].count("%s") == 4


def test_fetch_macro_daily_no_filters(monkeypatch) -> None:
    cur = _patch(monkeypatch, [("SPX", date(2026, 8, 7), "s", None, 1, 2, 3, 4, 5, 6, 7, 8, 9)])
    out = md.fetch_macro_daily(limit=5000)
    assert len(out) == 1
    assert cur.last_execute[0].count("%s") == 1


def test_fetch_last_closes(monkeypatch) -> None:
    rows = [(date(2026, 8, 7), 4.5), (date(2026, 8, 6), "bad")]
    _patch(monkeypatch, rows)
    assert md.fetch_last_closes("SPX", days=80) == [("2026-08-06", 0.0), ("2026-08-07", 4.5)]


def test_fetch_last_closes_batch(monkeypatch) -> None:
    assert md.fetch_last_closes_batch([], days=80) == {}
    assert md.fetch_last_closes_batch(["", " "]) == {}
    rows = [
        ("SPX", date(2026, 8, 6), 4.2),
        ("SPX", date(2026, 8, 7), 4.5),
        ("IXIC", date(2026, 8, 7), 9.0),
        ("UNKNOWN", date(2026, 8, 7), 1.0),
    ]
    cur = _patch(monkeypatch, rows)
    out = md.fetch_last_closes_batch(["SPX", "IXIC"], days=80)
    assert out["SPX"] == [("2026-08-06", 4.2), ("2026-08-07", 4.5)]
    assert out["IXIC"] == [("2026-08-07", 9.0)]
    assert out["UNKNOWN"] == [("2026-08-07", 1.0)]
    assert cur.last_execute[1][0] == ["SPX", "IXIC"]


def test_get_latest_rows_batch(monkeypatch) -> None:
    assert md.get_latest_rows_batch([]) == {}
    rows = [("SPX", date(2026, 8, 7), "tushare", "000001.SH", 1, 2, 3, 4, 5, 6, 7, 8, 9)]
    cur = _patch(monkeypatch, rows)
    out = md.get_latest_rows_batch(["SPX"])
    assert out["SPX"]["close"] == 4.0
    assert out["SPX"]["trade_date"] == "2026-08-07"
    assert out["SPX"]["underlying_ts_code"] == "000001.SH"


def test_get_latest_row(monkeypatch) -> None:
    cur = _patch(monkeypatch, [])
    assert md.get_latest_row("SPX") is None
    row = ("SPX", date(2026, 8, 7), "tushare", None, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    cur = _patch(monkeypatch, [row])
    out = md.get_latest_row("SPX")
    assert out["close"] == 4.0 and out["pct_chg"] == 7.0
    assert cur.last_execute[1] == ("SPX",)


def test_list_distinct_series_ids(monkeypatch) -> None:
    cur = _patch(monkeypatch, [("SPX",), ("IXIC",), (None,)])
    assert md.list_distinct_series_ids() == ["SPX", "IXIC"]
