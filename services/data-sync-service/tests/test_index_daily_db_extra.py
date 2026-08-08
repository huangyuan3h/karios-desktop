"""db/index_daily fetch/upsert coverage with fake connection."""

from __future__ import annotations

from datetime import date

import pandas as pd

from data_sync_service.db import index_daily as idd

COLS = ["ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount"]


class _Cur:
    def __init__(self, rows=None, description=None) -> None:
        self._rows = rows or []
        self._description = description or [type("C", (), {"name": n})() for n in COLS]
        self.executed: list = []
        self.last_execute: tuple | None = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def execute(self, sql, params=None):
        self.last_execute = (sql, params)
        return self

    def executemany(self, sql, rows):
        self.executed.extend(rows)
        return self

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    @property
    def description(self):
        return self._description


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


def _patch(monkeypatch, rows=None, description=None):
    cur = _Cur(rows, description)
    monkeypatch.setattr(idd, "ensure_table", lambda: None)
    monkeypatch.setattr(idd, "get_connection", lambda: _Conn(cur))
    return cur


def _row(*vals):
    return vals


# ---- fetch_index_daily -----------------------------------------------------

def test_fetch_index_daily_all_filters(monkeypatch) -> None:
    from datetime import datetime

    rows = [
        ("000001.SH", datetime(2026, 8, 7), 1, 2, 3, 4, 5, 6, 7, 8, 9),
        ("000001.SH", datetime(2026, 8, 6), None, None, None, None, None, None, None, None, None),
    ]
    cur = _patch(monkeypatch, rows)
    out = idd.fetch_index_daily(ts_code="000001.SH", start_date="2026-08-01", end_date="2026-08-07", limit=10)
    assert len(out) == 2
    assert out[0]["trade_date"] == "2026-08-06"  # reversed: oldest first
    assert out[0]["close"] is None
    assert out[1]["trade_date"] == "2026-08-07"
    assert out[1]["close"] == 4.0
    assert cur.last_execute[0].count("%s") == 4


def test_fetch_index_daily_no_filters(monkeypatch) -> None:
    cur = _patch(monkeypatch, [("000001.SH", date(2026, 8, 7), 1, 2, 3, 4, 5, 6, 7, 8, 9)])
    out = idd.fetch_index_daily(limit=5000)
    assert len(out) == 1
    assert out[0]["ts_code"] == "000001.SH"
    assert cur.last_execute[0].count("%s") == 1


def test_fetch_index_daily_empty(monkeypatch) -> None:
    _patch(monkeypatch, [])
    assert idd.fetch_index_daily() == []


# ---- fetch_last_closes family ----------------------------------------------

def test_fetch_last_closes(monkeypatch) -> None:
    rows = [(date(2026, 8, 7), 4.5), (date(2026, 8, 6), 4.2)]
    cur = _patch(monkeypatch, rows)
    out = idd.fetch_last_closes("000001.SH", days=60)
    assert out == [("2026-08-06", 4.2), ("2026-08-07", 4.5)]  # ASC


def test_fetch_last_closes_non_date_and_bad_close(monkeypatch) -> None:
    _patch(monkeypatch, [("20260807", "junk"), ("2026-08-05", None)])
    out = idd.fetch_last_closes("000001.SH", days=0)  # clamped to 1
    assert out == [("2026-08-05", 0.0), ("20260807", 0.0)]


def test_fetch_last_closes_upto(monkeypatch) -> None:
    rows = [(date(2026, 8, 5), 4.1)]
    cur = _patch(monkeypatch, rows)
    out = idd.fetch_last_closes_upto("000001.SH", "2026-08-05", days=60)
    assert out == [("2026-08-05", 4.1)]
    assert cur.last_execute[0].count("%s") == 3


def test_fetch_last_closes_vol(monkeypatch) -> None:
    rows = [(date(2026, 8, 7), 4.5, 1000.0), (date(2026, 8, 6), 4.2, "bad")]
    _patch(monkeypatch, rows)
    out = idd.fetch_last_closes_vol("000001.SH", days=80)
    assert out == [("2026-08-06", 4.2, 0.0), ("2026-08-07", 4.5, 1000.0)]


def test_fetch_last_closes_vol_upto(monkeypatch) -> None:
    rows = [(date(2026, 8, 5), 4.1, 900.0)]
    cur = _patch(monkeypatch, rows)
    out = idd.fetch_last_closes_vol_upto("000001.SH", "2026-08-05", days=80)
    assert out == [("2026-08-05", 4.1, 900.0)]
    assert cur.last_execute[0].count("%s") == 3


def test_fetch_last_closes_vol_batch(monkeypatch) -> None:
    assert idd.fetch_last_closes_vol_batch([]) == {}
    assert idd.fetch_last_closes_vol_batch(["", "  "]) == {}

    rows = [  # SQL returns ts_code ASC, trade_date ASC
        ("000001.SH", date(2026, 8, 6), 4.2, 90.0),
        ("000001.SH", date(2026, 8, 7), 4.5, 100.0),
        ("399001.SZ", date(2026, 8, 7), 9.0, 50.0),
        ("999999.X", date(2026, 8, 7), 1.0, 2.0),  # not requested -> setdefault bucket
    ]
    cur = _patch(monkeypatch, rows)
    out = idd.fetch_last_closes_vol_batch(["000001.SH", "399001.SZ"], days=80)
    assert out["000001.SH"] == [("2026-08-06", 4.2, 90.0), ("2026-08-07", 4.5, 100.0)]
    assert out["399001.SZ"] == [("2026-08-07", 9.0, 50.0)]
    assert out["999999.X"] == [("2026-08-07", 1.0, 2.0)]


def test_fetch_last_closes_vol_batch_with_as_of(monkeypatch) -> None:
    rows = [("000001.SH", date(2026, 8, 5), 4.1, 80.0)]
    cur = _patch(monkeypatch, rows)
    out = idd.fetch_last_closes_vol_batch(["000001.SH"], days=80, as_of_date="2026-08-06")
    assert out["000001.SH"] == [("2026-08-05", 4.1, 80.0)]
    assert cur.last_execute[0].count("%s") == 3


# ---- upsert / get_last_trade_date -----------------------------------------

def test_upsert_from_dataframe(monkeypatch) -> None:
    df = pd.DataFrame(
        [
            {"ts_code": "000001.SH", "trade_date": 20260807, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5,
             "pre_close": 1.4, "change": 0.1, "pct_chg": 7.1, "vol": 100, "amount": 200},
            {"ts_code": "000001.SH", "trade_date": "2026-08-06", "open": None, "high": None, "low": None,
             "close": None, "pre_close": None, "change": None, "pct_chg": None, "vol": None, "amount": None},
        ]
    )
    cur = _patch(monkeypatch)
    n = idd.upsert_from_dataframe(df)
    assert n == 2
    assert cur.executed[0][1] == "2026-08-07"
    assert cur.executed[0][10] == 200.0
    assert cur.executed[1][1] == "2026-08-06"
    assert cur.executed[1][2] is None


def test_upsert_from_dataframe_empty(monkeypatch) -> None:
    cur = _patch(monkeypatch)
    assert idd.upsert_from_dataframe(pd.DataFrame({"ts_code": []})) == 0
    assert cur.executed == []


def test_get_last_trade_date_hit(monkeypatch) -> None:
    cur = _patch(monkeypatch, [(date(2026, 8, 7),)])
    assert idd.get_last_trade_date("000001.SH") == date(2026, 8, 7)


def test_get_last_trade_date_miss(monkeypatch) -> None:
    cur = _patch(monkeypatch, [(None,)])
    assert idd.get_last_trade_date("000001.SH") is None
    cur2 = _patch(monkeypatch, [])
    assert idd.get_last_trade_date("000001.SH") is None
