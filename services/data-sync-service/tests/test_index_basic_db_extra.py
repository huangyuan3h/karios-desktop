"""db/index_basic.py coverage with fake connection."""

from __future__ import annotations

from datetime import date

import pandas as pd

from data_sync_service.db import index_basic as ib

COLS = ["ts_code", "trade_date", "total_mv", "float_mv", "total_share", "float_share",
        "free_share", "turnover_rate", "turnover_rate_f", "pe", "pe_ttm", "pb"]


class _Cur:
    def __init__(self, rows=None, description=None) -> None:
        self._rows = rows or []
        self._desc = description or [type("C", (), {"name": n})() for n in COLS]
        self.executed: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
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
    monkeypatch.setattr(ib, "ensure_table", lambda: None)
    monkeypatch.setattr(ib, "get_connection", lambda: _Conn(cur))
    return cur


def test_numeric_variants() -> None:
    assert ib._numeric(None) is None
    assert ib._numeric(float("nan")) is None
    assert ib._numeric("1.5") == 1.5
    assert ib._numeric("bad") is None
    assert ib._numeric(3) == 3.0


def test_date_str_variants() -> None:
    assert ib._date_str(None) is None
    assert ib._date_str("20260807") == "2026-08-07"
    assert ib._date_str("2026-08-07") == "2026-08-07"
    assert ib._date_str(date(2026, 8, 7)) == "2026-08-07"
    assert ib._date_str("  ") is None
    assert ib._date_str("bad") == "bad"


def test_upsert_from_dataframe(monkeypatch) -> None:
    df = pd.DataFrame(
        [
            {"ts_code": "000001.SH", "trade_date": "2026-08-07", "total_mv": 1.0, "float_mv": 2.0,
             "total_share": 3.0, "float_share": 4.0, "free_share": 5.0, "turnover_rate": 6.0,
             "turnover_rate_f": 7.0, "pe": 8.0, "pe_ttm": 9.0, "pb": 10.0},
            {"ts_code": "000001.SH", "trade_date": "", "total_mv": None, "float_mv": None,
             "total_share": None, "float_share": None, "free_share": None, "turnover_rate": None,
             "turnover_rate_f": None, "pe": None, "pe_ttm": None, "pb": None},
            {"ts_code": "399001.SZ", "trade_date": "bad!", "total_mv": 1.0, "float_mv": None,
             "total_share": None, "float_share": None, "free_share": None, "turnover_rate": None,
             "turnover_rate_f": None, "pe": None, "pe_ttm": None, "pb": None},
        ]
    )
    cur = _patch(monkeypatch)
    n = ib.upsert_from_dataframe(df)
    assert n == 2  # empty trade_date skipped; "bad!" kept (not None)
    params = cur.executed[0][1]
    assert params[0] == "000001.SH"
    assert params[1] == "2026-08-07"
    assert params[2] == 1.0 and params[11] == 10.0
    assert cur.executed[1][1][1] == "bad!"


def test_upsert_empty(monkeypatch) -> None:
    cur = _patch(monkeypatch)
    assert ib.upsert_from_dataframe(pd.DataFrame({"trade_date": []})) == 0
    assert cur.executed == []


def test_get_last_trade_date(monkeypatch) -> None:
    _ = _patch(monkeypatch, [(date(2026, 8, 7),)])
    assert ib.get_last_trade_date("000001.SH") == date(2026, 8, 7)
    _ = _patch(monkeypatch, [(None,)])
    assert ib.get_last_trade_date("000001.SH") is None
    _ = _patch(monkeypatch, [])
    assert ib.get_last_trade_date("000001.SH") is None


def test_fetch_index_basic_all_filters(monkeypatch) -> None:
    from datetime import datetime

    rows = [
        ("000001.SH", datetime(2026, 8, 7), 1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
        ("000001.SH", datetime(2026, 8, 6), None, None, None, None, None, None, None, None, None, None),
    ]
    cur = _patch(monkeypatch, rows)
    out = ib.fetch_index_basic(ts_code="000001.SH", start_date="2026-08-01", end_date="2026-08-07", limit=10)
    assert len(out) == 2
    assert out[0]["trade_date"] == "2026-08-06"
    assert out[0]["total_mv"] is None
    assert out[1]["trade_date"] == "2026-08-07"
    assert out[1]["pe"] == 8.0
    assert cur.executed[0][0].count("%s") == 4


def test_fetch_index_basic_no_filters(monkeypatch) -> None:
    cur = _patch(monkeypatch, [("000001.SH", date(2026, 8, 7), 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)])
    out = ib.fetch_index_basic(limit=5000)
    assert len(out) == 1
    assert out[0]["ts_code"] == "000001.SH"
    assert cur.executed[0][0].count("%s") == 1


def test_fetch_last_float_mv_turnover(monkeypatch) -> None:
    rows = [(date(2026, 8, 7), 100.0, 1.5), (date(2026, 8, 6), 90.0, "bad")]
    cur = _patch(monkeypatch, rows)
    out = ib.fetch_last_float_mv_turnover("000001.SH", days=80)
    assert out == [("2026-08-06", 90.0, 0.0), ("2026-08-07", 100.0, 1.5)]
    assert cur.executed[0][1] == ("000001.SH", 80)


def test_fetch_last_float_mv_turnover_non_date(monkeypatch) -> None:
    _patch(monkeypatch, [("20260807", None, 1.0)])
    out = ib.fetch_last_float_mv_turnover("000001.SH", days=0)
    assert out == [("20260807", 0.0, 1.0)]
