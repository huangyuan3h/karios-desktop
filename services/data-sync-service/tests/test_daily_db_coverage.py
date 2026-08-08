"""db/daily.py coverage: normalization helpers + upsert driver."""

from __future__ import annotations

import pandas as pd

from data_sync_service.db import daily as dbd


def test_numeric_scalar_date_helpers() -> None:
    import datetime as dt

    assert dbd._numeric(None) is None
    assert dbd._numeric(float("nan")) is None
    assert dbd._numeric("12.5") == 12.5
    assert dbd._numeric("bad") is None

    assert dbd._scalar(None) is None
    assert dbd._scalar("  x ") == "x"

    assert dbd._date_str(None) is None
    assert dbd._date_str(dt.date(2026, 7, 1)) == "2026-07-01"
    assert dbd._date_str("20260701") == "2026-07-01"
    assert dbd._date_str("2026-07-01") == "2026-07-01"


def test_upsert_from_dataframe_builds_rows(monkeypatch) -> None:
    """upsert_from_dataframe: rows built from df, executemany called once."""
    df = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "600000.SH"],
            "trade_date": ["20260701", "20260701"],
            "open": [10.0, 11.0],
            "high": [10.5, 11.5],
            "low": [9.8, 10.8],
            "close": [10.2, 11.2],
            "pre_close": [10.0, 11.0],
            "change": [0.2, 0.2],
            "pct_chg": [2.0, 1.8],
            "vol": [1000.0, 2000.0],
            "amount": [5000.0, 8000.0],
        }
    )
    monkeypatch.setattr(dbd, "ensure_table", lambda: None)

    captured: list[list[tuple]] = []

    class _Cur:
        def executemany(self, sql, rows):
            captured.append(list(rows))

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return None

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return None

        def cursor(self):
            return _Cur()

        def commit(self):
            return None

    monkeypatch.setattr(dbd, "get_connection", lambda: _Conn())
    n = dbd.upsert_from_dataframe(df)
    assert n == 2
    row = captured[0][0]
    assert row[0] == "000001.SZ"
    assert row[1] == "2026-07-01"
    assert row[5] == 10.2  # close


def test_upsert_from_dataframe_empty_df() -> None:
    assert dbd.upsert_from_dataframe(pd.DataFrame()) == 0
import datetime

import pandas as pd

from data_sync_service.db import daily as dd


class _Cur:
    def __init__(self, fetchone=None, fetchall=None, rowcount=0, description=None):
        self._one = fetchone
        self._all = fetchall
        self.rowcount = rowcount
        self.description = description or [type("C", (), {"name": n})() for n in
            ("ts_code", "trade_date", "open", "high", "low", "close", "pre_close",
             "change", "pct_chg", "vol", "amount", "adj_factor")]
        self.executed: list[tuple] = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        return self

    def executemany(self, sql, rows):
        self.executed.append((sql, rows))
        return self

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._all

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None


class _Conn:
    def __init__(self, cur):
        self._cur = cur

    def cursor(self):
        return self._cur

    def commit(self):
        return None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None


def _monkey(monkeypatch, cur):
    monkeypatch.setattr(dd, "ensure_table", lambda: None)
    monkeypatch.setattr(dd, "get_connection", lambda: _Conn(cur))


def test_get_last_adj_factor_date(monkeypatch) -> None:
    cur = _Cur(fetchone=(datetime.date(2026, 8, 4),))
    _monkey(monkeypatch, cur)
    assert dd.get_last_adj_factor_date("600000.SH") == datetime.date(2026, 8, 4)

    cur2 = _Cur(fetchone=(None,))
    _monkey(monkeypatch, cur2)
    assert dd.get_last_adj_factor_date("600000.SH") is None


def test_count_rows_for_trade_date(monkeypatch) -> None:
    cur = _Cur(fetchone=(5,))
    _monkey(monkeypatch, cur)
    assert dd.count_rows_for_trade_date("2026-08-04") == 5

    cur2 = _Cur(fetchone=("x",))
    _monkey(monkeypatch, cur2)
    assert dd.count_rows_for_trade_date("2026-08-04") == 0
    assert dd.count_rows_for_trade_date("bad") == 0


def test_fetch_daily(monkeypatch) -> None:
    cur = _Cur(fetchall=[
        ("600000.SH", datetime.date(2026, 8, 4), "10.0", "10.5", "9.8", "10.2", "9.9", "0.3", "3.0", 100, 1e6, "1.1"),
    ])
    _monkey(monkeypatch, cur)
    out = dd.fetch_daily(ts_code="600000.sh", start_date="2026-08-01", end_date="2026-08-04")
    assert out[0]["ts_code"] == "600000.SH"
    assert out[0]["trade_date"] == "2026-08-04"
    assert out[0]["close"] == "10.2"


def test_fetch_daily_for_codes(monkeypatch) -> None:
    cur = _Cur(fetchall=[
        ("600000.SH", datetime.date(2026, 8, 4), "10.0", "10.5", "9.8", "10.2", "9.9", "0.3", "3.0", 100, 1e6, "1.1"),
    ])
    _monkey(monkeypatch, cur)
    out = dd.fetch_daily_for_codes(["600000.sh", ""], "2026-08-01", "2026-08-04")
    assert len(out) == 1
    assert out[0]["close"] == "10.2"  # str stays str; no __float__ on str
    assert out[0]["trade_date"] == "2026-08-04"
    assert dd.fetch_daily_for_codes([], "2026-08-01", "2026-08-04") == []


def test_fetch_last_adj_factors(monkeypatch) -> None:
    cur = _Cur(fetchall=[("600000.sh", "1.2"), ("000001.SZ", 0.0), ("000002.SZ", None)])
    _monkey(monkeypatch, cur)
    out = dd.fetch_last_adj_factors(["600000.sh"], "2026-08-04")
    assert out == {"600000.SH": 1.2}
    assert dd.fetch_last_adj_factors([], "2026-08-04") == {}


def test_fetch_latest_trade_date_for_codes(monkeypatch) -> None:
    cur = _Cur(fetchone=(datetime.date(2026, 8, 4),))
    _monkey(monkeypatch, cur)
    assert dd.fetch_latest_trade_date_for_codes(["600000.SH"]) == "2026-08-04"
    cur2 = _Cur(fetchone=("2026-08-04",))
    _monkey(monkeypatch, cur2)
    assert dd.fetch_latest_trade_date_for_codes(["600000.SH"]) == "2026-08-04"
    cur3 = _Cur(fetchone=(None,))
    _monkey(monkeypatch, cur3)
    assert dd.fetch_latest_trade_date_for_codes(["600000.SH"]) is None
    assert dd.fetch_latest_trade_date_for_codes([]) is None


def test_fetch_trade_dates_for_codes(monkeypatch) -> None:
    cur = _Cur(fetchall=[(datetime.date(2026, 8, 4),), ("2026-08-05",)])
    _monkey(monkeypatch, cur)
    out = dd.fetch_trade_dates_for_codes(["600000.SH"], "2026-08-01", "2026-08-05")
    assert out == ["2026-08-04", "2026-08-05"]
    assert dd.fetch_trade_dates_for_codes([], "a", "b") == []


def test_update_adj_factor_from_dataframe(monkeypatch) -> None:
    df = pd.DataFrame({
        "ts_code": ["600000.SH", "600000.SH", "000001.SZ"],
        "trade_date": ["20260804", "20260805", "20260805"],
        "adj_factor": [1.1, 1.2, 1.3],
    })
    cur = _Cur()
    _monkey(monkeypatch, cur)
    assert dd.update_adj_factor_from_dataframe(df) == 3
    assert cur.executed[0][1][0] == (1.1, "600000.SH", "2026-08-04")

    df_skip = pd.DataFrame({"ts_code": ["", "600000.SH"], "trade_date": ["20260804", "20260805"], "adj_factor": [1, 1]})
    assert dd.update_adj_factor_from_dataframe(df_skip) == 1


def test_fetch_last_bars(monkeypatch) -> None:
    cur = _Cur(fetchall=[  # DESC order from SQL; reversed to ASC inside
        (datetime.date(2026, 8, 5), "10.2", "10.8", "10.0", "10.6", 120, 1.2e6),
        (datetime.date(2026, 8, 4), "10.0", "10.5", "9.8", "10.2", 100, 1e6),
    ])
    _monkey(monkeypatch, cur)
    out = dd.fetch_last_bars("600000.SH", days=999)
    assert len(out) == 2
    assert out[0]["date"] == "2026-08-04" and out[1]["date"] == "2026-08-05"
    assert out[1]["close"] == "10.6"
