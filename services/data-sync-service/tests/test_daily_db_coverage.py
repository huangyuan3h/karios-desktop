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
