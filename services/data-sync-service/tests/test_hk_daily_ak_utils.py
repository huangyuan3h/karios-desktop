"""Pure-Python unit tests for the akshare HK sync helper (no live calls)."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from typing import Any

import pandas as pd
import pytest


def test_ts_code_to_sina_padded() -> None:
    from data_sync_service.service.hk_daily_ak import _ts_code_to_sina

    assert _ts_code_to_sina("00700.HK") == "00700"
    assert _ts_code_to_sina("01810.HK") == "01810"
    assert _ts_code_to_sina("09988.HK") == "09988"
    assert _ts_code_to_sina("0005.HK") == "0005"


def test_ts_code_to_sina_invalid() -> None:
    from data_sync_service.service.hk_daily_ak import _ts_code_to_sina

    assert _ts_code_to_sina("") is None
    assert _ts_code_to_sina("600519.SH") is None
    assert _ts_code_to_sina("HK") is None
    assert _ts_code_to_sina("ABC.HK") is None
    assert _ts_code_to_sina(".HK") is None


def test_df_to_daily_rows_basic_chronological() -> None:
    """akshare rows are ascending by date — pre_close / change / pct_chg derive from prior bar."""
    from data_sync_service.service.hk_daily_ak import _df_to_daily_rows

    df = pd.DataFrame(
        {
            "date": [date(2026, 7, 24), date(2026, 7, 25), date(2026, 7, 28)],
            "open": [10.0, 11.0, 12.0],
            "high": [10.5, 11.5, 12.5],
            "low": [9.5, 10.5, 11.5],
            "close": [10.2, 11.1, 12.0],
            "volume": [1000.0, 1100.0, 1200.0],
            "amount": [10200.0, 12210.0, 14400.0],
        }
    )
    rows = _df_to_daily_rows("01810.HK", df, since=None)
    assert len(rows) == 3

    assert rows[0]["pre_close"] is None  # first bar has no prior
    assert rows[0]["change"] is None
    assert rows[0]["pct_chg"] is None
    assert rows[0]["ts_code"] == "01810.HK"
    assert rows[0]["trade_date"] == "2026-07-24"
    assert rows[0]["vol"] == 1000.0

    # Bar 2 derives from bar 1 close
    assert rows[1]["pre_close"] == 10.2
    assert rows[1]["change"] == pytest.approx(0.9)
    assert rows[1]["pct_chg"] == pytest.approx(8.8235, rel=1e-3)

    # Bar 3 derives from bar 2 close
    assert rows[2]["pre_close"] == 11.1
    assert rows[2]["change"] == pytest.approx(0.9)
    assert rows[2]["pct_chg"] == pytest.approx(8.1081, rel=1e-3)


def test_df_to_daily_rows_incremental_since_filter() -> None:
    """Rows on or before `since` are skipped but their close still seeds the next bar."""
    from data_sync_service.service.hk_daily_ak import _df_to_daily_rows

    df = pd.DataFrame(
        {
            "date": [date(2026, 7, 24), date(2026, 7, 25), date(2026, 7, 28)],
            "open": [10.0, 11.0, 12.0],
            "high": [10.5, 11.5, 12.5],
            "low": [9.5, 10.5, 11.5],
            "close": [10.2, 11.1, 12.0],
            "volume": [1000.0, 1100.0, 1200.0],
            "amount": [10200.0, 12210.0, 14400.0],
        }
    )
    rows = _df_to_daily_rows("01810.HK", df, since=date(2026, 7, 24))
    # Only 2026-07-25 and 2026-07-28 should be returned.
    assert [r["trade_date"] for r in rows] == ["2026-07-25", "2026-07-28"]
    # 2026-07-25 pre_close should be the prior bar (2026-07-24 close), not None.
    assert rows[0]["pre_close"] == 10.2


def test_df_to_daily_rows_skips_nan_close() -> None:
    from data_sync_service.service.hk_daily_ak import _df_to_daily_rows

    df = pd.DataFrame(
        {
            "date": [date(2026, 7, 24), date(2026, 7, 25)],
            "open": [10.0, float("nan")],
            "high": [10.5, float("nan")],
            "low": [9.5, float("nan")],
            "close": [float("nan"), 11.0],  # First bar missing close -> skipped
            "volume": [1000.0, 1100.0],
            "amount": [10200.0, 12210.0],
        }
    )
    rows = _df_to_daily_rows("01810.HK", df, since=None)
    assert len(rows) == 1
    assert rows[0]["trade_date"] == "2026-07-25"
    assert rows[0]["close"] == 11.0
    assert rows[0]["pre_close"] is None  # prior was skipped


def test_df_to_daily_rows_handles_empty_df() -> None:
    from data_sync_service.service.hk_daily_ak import _df_to_daily_rows

    assert _df_to_daily_rows("00700.HK", pd.DataFrame(), since=None) == []


def test_sync_hk_daily_for_ts_code_ak_validates_ts_code() -> None:
    from data_sync_service.service.hk_daily_ak import sync_hk_daily_for_ts_code_ak

    # Non-HK code: rejected without calling akshare.
    r = sync_hk_daily_for_ts_code_ak("600519.SH")
    assert r["ok"] is False
    assert "must end with .HK" in r["error"]


def test_sync_hk_daily_for_ts_code_ak_handles_missing_akshare(monkeypatch) -> None:
    """If akshare is not importable, return ok=False without raising."""
    from data_sync_service.service import hk_daily_ak

    monkeypatch.setattr(
        hk_daily_ak,
        "_ts_code_to_sina",
        lambda _tc: "00700",
    )

    import sys

    # Hide the actual akshare module so the import inside the function raises.
    monkeypatch.setitem(sys.modules, "akshare", None)
    r = hk_daily_ak.sync_hk_daily_for_ts_code_ak("00700.HK")
    assert r["ok"] is False
    assert "akshare unavailable" in r["error"]


def test_sync_hk_daily_for_ts_code_ak_writes_incremental(monkeypatch) -> None:
    """End-to-end: fake akshare returns 3 bars, last 2 are upserted."""
    from data_sync_service.service import hk_daily_ak

    df = pd.DataFrame(
        {
            "date": [date(2026, 7, 24), date(2026, 7, 25), date(2026, 7, 28)],
            "open": [10.0, 11.0, 12.0],
            "high": [10.5, 11.5, 12.5],
            "low": [9.5, 10.5, 11.5],
            "close": [10.2, 11.1, 12.0],
            "volume": [1000.0, 1100.0, 1200.0],
            "amount": [10200.0, 12210.0, 14400.0],
        }
    )

    fake_ak = SimpleNamespace(stock_hk_daily=lambda **_kw: df)
    monkeypatch.setitem(
        __import__("sys").modules,
        "akshare",
        fake_ak,
    )

    upsert_calls: list[Any] = []

    def fake_upsert(rows_df: pd.DataFrame) -> int:
        upsert_calls.append(rows_df)
        return len(rows_df)

    monkeypatch.setattr(hk_daily_ak, "upsert_from_dataframe", fake_upsert)
    monkeypatch.setattr(hk_daily_ak, "get_last_trade_date", lambda _tc: date(2026, 7, 24))

    r = hk_daily_ak.sync_hk_daily_for_ts_code_ak("00700.HK")
    assert r["ok"] is True
    assert r["source"] == "akshare"
    assert r["updated"] == 2  # only 2026-07-25 and 2026-07-28 since we have up to 07-24
    assert len(upsert_calls) == 1
    upserted = upsert_calls[0]
    assert list(upserted.columns) == [
        "ts_code",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "change",
        "pct_chg",
        "vol",
        "amount",
    ]
    assert upserted.iloc[0]["pre_close"] == 10.2


def test_sync_hk_daily_for_ts_code_ak_skips_when_already_up_to_date(monkeypatch) -> None:
    """If the latest akshare bar is older or equal to last_date, return updated=0 without upserting."""
    from data_sync_service.service import hk_daily_ak

    df = pd.DataFrame(
        {
            "date": [date(2026, 7, 24), date(2026, 7, 25)],
            "open": [10.0, 11.0],
            "high": [10.5, 11.5],
            "low": [9.5, 10.5],
            "close": [10.2, 11.1],
            "volume": [1000.0, 1100.0],
            "amount": [10200.0, 12210.0],
        }
    )
    fake_ak = SimpleNamespace(stock_hk_daily=lambda **_kw: df)
    monkeypatch.setitem(__import__("sys").modules, "akshare", fake_ak)
    monkeypatch.setattr(hk_daily_ak, "get_last_trade_date", lambda _tc: date(2026, 7, 25))
    upsert_called = []
    monkeypatch.setattr(
        hk_daily_ak,
        "upsert_from_dataframe",
        lambda df: upsert_called.append(df) or 0,
    )

    r = hk_daily_ak.sync_hk_daily_for_ts_code_ak("00700.HK")
    assert r["ok"] is True
    assert r["updated"] == 0
    assert r["skipped"] is True
    assert upsert_called == []


def test_sync_hk_daily_for_ts_code_ak_caps_first_sync_at_5y(monkeypatch) -> None:
    """When no prior bars exist, only the last 5 years of history should be upserted."""
    from datetime import timedelta

    from data_sync_service.service import hk_daily_ak

    today = hk_daily_ak._default_backfill_cutoff(5)  # today - 5y
    rows: list[dict[str, object]] = []
    # 6 years of daily rows: should keep only those on or after today-5y.
    for offset in range(-365 * 6, 30):
        rows.append(
            {
                "date": today + timedelta(days=offset),
                "open": 10.0,
                "high": 11.0,
                "low": 9.0,
                "close": 10.5,
                "volume": 1000.0,
                "amount": 10500.0,
            }
        )
    df = pd.DataFrame(rows)
    fake_ak = SimpleNamespace(stock_hk_daily=lambda **_kw: df)
    monkeypatch.setitem(__import__("sys").modules, "akshare", fake_ak)

    monkeypatch.setattr(hk_daily_ak, "get_last_trade_date", lambda _tc: None)

    captured: list[pd.DataFrame] = []
    monkeypatch.setattr(
        hk_daily_ak,
        "upsert_from_dataframe",
        lambda x: (captured.append(x), len(x))[1],
    )

    r = hk_daily_ak.sync_hk_daily_for_ts_code_ak("00700.HK")
    assert r["ok"] is True
    assert len(captured) == 1
    upserted = captured[0]
    # Rows before today-5y must be skipped. The remaining span is ~30 + 365*1 = ~395.
    assert len(upserted) < len(df)
    assert upserted["trade_date"].min() >= today.isoformat()
    assert r["backfill_years"] == 5


def test_sync_hk_daily_for_ts_code_ak_backfill_years_override(monkeypatch) -> None:
    """Custom backfill_years shrinks the look-back window for first-time sync."""
    from datetime import timedelta

    from data_sync_service.service import hk_daily_ak

    today = hk_daily_ak._default_backfill_cutoff(1)  # today - 1y
    rows = [
        {
            "date": today + timedelta(days=offset),
            "open": 10.0,
            "high": 11.0,
            "low": 9.0,
            "close": 10.5,
            "volume": 1000.0,
            "amount": 10500.0,
        }
        for offset in range(-365 * 3, 30)  # 3y of daily rows
    ]
    df = pd.DataFrame(rows)
    fake_ak = SimpleNamespace(stock_hk_daily=lambda **_kw: df)
    monkeypatch.setitem(__import__("sys").modules, "akshare", fake_ak)
    monkeypatch.setattr(hk_daily_ak, "get_last_trade_date", lambda _tc: None)

    captured: list[pd.DataFrame] = []
    monkeypatch.setattr(
        hk_daily_ak,
        "upsert_from_dataframe",
        lambda x: (captured.append(x), len(x))[1],
    )

    r = hk_daily_ak.sync_hk_daily_for_ts_code_ak("00700.HK", backfill_years=1)
    assert r["ok"] is True
    assert len(captured) == 1
    upserted = captured[0]
    assert upserted["trade_date"].min() >= today.isoformat()
    assert r["backfill_years"] == 1


def test_sync_hk_daily_for_ts_code_ak_incremental_ignores_backfill_window(monkeypatch) -> None:
    """When we already have bars, the backfill_years cap is ignored and we only fetch newer rows."""
    from datetime import date as date_t

    from data_sync_service.service import hk_daily_ak

    df = pd.DataFrame(
        {
            "date": [date_t(2026, 7, 24), date_t(2026, 7, 25)],
            "open": [10.0, 11.0],
            "high": [10.5, 11.5],
            "low": [9.5, 10.5],
            "close": [10.2, 11.1],
            "volume": [1000.0, 1100.0],
            "amount": [10200.0, 12210.0],
        }
    )
    fake_ak = SimpleNamespace(stock_hk_daily=lambda **_kw: df)
    monkeypatch.setitem(__import__("sys").modules, "akshare", fake_ak)
    # We already have up to 2026-07-24 — incremental sync should only fetch 2026-07-25.
    monkeypatch.setattr(hk_daily_ak, "get_last_trade_date", lambda _tc: date_t(2026, 7, 24))

    captured: list[pd.DataFrame] = []
    monkeypatch.setattr(
        hk_daily_ak,
        "upsert_from_dataframe",
        lambda x: (captured.append(x), len(x))[1],
    )

    # Pass a tiny backfill window — should have no effect on incremental path.
    r = hk_daily_ak.sync_hk_daily_for_ts_code_ak("00700.HK", backfill_years=1)
    assert r["ok"] is True
    assert len(captured) == 1
    assert list(captured[0]["trade_date"]) == ["2026-07-25"]