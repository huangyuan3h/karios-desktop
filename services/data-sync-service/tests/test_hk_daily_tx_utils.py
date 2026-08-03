"""Pure-Python unit tests for the Tencent ifzq HK sync helper (no live calls)."""

from __future__ import annotations

import sys
from datetime import date
from typing import Any

import pytest


@pytest.fixture(autouse=True)
def _non_darwin(monkeypatch):
    """Keep sys.platform neutral so darwin-specific guards don't interfere."""
    if sys.platform == "darwin":
        monkeypatch.setattr(sys, "platform", "linux")


def test_ts_code_to_tx() -> None:
    from data_sync_service.service.hk_daily_tx import _ts_code_to_tx

    assert _ts_code_to_tx("00700.HK") == "hk00700"
    assert _ts_code_to_tx("01810.HK") == "hk01810"
    assert _ts_code_to_tx("5.HK") == "hk00005"
    assert _ts_code_to_tx("") is None
    assert _ts_code_to_tx("600519.SH") is None
    assert _ts_code_to_tx("ABC.HK") is None


def test_default_backfill_cutoff() -> None:
    from datetime import UTC, datetime, timedelta

    from data_sync_service.service.hk_daily_tx import _default_backfill_cutoff

    cutoff = _default_backfill_cutoff(5)
    expected = datetime.now(UTC).date() - timedelta(days=365 * 5)
    assert cutoff == expected


def test_rows_to_daily_rows_converts_and_derives_preclose(monkeypatch) -> None:
    from data_sync_service.service.hk_daily_tx import _rows_to_daily_rows

    rows = [
        # [date, open, close, high, low, volume, extra, pct_chg, amount(万)]
        ["2026-07-28", "447.800", "447.200", "452.000", "441.600", "18143997", {}, "0.200", "810338.698"],
        ["2026-07-29", "453.000", "466.400", "469.400", "450.000", "36203193", {}, "0.400", "1675606.503"],
    ]
    out = _rows_to_daily_rows("00700.HK", rows, since=None)
    assert len(out) == 2
    r0, r1 = out
    assert r0["ts_code"] == "00700.HK"
    assert r0["trade_date"] == "2026-07-28"
    assert r0["open"] == 447.8
    assert r0["close"] == 447.2
    assert r0["high"] == 452.0
    assert r0["low"] == 441.6
    assert r0["vol"] == 18143997.0
    assert r0["amount"] == pytest.approx(810338.698 * 10000)
    assert r0["pre_close"] is None
    assert r0["change"] is None
    assert r0["pct_chg"] is None
    assert r1["pre_close"] == 447.2
    assert r1["change"] == pytest.approx(19.2)
    assert r1["pct_chg"] == pytest.approx(19.2 / 447.2 * 100.0)


def test_rows_to_daily_rows_incremental_filters_since() -> None:
    from data_sync_service.service.hk_daily_tx import _rows_to_daily_rows

    rows = [
        ["2026-07-27", "440.0", "441.0", "442.0", "439.0", "1000", {}, "0.1", "1.0"],
        ["2026-07-28", "442.0", "443.0", "444.0", "441.0", "2000", {}, "0.2", "2.0"],
        ["2026-07-29", "443.0", "444.0", "445.0", "442.0", "3000", {}, "0.3", "3.0"],
    ]
    out = _rows_to_daily_rows("00700.HK", rows, since=date(2026, 7, 27))
    assert [r["trade_date"] for r in out] == ["2026-07-28", "2026-07-29"]
    # pre_close of the first kept row derives from the filtered-out row.
    assert out[0]["pre_close"] == 441.0


def test_rows_to_daily_rows_skips_malformed() -> None:
    from data_sync_service.service.hk_daily_tx import _rows_to_daily_rows

    rows: list[list[Any]] = [
        ["not-a-date", "1", "2", "3", "4", "5", {}, "0", "1"],
        ["2026-07-28", "bad", "444.0", "445.0", "442.0", "3000", {}, "0.3", "3.0"],
        ["2026-07-29", "443.0", "444.0", "445.0", "442.0", "3000", {}, "0.3", "3.0"],
    ]
    out = _rows_to_daily_rows("00700.HK", rows, since=None)
    assert len(out) == 1
    assert out[0]["trade_date"] == "2026-07-29"


def test_sync_hk_daily_for_ts_code_tx_validates_ts_code() -> None:
    from data_sync_service.service.hk_daily_tx import sync_hk_daily_for_ts_code_tx

    r = sync_hk_daily_for_ts_code_tx("600519.SH")
    assert r["ok"] is False
    assert "must end with .HK" in r["error"]


def test_sync_hk_daily_for_ts_code_tx_writes_incremental(monkeypatch) -> None:
    """End-to-end: fake fetch returns bars, upsert writes them."""
    from data_sync_service.service import hk_daily_tx

    fetched: list[list[Any]] = [
        ["2026-07-30", "470.0", "472.0", "474.0", "469.0", "10000", {}, "0.5", "100.0"],
        ["2026-07-31", "472.0", "475.0", "476.0", "471.0", "20000", {}, "0.6", "200.0"],
    ]
    monkeypatch.setattr(hk_daily_tx, "_fetch_kline_since", lambda _sym, _since, _end: fetched)
    monkeypatch.setattr(hk_daily_tx, "get_last_trade_date", lambda _tc: date(2026, 7, 29))
    monkeypatch.setattr(hk_daily_tx, "upsert_from_dataframe", lambda df: int(len(df)))

    r = hk_daily_tx.sync_hk_daily_for_ts_code_tx("00700.HK")
    assert r["ok"] is True
    assert r["updated"] == 2
    assert r["source"] == "tencent"
    assert r["latest_trade_date"] == "2026-07-31"


def test_sync_hk_daily_for_ts_code_tx_skips_when_up_to_date(monkeypatch) -> None:
    from data_sync_service.service import hk_daily_tx

    monkeypatch.setattr(hk_daily_tx, "get_last_trade_date", lambda _tc: date(2030, 1, 1))
    r = hk_daily_tx.sync_hk_daily_for_ts_code_tx("00700.HK")
    assert r["ok"] is True
    assert r["skipped"] is True


def test_sync_hk_daily_for_ts_code_tx_no_rows_returns_skipped(monkeypatch) -> None:
    from data_sync_service.service import hk_daily_tx

    monkeypatch.setattr(hk_daily_tx, "get_last_trade_date", lambda _tc: None)
    monkeypatch.setattr(hk_daily_tx, "_fetch_kline_since", lambda _sym, _since, _end: [])
    r = hk_daily_tx.sync_hk_daily_for_ts_code_tx("00700.HK")
    assert r["ok"] is True
    assert r["skipped"] is True
    assert r["source"] == "tencent"


def test_fetch_kline_since_pages_backwards(monkeypatch) -> None:
    """When a page hits the 1000-row cap, walk an earlier window."""
    from data_sync_service.service import hk_daily_tx

    calls: list[tuple[str, Any]] = []

    def fake_page(symbol, start, end, count=1000):
        calls.append((start.isoformat(), end.isoformat(), count))
        if end == date(2026, 8, 3):
            # Simulate a capped page: exactly 1000 rows ending on end.
            return [
                [f"{d}", "1", "2", "3", "4", "5", {}, "0", "1"]
                for d in _ascending_dates("2024-01-02", end, 1000)
            ][-1000:]
        # Earlier window: fewer than 1000 rows (fully covered).
        return [
            [f"{d}", "1", "2", "3", "4", "5", {}, "0", "1"]
            for d in _ascending_dates("2024-01-01", end, 3)
        ]

    monkeypatch.setattr(hk_daily_tx, "_fetch_kline_page", fake_page)
    rows = hk_daily_tx._fetch_kline_since("hk00700", date(2024, 1, 1), date(2026, 8, 3))
    # 1000 (page 1) + 3 (page 2), oldest-first overall.
    assert len(rows) == 1003
    assert len(calls) == 2
    assert calls[0][1] == "2026-08-03"
    assert calls[1][1] == "2024-01-01"


def _ascending_dates(start_iso: str, end: date, n: int) -> list[str]:
    """Return n ascending ISO dates ending at ``end`` (repeats if n > span)."""
    from datetime import datetime, timedelta

    end_d = datetime.fromisoformat(end.isoformat()).date()
    start_d = datetime.fromisoformat(start_iso).date()
    total = (end_d - start_d).days + 1
    out: list[str] = []
    if total >= n:
        step = total / n
        for i in range(n):
            d = start_d + timedelta(days=int(i * step))
            out.append(d.isoformat())
    else:
        full = [d.isoformat() for d in _every_day(start_d, end_d)]
        out = full + [full[-1]] * (n - len(full))
    return out


def _every_day(start_d: date, end_d: date) -> list[date]:
    from datetime import timedelta

    out: list[date] = []
    d = start_d
    while d <= end_d:
        out.append(d)
        d += timedelta(days=1)
    return out
