"""Tests for close_sync non-trading-day catch-up logic."""

from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pytest

from data_sync_service.service import close_sync as cs


def _patch_cn_today(monkeypatch: pytest.MonkeyPatch, d: date) -> None:
    monkeypatch.setattr(cs, "_cn_today", lambda: d)
    monkeypatch.setattr(
        cs,
        "_cn_now",
        lambda: datetime(d.year, d.month, d.day, 18, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
    )


def _patch_extra_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    """close_sync runs cn_extra_sync best-effort after daily bars succeed —
    stub it so orchestration tests never touch tushare/DB fallbacks."""
    from data_sync_service.service import cn_extra_sync as extra

    monkeypatch.setattr(extra, "sync_margin_detail_for_dates", lambda dates: {"ok": True})
    monkeypatch.setattr(extra, "sync_moneyflow_for_dates", lambda dates: {"ok": True})
    monkeypatch.setattr(extra, "sync_hk_hold_for_dates", lambda dates: {"ok": True})
    monkeypatch.setattr(extra, "sync_financial_for_range", lambda s, e: {"ok": True})
    monkeypatch.setattr(extra, "sync_holder_for_range", lambda s, e: {"ok": True})


def test_non_trading_day_skips_when_data_current(monkeypatch: pytest.MonkeyPatch) -> None:
    saturday = date(2026, 7, 4)
    friday = date(2026, 7, 3)
    _patch_cn_today(monkeypatch, saturday)

    monkeypatch.setattr(cs, "get_today_run", lambda _jt: None)
    monkeypatch.setattr(cs, "is_trading_day", lambda exchange, d: d == friday)
    monkeypatch.setattr(cs, "last_open_date_on_or_before", lambda d, exchange="SSE": friday)
    monkeypatch.setattr(
        cs,
        "get_last_success",
        lambda _jt: {"last_ts_code": "20260703", "sync_at": "2026-07-03T10:00:00+08:00"},
    )
    monkeypatch.setattr(cs, "count_rows_for_trade_date", lambda _d: 5000)

    out = cs.sync_close(force=False)
    assert out["ok"] is True
    assert out.get("skipped") is True
    assert "not a trading day" in out.get("message", "")


def test_non_trading_day_catchup_when_stale(monkeypatch: pytest.MonkeyPatch) -> None:
    saturday = date(2026, 7, 4)
    friday = date(2026, 7, 3)
    thursday = date(2026, 7, 2)
    _patch_cn_today(monkeypatch, saturday)

    trade_dates = [thursday, friday]
    synced: list[str] = []

    monkeypatch.setattr(cs, "get_today_run", lambda _jt: None)
    monkeypatch.setattr(cs, "is_trading_day", lambda exchange, d: d in trade_dates)
    monkeypatch.setattr(cs, "last_open_date_on_or_before", lambda d, exchange="SSE": friday)
    monkeypatch.setattr(
        cs,
        "get_last_success",
        lambda _jt: {"last_ts_code": "20260701", "sync_at": "2026-07-01T10:00:00+08:00"},
    )
    monkeypatch.setattr(cs, "count_rows_for_trade_date", lambda _d: 5000)
    monkeypatch.setattr(cs, "get_open_dates", lambda **_: trade_dates)
    monkeypatch.setattr(cs, "get_settings", lambda: type("S", (), {"tu_share_api_key": "k"})())
    monkeypatch.setattr(cs.ts, "pro_api", lambda _k: object())
    monkeypatch.setattr(cs, "_fetch_paged_daily", lambda pro, td: synced.append(td) or 100)
    monkeypatch.setattr(cs, "_fetch_paged_adj_factor", lambda pro, td: 50)
    monkeypatch.setattr(cs, "insert_record", lambda *a, **k: None)
    _patch_extra_ok(monkeypatch)

    out = cs.sync_close(force=True)
    assert out["ok"] is True
    assert out.get("skipped") is not True
    assert synced == ["20260702", "20260703"]
    assert "non-trading day catchup" in out.get("message", "")


def test_non_trading_day_force_heals_missing_db_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    saturday = date(2026, 7, 4)
    friday = date(2026, 7, 3)
    _patch_cn_today(monkeypatch, saturday)

    synced: list[str] = []

    monkeypatch.setattr(cs, "get_today_run", lambda _jt: None)
    monkeypatch.setattr(cs, "is_trading_day", lambda exchange, d: d == friday)
    monkeypatch.setattr(cs, "last_open_date_on_or_before", lambda d, exchange="SSE": friday)
    monkeypatch.setattr(
        cs,
        "get_last_success",
        lambda _jt: {"last_ts_code": "20260703", "sync_at": "2026-07-03T10:00:00+08:00"},
    )
    monkeypatch.setattr(cs, "count_rows_for_trade_date", lambda _d: 0)
    monkeypatch.setattr(cs, "get_open_dates", lambda **_: [friday])
    monkeypatch.setattr(cs, "get_settings", lambda: type("S", (), {"tu_share_api_key": "k"})())
    monkeypatch.setattr(cs.ts, "pro_api", lambda _k: object())
    monkeypatch.setattr(cs, "_fetch_paged_daily", lambda pro, td: synced.append(td) or 100)
    monkeypatch.setattr(cs, "_fetch_paged_adj_factor", lambda pro, td: 50)
    monkeypatch.setattr(cs, "insert_record", lambda *a, **k: None)
    _patch_extra_ok(monkeypatch)

    out = cs.sync_close(force=True)
    assert out["ok"] is True
    assert synced == ["20260703"]
