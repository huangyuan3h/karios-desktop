import pytest
from fastapi.testclient import TestClient  # type: ignore[import-not-found]

from data_sync_service.main import app  # type: ignore[import-not-found]

pytestmark = pytest.mark.requires_postgres

def test_sync_hk_daily_no_stock_list(monkeypatch) -> None:
    import data_sync_service.service.hk_daily as hk_daily  # type: ignore[import-not-found]

    monkeypatch.setattr(hk_daily, "fetch_ts_codes_by_market", lambda _market: [])
    result = hk_daily.sync_hk_daily_full()
    assert result["ok"] is True
    assert result["updated"] == 0


def test_sync_hk_daily_endpoint_shape(monkeypatch) -> None:
    import data_sync_service.api.sync_routes as sync_routes  # type: ignore[import-not-found]

    monkeypatch.setattr(
        sync_routes,
        "sync_hk_daily_full",
        lambda: {"ok": True, "updated": 1},
    )

    client = TestClient(app)
    resp = client.post("/sync/hk-daily")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["ok"] is True
    assert payload["updated"] == 1


def test_sync_hk_daily_resume_from_last_ts_code(monkeypatch) -> None:
    """When today's run failed at ts_code X, the next run starts at X+1."""
    import data_sync_service.service.hk_daily as hk_daily

    ts_codes = ["00700.HK", "01810.HK", "09988.HK"]
    monkeypatch.setattr(hk_daily, "fetch_ts_codes_by_market", lambda _market: list(ts_codes))
    monkeypatch.setattr(
        hk_daily,
        "get_today_run",
        lambda _job: {"success": False, "last_ts_code": "00700.HK"},
    )

    yf_calls: list[str] = []
    ts_calls: list[str] = []

    def fake_yf(ts_code):
        yf_calls.append(ts_code)
        # YF returns 1 row -> no tushare fallback
        return {"ok": True, "updated": 1, "ts_code": ts_code}

    import sys
    stub = type("S", (), {"sync_hk_daily_for_ts_code_yf": staticmethod(fake_yf)})
    monkeypatch.setitem(sys.modules, "data_sync_service.service.hk_daily_yf", stub)

    monkeypatch.setattr(
        hk_daily,
        "_tushare_sync_one",
        lambda tc: ts_calls.append(tc) or {"ok": True, "updated": 0, "ts_code": tc},
    )
    monkeypatch.setattr(hk_daily.time, "sleep", lambda _s: None)
    monkeypatch.setattr(hk_daily, "insert_record", lambda **_kw: None)

    result = hk_daily.sync_hk_daily_full()
    assert result["ok"] is True
    # Resume from last_ts_code+1 => should start at 01810.HK, not 00700.HK
    assert yf_calls == ["01810.HK", "09988.HK"]
    # Tushare fallback should NOT be invoked when yfinance already returned rows
    assert ts_calls == []


def test_sync_hk_daily_skips_when_today_already_succeeded(monkeypatch) -> None:
    """If today's run already succeeded, full sync should not process any ticker."""
    import data_sync_service.service.hk_daily as hk_daily

    monkeypatch.setattr(
        hk_daily,
        "get_today_run",
        lambda _job: {"success": True, "last_ts_code": None},
    )
    monkeypatch.setattr(
        hk_daily, "fetch_ts_codes_by_market", lambda _m: ["00700.HK", "01810.HK"]
    )

    result = hk_daily.sync_hk_daily_full()
    assert result["ok"] is True
    assert result["skipped"] is True


def test_sync_hk_daily_continues_after_single_ticker_failure(monkeypatch) -> None:
    """A bad ticker should not abort the whole batch — count it as failed and continue."""
    import data_sync_service.service.hk_daily as hk_daily

    monkeypatch.setattr(hk_daily, "fetch_ts_codes_by_market", lambda _m: ["00700.HK", "BAD.HK", "01810.HK"])
    monkeypatch.setattr(hk_daily, "get_today_run", lambda _job: None)
    monkeypatch.setattr(hk_daily.time, "sleep", lambda _s: None)

    def fake_yf(tc):
        if tc == "BAD.HK":
            raise RuntimeError("yfinance network error")
        return {"ok": True, "updated": 0, "ts_code": tc}

    import sys
    stub = type("S", (), {"sync_hk_daily_for_ts_code_yf": staticmethod(fake_yf)})
    monkeypatch.setitem(sys.modules, "data_sync_service.service.hk_daily_yf", stub)

    monkeypatch.setattr(hk_daily, "_tushare_sync_one", lambda tc: {"ok": True, "updated": 0})
    monkeypatch.setattr(hk_daily, "insert_record", lambda **_kw: None)

    result = hk_daily.sync_hk_daily_full()
    assert result["ok"] is True
    assert result["failed_count"] == 1


def test_sync_hk_daily_status_endpoint(monkeypatch) -> None:
    import data_sync_service.api.sync_routes as sync_routes

    monkeypatch.setattr(
        sync_routes,
        "get_hk_daily_sync_status",
        lambda: {"job_type": "hk_daily_full", "today_run": {"success": True, "last_ts_code": None}},
    )

    client = TestClient(app)
    resp = client.get("/sync/hk-daily/status")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["job_type"] == "hk_daily_full"
    assert payload["today_run"]["success"] is True


def test_hk_daily_job_cron_is_daily() -> None:
    """Regression guard: hk_daily_job must run DAILY (not monthly) so newly added
    watchlist HK tickers get fresh bars without waiting for the 1st of next month."""
    from data_sync_service.scheduler.hk_daily_job import (
        CRON_EXPRESSION,
        TIMEZONE,
        build_trigger,
    )

    assert "1 * *" not in CRON_EXPRESSION, (
        f"hk_daily cron regressed to monthly: {CRON_EXPRESSION!r}"
    )
    assert TIMEZONE == "Asia/Shanghai"

    trigger = build_trigger()
    fields_by_name = dict(zip(trigger.FIELD_NAMES, trigger.fields, strict=False))
    # day and day_of_week must be wildcard so it fires every day
    assert str(fields_by_name["day"]) == "*"
    assert str(fields_by_name["day_of_week"]) in ("*", "?")
    # Scheduled at 17:30 Asia/Shanghai
    assert str(fields_by_name["hour"]) == "17"
    assert str(fields_by_name["minute"]) == "30"

