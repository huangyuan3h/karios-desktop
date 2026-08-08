"""sync_routes API coverage."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from data_sync_service.main import app

client = TestClient(app)

import data_sync_service.api.sync_routes as sr


@pytest.fixture(autouse=True)
def _patch_deps(monkeypatch):
    monkeypatch.setattr(sr, "sync_stock_basic", lambda: {"ok": True, "updated": 10})
    monkeypatch.setattr(sr, "sync_hk_basic", lambda **kw: {"ok": True})
    monkeypatch.setattr(sr, "sync_etf_fund_basic", lambda **kw: {"ok": True})
    monkeypatch.setattr(sr, "get_etf_fund_basic_sync_status", lambda: {"job_type": "etf_fund_basic_sync"})
    monkeypatch.setattr(sr, "sync_etf_daily_full", lambda: {"ok": True, "updated": 3})
    monkeypatch.setattr(sr, "get_etf_daily_sync_status", lambda: {"job_type": "etf_daily_full"})
    monkeypatch.setattr(sr, "sync_close", lambda **kw: {"ok": True, "updated_daily_rows": 5})
    monkeypatch.setattr(sr, "sync_hk_daily_full", lambda: {"ok": True})
    monkeypatch.setattr(sr, "get_hk_daily_sync_status", lambda: {"job_type": "hk_daily_full"})
    monkeypatch.setattr(sr, "sync_adj_factor_full", lambda: {"ok": True})
    monkeypatch.setattr(sr, "sync_index_daily_full", lambda: {"ok": True})
    monkeypatch.setattr(sr, "sync_index_basic_full", lambda: {"ok": True})
    monkeypatch.setattr(sr, "sync_macro_daily_full", lambda: {"ok": True})
    monkeypatch.setattr(sr, "sync_trade_calendar", lambda **kw: {"ok": True})
    monkeypatch.setattr(sr, "sync_etf_fund_flow_watchlist", lambda force=False: {"ok": True})
    monkeypatch.setattr(sr, "sync_option_iv_daily", lambda force=False, trade_date=None: {"ok": True})
    monkeypatch.setattr(sr, "sync_top_inst_watchlist", lambda force=False, trade_date=None: {"ok": True})
    monkeypatch.setattr(sr, "run_post_close_sync", lambda: {"postClose": True})
    from data_sync_service.db import sync_job_record as sjr

    monkeypatch.setattr(sjr, "ensure_table", lambda: None)
    monkeypatch.setattr(sjr, "get_connection", lambda: _Conn())
    yield


class _Cur:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def execute(self, sql, params=None):
        return self


class _Conn:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def cursor(self):
        return _Cur()

    def commit(self) -> None:
        pass


def test_eastmoney_industry_status(monkeypatch) -> None:
    from data_sync_service.service import eastmoney_industry as ei

    monkeypatch.setattr(ei, "get_eastmoney_industry_sync_status", lambda: {"ok": True})
    r = client.get("/sync/eastmoney-industry/status")
    assert r.status_code == 200 and r.json()["ok"] is True


def test_sync_eastmoney_industry_symbols(monkeypatch) -> None:
    from data_sync_service.service import eastmoney_industry as ei

    monkeypatch.setattr(ei, "sync_eastmoney_industry", lambda symbols=None, limit=500: {"ok": True, "updated": 2})
    r = client.post("/sync/eastmoney-industry")
    assert r.status_code == 200 and r.json()["updated"] == 2


def test_sync_eastmoney_industry_missing(monkeypatch) -> None:
    from data_sync_service.service import eastmoney_industry as ei

    monkeypatch.setattr(ei, "sync_eastmoney_industry_incremental", lambda **kw: {"ok": True, "mode": kw["mode"]})
    r = client.post("/sync/eastmoney-industry", params={"mode": "missing", "limit": 100})
    assert r.status_code == 200 and r.json()["mode"] == "missing"


def test_sync_stock_basic() -> None:
    r = client.post("/sync/stock-basic")
    assert r.json()["ok"] is True and r.json()["updated"] == 10


def test_sync_hk_basic() -> None:
    r = client.post("/sync/hk-basic", params={"ts_code": "00005.HK", "list_status": "D", "force": "true"})
    assert r.status_code == 200


def test_sync_etf_fund_basic() -> None:
    r = client.post("/sync/etf-fund-basic", params={"force": "true"})
    assert r.status_code == 200
    client.get("/sync/etf-fund-basic/status")


def test_sync_etf_daily() -> None:
    r = client.post("/sync/etf-daily")
    assert r.json()["updated"] == 3
    client.get("/sync/etf-daily/status")


def test_market_sync_ok() -> None:
    r = client.post("/market/sync")
    body = r.json()
    assert body["ok"] is True and body["stocks"] == 10
    assert "syncedAt" in body


def test_market_sync_error(monkeypatch) -> None:
    monkeypatch.setattr(sr, "sync_stock_basic", lambda: {"ok": False, "error": "TU_SHARE_API_KEY is not set"})
    r = client.post("/market/sync")
    body = r.json()
    assert body["ok"] is False and "API_KEY" in body["error"]


def test_sync_daily_deprecated(monkeypatch) -> None:
    monkeypatch.setattr(sr, "sync_close", lambda **kw: {"ok": False, "error": "too early"})
    r = client.post("/sync/daily")
    assert r.json()["deprecated"] == "use /sync/close"


def test_sync_hk_daily() -> None:
    client.post("/sync/hk-daily")
    client.get("/sync/hk-daily/status")


def test_sync_hk_industry(monkeypatch) -> None:
    from data_sync_service.service import hk_industry as hi

    monkeypatch.setattr(hi, "sync_hk_industry", lambda symbols=None, limit=500: {"ok": True, "updated": 1})
    r = client.post("/sync/hk-industry", params={"symbols": ["00700.HK"]})
    assert r.status_code == 200 and r.json()["updated"] == 1
    monkeypatch.setattr(hi, "get_hk_industry_status", lambda: {"mapped": 100})
    assert client.get("/sync/hk-industry/status").json()["mapped"] == 100


def test_sync_adj_factor() -> None:
    assert client.post("/sync/adj-factor").status_code == 200


def test_sync_index_daily_force(monkeypatch) -> None:
    assert client.post("/sync/index-daily", params={"force": "true"}).status_code == 200
    client.post("/sync/index-daily")


def test_sync_index_basic() -> None:
    assert client.post("/sync/index-basic").status_code == 200


def test_sync_macro_daily_force() -> None:
    assert client.post("/sync/macro-daily", params={"force": "true"}).status_code == 200


def test_sync_trade_cal() -> None:
    r = client.post("/sync/trade-cal", params={"exchange": "SSE", "start_date": "20260801", "end_date": "20260810"})
    assert r.status_code == 200 and r.json()["ok"] is True


def test_sync_etf_fund_flow_watchlist() -> None:
    assert client.post("/sync/etf-fund-flow-watchlist", params={"force": "true"}).status_code == 200


def test_sync_option_iv_daily() -> None:
    r = client.post("/sync/option-iv-daily", params={"trade_date": "20260807"})
    assert r.status_code == 200 and r.json()["ok"] is True


def test_sync_top_inst_watchlist() -> None:
    assert client.post("/sync/top-inst-watchlist").status_code == 200


def test_sync_close_runs_post_sync() -> None:
    r = client.post("/sync/close")
    body = r.json()
    assert body["ok"] is True and body["postClose"] is True


def test_sync_close_failure_no_post(monkeypatch) -> None:
    monkeypatch.setattr(sr, "sync_close", lambda **kw: {"ok": False, "error": "x"})
    r = client.post("/sync/close")
    assert "postClose" not in r.json()


def test_sync_jobs_aggregate(monkeypatch) -> None:
    from data_sync_service.db import sync_job_record as sjr
    from data_sync_service.service import hk_industry as hi
    from data_sync_service.service import alpha_radar_pipeline as ap
    from data_sync_service.db import watchlist_automation as wa

    monkeypatch.setattr(sjr, "get_today_run", lambda jt: {"job_type": jt} if jt == "stock_basic_sync" else None)
    monkeypatch.setattr(sjr, "get_last_success", lambda jt: {"job_type": jt})
    monkeypatch.setattr(hi, "get_hk_industry_status", lambda: {"mapped": 1})
    monkeypatch.setattr(ap, "pipeline_status", lambda: {"phase": "idle"})
    monkeypatch.setattr(wa, "get_latest_run", lambda: {"runId": "r1"})
    r = client.get("/sync/jobs")
    body = r.json()
    assert body["ok"] is True
    assert body["jobs"]["stock_basic_sync"]["todayRun"] == {"job_type": "stock_basic_sync"}
    assert body["jobs"]["etf_daily_full"]["lastSuccess"]["job_type"] == "etf_daily_full"
    assert body["hkIndustryCoverage"] == {"mapped": 1}
    assert body["alphaRadar"] == {"phase": "idle"}
    assert body["watchlistAutomation"] == {"runId": "r1"}


def test_sync_jobs_aggregate_errors(monkeypatch) -> None:
    from data_sync_service.db import sync_job_record as sjr
    from data_sync_service.service import hk_industry as hi
    from data_sync_service.service import alpha_radar_pipeline as ap
    from data_sync_service.db import watchlist_automation as wa

    monkeypatch.setattr(sjr, "get_today_run", lambda jt: None)
    monkeypatch.setattr(sjr, "get_last_success", lambda jt: None)
    monkeypatch.setattr(hi, "get_hk_industry_status", lambda: (_ for _ in ()).throw(RuntimeError("hk down")))
    monkeypatch.setattr(ap, "pipeline_status", lambda: (_ for _ in ()).throw(RuntimeError("pipeline down")))
    monkeypatch.setattr(wa, "get_latest_run", lambda: (_ for _ in ()).throw(RuntimeError("wa down")))
    body = client.get("/sync/jobs").json()
    assert body["hkIndustryCoverage"] == {"ok": False, "error": "hk down"}
    assert body["alphaRadar"] == {"ok": False, "error": "pipeline down"}
    assert body["watchlistAutomation"] == {"ok": False, "error": "wa down"}
