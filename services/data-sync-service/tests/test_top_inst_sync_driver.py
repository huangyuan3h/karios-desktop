"""sync_top_inst_watchlist driver tests (mocked provider)."""

from __future__ import annotations

from data_sync_service.service import top_inst_flow as tif
from data_sync_service.service.top_inst_flow import TopInstProviderResult


def _result(**kw) -> TopInstProviderResult:
    defaults = dict(
        source="tushare",
        lhb_tickers=set(),
        org_by_ticker={},
        buy_seats_by_ts_code={},
        inst_seats_by_ts_code={},
    )
    defaults.update(kw)
    return TopInstProviderResult(**defaults)


def test_sync_skips_non_trading_day(monkeypatch) -> None:
    monkeypatch.setattr(tif, "ensure_table", lambda: None)
    monkeypatch.setattr(tif, "_latest_cn_trade_date_yyyymmdd", lambda: "20260808")
    monkeypatch.setattr(tif, "is_trading_day", lambda exchange, cal_date: False)
    out = tif.sync_top_inst_watchlist()
    assert out["skipped"] is True
    assert out["reason"] == "not_trading_day"


def test_sync_skips_empty_watchlist(monkeypatch) -> None:
    monkeypatch.setattr(tif, "ensure_table", lambda: None)
    monkeypatch.setattr(tif, "_latest_cn_trade_date_yyyymmdd", lambda: "20260807")
    monkeypatch.setattr(tif, "is_trading_day", lambda exchange, cal_date: True)
    monkeypatch.setattr(tif, "_watchlist_ts_codes", lambda: [])
    monkeypatch.setattr(tif, "insert_record", lambda **kw: None)
    out = tif.sync_top_inst_watchlist()
    assert out["skipped"] is True
    assert out["reason"] == "empty_watchlist"


def test_sync_already_synced_today(monkeypatch) -> None:
    monkeypatch.setattr(tif, "ensure_table", lambda: None)
    monkeypatch.setattr(tif, "_latest_cn_trade_date_yyyymmdd", lambda: "20260807")
    monkeypatch.setattr(tif, "is_trading_day", lambda exchange, cal_date: True)
    monkeypatch.setattr(tif, "_watchlist_ts_codes", lambda: ["600000.SH", "000001.SZ"])
    monkeypatch.setattr(tif, "get_today_run", lambda job: {"success": True})
    monkeypatch.setattr(tif, "_missing_summary_codes", lambda codes, trade_date_iso: [])
    out = tif.sync_top_inst_watchlist()
    assert out["skipped"] is True
    assert out["reason"] == "already_synced_today"
    assert out["covered"] == 2


def test_sync_provider_error_records_failure(monkeypatch) -> None:
    monkeypatch.setattr(tif, "ensure_table", lambda: None)
    monkeypatch.setattr(tif, "_latest_cn_trade_date_yyyymmdd", lambda: "20260807")
    monkeypatch.setattr(tif, "is_trading_day", lambda exchange, cal_date: True)
    monkeypatch.setattr(tif, "_watchlist_ts_codes", lambda: ["600000.SH"])
    monkeypatch.setattr(tif, "get_today_run", lambda job: None)
    monkeypatch.setattr(
        tif,
        "fetch_top_inst_provider_result",
        lambda td: (_ for _ in ()).throw(RuntimeError("provider down")),
    )
    monkeypatch.setattr(tif, "insert_record", lambda **kw: None)
    out = tif.sync_top_inst_watchlist()
    assert out["ok"] is False
    assert "provider down" in out["error"]


def test_sync_empty_lhb_suspicious(monkeypatch) -> None:
    monkeypatch.setattr(tif, "ensure_table", lambda: None)
    monkeypatch.setattr(tif, "_latest_cn_trade_date_yyyymmdd", lambda: "20260807")
    monkeypatch.setattr(tif, "is_trading_day", lambda exchange, cal_date: True)
    monkeypatch.setattr(tif, "_watchlist_ts_codes", lambda: ["600000.SH"])
    monkeypatch.setattr(tif, "get_today_run", lambda job: None)
    monkeypatch.setattr(
        tif, "fetch_top_inst_provider_result", lambda td: (_result(), [])
    )
    monkeypatch.setattr(tif, "insert_record", lambda **kw: None)
    out = tif.sync_top_inst_watchlist()
    assert out["ok"] is False
    assert out["reason"] == "suspicious_empty_lhb"


def test_sync_full_flow_with_upserts(monkeypatch) -> None:
    monkeypatch.setattr(tif, "ensure_table", lambda: None)
    monkeypatch.setattr(tif, "_latest_cn_trade_date_yyyymmdd", lambda: "20260807")
    monkeypatch.setattr(tif, "is_trading_day", lambda exchange, cal_date: True)
    monkeypatch.setattr(tif, "_watchlist_ts_codes", lambda: ["600000.SH", "000001.SZ"])
    monkeypatch.setattr(tif, "get_today_run", lambda job: None)
    monkeypatch.setattr(tif, "_missing_summary_codes", lambda codes, trade_date_iso: codes)

    provider = _result(
        lhb_tickers={"600000", "000001"},
        org_by_ticker={
            "600000": {"NET_BUY_AMT": 50000000.0, "EXPLANATION": "机构买入"},
            "000001": {},
        },
        inst_seats_by_ts_code={
            "600000.SH": [{"exalter": "机构专用", "buy": 100.0, "sell": 20.0, "net_buy": 80.0, "side": "买"}],
        },
    )
    monkeypatch.setattr(tif, "fetch_top_inst_provider_result", lambda td: (provider, []))
    monkeypatch.setattr(tif, "fetch_em_seat_bundles_parallel", lambda codes, trade_date_iso: {})
    monkeypatch.setattr(tif, "upsert_daily_rows", lambda rows: len(rows))
    monkeypatch.setattr(tif, "upsert_summary_rows", lambda rows: len(rows))
    monkeypatch.setattr(tif, "insert_record", lambda **kw: None)

    out = tif.sync_top_inst_watchlist()
    assert out["ok"] is True
    assert out["onBoardCount"] == 2
    assert out["dailyRows"] == 1
    assert out["summaryRows"] == 2
