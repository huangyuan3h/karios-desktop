"""etf_fund_flow: pure helpers + signal aggregation."""

from __future__ import annotations

from data_sync_service.service import etf_fund_flow as eff


def test_prev_open_iso() -> None:
    opens = ["2026-08-03", "2026-08-04", "2026-08-05"]
    assert eff._prev_open_iso(opens, "2026-08-05") == "2026-08-04"
    assert eff._prev_open_iso(opens, "2026-08-03") is None
    assert eff._prev_open_iso([], "2026-08-05") is None


def test_estimate_net_1d_from_em_none_cases() -> None:
    assert eff._estimate_net_1d_from_em(
        symbol="ETF:510300", as_of="2026-08-05", rows_by_date={}, open_iso=[], em_spot=None
    ) is None
    assert eff._estimate_net_1d_from_em(
        symbol="ETF:510300", as_of="2026-08-05", rows_by_date={}, open_iso=[],
        em_spot={"ETF:510300": {"dataDate": "2026-08-04", "mainNetInflow": 100.0}},
    ) is None  # stale dataDate


def test_estimate_net_1d_from_em_main_net() -> None:
    out = eff._estimate_net_1d_from_em(
        symbol="ETF:510300", as_of="2026-08-05", rows_by_date={}, open_iso=[],
        em_spot={"ETF:510300": {"dataDate": "2026-08-05", "mainNetInflow": "123.5"}},
    )
    assert out == 123.5


def test_estimate_net_1d_from_em_share_fallback(monkeypatch) -> None:
    monkeypatch.setattr(
        eff, "compute_net_inflow_1d", lambda **kw: 42.0
    )
    out = eff._estimate_net_1d_from_em(
        symbol="ETF:510300", as_of="2026-08-05",
        rows_by_date={"2026-08-04": {"fd_share": 100.0, "avg_price": 4.0}},
        open_iso=["2026-08-04"],
        em_spot={"ETF:510300": {"dataDate": "2026-08-05", "fdShareWan": 200.0}},
    )
    assert out == 42.0


def test_aggregate_etf_flow_signal_confirm() -> None:
    bundle = {
        "asOfDate": "2026-08-05",
        "intradaySafe": True,
        "shareLag": False,
        "items": [
            {"category": "broad", "signal": "National Team Buy"},
            {"category": "sector", "signal": "Sector Momentum"},
        ],
    }
    out = eff.aggregate_etf_flow_signal(bundle)
    assert out["verdict"] == "confirm"
    assert out["confirmCount"] == 2
    assert out["incomplete"] is False


def test_aggregate_etf_flow_signal_contradict_and_mixed() -> None:
    bundle = {
        "asOfDate": "2026-08-05",
        "items": [
            {"category": "broad", "signal": "National Team Outflow"},
            {"category": "sector", "signal": "Sector Momentum"},
        ],
    }
    out = eff.aggregate_etf_flow_signal(bundle)
    assert out["verdict"] == "neutral"  # positive + negative both present

    mixed_bundle = {
        "asOfDate": "2026-08-05",
        "items": [
            {"category": "broad", "signal": "National Team Buy"},
            {"category": "broad", "signal": "National Team Outflow"},
        ],
    }
    out2 = eff.aggregate_etf_flow_signal(mixed_bundle)
    assert out2["broadDirection"] == "mixed"
    assert out2["verdict"] == "neutral"


def test_aggregate_etf_flow_signal_incomplete_flags() -> None:
    bundle = {
        "asOfDate": "2026-08-05",
        "shareLag": True,
        "intradaySafe": False,
        "items": [],
    }
    out = eff.aggregate_etf_flow_signal(bundle)
    assert out["verdict"] == "neutral"
    assert out["incomplete"] is True
"""etf_fund_flow: watchlist sync driver + tushare history + helpers."""

import datetime

from data_sync_service.service import etf_fund_flow as eff


def test_sync_skip_today(monkeypatch) -> None:
    monkeypatch.setattr(eff, "_should_skip_etf_sync_today", lambda force: True)
    out = eff.sync_etf_fund_flow_watchlist()
    assert out["skipped"] is True
    monkeypatch.setattr(eff, "_should_skip_etf_sync_today", lambda force: False)
    monkeypatch.setattr(eff, "ensure_table", lambda: None)
    monkeypatch.setattr(eff, "_shanghai_today_yyyymmdd", lambda: "20260807")
    monkeypatch.setattr(eff, "_yyyymmdd_to_iso", lambda d: "2026-08-07")
    monkeypatch.setattr(eff, "_now_iso", lambda: "2026-08-07T10:00:00+08:00")
    monkeypatch.setattr(eff, "fetch_em_etf_realtime_flow_for_symbols", lambda symbols: {})
    monkeypatch.setattr(eff, "get_last_em_etf_fetch_error", lambda: "em down")
    monkeypatch.setattr(eff, "_sync_tushare_history_if_available", lambda **kw: 0)
    monkeypatch.setattr(eff, "upsert_daily_rows", lambda rows: 0)
    monkeypatch.setattr(eff, "insert_record", lambda **kw: None)
    out2 = eff.sync_etf_fund_flow_watchlist()
    assert out2["ok"] is False
    assert out2["error"] == "em down"
    assert out2["missingSymbols"] == [w["symbol"] for w in eff.ETF_WATCHLIST]


def test_sync_full_success(monkeypatch) -> None:
    symbols = [w["symbol"] for w in eff.ETF_WATCHLIST]
    flow = {}
    for s in symbols:
        flow[s] = {
            "tsCode": s, "fdShareWan": 100.0, "mainNetInflow": 500.0,
            "dataDate": "2026-08-07", "pctChg": 1.0,
        }
    monkeypatch.setattr(eff, "_should_skip_etf_sync_today", lambda force: False)
    monkeypatch.setattr(eff, "ensure_table", lambda: None)
    monkeypatch.setattr(eff, "_shanghai_today_yyyymmdd", lambda: "20260807")
    monkeypatch.setattr(eff, "_yyyymmdd_to_iso", lambda d: "2026-08-07")
    monkeypatch.setattr(eff, "_now_iso", lambda: "2026-08-07T10:00:00+08:00")
    monkeypatch.setattr(eff, "fetch_em_etf_realtime_flow_for_symbols", lambda symbols: flow)
    monkeypatch.setattr(eff, "get_last_em_etf_fetch_error", lambda: None)
    monkeypatch.setattr(eff, "_em_flow_trade_date", lambda flow, fallback_iso: "2026-08-07")
    monkeypatch.setattr(eff, "_is_current_realtime_trade_date", lambda td, fallback_iso: True)
    monkeypatch.setattr(eff, "_em_flow_to_daily_row", lambda **kw: {"ts_code": kw["ts_code"], "net_inflow": 500.0})
    monkeypatch.setattr(eff, "_sync_tushare_history_if_available", lambda **kw: 3)
    monkeypatch.setattr(eff, "upsert_daily_rows", lambda rows: len(rows))
    monkeypatch.setattr(eff, "insert_record", lambda **kw: None)
    out = eff.sync_etf_fund_flow_watchlist()
    assert out["ok"] is True
    assert out["updated"] == len(symbols)
    assert out["historyUpdated"] == 3 * len(symbols)  # per-symbol history sync
    assert out["missingSymbols"] == []


def test_sync_stale_symbol(monkeypatch) -> None:
    symbols = [w["symbol"] for w in eff.ETF_WATCHLIST]
    flow = {s: {"dataDate": "2026-08-06"} for s in symbols}
    monkeypatch.setattr(eff, "_should_skip_etf_sync_today", lambda force: False)
    monkeypatch.setattr(eff, "ensure_table", lambda: None)
    monkeypatch.setattr(eff, "_shanghai_today_yyyymmdd", lambda: "20260807")
    monkeypatch.setattr(eff, "_yyyymmdd_to_iso", lambda d: "2026-08-07")
    monkeypatch.setattr(eff, "_now_iso", lambda: "x")
    monkeypatch.setattr(eff, "fetch_em_etf_realtime_flow_for_symbols", lambda symbols: flow)
    monkeypatch.setattr(eff, "get_last_em_etf_fetch_error", lambda: None)
    monkeypatch.setattr(eff, "_em_flow_trade_date", lambda flow, fallback_iso: "2026-08-06")
    monkeypatch.setattr(eff, "_is_current_realtime_trade_date", lambda td, fallback_iso: False)
    monkeypatch.setattr(eff, "_sync_tushare_history_if_available", lambda **kw: 0)
    monkeypatch.setattr(eff, "upsert_daily_rows", lambda rows: 0)
    monkeypatch.setattr(eff, "insert_record", lambda **kw: None)
    out = eff.sync_etf_fund_flow_watchlist()
    assert out["ok"] is False
    assert out["staleSymbols"] == symbols


def test_sync_tushare_history_no_key(monkeypatch) -> None:
    monkeypatch.setattr(eff, "get_settings", lambda: type("S", (), {"tu_share_api_key": ""})())
    assert eff._sync_tushare_history_if_available(ts_code="510300.SH", end_date="20260807", updated_at="x") == 0


def test_sync_tushare_history_full_flow(monkeypatch) -> None:
    pro = type("Pro", (), {})()
    monkeypatch.setattr(eff, "get_settings", lambda: type("S", (), {"tu_share_api_key": "key"})())
    monkeypatch.setattr(eff, "ts", type("TS", (), {"pro_api": staticmethod(lambda k: pro)})())
    monkeypatch.setattr(eff, "get_last_trade_date", lambda code: None)
    monkeypatch.setattr(eff, "fetch_rows_for_codes", lambda codes: [1, 2])
    monkeypatch.setattr(eff, "_date_to_yyyymmdd", lambda d: "20260101")
    monkeypatch.setattr(eff, "_with_retry", lambda fn: type("DF", (), {"empty": lambda self: False})())
    monkeypatch.setattr(eff, "_merge_tushare_frames", lambda code, **kw: [{"r": 1}])
    monkeypatch.setattr(eff, "upsert_daily_rows", lambda merged: 2)
    monkeypatch.setattr(eff, "_recompute_net_inflows_for_code", lambda code, updated_at=None: 1)
    out = eff._sync_tushare_history_if_available(ts_code="510300.SH", end_date="20260807", updated_at="x")
    assert out == 3


def test_sync_tushare_history_dates_exhausted(monkeypatch) -> None:
    last = datetime.date(2026, 8, 6)
    monkeypatch.setattr(eff, "get_settings", lambda: type("S", (), {"tu_share_api_key": "key"})())
    monkeypatch.setattr(eff, "ts", type("TS", (), {"pro_api": staticmethod(lambda k: object())})())
    monkeypatch.setattr(eff, "get_last_trade_date", lambda code: last)
    monkeypatch.setattr(eff, "fetch_rows_for_codes", lambda codes: [1, 2, 3, 4, 5])
    monkeypatch.setattr(eff, "_date_to_yyyymmdd", lambda d: "20260807")
    monkeypatch.setattr(eff, "_with_retry", lambda fn: None)
    monkeypatch.setattr(eff, "_merge_tushare_frames", lambda code, **kw: [])
    monkeypatch.setattr(eff, "upsert_daily_rows", lambda merged: 0)
    monkeypatch.setattr(eff, "_recompute_net_inflows_for_code", lambda code, updated_at=None: 0)
    assert eff._sync_tushare_history_if_available(ts_code="510300.SH", end_date="20260807", updated_at="x") == 0


def test_sum_net_inflow_for_dates() -> None:
    rows = {"2026-08-05": {"net_inflow": 1.0}, "2026-08-06": {"net_inflow": "2.5"}}
    assert eff._sum_net_inflow_for_dates(rows, ["2026-08-05", "2026-08-06"]) == 3.5
    assert eff._sum_net_inflow_for_dates(rows, ["2026-08-05", "2026-08-07"]) is None
    assert eff._sum_net_inflow_for_dates({"2026-08-05": {"net_inflow": None}}, ["2026-08-05"]) is None


def test_latest_net_inflow_row() -> None:
    rows = {
        "2026-08-04": {"net_inflow": 1.0},
        "2026-08-05": {},
        "2026-08-06": {"net_inflow": "bad"},
    }
    opens = ["2026-08-03", "2026-08-04", "2026-08-05", "2026-08-06"]
    assert eff._latest_net_inflow_row(rows, opens, up_to="2026-08-06") == ("2026-08-04", 1.0)
    assert eff._latest_net_inflow_row(rows, opens, up_to="2026-08-03") == (None, None)
    assert eff._latest_net_inflow_row({}, opens, up_to="2026-08-06") == (None, None)


def test_apply_em_spot_fallback() -> None:
    monkeypatch = __import__("pytest").MonkeyPatch()

    def fake_spot(symbols):
        return {"ETF:510300": {"fdShareWan": 55.0, "mainNetInflow": 77.0}}

    monkeypatch.setattr(eff, "fetch_em_etf_spot_for_symbols", fake_spot)
    monkeypatch.setattr(eff, "upsert_daily_rows", lambda rows: len(rows))
    rows = {"2026-08-07": {"fd_share": 1.0, "net_inflow": None}}
    ok = eff._apply_em_spot_fallback(
        ts_code="510300.SH", symbol="ETF:510300", trade_date_iso="2026-08-07",
        rows_by_date=rows, updated_at="x",
    )
    assert ok is True
    assert rows["2026-08-07"]["fd_share"] == 55.0
    assert rows["2026-08-07"]["net_inflow"] == 77.0
    assert rows["2026-08-07"]["emMainNetInflow"] is True
    monkeypatch.undo()


def test_apply_em_spot_fallback_noop() -> None:
    monkeypatch = __import__("pytest").MonkeyPatch()
    monkeypatch.setattr(eff, "fetch_em_etf_spot_for_symbols", lambda symbols: {"ETF:510300": {}})
    monkeypatch.setattr(eff, "upsert_daily_rows", lambda rows: len(rows))
    rows = {}
    ok = eff._apply_em_spot_fallback(
        ts_code="510300.SH", symbol="ETF:510300", trade_date_iso="2026-08-07",
        rows_by_date=rows, updated_at="x",
    )
    assert ok is False
    monkeypatch.undo()
