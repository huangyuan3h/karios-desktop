from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from data_sync_service.service import etf_fund_flow as svc
from data_sync_service.service import etf_fund_flow_em as em
from data_sync_service.service.etf_fund_flow_em import EM_ETF_FLOW_SOURCE


def _flow_for_symbol(symbol: str, *, main_net: float = 12_000_000.0) -> dict[str, Any]:
    return {
        "fdShareWan": None,
        "latestPrice": 4.3,
        "mainNetInflow": main_net,
        "superLargeNetInflow": 7_000_000.0,
        "largeNetInflow": 5_000_000.0,
        "mediumNetInflow": -1_000_000.0,
        "smallNetInflow": -11_000_000.0,
        "tradeTime": "2026-06-22T06:30:00+00:00",
        "dataDate": "2026-06-22",
        "source": EM_ETF_FLOW_SOURCE,
    }


def test_compute_avg_price_vwap() -> None:
    # amount 千元, vol 手 -> price = amount*10/vol
    p = svc.compute_avg_price(close=4.0, vol=1000.0, amount=40000.0)
    assert p == pytest.approx(400.0)


def test_compute_avg_price_fallback_close() -> None:
    p = svc.compute_avg_price(close=3.5, vol=0.0, amount=0.0)
    assert p == pytest.approx(3.5)


def test_compute_net_inflow_1d() -> None:
    # delta 1 万份 -> 10_000 shares * price 4.0 = 40_000 CNY
    n = svc.compute_net_inflow_1d(fd_share_today=101.0, fd_share_prev=100.0, avg_price=4.0)
    assert n == pytest.approx(40_000.0)


def test_classify_signal_broad_buy() -> None:
    assert svc.classify_signal(category="broad", net_flow_1d=1.0, net_flow_3d=2.0) == (
        "National Team Buy"
    )


def test_classify_signal_broad_outflow() -> None:
    assert svc.classify_signal(category="broad", net_flow_1d=-1.0, net_flow_3d=-2.0) == (
        "National Team Outflow"
    )


def test_classify_signal_sector_momentum() -> None:
    assert svc.classify_signal(
        category="sector", net_flow_1d=1.0, net_flow_3d=svc.SECTOR_MOMENTUM_3D_THRESHOLD + 1
    ) == "Sector Momentum"


def test_classify_signal_sector_outflow() -> None:
    assert svc.classify_signal(category="sector", net_flow_1d=-1.0, net_flow_3d=-2.0) == (
        "Inst Outflow"
    )


def test_classify_signal_neutral_missing() -> None:
    assert svc.classify_signal(category="broad", net_flow_1d=None, net_flow_3d=1.0) == "Neutral"


def test_signal_display() -> None:
    assert "National Team Buy" in svc.signal_display("National Team Buy")
    assert "Inst Outflow" in svc.signal_display("Inst Outflow")


def test_build_etf_fund_flow_bundle_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = [
        {
            "ts_code": "510300.SH",
            "trade_date": "2026-06-16",
            "fd_share": 100.0,
            "close": 4.0,
            "avg_price": 4.0,
            "net_inflow": 10_000_000.0,
            "updated_at": "t",
        },
        {
            "ts_code": "510300.SH",
            "trade_date": "2026-06-17",
            "fd_share": 101.0,
            "close": 4.1,
            "avg_price": 4.1,
            "net_inflow": 20_000_000.0,
            "updated_at": "t",
        },
        {
            "ts_code": "510300.SH",
            "trade_date": "2026-06-18",
            "fd_share": 102.0,
            "close": 4.2,
            "avg_price": 4.2,
            "net_inflow": 52_300_000.0,
            "updated_at": "t",
        },
    ]
    open_dates = [
        date(2026, 6, 16),
        date(2026, 6, 17),
        date(2026, 6, 18),
    ]

    monkeypatch.setattr(svc, "ensure_table", lambda: None)
    monkeypatch.setattr(svc, "get_latest_date", lambda: "2026-06-18")
    monkeypatch.setattr(svc, "fetch_rows_for_codes", lambda *_a, **_k: rows)
    monkeypatch.setattr(svc, "get_open_dates", lambda *_a, **_k: open_dates)

    out = svc.build_etf_fund_flow_bundle(as_of_date="2026-06-18")
    assert out["asOfDate"] == "2026-06-18"
    assert isinstance(out["items"], list)
    assert len(out["items"]) == len(svc.ETF_WATCHLIST)

    hs300 = next(x for x in out["items"] if x["symbol"] == "510300")
    assert hs300["netFlow1d"] == pytest.approx(52_300_000.0)
    assert hs300["netFlow3d"] == pytest.approx(82_300_000.0)
    assert hs300["signal"] == "National Team Buy"
    assert "National Team Buy" in hs300["signalDisplay"]


def test_build_etf_fund_flow_bundle_t1_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = [
        {
            "ts_code": "510300.SH",
            "trade_date": "2026-06-18",
            "fd_share": 102.0,
            "close": 4.2,
            "avg_price": 4.2,
            "net_inflow": 52_300_000.0,
            "updated_at": "t",
        },
        {
            "ts_code": "510300.SH",
            "trade_date": "2026-06-22",
            "fd_share": None,
            "close": 4.3,
            "avg_price": 4.3,
            "net_inflow": None,
            "updated_at": "t",
        },
    ]
    open_dates = [date(2026, 6, 18), date(2026, 6, 19), date(2026, 6, 22)]

    monkeypatch.setattr(svc, "ensure_table", lambda: None)
    monkeypatch.setattr(svc, "get_latest_date", lambda: "2026-06-22")
    monkeypatch.setattr(svc, "fetch_rows_for_codes", lambda *_a, **_k: rows)
    monkeypatch.setattr(svc, "get_open_dates", lambda *_a, **_k: open_dates)
    monkeypatch.setattr(svc, "_is_shanghai_sync_window", lambda: False)
    # Market is closed (after hours / pre-market) -> should report MarketClosed, not Stale.
    monkeypatch.setattr(
        svc,
        "compute_market_status",
        lambda: {"phase": "Closed", "isMarketOpen": False, "isPreMarket": False},
    )

    out = svc.build_etf_fund_flow_bundle(as_of_date="2026-06-22")
    hs300 = next(x for x in out["items"] if x["symbol"] == "510300")
    assert out["shareLag"] is True
    assert out["intradaySafe"] is False
    assert hs300["netFlow1d"] is None
    assert hs300["netFlow1dLagged"] == pytest.approx(52_300_000.0)
    assert hs300["flowAsOfDate"] == "2026-06-18"
    assert hs300["signal"] == "Data Lag"
    assert hs300["live"] is False
    assert hs300["flowStatus"] == "MarketClosed"
    assert hs300["flowProvider"] == "tushare"


def test_build_etf_fund_flow_bundle_stale_when_market_open(monkeypatch: pytest.MonkeyPatch) -> None:
    """When market is OPEN but today's net_inflow is missing, status is Stale (data error)."""
    rows = [
        {
            "ts_code": "510300.SH",
            "trade_date": "2026-06-18",
            "fd_share": 102.0,
            "close": 4.2,
            "avg_price": 4.2,
            "net_inflow": 52_300_000.0,
            "updated_at": "t",
        },
        {
            "ts_code": "510300.SH",
            "trade_date": "2026-06-22",
            "fd_share": None,
            "close": 4.3,
            "avg_price": 4.3,
            "net_inflow": None,
            "updated_at": "t",
        },
    ]
    open_dates = [date(2026, 6, 18), date(2026, 6, 19), date(2026, 6, 22)]

    monkeypatch.setattr(svc, "ensure_table", lambda: None)
    monkeypatch.setattr(svc, "get_latest_date", lambda: "2026-06-22")
    monkeypatch.setattr(svc, "fetch_rows_for_codes", lambda *_a, **_k: rows)
    monkeypatch.setattr(svc, "get_open_dates", lambda *_a, **_k: open_dates)
    monkeypatch.setattr(svc, "_is_shanghai_sync_window", lambda: True)
    monkeypatch.setattr(
        svc,
        "compute_market_status",
        lambda: {"phase": "Open", "isMarketOpen": True, "isPreMarket": False},
    )

    out = svc.build_etf_fund_flow_bundle(as_of_date="2026-06-22")
    hs300 = next(x for x in out["items"] if x["symbol"] == "510300")
    assert hs300["netFlow1d"] is None
    assert hs300["netFlow1dLagged"] == pytest.approx(52_300_000.0)
    assert hs300["flowStatus"] == "Stale"
    assert "Data Lag" in hs300["signalDisplay"]
    assert out["intradaySafe"] is False


def test_build_etf_fund_flow_bundle_em_read_path(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = [
        {
            "ts_code": "510300.SH",
            "trade_date": "2026-06-18",
            "fd_share": 102.0,
            "close": 4.2,
            "avg_price": 4.2,
            "net_inflow": 52_300_000.0,
            "updated_at": "t",
        },
        {
            "ts_code": "510300.SH",
            "trade_date": "2026-06-22",
            "fd_share": None,
            "close": 4.3,
            "avg_price": 4.3,
            "net_inflow": None,
            "updated_at": "t",
        },
    ]
    open_dates = [date(2026, 6, 18), date(2026, 6, 19), date(2026, 6, 22)]

    monkeypatch.setattr(svc, "ensure_table", lambda: None)
    monkeypatch.setattr(svc, "get_latest_date", lambda: "2026-06-22")
    monkeypatch.setattr(svc, "fetch_rows_for_codes", lambda *_a, **_k: rows)
    monkeypatch.setattr(svc, "get_open_dates", lambda *_a, **_k: open_dates)
    monkeypatch.setattr(svc, "_is_shanghai_sync_window", lambda: True)
    monkeypatch.setattr(
        svc,
        "fetch_em_etf_spot_for_symbols",
        lambda _syms: {
            "510300": {
                "fdShareWan": 103.0,
                "mainNetInflow": 12_000_000.0,
                "superLargeNetInflow": 7_000_000.0,
                "largeNetInflow": 5_000_000.0,
                "tradeTime": "2026-06-22T06:30:00+00:00",
                "dataDate": "2026-06-22",
            }
        },
    )

    out = svc.build_etf_fund_flow_bundle(as_of_date="2026-06-22")
    hs300 = next(x for x in out["items"] if x["symbol"] == "510300")
    assert hs300["netFlow1d"] == pytest.approx(12_000_000.0)
    assert hs300["flowAsOfDate"] is None
    assert hs300["source"] == EM_ETF_FLOW_SOURCE
    assert hs300["tradeTime"] == "2026-06-22T06:30:00+00:00"
    assert hs300["superLargeNetInflow"] == pytest.approx(7_000_000.0)
    assert hs300["largeNetInflow"] == pytest.approx(5_000_000.0)
    assert hs300["signal"] == "National Team Buy"
    assert hs300["live"] is True
    assert hs300["flowStatus"] == "Live"
    assert hs300["flowProvider"] == "eastmoney"
    assert out["intradaySafe"] is False


def test_build_etf_fund_flow_bundle_realtime_row_not_lagged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [
        {
            "ts_code": "510300.SH",
            "trade_date": "2026-06-18",
            "fd_share": 102.0,
            "close": 4.2,
            "avg_price": 4.2,
            "net_inflow": 52_300_000.0,
            "updated_at": "t",
        },
        {
            "ts_code": "510300.SH",
            "trade_date": "2026-06-19",
            "fd_share": 103.0,
            "close": 4.25,
            "avg_price": 4.25,
            "net_inflow": 30_000_000.0,
            "updated_at": "t",
        },
        {
            "ts_code": "510300.SH",
            "trade_date": "2026-06-22",
            "fd_share": None,
            "close": 4.3,
            "avg_price": 4.3,
            "net_inflow": 12_000_000.0,
            "updated_at": "t",
            "source": EM_ETF_FLOW_SOURCE,
            "trade_time": "2026-06-22T06:30:00+00:00",
            "main_net_inflow": 12_000_000.0,
            "super_large_net_inflow": 7_000_000.0,
            "large_net_inflow": 5_000_000.0,
            "medium_net_inflow": -1_000_000.0,
            "small_net_inflow": -11_000_000.0,
        },
    ]
    open_dates = [date(2026, 6, 18), date(2026, 6, 19), date(2026, 6, 22)]

    monkeypatch.setattr(svc, "ensure_table", lambda: None)
    monkeypatch.setattr(svc, "get_latest_date", lambda: "2026-06-22")
    monkeypatch.setattr(svc, "fetch_rows_for_codes", lambda *_a, **_k: rows)
    monkeypatch.setattr(svc, "get_open_dates", lambda *_a, **_k: open_dates)

    out = svc.build_etf_fund_flow_bundle(as_of_date="2026-06-22")
    hs300 = next(x for x in out["items"] if x["symbol"] == "510300")
    assert out["shareLag"] is True
    assert hs300["netFlow1d"] == pytest.approx(12_000_000.0)
    assert hs300["flowAsOfDate"] is None
    assert hs300["source"] == EM_ETF_FLOW_SOURCE
    assert hs300["tradeTime"] == "2026-06-22T06:30:00+00:00"
    assert hs300["superLargeNetInflow"] == pytest.approx(7_000_000.0)
    assert hs300["largeNetInflow"] == pytest.approx(5_000_000.0)
    assert hs300["signal"] == "National Team Buy"
    assert hs300["live"] is True
    assert hs300["flowStatus"] == "Live"
    assert hs300["flowProvider"] == "eastmoney"


def test_sync_etf_fund_flow_watchlist_uses_eastmoney_primary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[dict] = []
    records: list[dict] = []

    monkeypatch.setattr(svc, "_should_skip_etf_sync_today", lambda *, force: False)
    monkeypatch.setattr(svc, "ensure_table", lambda: None)
    monkeypatch.setattr(svc, "_shanghai_today_yyyymmdd", lambda: "20260622")
    monkeypatch.setattr(svc, "last_open_date_on_or_before", lambda _d: date(2026, 6, 22))
    monkeypatch.setattr(svc, "_now_iso", lambda: "now")
    monkeypatch.setattr(svc, "_sync_tushare_history_if_available", lambda **_kw: 0)
    monkeypatch.setattr(
        svc,
        "fetch_em_etf_realtime_flow_for_symbols",
        lambda _symbols: {"510300": _flow_for_symbol("510300")},
    )
    monkeypatch.setattr(svc, "get_last_em_etf_fetch_error", lambda: None)
    monkeypatch.setattr(
        svc,
        "upsert_daily_rows",
        lambda rows: captured.extend(rows) or len(rows),
    )
    monkeypatch.setattr(
        svc,
        "insert_record",
        lambda **kwargs: records.append(kwargs),
    )

    out = svc.sync_etf_fund_flow_watchlist(force=True)

    assert out["ok"] is True
    assert out["source"] == EM_ETF_FLOW_SOURCE
    assert captured
    row = captured[0]
    assert row["ts_code"] == "510300.SH"
    assert row["trade_date"] == "2026-06-22"
    assert row["fd_share"] is None
    assert row["net_inflow"] == pytest.approx(12_000_000.0)
    assert row["main_net_inflow"] == pytest.approx(12_000_000.0)
    assert row["super_large_net_inflow"] == pytest.approx(7_000_000.0)
    assert row["large_net_inflow"] == pytest.approx(5_000_000.0)
    assert row["source"] == EM_ETF_FLOW_SOURCE
    assert records and records[0]["success"] is True


def test_sync_etf_fund_flow_watchlist_rejects_stale_eastmoney_date(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[dict] = []
    records: list[dict] = []
    stale_flow = _flow_for_symbol("510300") | {"dataDate": "2026-06-18"}

    monkeypatch.setattr(svc, "_should_skip_etf_sync_today", lambda *, force: False)
    monkeypatch.setattr(svc, "ensure_table", lambda: None)
    monkeypatch.setattr(svc, "_shanghai_today_yyyymmdd", lambda: "20260622")
    monkeypatch.setattr(svc, "last_open_date_on_or_before", lambda _d: date(2026, 6, 22))
    monkeypatch.setattr(svc, "_now_iso", lambda: "now")
    monkeypatch.setattr(svc, "_sync_tushare_history_if_available", lambda **_kw: 0)
    monkeypatch.setattr(
        svc,
        "fetch_em_etf_realtime_flow_for_symbols",
        lambda _symbols: {"510300": stale_flow},
    )
    monkeypatch.setattr(svc, "get_last_em_etf_fetch_error", lambda: None)
    monkeypatch.setattr(svc, "upsert_daily_rows", lambda rows: captured.extend(rows) or len(rows))
    monkeypatch.setattr(svc, "insert_record", lambda **kwargs: records.append(kwargs))

    out = svc.sync_etf_fund_flow_watchlist(force=True)

    assert out["ok"] is False
    assert captured == []
    assert "510300" in out["staleSymbols"]
    assert "510300" in out["missingSymbols"]
    assert records and records[0]["success"] is False


def test_build_etf_fund_flow_bundle_full_realtime_watchlist_intraday_safe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows: list[dict[str, Any]] = []
    for idx, spec in enumerate(svc.ETF_WATCHLIST):
        rows.extend(
            [
                {
                    "ts_code": spec["ts_code"],
                    "trade_date": "2026-06-18",
                    "fd_share": 102.0 + idx,
                    "close": 4.2,
                    "avg_price": 4.2,
                    "net_inflow": 52_300_000.0 + idx,
                    "updated_at": "t",
                },
                {
                    "ts_code": spec["ts_code"],
                    "trade_date": "2026-06-19",
                    "fd_share": 103.0 + idx,
                    "close": 4.25,
                    "avg_price": 4.25,
                    "net_inflow": 30_000_000.0 + idx,
                    "updated_at": "t",
                },
                {
                    "ts_code": spec["ts_code"],
                    "trade_date": "2026-06-22",
                    "fd_share": None,
                    "close": 4.3,
                    "avg_price": 4.3,
                    "net_inflow": 12_000_000.0 + idx,
                    "updated_at": "t",
                    "source": EM_ETF_FLOW_SOURCE,
                    "trade_time": "2026-06-22T06:30:00+00:00",
                    "main_net_inflow": 12_000_000.0 + idx,
                    "super_large_net_inflow": 7_000_000.0 + idx,
                    "large_net_inflow": 5_000_000.0 + idx,
                    "medium_net_inflow": -1_000_000.0,
                    "small_net_inflow": -11_000_000.0,
                },
            ]
        )

    monkeypatch.setattr(svc, "ensure_table", lambda: None)
    monkeypatch.setattr(svc, "get_latest_date", lambda: "2026-06-22")
    monkeypatch.setattr(svc, "fetch_rows_for_codes", lambda *_a, **_k: rows)
    monkeypatch.setattr(
        svc,
        "get_open_dates",
        lambda *_a, **_k: [date(2026, 6, 18), date(2026, 6, 19), date(2026, 6, 22)],
    )

    out = svc.build_etf_fund_flow_bundle(as_of_date="2026-06-22")

    assert out["intradaySafe"] is True
    assert out["shareLag"] is False
    assert len(out["items"]) == len(svc.ETF_WATCHLIST)
    assert {it["source"] for it in out["items"]} == {EM_ETF_FLOW_SOURCE}
    assert all(it["netFlow1d"] is not None for it in out["items"])
    assert all(it["live"] is True for it in out["items"])
    assert {it["flowStatus"] for it in out["items"]} == {"Live"}


def test_em_etf_fetcher_host_fallback_and_normalization(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    def fake_get_json(url: str, *, params: dict[str, str], referer: str) -> dict[str, Any]:
        calls.append(url)
        assert params["_"]
        # Primary host (push2delay) fails, fallback to push2 succeeds
        if "push2delay.eastmoney.com" in url:
            raise RuntimeError("blocked")
        return {
            "data": {
                "total": 1,
                "diff": [
                    {
                        "f12": "510300",
                        "f14": "沪深300ETF",
                        "f2": "4.30",
                        "f3": "1.2",
                        "f38": "1030000",
                        "f62": "12000000",
                        "f66": "7000000",
                        "f69": "2.1",
                        "f72": "5000000",
                        "f75": "1.5",
                        "f78": "-1000000",
                        "f81": "-0.3",
                        "f84": "-11000000",
                        "f87": "-3.3",
                        "f124": "1782119400",
                        "f184": "3.6",
                        "f297": "20260622",
                    }
                ],
            }
        }

    monkeypatch.setattr(em, "em_get_json", fake_get_json)

    out = em.fetch_em_etf_realtime_flow_for_symbols(["510300"])

    assert len(calls) == 2
    assert "push2delay" in calls[0]
    assert "push2.eastmoney.com" in calls[1]
    assert out["510300"]["fdShareWan"] == pytest.approx(103.0)
    assert out["510300"]["mainNetInflow"] == pytest.approx(12_000_000.0)
    assert out["510300"]["superLargeNetInflow"] == pytest.approx(7_000_000.0)
    assert out["510300"]["largeNetInflow"] == pytest.approx(5_000_000.0)
    assert out["510300"]["dataDate"] == "2026-06-22"
    assert out["510300"]["source"] == EM_ETF_FLOW_SOURCE
    assert em.get_last_em_etf_fetch_error() is None


def test_em_etf_fetcher_empty_diff_exposes_diagnostic(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        em,
        "em_get_json",
        lambda *_a, **_k: {"data": {"total": 0, "diff": []}},
    )

    out = em.fetch_em_etf_realtime_flow_for_symbols(["510300"])

    assert out == {}
    assert em.get_last_em_etf_fetch_error()
    assert "empty_diff" in str(em.get_last_em_etf_fetch_error())


def test_em_etf_fetcher_handles_page_size_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    """Server caps pz=500 to 100 rows/page but reports total=1516.

    The target symbol lives on page 10, so the fetcher must compute
    total_pages from the *actual* page size (100), not the requested
    page size (500). Otherwise it stops at page 4 and misses the symbol.
    """
    requested_pages: list[int] = []
    target_symbol = "510300"
    target_page = 10
    total = 1516

    def fake_get_json(_url: str, *, params: dict[str, str], referer: str) -> dict[str, Any]:
        pn = int(params["pn"])
        requested_pages.append(pn)
        diff: list[dict[str, Any]] = []
        if pn == target_page:
            diff.append(
                {
                    "f12": target_symbol,
                    "f14": "沪深300ETF",
                    "f2": 4.3,
                    "f3": 1.2,
                    "f38": 1030000,
                    "f62": 12000000,
                    "f66": 7000000,
                    "f69": 2.1,
                    "f72": 5000000,
                    "f75": 1.5,
                    "f78": -1000000,
                    "f81": -0.3,
                    "f84": -11000000,
                    "f87": -3.3,
                    "f124": 1782119400,
                    "f184": 3.6,
                    "f297": 20260622,
                }
            )
        else:
            for i in range(100):
                diff.append({"f12": f"filler{pn}_{i}", "f14": "filler", "f62": 0})
        return {"data": {"total": total, "diff": diff}}

    monkeypatch.setattr(em, "em_get_json", fake_get_json)

    out = em.fetch_em_etf_realtime_flow_for_symbols([target_symbol])

    assert target_symbol in out
    assert out[target_symbol]["mainNetInflow"] == pytest.approx(12_000_000.0)
    assert out[target_symbol]["dataDate"] == "2026-06-22"
    # Must have fetched at least 10 pages (target lives on page 10)
    assert max(requested_pages) >= target_page
    assert em.get_last_em_etf_fetch_error() is None
