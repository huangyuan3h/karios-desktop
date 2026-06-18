from __future__ import annotations

from datetime import date

import pytest

from data_sync_service.service import etf_fund_flow as svc


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
