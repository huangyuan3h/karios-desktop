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
