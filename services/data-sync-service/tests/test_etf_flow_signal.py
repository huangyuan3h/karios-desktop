"""Tests for the ETF flow confirmation-signal aggregation (no DB required)."""

from __future__ import annotations

from typing import Any

from data_sync_service.service.etf_fund_flow import aggregate_etf_flow_signal


def _item(name: str, category: str, signal: str) -> dict[str, Any]:
    return {"name": name, "category": category, "signal": signal}


def _bundle(items: list[dict[str, Any]], **meta: Any) -> dict[str, Any]:
    out: dict[str, Any] = {
        "asOfDate": "2026-07-27",
        "intradaySafe": True,
        "shareLag": False,
        "items": items,
    }
    out.update(meta)
    return out


def test_confirm_when_broad_buy() -> None:
    sig = aggregate_etf_flow_signal(
        _bundle(
            [
                _item("沪深300 ETF", "broad", "National Team Buy"),
                _item("上证50 ETF", "broad", "Neutral"),
                _item("半导体 ETF", "sector", "Neutral"),
            ]
        )
    )
    assert sig["verdict"] == "confirm"
    assert sig["broadDirection"] == "buy"
    assert sig["sectorDirection"] == "neutral"
    assert sig["confirmCount"] == 1
    assert sig["incomplete"] is False


def test_confirm_when_sector_momentum() -> None:
    sig = aggregate_etf_flow_signal(
        _bundle(
            [
                _item("沪深300 ETF", "broad", "Neutral"),
                _item("半导体 ETF", "sector", "Sector Momentum"),
            ]
        )
    )
    assert sig["verdict"] == "confirm"
    assert sig["sectorDirection"] == "buy"


def test_contradict_when_broad_outflow() -> None:
    sig = aggregate_etf_flow_signal(
        _bundle(
            [
                _item("沪深300 ETF", "broad", "National Team Outflow"),
                _item("半导体 ETF", "sector", "Neutral"),
            ]
        )
    )
    assert sig["verdict"] == "contradict"
    assert sig["broadDirection"] == "outflow"
    assert sig["contradictCount"] == 1


def test_mixed_is_neutral() -> None:
    sig = aggregate_etf_flow_signal(
        _bundle(
            [
                _item("沪深300 ETF", "broad", "National Team Buy"),
                _item("半导体 ETF", "sector", "Inst Outflow"),
            ]
        )
    )
    assert sig["verdict"] == "neutral"
    assert sig["broadDirection"] == "buy"
    assert sig["sectorDirection"] == "outflow"


def test_all_neutral_is_neutral() -> None:
    sig = aggregate_etf_flow_signal(_bundle([_item("沪深300 ETF", "broad", "Neutral")]))
    assert sig["verdict"] == "neutral"


def test_incomplete_when_share_lag() -> None:
    sig = aggregate_etf_flow_signal(
        _bundle(
            [_item("沪深300 ETF", "broad", "National Team Buy")],
            shareLag=True,
        )
    )
    assert sig["verdict"] == "confirm"
    assert sig["incomplete"] is True


def test_incomplete_when_intraday_unsafe() -> None:
    sig = aggregate_etf_flow_signal(
        _bundle(
            [_item("沪深300 ETF", "broad", "Data Lag")],
            intradaySafe=False,
        )
    )
    assert sig["incomplete"] is True


def test_empty_items_is_neutral_and_safe() -> None:
    sig = aggregate_etf_flow_signal(_bundle([]))
    assert sig["verdict"] == "neutral"
    assert sig["broadDirection"] == "neutral"
    assert sig["sectorDirection"] == "neutral"
    assert sig["incomplete"] is False
