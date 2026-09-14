"""Tests for the frozen strategy catalog (display data + route contract)."""

from __future__ import annotations

from data_sync_service.api import backtest_routes as br
from data_sync_service.service.strategy_catalog import strategy_catalog

WINDOW_KEYS = {"OOS2", "train", "valid", "long"}
METRIC_KEYS = {"total", "cagr", "mdd", "sharpe"}


def test_catalog_has_five_family_strategies_in_order() -> None:
    rows = strategy_catalog()
    assert [r["key"] for r in rows] == [
        "harbor",
        "homeport",
        "starport",
        "starship",
        "twin_star",
    ]
    assert [r["name"] for r in rows] == ["港湾", "母港", "星港", "星舰", "双子星"]


def test_catalog_rows_are_complete() -> None:
    for row in strategy_catalog():
        assert row["updated"] == "2026-09-14"
        assert row["structure"] and row["statusLabel"] and row["doc"] and row["tag"]
        assert set(row["windows"]) == WINDOW_KEYS
        for w in row["windows"].values():
            assert set(w) == METRIC_KEYS
            assert all(isinstance(v, (int, float)) for v in w.values())
        assert row["pros"] and row["cons"]


def test_catalog_returns_fresh_copies() -> None:
    first = strategy_catalog()
    first[0]["windows"]["long"]["total"] = 999.9
    first[0]["pros"].append("mutated")
    second = strategy_catalog()
    assert second[0]["windows"]["long"]["total"] == 201.5
    assert "mutated" not in second[0]["pros"]


def test_catalog_route_contract() -> None:
    out = br.backtest_strategy_catalog()
    assert out["ok"] is True
    assert len(out["strategies"]) == 5
    assert out["strategies"][0]["timelineStrategy"] == "harbor"
    assert out["strategies"][4]["status"] == "parallel_candidate"
    assert out["strategies"][4]["timelineStrategy"] == "twin_star"
