"""Tests for the frozen strategy catalog (display data + route contract)."""

from __future__ import annotations

from data_sync_service.api import backtest_routes as br
from data_sync_service.service.strategy_catalog import strategy_catalog

WINDOW_KEYS = {"OOS2", "train", "valid", "long"}
METRIC_KEYS = {"total", "cagr", "mdd", "sharpe"}


def test_catalog_has_three_tiers_plus_sunset_in_order() -> None:
    rows = strategy_catalog()
    assert [r["key"] for r in rows] == [
        "starship",
        "starport",
        "homeport",
        "harbor",
        "twin_star",
    ]
    assert [r["name"] for r in rows] == ["星舰", "星港", "母港", "港湾", "双子星"]
    assert [(r["role"], r["roleLabel"]) for r in rows] == [
        ("offense", "进攻"),
        ("balanced", "均衡"),
        ("defense", "防守"),
        ("live", "Live 底座"),
        ("balanced", "对照"),
    ]


def test_catalog_rows_are_complete() -> None:
    for row in strategy_catalog():
        assert row["updated"] == "2026-09-17"
        assert row["structure"] and row["statusLabel"] and row["doc"] and row["tag"]
        assert row["role"] and row["roleLabel"]
        assert set(row["windows"]) == WINDOW_KEYS
        for w in row["windows"].values():
            assert set(w) == METRIC_KEYS
            assert all(isinstance(v, (int, float)) for v in w.values())
        assert row["pros"] and row["cons"]


def test_homeport_row_is_m30_defensive_tier() -> None:
    row = next(r for r in strategy_catalog() if r["key"] == "homeport")
    assert row["timelineStrategy"] == "homeport_m30"
    assert row["windows"]["long"] == {"total": 152.5, "cagr": 21.2, "mdd": -16.4, "sharpe": 1.1}


def test_every_strategy_carries_a_regime_map() -> None:
    for row in strategy_catalog():
        reg = row.get("regime")
        assert reg, f"{row['key']} missing regime"
        assert reg["fit"] and reg["unfit"] and reg["evidence"]
        assert reg["note"]
        assert all(set(e) == {"label", "value"} for e in reg["evidence"])


def test_catalog_returns_fresh_copies() -> None:
    first = strategy_catalog()
    first[0]["windows"]["long"]["total"] = 999.9
    first[0]["pros"].append("mutated")
    first[0]["regime"]["fit"].append("mutated")
    second = strategy_catalog()
    assert second[0]["windows"]["long"]["total"] == 975.0
    assert "mutated" not in second[0]["pros"]


def test_catalog_route_contract() -> None:
    out = br.backtest_strategy_catalog()
    assert out["ok"] is True
    assert len(out["strategies"]) == 5
    assert out["strategies"][0]["timelineStrategy"] == "starship"
    assert out["strategies"][2]["status"] == "product_candidate"
    assert out["strategies"][2]["timelineStrategy"] == "homeport_m30"
    assert out["strategies"][3]["status"] == "live"
