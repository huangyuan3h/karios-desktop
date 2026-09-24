"""Tests for the frozen strategy catalog (display data + route contract)."""

from __future__ import annotations

from data_sync_service.api import backtest_routes as br
from data_sync_service.service.strategy_catalog import strategy_catalog

WINDOW_KEYS = {"OOS2", "train", "valid", "long"}
METRIC_KEYS = {"total", "cagr", "mdd", "sharpe"}


def test_catalog_has_three_tiers_plus_sunset_in_order() -> None:
    rows = strategy_catalog()
    assert sum(bool(row.get("canonical")) for row in rows) == 1
    assert [r["key"] for r in rows] == [
        "starship",
        "starship_robust",
        "starship_b",
        "starport",
        "homeport",
        "harbor",
        "twin_star",
    ]
    assert [r["name"] for r in rows] == [
        "星舰",
        "稳健星舰 H2-a25",
        "星舰 B",
        "星港",
        "母港",
        "港湾",
        "双子星",
    ]
    assert [(r["role"], r["roleLabel"]) for r in rows] == [
        ("offense", "进攻"),
        ("balanced", "稳健"),
        ("balanced", "稳健"),
        ("balanced", "均衡"),
        ("defense", "防守"),
        ("live", "Live 底座"),
        ("balanced", "均衡"),
    ]


def test_catalog_rows_are_complete() -> None:
    for row in strategy_catalog():
        assert row["updated"] in {"2026-09-21", "2026-09-24"}
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
    assert row["windows"]["long"] == {"total": 146.6, "cagr": 20.59, "mdd": -16.4, "sharpe": 1.06}


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
    assert second[0]["windows"]["long"]["total"] == 962.7
    assert "mutated" not in second[0]["pros"]


def test_catalog_route_contract() -> None:
    out = br.backtest_strategy_catalog()
    assert out["ok"] is True
    assert len(out["strategies"]) == 7
    by_key = {r["key"]: r for r in out["strategies"]}
    assert by_key["starship"]["timelineStrategy"] == "starship"
    assert by_key["starship"]["windows"]["long"] == {
        "total": 962.7,
        "cagr": 63.26,
        "mdd": -30.5,
        "sharpe": 2.14,
    }
    assert by_key["starship_robust"]["timelineStrategy"] == "starship_robust"
    assert by_key["starship_robust"]["status"] == "product_candidate"
    assert by_key["starship"]["updated"] == "2026-09-24"
    assert by_key["starship_robust"]["updated"] == "2026-09-24"
    assert by_key["starship_robust"]["canonical"] is True
    assert by_key["starship_robust"]["windows"]["long"] == {
        "total": 738.5,
        "cagr": 55.43,
        "mdd": -8.4,
        "sharpe": 3.5,
    }
    assert by_key["starship_robust"]["variant"] == {
        "sleeveMode": "h2",
        "hystBand": 0.02,
        "sleeveWeight": 0.25,
        "b3Weight": 0.75,
    }
    assert "K3" in by_key["starship_robust"]["risk"]
    assert by_key["starship_b"]["timelineStrategy"] == "starship_b"
    assert by_key["starship_b"]["windows"]["long"] == {
        "total": 669.6,
        "cagr": 52.69,
        "mdd": -5.5,
        "sharpe": 3.9,
    }
    assert by_key["starport"]["status"] == "product_candidate_increment"
    assert by_key["homeport"]["timelineStrategy"] == "homeport_m30"
    assert by_key["harbor"]["status"] == "live"
