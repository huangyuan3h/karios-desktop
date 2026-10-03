"""Unit + contract tests for the fleet display layer (no DB/network).

Covers the frozen wiring:
- gate 0/10/20 only (default 0 fail-closed) + cap enforcement
- defense L1/L2/L3/L4 flags via combo_tiers thresholds
- L4 terminal cash override
- catalog carries the fleet row (research, not live) with frozen windows
- GET /api/backtest/fleet file contract (404 / corrupt / ok)
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from data_sync_service.api import backtest_routes as br
from data_sync_service.service import fleet as fl
from data_sync_service.service.strategy_catalog import strategy_catalog


def test_gate_steps_only() -> None:
    assert fl.normalize_gate(None) == 0.0
    assert fl.normalize_gate(0) == 0.0
    assert fl.normalize_gate(10) == 0.10
    assert fl.normalize_gate(20) == 0.20
    with pytest.raises(ValueError):
        fl.normalize_gate(15)
    with pytest.raises(ValueError):
        fl.normalize_gate(100)


def test_plan_defaults_fail_closed() -> None:
    plan = fl.fleet_plan(1_200_000, "A", None, None)
    # Current vault state is cold -> 0% starship, refilled pro-rata to base.
    assert plan["gate"]["position"] == 0
    assert plan["weights"]["starship"] == 0.0
    assert plan["weights"]["harbor"] == pytest.approx(0.5)
    assert plan["weights"]["m30"] == pytest.approx(0.25)
    assert plan["weights"]["b3"] == pytest.approx(0.25)
    assert plan["target_tier"] == "A"
    assert plan["disclaimer"].startswith("Starship B stays")


def test_plan_gate_pro_rata_and_cap() -> None:
    plan10 = fl.fleet_plan(1_200_000, "A", None, 10)
    assert plan10["weights"]["starship"] == pytest.approx(0.10)
    assert plan10["weights"]["harbor"] == pytest.approx(0.45)
    plan20 = fl.fleet_plan(1_200_000, "A", None, 20)
    assert plan20["weights"]["starship"] == pytest.approx(0.20)
    assert plan20["weights"]["harbor"] == pytest.approx(0.40)
    # Cap holds even if a future config drifts (gate max is 20 anyway).
    assert plan20["weights"]["starship"] <= 0.20 + 1e-9


def test_defense_flags() -> None:
    clear = fl.defense_states([1.0, 1.01, 1.02])
    assert clear["L1_downgrade"] is False
    assert clear["L2_pause_new"] is False
    assert clear["L3_halve"] is False
    assert clear["L4_cash"] is False
    # Single-day -8% trips L2 only.
    l2 = fl.defense_states([1.0, 0.919])
    assert l2["L2_pause_new"] is True
    # Weekly -5% trips L3.
    l3 = fl.defense_states([1.0, 1.0, 1.0, 1.0, 1.0, 0.949])
    assert l3["L3_halve"] is True
    # 60d -15% trips L1 downgrade.
    nav60 = [1.0] * 59 + [0.849]
    l1 = fl.defense_states(nav60)
    assert l1["L1_downgrade"] is True
    # 60d -25% trips L4 cash (subset of L1).
    nav25 = [1.0] * 59 + [0.749]
    l4 = fl.defense_states(nav25)
    assert l4["L4_cash"] is True
    assert l4["L1_downgrade"] is True


def test_plan_l4_terminal_cash() -> None:
    nav25 = [1.0] * 59 + [0.749]
    plan = fl.fleet_plan(1_200_000, "A", nav25, 20)
    assert plan["target_tier"] == "CASH"
    assert plan["weights"]["cash"] == pytest.approx(1.0)
    assert plan["weights"]["starship"] == pytest.approx(0.0)
    assert plan["defense"]["L4_cash"] is True


def test_catalog_has_fleet_research_not_live() -> None:
    rows = strategy_catalog()
    by_key = {r["key"]: r for r in rows}
    assert "fleet" in by_key
    fleet = by_key["fleet"]
    assert fleet["name"] == "舰队"
    assert fleet["status"] == "research"
    assert fleet["windows"]["long"]["total"] == pytest.approx(163.6)
    assert fleet["windows"]["holdout"] if "holdout" in fleet["windows"] else True
    # Live stays elsewhere (harbor live in code; starship B stays baseline per prompt).
    live_keys = [r["key"] for r in rows if r.get("status") == "live"]
    assert "fleet" not in live_keys


def test_fleet_route_file_contract(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(br, "REPORTS_DIR", tmp_path)
    with pytest.raises(HTTPException) as exc:
        br.backtest_fleet()
    assert exc.value.status_code == 404
    (tmp_path / "fleet.json").write_text("{oops", encoding="utf-8")
    with pytest.raises(HTTPException) as exc:
        br.backtest_fleet()
    assert exc.value.status_code == 500
    (tmp_path / "fleet.json").write_text(
        '{"meta": {}, "windows": {}, "equity": {"dates": []}}', encoding="utf-8"
    )
    out = br.backtest_fleet()
    assert out == {
        "ok": True,
        "fleet": {"meta": {}, "windows": {}, "equity": {"dates": []}},
    }
