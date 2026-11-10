"""P0 agent contract + overlay tests (no Postgres, no network).

The contract must be readable as one JSON, the whip flag must come from the
server, and an accepted overlay must change the *copy* — never the engine's
``sleeve.action``.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from data_sync_service.main import app
from data_sync_service.service import agent_contract, agent_overlays, agent_whip

client = TestClient(app)


@pytest.fixture()
def memory_settings(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """In-memory stand-in for the app_settings JSONB store."""
    store: dict[str, Any] = {}

    def _get(key: str, default: Any = None) -> Any:
        return store.get(key, default)

    def _set(key: str, value: Any) -> None:
        store[key] = value

    monkeypatch.setattr("data_sync_service.db.app_settings.get_setting", _get)
    monkeypatch.setattr("data_sync_service.db.app_settings.set_setting", _set)
    return store


def _sleeve(**overrides: Any) -> dict[str, Any]:
    base = {
        "active": True,
        "action": "SELL_TO_REPO",
        "label": "卖出转repo(止损)",
        "message": "ETF:513350峰值回撤8.5% ≥8% → 次日开盘卖转逆回购",
        "holding": True,
        "symbol": "ETF:513350",
        "trailExit": True,
        "pendingOpenSell": False,
        "signalDate": "2026-11-09",
        "executeHint": "次日开盘卖出转逆回购",
        "idlePct": 0,
        "parkPct": 63.5,
        "pick": {
            "key": "OIL",
            "symbol": "ETF:513350",
            "name": "富国油气QDII",
            "mom60": 4.8,
            "close": 1.28,
            "ma200": 1.5,
            "above_ma200": True,
            "all_mom": {"OIL": 4.8, "BOND10": 1.1, "GOLD": -2.0},
            "all_above": {"OIL": True, "BOND10": True, "GOLD": False},
        },
        "etfPick": {"key": "OIL", "symbol": "ETF:513350"},
    }
    base.update(overrides)
    return base


def _fake_health(sleeve: dict[str, Any], holdings: list[dict[str, Any]] | None = None):
    def _build(*, trade_date: str | None = None, markets: tuple[str, ...] = ("CN",)) -> dict:
        return {
            "tradeDate": trade_date or "2026-11-10",
            "regime": "Strong",
            "sentiment": "hot",
            "panicCooldown": {"active": False},
            "circuitBlocked": False,
            "holdings": [{"symbol": "CN:600000", "action": "HOLD"}],
            "hkHealth": {"holdings": []},
            "s3Candidates": [{"symbol": "CN:600000"}],
            "scoreFresh": True,
            "multiAssetSleeve": sleeve,
            "multiAssetHoldings": holdings
            if holdings is not None
            else [
                {
                    "symbol": "ETF:513350",
                    "key": "OIL",
                    "positionPct": 63.5,
                    "trailExit": True,
                    "signalDate": "2026-11-09",
                }
            ],
        }

    return _build


def _patch_factor_reports(
    monkeypatch: pytest.MonkeyPatch,
    *,
    starship_status: str = "cold",
    net: float = -14.79,
    percentile: float = 2.2,
    revival_pos: int = 0,
    mean: float = -14.79,
) -> None:
    """Deterministic factor-vault / S-gap reports (no file or network read)."""

    def _read(name: str) -> dict[str, Any] | None:
        if name == "factor_vault.json":
            return {
                "factors": [
                    {
                        "id": "starship_b",
                        "name": "星舰B默认",
                        "status": starship_status,
                        "badge": "冷库" if starship_status == "cold" else "复活",
                        "percentile": percentile,
                        "net": net,
                        "position": 0,
                    },
                    {
                        "id": "starship_a25",
                        "name": "H2-a25",
                        "status": starship_status,
                        "badge": "冷库" if starship_status == "cold" else "复活",
                        "percentile": None,
                        "net": net,
                        "position": 0,
                    },
                ]
            }
        if name == "sgap_decay.json":
            return {
                "summary": {
                    "revival_pos": revival_pos,
                    "latest_r40_mean": mean,
                    "latest_r40_percentile": percentile,
                    "latest_r40_win_rate": 0.3,
                }
            }
        return None

    monkeypatch.setattr(agent_contract, "_read_report_json", _read)


def test_mode_homeport_maps_to_product_m30_not_m50() -> None:
    assert agent_contract.timeline_strategy_for_mode("homeport") == "homeport_m30"
    assert agent_contract.timeline_strategy_for_mode("harbor") == "harbor"
    assert agent_contract.timeline_strategy_for_mode("starship_b") == "starship_b"
    # Unknown mode falls back to the product default, never to the frozen M50.
    assert agent_contract.timeline_strategy_for_mode("nope") == "homeport_m30"


def test_contract_json_shape_and_whip(
    monkeypatch: pytest.MonkeyPatch, memory_settings: dict[str, Any]
) -> None:
    monkeypatch.setattr(
        "data_sync_service.service.portfolio_health.build_portfolio_health",
        _fake_health(_sleeve()),
    )
    monkeypatch.setattr(
        "data_sync_service.api.settings_routes.selected_strategy_mode", lambda: "homeport"
    )
    out = agent_contract.build_decision_contract(trade_date="2026-11-10")

    assert out["ok"] is True
    assert out["asOfDate"] == "2026-11-10"
    assert out["strategy"]["mode"] == "homeport"
    assert out["strategy"]["timelineStrategy"] == "homeport_m30"
    assert out["strategy"]["timelineLabel"] == "母港 M30"
    assert out["sleeve"]["action"] == "SELL_TO_REPO"
    assert out["sleeve"]["sellToRepo"] is True
    assert out["sleeve"]["trailExit"] is True
    assert out["sleeve"]["pendingOpenSell"] is False
    assert out["sleeve"]["pickKey"] == "OIL"
    assert out["nextLeg"]["key"] == "OIL"
    assert out["nextLeg"]["aboveMa200"] is True
    assert {c["key"] for c in out["nextLeg"]["candidates"]} == {"OIL", "BOND10", "GOLD"}
    assert out["health"]["holdingsCount"] == 2
    assert out["health"]["parkingHoldingsCount"] == 1
    assert out["health"]["candidateCount"] == 1
    assert out["whip"]["sameKeyRebuy"] is True
    assert out["whip"]["annotation"] == "刚止损，回买=鞭打，请确认"
    assert out["whip"]["copy"] == "刚止损，回买=鞭打，请确认"
    assert out["contractId"].startswith("dc:2026-11-10:homeport_m30:")
    assert out["authority"][0] == "frozen_engine_exits"


def test_whip_false_when_pick_is_a_different_key(
    monkeypatch: pytest.MonkeyPatch, memory_settings: dict[str, Any]
) -> None:
    sleeve = _sleeve(
        pick={
            "key": "BOND10",
            "symbol": "ETF:511260",
            "name": "10年国债ETF",
            "mom60": 1.1,
            "above_ma200": True,
            "all_mom": {"OIL": 4.8, "BOND10": 1.1},
            "all_above": {"OIL": True, "BOND10": True},
        },
        etfPick={"key": "BOND10", "symbol": "ETF:511260"},
    )
    monkeypatch.setattr(
        "data_sync_service.service.portfolio_health.build_portfolio_health",
        _fake_health(sleeve, []),
    )
    monkeypatch.setattr(
        "data_sync_service.api.settings_routes.selected_strategy_mode", lambda: "harbor"
    )
    out = agent_contract.build_decision_contract(trade_date="2026-11-10")
    assert out["sleeve"]["action"] == "SELL_TO_REPO"
    assert out["whip"]["sameKeyRebuy"] is False
    assert out["whip"]["copy"] is None


def test_accepted_overlay_changes_copy_but_not_engine_action(
    monkeypatch: pytest.MonkeyPatch, memory_settings: dict[str, Any]
) -> None:
    sleeve = _sleeve()
    monkeypatch.setattr(
        "data_sync_service.service.portfolio_health.build_portfolio_health",
        _fake_health(sleeve),
    )
    monkeypatch.setattr(
        "data_sync_service.api.settings_routes.selected_strategy_mode", lambda: "homeport"
    )

    before = agent_contract.build_decision_contract(trade_date="2026-11-10")
    assert before["sleeve"]["action"] == "SELL_TO_REPO"

    overlay = agent_overlays.upsert_overlay(
        pick_key="OIL",
        symbol="ETF:513350",
        status="accepted",
        valid_until="2026-11-20",
        source="test",
        day="2026-11-10",
    )
    assert overlay["effectiveStatus"] == "accepted"

    after = agent_contract.build_decision_contract(trade_date="2026-11-10")
    # The engine decision is untouched — the overlay is annotation only.
    assert after["sleeve"]["action"] == "SELL_TO_REPO"
    assert after["sleeve"]["sellToRepo"] is True
    assert after["whip"]["sameKeyRebuy"] is True
    assert after["whip"]["overlay"]["id"] == overlay["id"]
    assert after["whip"]["copy"] == "建议暂缓 · 需确认"
    assert after["overlays"]["active"][0]["id"] == overlay["id"]


def test_overlay_expiry_falls_back_to_whip_annotation(
    monkeypatch: pytest.MonkeyPatch, memory_settings: dict[str, Any]
) -> None:
    agent_overlays.upsert_overlay(
        pick_key="OIL",
        status="accepted",
        valid_until="2026-11-09",
        source="test",
        day="2026-11-10",
    )
    items = agent_overlays.list_overlays(day="2026-11-10", pick_key="OIL")
    assert items[0]["effectiveStatus"] == "expired"
    assert agent_overlays.find_same_key_overlay("OIL", day="2026-11-10") is None

    state = agent_whip.evaluate_whip(_sleeve(), [], day="2026-11-10", observe=False)
    assert state["sameKeyRebuy"] is True
    assert state["copy"] == "刚止损，回买=鞭打，请确认"


def test_overlay_upsert_confirm_and_clear(memory_settings: dict[str, Any]) -> None:
    proposed = agent_overlays.upsert_overlay(
        pick_key="OIL", status="proposed", source="agent", day="2026-11-10"
    )
    assert proposed["effectiveStatus"] == "proposed"
    accepted = agent_overlays.upsert_overlay(
        pick_key="oil", status="accepted", source="yuan", day="2026-11-10"
    )
    assert accepted["id"] == proposed["id"]
    assert accepted["effectiveStatus"] == "accepted"
    cleared = agent_overlays.upsert_overlay(
        pick_key="OIL", status="cleared", source="yuan", day="2026-11-10"
    )
    assert cleared["id"] == proposed["id"]
    assert cleared["effectiveStatus"] == "cleared"
    assert agent_overlays.find_same_key_overlay("OIL", day="2026-11-10") is None


def test_decision_contract_route(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "data_sync_service.api.v1_agent_routes.build_decision_contract",
        lambda trade_date=None: {"ok": True, "contractId": "dc:test"},
    )
    resp = client.get("/v1/agent/decision-contract")
    assert resp.status_code == 200
    assert resp.json()["contractId"] == "dc:test"


def test_overlays_route_shape(monkeypatch: pytest.MonkeyPatch, memory_settings) -> None:
    monkeypatch.setattr(
        "data_sync_service.api.v1_agent_routes.shanghai_today_iso", lambda: "2026-11-10"
    )
    monkeypatch.setattr("data_sync_service.service.agent_overlays._today_iso", lambda: "2026-11-10")
    resp = client.post(
        "/v1/agent/overlays",
        json={"pickKey": "OIL", "status": "proposed", "note": "test"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["overlay"]["pickKey"] == "OIL"
    assert body["overlays"]["proposed"][0]["pickKey"] == "OIL"

    got = client.get("/v1/agent/overlays?day=2026-11-10")
    assert got.status_code == 200
    assert got.json()["overlays"]["proposed"][0]["pickKey"] == "OIL"


def test_decision_log_route(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, Any] = {}

    def _insert(**kwargs: Any) -> dict[str, Any]:
        seen.update(kwargs)
        return {"id": 1, **kwargs}

    monkeypatch.setattr("data_sync_service.db.decision.insert_action", _insert)
    resp = client.post(
        "/v1/agent/decision-log",
        json={
            "tradeDate": "2026-11-10",
            "judgment": "破线卖油 → 逆回购；暂缓同键回买",
            "contractSnapshotId": "dc:2026-11-10:homeport_m30:SELL_TO_REPO:OIL",
            "adoptedOverlay": True,
        },
    )
    assert resp.status_code == 200
    assert resp.json()["ok"] is True
    assert seen["action"] == "hold_same_key_rebuy"
    assert seen["status"] == "logged"
    assert seen["outcome"]["adoptedOverlay"] is True


# ---------------------------------------------------------------------------
# judgment truth (2026-11-11): path mismatch / followability / fill clock / oil
# ---------------------------------------------------------------------------


def _bond_sleeve(**overrides: Any) -> dict[str, Any]:
    base = _sleeve(
        action="HOLD",
        label="持有",
        message="持有 10年国债ETF",
        symbol="ETF:511260",
        trailExit=False,
        pendingOpenSell=False,
        executeHint=None,
        pick={
            "key": "BOND10",
            "symbol": "ETF:511260",
            "name": "10年国债ETF",
            "mom60": 1.1,
            "close": 141.614,
            "ma200": 138.792,
            "above_ma200": True,
            "all_mom": {"OIL": 4.8, "BOND10": 1.1},
            "all_above": {"OIL": True, "BOND10": True},
        },
        etfPick={"key": "BOND10", "symbol": "ETF:511260"},
    )
    base.update(overrides)
    return base


def test_starship_b_with_harbor_sleeve_is_prominent_and_not_followable(
    monkeypatch: pytest.MonkeyPatch, memory_settings: dict[str, Any]
) -> None:
    monkeypatch.setattr(
        "data_sync_service.service.portfolio_health.build_portfolio_health",
        _fake_health(_bond_sleeve(), []),
    )
    monkeypatch.setattr(
        "data_sync_service.api.settings_routes.selected_strategy_mode", lambda: "starship_b"
    )
    _patch_factor_reports(monkeypatch)

    out = agent_contract.build_decision_contract(trade_date="2026-11-10")
    strat = out["strategy"]

    # The mismatch is spelled out, not left as sleeveRelevantToMode=false.
    assert strat["sleeveRelevantToMode"] is False
    path = strat["pathMismatch"]
    assert path["active"] is True
    assert "星舰 B" in path["message"]
    assert "港湾" in path["message"]
    assert "HOLD" in path["message"]
    assert "不是同一条跟单路径" in path["message"]

    # Cold starship factor → not followable, recommend 母港 M30 (advice only).
    follow = strat["followability"]
    assert follow["followable"] is False
    assert follow["status"] == "cold"
    assert "starship_b" in follow["reason"]
    assert follow["recommendedMode"] == "homeport"
    assert follow["recommendedModeLabel"] == "母港 M30"
    assert follow["recommendedTimeline"] == "homeport_m30"
    assert follow["adviceOnly"] is True

    # Close-basis fill clock is always present and never claims tradability.
    fill = out["fillClock"]
    assert fill["basis"] == "close"
    assert fill["closeBasis"] is True
    assert fill["tradableNow"] is False
    assert fill["label"]
    # Settings were not rewritten by the recommendation.
    assert not any("strategy" in str(key) for key in memory_settings)


def test_homeport_is_not_gated_by_cold_starship_reports(
    monkeypatch: pytest.MonkeyPatch, memory_settings: dict[str, Any]
) -> None:
    monkeypatch.setattr(
        "data_sync_service.service.portfolio_health.build_portfolio_health",
        _fake_health(_bond_sleeve(), []),
    )
    monkeypatch.setattr(
        "data_sync_service.api.settings_routes.selected_strategy_mode", lambda: "homeport"
    )
    _patch_factor_reports(monkeypatch)

    out = agent_contract.build_decision_contract(trade_date="2026-11-10")
    follow = out["strategy"]["followability"]
    assert follow["followable"] is True
    assert follow["status"] == "not_gated"
    assert follow["recommendedMode"] is None
    assert out["strategy"]["pathMismatch"]["active"] is False


def test_starship_followability_fails_closed_when_reports_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(agent_contract, "_read_report_json", lambda name: None)
    follow = agent_contract._followability("starship_b")
    assert follow["followable"] is False
    assert follow["status"] == "unknown"
    assert follow["recommendedMode"] == "homeport"


def test_fill_clock_mirrors_close_basis_timing() -> None:
    from datetime import datetime

    tz = agent_contract.SHANGHAI_TZ
    weekday = datetime(2026, 11, 11, 16, 0, tzinfo=tz)
    sell = agent_contract._fill_clock({"action": "SELL_TO_REPO"}, now=weekday)
    assert sell["label"] == "明日开盘卖"
    assert sell["tradableNow"] is False
    buy = agent_contract._fill_clock({"action": "BUY"}, now=weekday)
    assert buy["label"] == "明日开盘跟"
    pending = agent_contract._fill_clock({"pendingOpenSell": True}, now=weekday)
    assert pending["label"] == "收盘待确认 → 次日开盘卖"
    weekend = agent_contract._fill_clock(
        {"action": "BUY"}, now=datetime(2026, 11, 14, 10, 0, tzinfo=tz)
    )
    assert weekend["label"] == "下一交易日开盘跟"


def test_oil_whip_context_survives_pick_rotation_to_bond10(
    monkeypatch: pytest.MonkeyPatch, memory_settings: dict[str, Any]
) -> None:
    # OIL was stopped out yesterday; today's pick already rotated to BOND10.
    memory_settings[agent_whip.WHIP_MEMORY_KEY] = {"OIL": "2026-11-10"}
    monkeypatch.setattr(
        "data_sync_service.service.portfolio_health.build_portfolio_health",
        _fake_health(_bond_sleeve(), []),
    )
    monkeypatch.setattr(
        "data_sync_service.api.settings_routes.selected_strategy_mode", lambda: "harbor"
    )

    out = agent_contract.build_decision_contract(trade_date="2026-11-10")
    whip = out["whip"]
    assert whip["sameKeyRebuy"] is False
    assert "OIL" in whip["keys"]
    ctx = whip["context"]
    oil = ctx["oil"]
    assert oil["active"] is True
    assert oil["lastExitDate"] == "2026-11-10"
    assert "513350" in oil["message"]
    assert "BOND10" in oil["message"]
    assert "不要自动回买油" in oil["message"]
    assert "/v1/agent/overlays" in oil["message"]
    assert ctx["priority"] == "must_act_sell > same_key_rebuy"
    assert ctx["cooldownDays"] == 0
    assert ctx["engineCooldown"] is False
