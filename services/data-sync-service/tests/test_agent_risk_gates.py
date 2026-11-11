"""Universal stop coverage + same-key rebuy gates (2026-11-11, maturity ④).

Display / advice only: the brief and the 14:30 table must say when a risky
holding / proposed buy has no stop, and must flag a rebuy into a parking key
that was just trailed out. Must-act SELL outranks the rebuy; an event / geo tag
can never veto an exit nor swallow the missing-stop warning.
"""

from __future__ import annotations

from typing import Any

import pytest

from data_sync_service.service import (
    agent_brief,
    agent_contract,
    agent_event_tags,
    agent_overlays,
    agent_whip,
)
from data_sync_service.service.agent_risk_gates import (
    GATE_PRIORITY,
    NO_STOP_MESSAGE,
    build_risk_gates,
    is_cash_like,
    is_risky_asset,
    parking_rule,
    stop_coverage,
)
from data_sync_service.service.daily_adjust import NO_CHANGE_TEXT, compose_daily_adjust


@pytest.fixture(autouse=True)
def _reset_brief_cache() -> Any:
    agent_brief.reset_judgment_brief_cache()
    yield
    agent_brief.reset_judgment_brief_cache()


@pytest.fixture()
def memory_settings(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    store: dict[str, Any] = {}
    monkeypatch.setattr(
        "data_sync_service.db.app_settings.get_setting", lambda k, d=None: store.get(k, d)
    )
    monkeypatch.setattr(
        "data_sync_service.db.app_settings.set_setting", lambda k, v: store.__setitem__(k, v)
    )
    return store


# ---------------------------------------------------------------------------
# unit: risk classification + stop coverage
# ---------------------------------------------------------------------------


def test_risky_vs_cash_like_classification() -> None:
    assert is_risky_asset(symbol="CN:600000")
    assert is_risky_asset(symbol="HK:00700")
    assert is_risky_asset(symbol="ETF:518880")
    assert is_risky_asset(symbol="513350")
    assert is_risky_asset(symbol="000978.SZ")
    assert is_risky_asset(key="GOLD")
    # 逆回购 / cash is never flagged as missing a stop.
    assert is_cash_like(symbol="REPO", key="REPO")
    assert is_cash_like(name="逆回购")
    assert not is_risky_asset(symbol="REPO", key="REPO")
    assert not is_risky_asset(name="现金")


def test_parking_leg_is_covered_by_the_engine_trail() -> None:
    from data_sync_service.service.harbor import TRAIL_PCT

    # Do not edit the constant: the gate must read it, not invent a new stop.
    assert TRAIL_PCT == 8.0
    rule = parking_rule(symbol="ETF:513350")
    assert rule is not None
    assert rule["trailPct"] == TRAIL_PCT
    assert rule["stopRule"] == "harbor_trail_8pct"

    out = stop_coverage(
        holdings=[
            {"symbol": "ETF:513350", "key": "OIL", "name": "富国油气QDII"},
            {"symbol": "ETF:511260", "key": "BOND10", "stopRule": "harbor_trail"},
        ]
    )
    assert out["status"] == "ok"
    assert out["missingCount"] == 0
    assert not out["warnings"]


def test_missing_stop_is_warned_for_holding_and_proposed_buy() -> None:
    out = stop_coverage(
        holdings=[
            {"symbol": "CN:600000", "name": "测试股"},
            {"symbol": "HK:00700", "name": "腾讯", "stopLossLine": 300},
        ],
        proposed_buys=[{"symbol": "CN:000001", "side": "BUY"}],
    )
    assert out["status"] == "warning"
    assert out["missingCount"] == 2
    assert out["checked"] == 3
    missing = {r["symbol"] for r in out["missing"]}
    assert missing == {"CN:600000", "CN:000001"}
    assert all(NO_STOP_MESSAGE in w for w in out["warnings"])
    # A stop line anywhere counts; the covered row is still listed, not missing.
    assert all(r["hasStop"] for r in out["rows"] if r["symbol"] == "HK:00700")


def test_build_risk_gates_is_stable_and_advice_only() -> None:
    gates = build_risk_gates(
        whip={"sameKeyRebuy": False, "recentExits": {"OIL": "2026-11-10"}, "pickKey": "BOND10"},
        holdings=[{"symbol": "CN:600000"}],
    )
    assert gates["available"] is True
    assert gates["adviceOnly"] is True
    assert gates["priority"] == GATE_PRIORITY == "must_act_sell > same_key_rebuy"
    assert gates["eventTagsNeverVetoExits"] is True
    assert gates["stops"]["missingCount"] == 1
    assert gates["sameKeyRebuy"]["recentExits"] == {"OIL": "2026-11-10"}


# ---------------------------------------------------------------------------
# 14:30 table: same-key rebuy annotation (visible, not deleted)
# ---------------------------------------------------------------------------


def test_daily_adjust_flags_same_key_rebuy_but_keeps_must_act_sell_first(
    memory_settings: dict[str, Any],
) -> None:
    # OIL was trailed out yesterday; today the parking leg rotates GOLD → OIL.
    memory_settings[agent_whip.WHIP_MEMORY_KEY] = {"OIL": "2026-11-10"}
    sleeve = {
        "active": True,
        "action": "ROTATE",
        "message": "港湾停车轮动：卖出 ETF:518880 → ETF:513350",
        "symbol": "ETF:518880",
        "pick": {"key": "OIL", "symbol": "ETF:513350"},
        "executeHint": "次日开盘执行轮动",
    }
    out = compose_daily_adjust("homeport", trade_date="2026-11-11", sleeve=sleeve)

    sides = [a["side"] for a in out["adjustments"]]
    assert sides == ["SELL", "BUY"]  # must-act sell is still first
    assert out["mustAct"] is True
    assert out["adjustments"][0]["symbol"] == "ETF:518880"
    buy = out["adjustments"][1]
    assert buy["symbol"] == "ETF:513350"
    assert buy["sameKeyRebuy"] is True
    assert buy["deferRebuy"] is True
    assert buy["annotation"] == agent_whip.WHIP_ANNOTATION
    joined = "\n".join(out["lines"])
    assert "刚止损，回买=鞭打，请确认" in joined
    assert "ETF:513350" in joined
    assert out["riskGates"]["sameKeyRebuy"]["proposedRebuys"]


def test_daily_adjust_accepts_the_overlay_copy(memory_settings: dict[str, Any]) -> None:
    memory_settings[agent_whip.WHIP_MEMORY_KEY] = {"OIL": "2026-11-10"}
    agent_overlays.upsert_overlay(pick_key="OIL", status="accepted", day="2026-11-11")
    out = compose_daily_adjust(
        "harbor",
        trade_date="2026-11-11",
        sleeve={
            "active": True,
            "action": "BUY",
            "message": "闲置现金停入 OIL",
            "symbol": "ETF:513350",
            "pick": {"key": "OIL", "symbol": "ETF:513350"},
        },
    )
    buy = out["adjustments"][0]
    assert buy["side"] == "BUY"
    assert buy["annotation"] == agent_whip.WHIP_OVERLAY_COPY
    assert "建议暂缓 · 需确认" in "\n".join(out["lines"])


def test_daily_adjust_pending_trail_sell_is_not_replaced_by_a_rebuy(
    memory_settings: dict[str, Any],
) -> None:
    memory_settings[agent_whip.WHIP_MEMORY_KEY] = {"OIL": "2026-11-10"}
    out = compose_daily_adjust(
        "homeport",
        trade_date="2026-11-11",
        sleeve={
            "active": True,
            "action": "SELL_TO_REPO",
            "message": "ETF:513350峰值回撤8.6% ≥8% → 次日开盘卖转逆回购",
            "symbol": "ETF:513350",
            "trailExit": True,
            "pendingOpenSell": True,
            "signalDate": "2026-11-10",
            "pick": {"key": "OIL", "symbol": "ETF:513350"},
        },
    )
    assert out["adjustments"][0]["side"] == "SELL"
    assert out["adjustments"][0]["to"] == "逆回购"
    assert out["mustAct"] is True
    assert "卖出" in out["lines"][0]
    # No BUY row was invented for the same key.
    assert all(a["side"] != "BUY" for a in out["adjustments"])


# ---------------------------------------------------------------------------
# 14:30 table: universal stop coverage
# ---------------------------------------------------------------------------


def test_daily_adjust_warns_missing_stop_without_flipping_no_change() -> None:
    out = compose_daily_adjust(
        "homeport",
        trade_date="2026-11-11",
        sleeve={
            "active": True,
            "action": "HOLD",
            "message": "港湾停车：持有 ETF:511260",
            "holding": True,
            "symbol": "ETF:511260",
            "pick": {"key": "BOND10", "symbol": "ETF:511260"},
        },
        holdings=[{"symbol": "CN:600000", "name": "测试股"}],
    )
    joined = "\n".join(out["lines"])
    assert out["adjustments"] == []
    assert out["noChange"] is True
    assert out["mustAct"] is False
    assert out["lines"][0] == NO_CHANGE_TEXT  # still no action required
    assert "止损缺口" in joined
    assert NO_STOP_MESSAGE in joined
    assert out["riskGates"]["stops"]["missingCount"] == 1
    # The parking ETF itself is covered by the Harbor trail, not flagged.
    assert {r["symbol"] for r in out["riskGates"]["stops"]["missing"]} == {"CN:600000"}


def test_daily_adjust_never_flags_repo_cash() -> None:
    out = compose_daily_adjust(
        "homeport",
        trade_date="2026-11-11",
        sleeve={
            "active": True,
            "action": "HOLD",
            "message": "空仓：闲置现金在逆回购",
            "holding": True,
            "symbol": "REPO",
            "pick": {"key": "REPO", "symbol": "REPO"},
        },
    )
    assert out["riskGates"]["stops"]["missingCount"] == 0
    assert "止损缺口" not in "\n".join(out["lines"])


def test_daily_adjust_flags_a_satellite_buy_without_a_stop() -> None:
    panel = {
        "tradeDate": "2026-11-11",
        "decisionAvailable": True,
        "gateOpen": True,
        "breadth1430": 0.4,
        "exits": [],
        "heldLegs": [],
        "ranked": [{"ts": "512000.SH", "inBucket": True, "fillable": True}],
    }
    out = compose_daily_adjust("starship", trade_date="2026-11-11", panel=panel)
    buys = [a for a in out["adjustments"] if a["side"] == "BUY"]
    assert buys and buys[0]["symbol"] == "512000.SH"
    assert buys[0]["stopMissing"] is True
    assert NO_STOP_MESSAGE in buys[0]["stopWarning"]
    assert "止损缺口" in "\n".join(out["lines"])


def test_whip_state_never_writes_memory_from_the_composer(
    memory_settings: dict[str, Any],
) -> None:
    memory_settings[agent_whip.WHIP_MEMORY_KEY] = {"OIL": "2026-11-10"}
    compose_daily_adjust(
        "homeport",
        trade_date="2026-11-11",
        sleeve={
            "active": True,
            "action": "HOLD",
            "message": "持有",
            "holding": True,
            "symbol": "ETF:511260",
            "pick": {"key": "BOND10", "symbol": "ETF:511260"},
        },
    )
    # observe=False: composing text must not persist a new exit date.
    assert memory_settings[agent_whip.WHIP_MEMORY_KEY] == {"OIL": "2026-11-10"}


# ---------------------------------------------------------------------------
# event tags can never veto an exit or swallow the stop warning
# ---------------------------------------------------------------------------


def _patch_contract_env(monkeypatch: pytest.MonkeyPatch, sleeve: dict[str, Any]) -> None:
    monkeypatch.setattr(
        "data_sync_service.service.portfolio_health.build_portfolio_health",
        lambda **kw: {
            "tradeDate": kw.get("trade_date") or "2026-11-11",
            "regime": "Strong",
            "sentiment": "hot",
            "panicCooldown": {"active": False},
            "circuitBlocked": False,
            # No stop fields on this holding → the gate must warn.
            "holdings": [{"symbol": "CN:600000", "name": "测试股", "action": "HOLD"}],
            "hkHealth": {"holdings": []},
            "s3Candidates": [],
            "scoreFresh": True,
            "multiAssetSleeve": sleeve,
            "multiAssetHoldings": [],
        },
    )
    monkeypatch.setattr(
        "data_sync_service.api.settings_routes.selected_strategy_mode", lambda: "homeport"
    )


def test_geo_tag_cannot_veto_trail_or_hide_missing_stop(
    monkeypatch: pytest.MonkeyPatch, memory_settings: dict[str, Any]
) -> None:
    sleeve = {
        "active": True,
        "action": "SELL_TO_REPO",
        "label": "卖出转repo(止损)",
        "message": "ETF:513350峰值回撤8.5% ≥8% → 次日开盘卖转逆回购",
        "holding": True,
        "symbol": "ETF:513350",
        "trailExit": True,
        "pendingOpenSell": False,
        "signalDate": "2026-11-10",
        "pick": {"key": "OIL", "symbol": "ETF:513350", "name": "富国油气QDII"},
    }
    _patch_contract_env(monkeypatch, sleeve)
    monkeypatch.setattr(agent_event_tags, "_today_iso", lambda: "2026-11-11")
    agent_event_tags.record_event_tag(
        tag="oil_political_calendar",
        effect="advise_defer_same_key_rebuy",
        confidence=0.8,
        related_assets=["OIL", "ETF:513350"],
        sources=[{"url": "https://example.com/oil-calendar"}],
        note="中东风",
    )

    contract = agent_contract.build_decision_contract(trade_date="2026-11-11")
    assert contract["sleeve"]["action"] == "SELL_TO_REPO"
    assert contract["sleeve"]["trailExit"] is True
    assert contract["fillClock"]["tradableNow"] is False
    assert contract["eventTags"]["neverVetoesExits"] is True
    assert any(t["effect"] == "advise_defer_same_key_rebuy" for t in contract["eventTags"]["tags"])

    # Render path: the alert block keeps the sell clock, the rebuy advice and
    # the missing-stop warning side by side.
    gates = build_risk_gates(
        whip=contract["whip"],
        holdings=[{"symbol": "CN:600000", "name": "测试股"}],
    )
    md = "\n".join(
        agent_brief._render_alerts(
            {
                "contract": contract,
                "riskGates": gates,
                "eventTags": contract["eventTags"],
                "geoRegime": contract["geoRegime"],
            }
        )
    )
    assert "可成交时钟" in md
    assert "同键回买" in md
    assert "止损缺口" in md
    assert NO_STOP_MESSAGE in md
    assert "不影响已触发的卖出" in md


def test_whip_context_is_generalized_beyond_oil(
    memory_settings: dict[str, Any],
) -> None:
    whip = {
        "sameKeyRebuy": False,
        "keys": ["OIL", "GOLD"],
        "pickKey": "NASDAQ",
        "recentExits": {"OIL": "2026-11-10", "GOLD": "2026-11-09"},
    }
    ctx = agent_contract._whip_context(whip, {"pick": {"key": "NASDAQ", "symbol": "ETF:513100"}})
    by_key = ctx["rebuy"]["byKey"]
    assert set(by_key) == {"OIL", "GOLD"}
    assert by_key["GOLD"]["symbol"] == "ETF:518880"
    assert "不要自动回买黄金" in by_key["GOLD"]["message"]
    assert "NASDAQ" in by_key["GOLD"]["message"]
    # The OIL alias is preserved for today's 513350 case.
    assert ctx["oil"]["active"] is True
    assert "不要自动回买油" in ctx["oil"]["message"]
    assert ctx["cooldownDays"] == 0  # parking cooldown stays k=0


# ---------------------------------------------------------------------------
# judgment brief wiring: one riskGates block + shared 14:30 table
# ---------------------------------------------------------------------------


def _patch_brief_env(monkeypatch: pytest.MonkeyPatch, sleeve: dict[str, Any]) -> None:
    _patch_contract_env(monkeypatch, sleeve)
    monkeypatch.setattr(
        "data_sync_service.service.dashboard.dashboard_summary",
        lambda **_: {
            "asOfDate": "2026-11-11",
            "marketSentiment": {"asOfDate": "2026-11-11", "items": [], "executionGate": {}},
            "macroSnapshot": {"cnIndexSignals": [], "macro": []},
            "industryFundFlow": {},
            "news": {"hours": 24, "total": 0, "items": []},
        },
    )
    monkeypatch.setattr("data_sync_service.api.health_routes.datasource_freshness", lambda: [])
    monkeypatch.setattr("data_sync_service.api.backtest_routes.backtest_overview", lambda: {})
    monkeypatch.setattr("data_sync_service.service.strategy_catalog.strategy_catalog", lambda: [])
    monkeypatch.setattr(
        "data_sync_service.api.backtest_routes.warm_timeline_recent_stats",
        lambda strategy, sessions=20: None,
    )
    monkeypatch.setattr(agent_brief, "_live_quotes", lambda symbols: {"available": False})
    monkeypatch.setattr(agent_brief, "_decision_log_block", lambda *a, **k: {"available": False})
    monkeypatch.setattr("data_sync_service.db.paper_trading.list_paper_trades", lambda **kw: [])
    monkeypatch.setattr("data_sync_service.db.execution_journal.list_changes", lambda **kw: [])
    monkeypatch.setattr(
        "data_sync_service.db.execution_journal.fetch_latest_snapshot", lambda **kw: None
    )


def test_brief_carries_risk_gates_and_shares_the_daily_table(
    monkeypatch: pytest.MonkeyPatch, memory_settings: dict[str, Any]
) -> None:
    sleeve = {
        "active": True,
        "action": "HOLD",
        "label": "持有",
        "message": "持有 10年国债ETF",
        "holding": True,
        "symbol": "ETF:511260",
        "trailExit": False,
        "pendingOpenSell": False,
        "pick": {"key": "BOND10", "symbol": "ETF:511260", "name": "10年国债ETF"},
    }
    _patch_brief_env(monkeypatch, sleeve)

    brief = agent_brief.build_judgment_brief(trade_date="2026-11-11", mode="compact")
    gates = brief["riskGates"]
    assert gates["priority"] == "must_act_sell > same_key_rebuy"
    assert gates["eventTagsNeverVetoExits"] is True
    assert gates["adviceOnly"] is True
    # The stop-less CN holding is warned; the parking ETF is covered by trail.
    assert gates["stops"]["missingCount"] == 1
    assert gates["stops"]["missing"][0]["symbol"] == "CN:600000"
    assert brief["judgment"]["riskGates"]["stopMissingCount"] == 1
    # The 14:30 table is composed for the SELECTED mode and shares the warning.
    daily = gates["dailyAdjust"]
    assert daily["mode"] == "homeport"
    assert daily["noChange"] is True
    assert "止损缺口" in "\n".join(daily["lines"])

    md = agent_brief.render_judgment_brief_markdown(brief, compact=True)
    alerts = md.split("## 1. 契约 contract")[0]
    assert "止损缺口" in alerts
    assert NO_STOP_MESSAGE in alerts
    assert "该资产没有止损" in md
