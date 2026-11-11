"""Advice-only event / geo tag tests (no Postgres, no network).

The event-tag layer may recommend "defer this same-key rebuy" or "keep the
current mode", but it can never veto a triggered stop-loss / trail exit, change
the fill clock, or write the settings mode. Fail-open: no tags (or a tag below
the confidence threshold) keeps the exact Step-1 contract / brief shape.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from data_sync_service.main import app
from data_sync_service.service import agent_brief, agent_contract, agent_event_tags

client = TestClient(app)


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
            "all_mom": {"OIL": 4.8, "BOND10": 1.1},
            "all_above": {"OIL": True, "BOND10": True},
        },
        "etfPick": {"key": "OIL", "symbol": "ETF:513350"},
    }
    base.update(overrides)
    return base


def _neutral_sleeve() -> dict[str, Any]:
    return _sleeve(
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


def _fake_health(sleeve: dict[str, Any]) -> Any:
    def _build(*, trade_date: str | None = None, markets: tuple[str, ...] = ("CN",)) -> dict:
        return {
            "tradeDate": trade_date or "2026-11-10",
            "regime": "Strong",
            "sentiment": "hot",
            "panicCooldown": {"active": False},
            "circuitBlocked": False,
            "holdings": [{"symbol": "CN:600000", "action": "HOLD"}],
            "hkHealth": {"holdings": []},
            "s3Candidates": [],
            "scoreFresh": True,
            "multiAssetSleeve": sleeve,
            "multiAssetHoldings": [],
        }

    return _build


def _patch_contract(monkeypatch: pytest.MonkeyPatch, *, mode: str, sleeve: dict[str, Any]) -> None:
    monkeypatch.setattr(
        "data_sync_service.service.portfolio_health.build_portfolio_health",
        _fake_health(sleeve),
    )
    monkeypatch.setattr(
        "data_sync_service.api.settings_routes.selected_strategy_mode", lambda: mode
    )


def _patch_factor_reports(monkeypatch: pytest.MonkeyPatch) -> None:
    def _read(name: str) -> dict[str, Any] | None:
        if name == "factor_vault.json":
            return {
                "factors": [
                    {
                        "id": "starship_b",
                        "name": "星舰B默认",
                        "status": "cold",
                        "badge": "冷库",
                        "percentile": 2.2,
                        "net": -14.79,
                        "position": 0,
                    }
                ]
            }
        if name == "sgap_decay.json":
            return {
                "summary": {
                    "revival_pos": 0,
                    "latest_r40_mean": -14.79,
                    "latest_r40_percentile": 2.2,
                    "latest_r40_win_rate": 0.3,
                }
            }
        return None

    monkeypatch.setattr(agent_contract, "_read_report_json", _read)


def _patch_brief_env(monkeypatch: pytest.MonkeyPatch, *, mode: str, sleeve: dict[str, Any]) -> None:
    _patch_contract(monkeypatch, mode=mode, sleeve=sleeve)
    monkeypatch.setattr(
        "data_sync_service.service.dashboard.dashboard_summary",
        lambda **_: {
            "asOfDate": "2026-11-10",
            "marketSentiment": {
                "asOfDate": "2026-11-10",
                "items": [],
                "executionGate": {
                    "mode": "ATTACK",
                    "allowNewEntries": True,
                    "marketRegime": "Strong",
                    "indexLight": "green",
                    "reasons": [],
                    "hkGate": {
                        "mode": "HOLD_ONLY",
                        "allowNewEntries": False,
                        "marketRegime": "Diverging",
                        "indexLight": "yellow",
                        "reasons": [],
                    },
                },
            },
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
    monkeypatch.setattr("data_sync_service.db.paper_trading.list_paper_trades", lambda **kwargs: [])
    monkeypatch.setattr("data_sync_service.db.execution_journal.list_changes", lambda **kwargs: [])
    monkeypatch.setattr(
        "data_sync_service.db.execution_journal.fetch_latest_snapshot", lambda **kwargs: None
    )


# ---------------------------------------------------------------------------
# invariant: a triggered exit keeps its action + fill clock
# ---------------------------------------------------------------------------


def test_defer_same_key_rebuy_tag_does_not_change_exit_or_fill_clock(
    monkeypatch: pytest.MonkeyPatch, memory_settings: dict[str, Any]
) -> None:
    _patch_contract(monkeypatch, mode="homeport", sleeve=_sleeve())
    monkeypatch.setattr(agent_event_tags, "_today_iso", lambda: "2026-11-09")
    tag = agent_event_tags.record_event_tag(
        tag="oil_political_calendar",
        effect="advise_defer_same_key_rebuy",
        confidence=0.8,
        related_assets=["OIL", "ETF:513350"],
        sources=[{"url": "https://example.com/oil-calendar"}],
        valid_from="2026-10-15",
        valid_until="2026-11-20",
        note="中东风 + 美国政治日历",
    )
    assert tag["effectiveStatus"] == "active"

    out = agent_contract.build_decision_contract(trade_date="2026-11-09")
    assert out["sleeve"]["action"] == "SELL_TO_REPO"
    assert out["sleeve"]["trailExit"] is True
    assert out["fillClock"]["kind"] == "close-sell-confirmed"
    assert out["fillClock"]["tradableNow"] is False
    # The tag is attached, but the clock equals the untouched close-basis clock.
    assert out["fillClock"] == agent_contract._fill_clock(
        {"action": "SELL_TO_REPO", "trailExit": True}
    )
    assert any(t["effect"] == "advise_defer_same_key_rebuy" for t in out["eventTags"]["tags"])
    assert out["eventTags"]["neverVetoesExits"] is True


def test_invariant_rejects_a_forbidden_effect() -> None:
    sleeve = {"action": "SELL_TO_REPO", "trailExit": True, "pendingOpenSell": False}
    clock = agent_contract._fill_clock(sleeve)
    with pytest.raises(ValueError):
        agent_event_tags.assert_event_tags_never_override_exit(
            sleeve=sleeve, fill_clock=clock, tags=[{"effect": "veto_exit"}]
        )
    # A legitimate advice tag passes untouched.
    agent_event_tags.assert_event_tags_never_override_exit(
        sleeve=sleeve,
        fill_clock=clock,
        tags=[{"effect": "advise_defer_same_key_rebuy"}],
    )


# ---------------------------------------------------------------------------
# fail-open: no tags / low confidence = Step-1 shape
# ---------------------------------------------------------------------------


def test_no_tags_keeps_step1_contract_and_brief_shape(
    monkeypatch: pytest.MonkeyPatch, memory_settings: dict[str, Any]
) -> None:
    _patch_brief_env(monkeypatch, mode="homeport", sleeve=_neutral_sleeve())

    contract = agent_contract.build_decision_contract(trade_date="2026-11-10")
    assert "eventTags" not in contract
    assert "geoRegime" not in contract

    brief = agent_brief.build_judgment_brief(trade_date="2026-11-10", mode="compact")
    assert "eventTags" not in brief
    assert "eventTagCount" not in brief["judgment"]
    md = agent_brief.render_judgment_brief_markdown(brief, compact=True)
    assert "事件/地缘标签" not in md
    assert "不影响已触发的卖出" not in md


def test_low_confidence_tag_is_ignored(
    monkeypatch: pytest.MonkeyPatch, memory_settings: dict[str, Any]
) -> None:
    _patch_brief_env(monkeypatch, mode="homeport", sleeve=_neutral_sleeve())
    agent_event_tags.record_event_tag(
        tag="weak_rumor",
        effect="advise_defer_same_key_rebuy",
        confidence=0.3,
        related_assets=["OIL"],
        sources=[{"url": "https://example.com/rumor"}],
    )
    contract = agent_contract.build_decision_contract(trade_date="2026-11-10")
    assert "eventTags" not in contract
    brief = agent_brief.build_judgment_brief(trade_date="2026-11-10", mode="compact")
    assert "eventTags" not in brief


# ---------------------------------------------------------------------------
# naming: geo regime is NOT market_regime Weak/Strong
# ---------------------------------------------------------------------------


def test_geo_regime_does_not_collide_with_market_regime(
    monkeypatch: pytest.MonkeyPatch, memory_settings: dict[str, Any]
) -> None:
    _patch_contract(monkeypatch, mode="homeport", sleeve=_neutral_sleeve())
    monkeypatch.setattr(agent_event_tags, "_today_iso", lambda: "2026-11-10")
    agent_event_tags.record_event_tag(
        tag="oil_political_calendar",
        effect="advise_defer_same_key_rebuy",
        confidence=0.8,
        related_assets=["OIL", "ETF:513350"],
        sources=[{"url": "https://example.com/oil-calendar"}],
    )
    out = agent_contract.build_decision_contract(trade_date="2026-11-10")
    geo = out["geoRegime"]
    assert geo["active"] is True
    assert geo["label"] == "oil_political"
    assert geo["notMarketRegime"] is True
    assert "regime" not in geo
    assert geo.get("label") not in {"Weak", "Strong"}
    assert not any("regime" in tag for tag in out["eventTags"]["tags"])
    # The S-3 gate value stays where it belongs.
    assert out["health"]["regime"] == "Strong"
    assert out["eventTags"]["tags"][0]["effect"] == "advise_defer_same_key_rebuy"


# ---------------------------------------------------------------------------
# POST + brief surfacing
# ---------------------------------------------------------------------------


def test_posted_sourced_tag_appears_in_contract_and_brief(
    monkeypatch: pytest.MonkeyPatch, memory_settings: dict[str, Any]
) -> None:
    _patch_brief_env(monkeypatch, mode="homeport", sleeve=_neutral_sleeve())
    monkeypatch.setattr(
        "data_sync_service.api.v1_agent_routes.shanghai_today_iso", lambda: "2026-11-10"
    )
    monkeypatch.setattr(agent_event_tags, "_today_iso", lambda: "2026-11-10")

    resp = client.post(
        "/v1/agent/event-tags",
        json={
            "tag": "oil_political_calendar",
            "effect": "advise_defer_same_key_rebuy",
            "relatedAssets": ["OIL", "ETF:513350"],
            "confidence": 0.72,
            "sources": [
                {
                    "url": "https://example.com/report",
                    "title": "事件报道",
                }
            ],
            "validFrom": "2026-10-15",
            "validUntil": "2026-11-20",
            "note": "仅建议暂缓回买",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["eventTag"]["sources"][0]["url"] == "https://example.com/report"

    contract = agent_contract.build_decision_contract(trade_date="2026-11-10")
    ids = [t["id"] for t in contract["eventTags"]["tags"]]
    assert body["eventTag"]["id"] in ids

    brief = agent_brief.build_judgment_brief(trade_date="2026-11-10", mode="compact")
    assert brief["briefVersion"] == agent_brief.BRIEF_VERSION
    assert any(t["id"] == body["eventTag"]["id"] for t in brief["eventTags"]["tags"])
    md = agent_brief.render_judgment_brief_markdown(brief, compact=True)
    assert "### 事件/地缘标签 event tags（仅建议）" in md
    assert "oil_political_calendar" in md
    assert "advise_defer_same_key_rebuy" in md
    assert "不影响已触发的卖出" in md


def test_forbidden_effect_and_missing_source_are_rejected(
    monkeypatch: pytest.MonkeyPatch, memory_settings: dict[str, Any]
) -> None:
    monkeypatch.setattr(
        "data_sync_service.api.v1_agent_routes.shanghai_today_iso", lambda: "2026-11-10"
    )
    veto = client.post(
        "/v1/agent/event-tags",
        json={
            "tag": "no_sell_today",
            "effect": "veto_exit",
            "confidence": 0.9,
            "sources": [{"url": "https://example.com/x"}],
        },
    )
    assert veto.status_code == 422
    assert "forbidden" in veto.json()["detail"]

    unsourced = client.post(
        "/v1/agent/event-tags",
        json={
            "tag": "no_source",
            "effect": "advise_strategy",
            "confidence": 0.9,
            "sources": [],
        },
    )
    assert unsourced.status_code == 422
    assert "source" in unsourced.json()["detail"]

    with pytest.raises(ValueError):
        agent_event_tags.record_event_tag(
            tag="x",
            effect="delay_exit",
            confidence=0.9,
            sources=[{"url": "https://example.com/x"}],
        )


# ---------------------------------------------------------------------------
# strategy advice: advice-only, confirmation required
# ---------------------------------------------------------------------------


def test_strategy_advice_is_advice_only_and_requires_confirmation(
    monkeypatch: pytest.MonkeyPatch, memory_settings: dict[str, Any]
) -> None:
    _patch_contract(monkeypatch, mode="starship_b", sleeve=_neutral_sleeve())
    _patch_factor_reports(monkeypatch)

    out = agent_contract.build_decision_contract(trade_date="2026-11-10")
    advice = out["eventTags"]["strategyAdvice"]
    assert advice["advice"] == "switch_after_confirmation"
    assert advice["currentMode"] == "starship_b"
    assert advice["recommendedMode"] == "homeport"
    assert advice["adviceOnly"] is True
    assert advice["settingsUnchanged"] is True
    assert advice["requiresConfirmation"] is True
    assert "确认" in advice["confirmationNote"]
    assert "/settings/strategy-mode" in advice["confirmationNote"]
    # The advice tag is advice only.
    tags = {t["effect"]: t for t in out["eventTags"]["tags"]}
    assert tags["advise_strategy"]["changesAction"] is False
    # No settings write happened.
    assert "strategy_mode" not in memory_settings
