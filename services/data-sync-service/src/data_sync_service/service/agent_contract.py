"""Agent decision contract — one stable read for Karios' AI assistant.

The contract packages the facts the decision agent used to assemble by hand
from six endpoints: the selected strategy mode (and the timeline key it maps
to), the Harbor parking sleeve, the S-3 portfolio-health summary, the next
parking candidate (with its MA200 gate), the same-key rebuy whip state and the
effective overlays.

Read-only. It never changes ``sleeve.action``, ``TRAIL_PCT``, the parking
cooldown (k=0) or any strategy weight. The authority order is:

1. frozen engine exits (trail / stop / expiry)
2. selected strategy-mode target + execution clock
3. human-confirmed overlays
4. sourced regime tags / strategy suggestions (P1)
5. chat narrative
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from data_sync_service.service.agent_overlays import group_overlays, list_overlays
from data_sync_service.service.agent_whip import evaluate_whip

logger = logging.getLogger(__name__)

# Settings mode -> Timeline strategy key. The trap: settings ``homeport`` is
# the product M30 tier, whose timeline key is ``homeport_m30``. The timeline's
# ``homeport`` key is the frozen M50 base (starport's base) — never use it for
# the product mode.
MODE_TO_TIMELINE: dict[str, str] = {
    "harbor": "harbor",
    "homeport": "homeport_m30",
    "starport": "starport",
    "starship": "starship",
    "starship_robust": "starship_robust",
    "starship_b": "starship_b",
    "twin_star": "twin_star",
}
MODE_LABELS: dict[str, str] = {
    "harbor": "港湾",
    "homeport": "母港 M30",
    "starport": "星港",
    "starship": "星舰",
    "starship_robust": "稳健星舰 H2-a25",
    "starship_b": "星舰 B",
    "twin_star": "双子星",
}
TIMELINE_LABELS: dict[str, str] = {
    "harbor": "港湾",
    "homeport": "母港 M50",
    "homeport_m30": "母港 M30",
    "starport": "星港",
    "starship": "星舰",
    "starship_robust": "稳健星舰 H2-a25",
    "starship_b": "星舰 B",
    "twin_star": "双子星",
}
# Modes whose parking leg is the Harbor ETF sleeve. The starship family parks
# idle cash in H2/B3 instead; the sleeve block is then a base-leg reference.
SLEEVE_MODES = frozenset({"harbor", "homeport", "starport", "twin_star"})
MAPPING_NOTE = (
    "settings mode 'homeport' is the product M30 tier; its timeline key is "
    "'homeport_m30'. The timeline key 'homeport' is the frozen M50 base."
)
AUTHORITY = [
    "frozen_engine_exits",
    "selected_strategy_mode_target",
    "accepted_overlays",
    "regime_tags_and_suggestions",
    "chat_narrative",
]


def timeline_strategy_for_mode(mode: str | None) -> str:
    return MODE_TO_TIMELINE.get(str(mode or ""), "homeport_m30")


def _today_iso() -> str:
    from data_sync_service.service.trade_calendar_utils import shanghai_today_iso

    return shanghai_today_iso()


def _as_bool(value: Any) -> bool:
    return bool(value)


def _health_summary(health: dict[str, Any]) -> dict[str, Any]:
    hk = health.get("hkHealth") or {}
    cn_holdings = health.get("holdings") or []
    hk_holdings = (hk.get("holdings") or []) if isinstance(hk, dict) else []
    parking_holdings = health.get("multiAssetHoldings") or []
    exits: list[str] = []
    for h in list(cn_holdings) + list(hk_holdings) + list(parking_holdings):
        if (
            str(h.get("action") or "").upper() == "EXIT"
            or h.get("trailExit")
            or h.get("pendingOpenSell")
        ):
            sym = str(h.get("symbol") or "")
            if sym:
                exits.append(sym)
    panic = health.get("panicCooldown") or {}
    regime = health.get("regime")
    circuit = _as_bool(health.get("circuitBlocked"))
    gate_open = regime in ("Strong", "Diverging") and not panic.get("active") and not circuit
    return {
        "tradeDate": health.get("tradeDate"),
        "regime": regime,
        "strength": health.get("strength"),
        "sentiment": health.get("sentiment"),
        "panicActive": bool(panic.get("active")),
        "circuitBlocked": circuit,
        "gateOpen": gate_open,
        "holdingsCount": len(cn_holdings) + len(hk_holdings) + len(parking_holdings),
        "cnHoldingsCount": len(cn_holdings),
        "hkHoldingsCount": len(hk_holdings),
        "parkingHoldingsCount": len(parking_holdings),
        "exits": exits,
        "candidateCount": len(health.get("s3Candidates") or []),
        "scoreFresh": health.get("scoreFresh"),
    }


def _next_leg(sleeve: dict[str, Any]) -> dict[str, Any]:
    pick = sleeve.get("pick") or {}
    if not pick:
        return {
            "key": None,
            "symbol": None,
            "name": None,
            "mom60": None,
            "close": None,
            "ma200": None,
            "aboveMa200": None,
            "reason": "no_candidate_above_ma200",
            "candidates": [],
        }
    all_mom = pick.get("all_mom") or {}
    all_above = pick.get("all_above") or {}
    candidates = [
        {
            "key": key,
            "mom60": value,
            "aboveMa200": bool(all_above.get(key)),
            "eligible": bool(all_above.get(key)),
        }
        for key, value in all_mom.items()
    ]
    candidates.sort(key=lambda c: (c["eligible"], c["mom60"] or -1e9), reverse=True)
    return {
        "key": pick.get("key"),
        "symbol": pick.get("symbol"),
        "name": pick.get("name"),
        "mom60": pick.get("mom60"),
        "close": pick.get("close"),
        "ma200": pick.get("ma200"),
        "aboveMa200": bool(pick.get("above_ma200")),
        "reason": "mom60_argmax_above_ma200",
        "candidates": candidates,
    }


def _sleeve_summary(sleeve: dict[str, Any]) -> dict[str, Any]:
    action = str(sleeve.get("action") or "NONE")
    pick = sleeve.get("pick") or {}
    return {
        "action": action,
        "label": sleeve.get("label"),
        "message": sleeve.get("message"),
        "pickKey": pick.get("key"),
        "pick": pick or None,
        "etfPick": sleeve.get("etfPick") or None,
        "trailExit": bool(sleeve.get("trailExit")),
        "pendingOpenSell": bool(sleeve.get("pendingOpenSell")),
        "sellToRepo": action == "SELL_TO_REPO",
        "symbol": sleeve.get("symbol"),
        "signalDate": sleeve.get("signalDate"),
        "executeHint": sleeve.get("executeHint"),
        "intradayWarn": sleeve.get("intradayWarn"),
        "holding": bool(sleeve.get("holding")),
        "hystBlocked": bool(sleeve.get("hystBlocked")),
        "idlePct": sleeve.get("idlePct"),
        "parkPct": sleeve.get("parkPct"),
    }


def build_decision_contract(*, trade_date: str | None = None) -> dict[str, Any]:
    """Assemble the agent contract for ``trade_date`` (default: today)."""
    from data_sync_service.api.settings_routes import selected_strategy_mode
    from data_sync_service.service.portfolio_health import build_portfolio_health

    day = str(trade_date or _today_iso())[:10]
    mode = selected_strategy_mode()
    timeline_key = timeline_strategy_for_mode(mode)
    health = build_portfolio_health(trade_date=day, markets=("CN", "HK"))
    sleeve = health.get("multiAssetSleeve") or {}
    holdings = health.get("multiAssetHoldings") or []
    whip = health.get("whip")
    if not isinstance(whip, dict):
        whip = evaluate_whip(sleeve, holdings, day=day, observe=True)
    overlays = health.get("overlays")
    if not isinstance(overlays, dict):
        overlays = group_overlays(list_overlays(day=day))

    action = str(sleeve.get("action") or "NONE")
    pick_key = (sleeve.get("pick") or {}).get("key") or "REPO"
    contract_id = f"dc:{day}:{timeline_key}:{action}:{pick_key}"
    return {
        "ok": True,
        "contractId": contract_id,
        "asOfDate": day,
        "generatedAt": datetime.now(UTC).isoformat(),
        "strategy": {
            "mode": mode,
            "timelineStrategy": timeline_key,
            "label": MODE_LABELS.get(mode, mode),
            "timelineLabel": TIMELINE_LABELS.get(timeline_key, timeline_key),
            "mappingNote": MAPPING_NOTE,
            "sleeveRelevantToMode": mode in SLEEVE_MODES,
        },
        "sleeve": _sleeve_summary(sleeve),
        "health": _health_summary(health),
        "nextLeg": _next_leg(sleeve),
        "whip": whip,
        "overlays": overlays,
        "authority": AUTHORITY,
    }
