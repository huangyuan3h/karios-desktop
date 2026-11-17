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

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from data_sync_service.service.agent_event_tags import (
    assert_event_tags_never_override_exit,
    build_event_tag_block,
)
from data_sync_service.service.agent_overlays import group_overlays, list_overlays
from data_sync_service.service.agent_whip import evaluate_whip

logger = logging.getLogger(__name__)

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")

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

# Modes whose economics depend on the S-gap / starship cold-storage factor.
# These are the only modes that get a factor-vault followability gate; the
# product baseline (homeport M30) and the Harbor sleeve are not gated here.
STARSHIP_FAMILY_MODES = frozenset({"starship", "starship_robust", "starship_b"})
STARSHIP_FACTOR_IDS: dict[str, tuple[str, ...]] = {
    "starship": ("starship_h2", "starship_b3", "starship_cap50"),
    "starship_robust": ("starship_a25",),
    "starship_b": ("starship_b",),
}
PARKING_KEYS = frozenset({"GOLD", "OIL", "NASDAQ", "BOND10"})
# Tradable asset behind each parking key (copy only; the engine owns the map).
_PARKING_ASSETS: dict[str, str] = {
    "GOLD": "ETF:518880",
    "OIL": "ETF:513350",
    "NASDAQ": "ETF:513100",
    "BOND10": "ETF:511260",
}
_PARKING_LABELS: dict[str, str] = {
    "GOLD": "黄金",
    "OIL": "油",
    "NASDAQ": "纳指",
    "BOND10": "10年国债",
}
# The recommendation is advice in the payload only — the settings mode and the
# live sleeve are never rewritten by this module.
FOLLOW_RECOMMENDATION = {
    "mode": "homeport",
    "label": MODE_LABELS["homeport"],
    "timelineStrategy": "homeport_m30",
}
CLOSE_BASIS_NOTE = "信号按收盘价计算；收盘价不可成交，只能按下面的执行时钟跟。"


def timeline_strategy_for_mode(mode: str | None) -> str:
    return MODE_TO_TIMELINE.get(str(mode or ""), "homeport_m30")


def _read_report_json(name: str) -> dict[str, Any] | None:
    """Read one frozen backtest report (factor vault / S-gap decay).

    Fail-open: a missing file returns ``None`` and the caller decides how
    conservative to be. No DB, no network, no clock.
    """
    try:
        from data_sync_service.api import backtest_routes as br

        path = Path(br.REPORTS_DIR) / name
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
    except Exception as exc:  # noqa: BLE001 — display/advice only
        logger.warning("agent contract %s unavailable: %s", name, exc)
        return None


def _starship_factor_state(mode: str) -> dict[str, Any]:
    """Frozen factor-vault + S-gap state for a starship-family mode."""
    ids = STARSHIP_FACTOR_IDS.get(mode) or ()
    vault = _read_report_json("factor_vault.json") or {}
    factors = {
        str(f.get("id")): f
        for f in (vault.get("factors") or [])
        if isinstance(f, dict) and f.get("id")
    }
    states = [
        {
            "id": fid,
            "name": (factors.get(fid) or {}).get("name"),
            "status": (factors.get(fid) or {}).get("status"),
            "badge": (factors.get(fid) or {}).get("badge"),
            "percentile": (factors.get(fid) or {}).get("percentile"),
            "net": (factors.get(fid) or {}).get("net"),
            "position": (factors.get(fid) or {}).get("position"),
        }
        for fid in ids
        if factors.get(fid)
    ]
    decay = _read_report_json("sgap_decay.json") or {}
    summary = decay.get("summary") or {}
    sgap = {
        "revivalPos": summary.get("revival_pos"),
        "latestR40Mean": summary.get("latest_r40_mean"),
        "latestR40Percentile": summary.get("latest_r40_percentile"),
        "latestR40WinRate": summary.get("latest_r40_win_rate"),
    }
    return {"vaultAvailable": bool(vault), "factors": states, "sgap": sgap}


def _followability(mode: str) -> dict[str, Any]:
    """Can the decision agent follow the selected mode *now*?

    Starship-family modes depend on the S-gap edge. The frozen factor vault
    marks them 冷库 until the rolling-40 percentile is >= 75 AND net > 0, and
    the S-gap decay panel must confirm. A cold/expired factor is not
    followable even though the settings mode is still selectable.

    Non-starship modes (homeport M30 / harbor / starport / twin_star) are not
    gated here — this function never disables the product baseline.
    """
    if mode not in STARSHIP_FAMILY_MODES:
        return {
            "mode": mode,
            "followable": True,
            "status": "not_gated",
            "reason": "not a starship-family mode; no cold-factor gate",
            "recommendedMode": None,
            "recommendedModeLabel": None,
            "recommendedTimeline": None,
            "adviceOnly": True,
        }

    state = _starship_factor_state(mode)
    factors = state["factors"]
    sgap = state["sgap"]
    cold = [f for f in factors if str(f.get("status") or "") == "cold"]
    revived = [f for f in factors if str(f.get("status") or "") == "revived"]
    sgap_cold = (
        (sgap.get("revivalPos") is not None and float(sgap["revivalPos"]) <= 0)
        or (sgap.get("latestR40Percentile") is not None and float(sgap["latestR40Percentile"]) < 50)
        or (sgap.get("latestR40Mean") is not None and float(sgap["latestR40Mean"]) <= 0)
    )
    known = bool(factors) or sgap.get("revivalPos") is not None
    followable = bool(revived) and not sgap_cold and not cold
    if not known:
        status = "unknown"
        reason = (
            "factor_vault.json / sgap_decay.json unavailable — starship family "
            "cannot be verified as revived; treated as not followable"
        )
    elif followable:
        status = "revived"
        reason = "factor vault and S-gap rolling-40 both confirm a revival"
    else:
        status = "cold" if cold else "watch"
        bits = []
        for f in factors:
            bits.append(
                f"{f.get('id')}={f.get('status')}"
                f"({f.get('badge') or '—'}, net {f.get('net')}, pos {f.get('position')}%)"
            )
        if sgap:
            bits.append(
                "S-gap rolling40 "
                f"mean={sgap.get('latestR40Mean')} "
                f"percentile={sgap.get('latestR40Percentile')} "
                f"revival_pos={sgap.get('revivalPos')}"
            )
        reason = "; ".join(bits) or "cold factor"
    return {
        "mode": mode,
        "followable": followable,
        "status": status,
        "reason": reason,
        "factors": factors,
        "sgap": sgap,
        "recommendedMode": FOLLOW_RECOMMENDATION["mode"],
        "recommendedModeLabel": FOLLOW_RECOMMENDATION["label"],
        "recommendedTimeline": FOLLOW_RECOMMENDATION["timelineStrategy"],
        "adviceOnly": True,
        "note": (
            "advice only — settings mode and the live sleeve are unchanged; "
            "Yuan must switch /settings/strategy-mode explicitly"
        ),
    }


def _path_mismatch(mode: str, sleeve: dict[str, Any]) -> dict[str, Any]:
    """Spell out settings-mode vs parking-sleeve mismatch (not a buried bool)."""
    action = str(sleeve.get("action") or "NONE").upper()
    pick = sleeve.get("pick") or {}
    pick_key = str(pick.get("key") or "").upper()
    symbol = sleeve.get("symbol") or pick.get("symbol")
    holding = bool(sleeve.get("holding"))
    is_parking = pick_key in PARKING_KEYS or str(symbol or "").upper().startswith("ETF:")
    active = bool(
        is_parking
        and mode not in SLEEVE_MODES
        and (holding or action in {"SELL_TO_REPO", "BUY", "ROTATE", "HOLD"})
    )
    mode_label = MODE_LABELS.get(mode, mode)
    leg = f"{action} {symbol or pick_key or '—'}"
    message = (
        f"settings 是 {mode_label}（`{mode}`），但停车腿仍是 港湾 {leg}。"
        f"这不是同一条跟单路径：不要把这个港湾停车腿当成 {mode_label} 在跟；"
        "先看 followability 与回测对照，再决定跟哪条。"
        if active
        else f"settings {mode_label} 与当前停车腿一致，或当前没有可执行的停车腿。"
    )
    return {
        "active": active,
        "settingsMode": mode,
        "settingsLabel": mode_label,
        "sleeveFamily": "harbor" if is_parking else None,
        "sleeveFamilyLabel": "港湾停车腿" if is_parking else None,
        "sleeveAction": action,
        "sleeveSymbol": symbol,
        "sleevePickKey": pick_key or None,
        "sleeveRelevantToMode": mode in SLEEVE_MODES,
        "message": message,
    }


def _fill_clock(sleeve: dict[str, Any], *, now: datetime | None = None) -> dict[str, Any]:
    """Server mirror of ``follow-timing.ts::executionTiming`` (close-basis).

    A paper close is not tradable. The clock is the only honest answer to
    "when can this actually be filled": 明日开盘跟 / 盘中可追 /
    收盘待确认 → 次日开盘卖.
    """
    now = now or datetime.now(SHANGHAI_TZ)
    next_word = "明日" if now.weekday() < 5 else "下一交易日"
    action = str(sleeve.get("action") or "NONE").upper()
    pending = bool(sleeve.get("pendingOpenSell"))
    trail = bool(sleeve.get("trailExit"))
    if pending:
        kind, label, detail, tone = (
            "close-sell-pending",
            "收盘待确认 → 次日开盘卖",
            "14:30 现价触线只做预警；收盘仍触发才卖，次日开盘执行",
            "warn",
        )
    elif action == "SELL_TO_REPO" or trail:
        kind, label, detail, tone = (
            "close-sell-confirmed",
            f"{next_word}开盘卖",
            "信号已在收盘确认 → 次日开盘执行（不盘中挂单）",
            "sell",
        )
    elif action in {"BUY", "ROTATE"}:
        kind, label, detail, tone = (
            "close-buy",
            f"{next_word}开盘跟",
            "收盘后信号 → 次日开盘执行；不追收盘价",
            "buy",
        )
    else:
        kind, label, detail, tone = (
            "hold",
            "无需操作",
            "未达阈值/无新腿；下一步仍按收盘信号判断",
            "muted",
        )
    return {
        "kind": kind,
        "label": label,
        "detail": detail,
        "tone": tone,
        "basis": "close",
        "closeBasis": True,
        "tradableNow": False,
        "nextSessionWord": next_word,
        "note": CLOSE_BASIS_NOTE,
    }


def _rebuy_key_context(
    key: str,
    *,
    last_exit_date: str | None,
    pick_key: str | None,
) -> dict[str, Any]:
    """Per-key "just stopped, do not auto-rebuy" copy (not oil-only)."""
    assets = _PARKING_ASSETS.get(key)
    label = _PARKING_LABELS.get(key, key)
    if pick_key and pick_key != key:
        current = f"当前 pick={pick_key}，不是 {key}；"
    elif pick_key == key:
        current = f"当前 pick 仍是 {key}；"
    else:
        current = ""
    message = (
        f"刚卖出/止损 {key}（{assets or '停车 ETF'}，{last_exit_date or '近10日内'}）。{current}"
        f"不要自动回买{label}；若下一腿回到 {key}，先确认再买。"
        "人工覆盖（annotate only）：/v1/agent/overlays。"
    )
    return {
        "active": True,
        "key": key,
        "symbol": assets,
        "lastExitDate": last_exit_date,
        "pickIsSameKey": bool(pick_key and pick_key == key),
        "pickIsOil": key == "OIL" and pick_key == "OIL",
        "parkingKey": key in PARKING_KEYS,
        "message": message,
    }


def _whip_context(whip: dict[str, Any], sleeve: dict[str, Any]) -> dict[str, Any]:
    """Make a just-trailed parking key visible even when today's pick rotated.

    Generalizes the 2026-11-03 OIL case to every parking key (GOLD / NASDAQ /
    BOND10): ``context.oil`` stays as a convenience alias when the key is OIL,
    and ``context.rebuy.byKey`` carries the generic per-key annotation.
    """
    keys = [str(k).upper() for k in (whip.get("keys") or []) if k]
    pick = sleeve.get("pick") or {}
    pick_key = str(pick.get("key") or "").upper() or None
    recent = whip.get("recentExits") or {}
    last = whip.get("lastExit") or {}
    by_key: dict[str, Any] = {}
    for key in keys:
        date = recent.get(key)
        if not date and pick_key == key:
            date = last.get("date")
        if not date:
            continue
        by_key[key] = _rebuy_key_context(key, last_exit_date=str(date)[:10], pick_key=pick_key)
    context: dict[str, Any] = {
        "keys": keys,
        "recentExits": recent,
        "sameKeyRebuy": bool(whip.get("sameKeyRebuy")),
        "pickKey": pick_key,
        "priority": "must_act_sell > same_key_rebuy",
        "mustActSellFirst": True,
        "overlaysPath": "/v1/agent/overlays",
        "cooldownDays": 0,
        "engineCooldown": False,
        "note": (
            "judgment annotation only — must-act sell outranks a same-key rebuy; "
            "the parking cooldown stays k=0 (H-PARK-C REJECT)"
        ),
    }
    if by_key:
        context["rebuy"] = {
            "active": True,
            "keys": sorted(by_key),
            "byKey": by_key,
            "pickKey": pick_key,
            "sameKeyRebuy": bool(whip.get("sameKeyRebuy")),
            "priority": context["priority"],
            "adviceOnly": True,
            "note": (
                "any parking key (not only OIL) that was trailed out inside the "
                "10-day whip memory; advice only, never a veto"
            ),
        }
    if "OIL" in by_key:
        # Convenience alias for the 2026-11-03 OIL case (never the only path).
        context["oil"] = by_key["OIL"]
    return context


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


def build_decision_contract(
    *,
    trade_date: str | None = None,
    health: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Assemble the agent contract for ``trade_date`` (default: today).

    ``health`` lets a caller that already built ``build_portfolio_health`` for
    the same day reuse it (the judgment brief does) instead of paying for a
    second holdings read + realtime quote round.
    """
    from data_sync_service.api.settings_routes import selected_strategy_mode
    from data_sync_service.service.portfolio_health import build_portfolio_health

    day = str(trade_date or _today_iso())[:10]
    mode = selected_strategy_mode()
    timeline_key = timeline_strategy_for_mode(mode)
    if not isinstance(health, dict) or not health:
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
    path_mismatch = _path_mismatch(mode, sleeve)
    whip_payload = dict(whip)
    whip_payload["context"] = _whip_context(whip, sleeve)
    fill_clock = _fill_clock(sleeve)
    payload = {
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
            "pathMismatch": path_mismatch,
            "followability": _followability(mode),
        },
        "sleeve": _sleeve_summary(sleeve),
        "health": _health_summary(health),
        "nextLeg": _next_leg(sleeve),
        "fillClock": fill_clock,
        "whip": whip_payload,
        "overlays": overlays,
        "authority": AUTHORITY,
    }
    # Advice-only event / geo tags (2026-11-11). Attached after the sleeve,
    # fill clock and overlay blocks are final; the invariant below proves a
    # triggered exit keeps both its action and its close-basis fill clock.
    # No stored tag at/above MIN_CONFIDENCE → no block (Step-1 shape).
    event_block = build_event_tag_block(
        contract=payload,
        day=day,
        timeline=timeline_key,
    )
    if event_block:
        assert_event_tags_never_override_exit(
            sleeve=payload["sleeve"],
            fill_clock=fill_clock,
            tags=event_block["eventTags"]["tags"],
        )
        payload["eventTags"] = event_block["eventTags"]
        payload["geoRegime"] = event_block["geoRegime"]
    return payload
