"""Universal stop coverage + same-key rebuy gates (2026-11-11, maturity item ④).

Two advice-only checks share one shape so the judgment brief and the 14:30
「今日调整」 table cannot drift:

1. **Universal stop coverage** — every *risky* position that is held, and every
   risky symbol a sleeve / daily table proposes to buy, must say whether a
   stop or trailing line is present. Risky = A-share / HK / US / commodity /
   bond ETF + stock. Cash / 逆回购 / idle repo is exempt (a repo has no stop by
   construction). "Has a stop" = ``stopLossLine`` or ``trailingLine`` or a
   non-empty ``stopRule``; the Harbor parking 8% trail (``harbor.TRAIL_PCT``)
   counts for a parking leg. Missing → the warning copy below, never a veto.

2. **Same-key rebuy after a trail** — a BUY / ROTATE into a parking key that
   was trailed out inside the whip memory window (``agent_whip``) is annotated
   ``刚止损，回买=鞭打，请确认`` (or the accepted-overlay copy). The row stays
   visible; a must-act sell still outranks it.

This module is display / advice only. It never changes ``sleeve.action``,
``TRAIL_PCT``, the parking cooldown (k=0) or any target weight, and it never
places an order. ``eventTags`` / ``geoRegime`` may only advise a rebuy or a
strategy choice — they can never hide or delay a stop / trail / must-act sell.

Stable shape (documented; both callers rely on it)::

    {
      "available": True,
      "adviceOnly": True,
      "priority": "must_act_sell > same_key_rebuy",
      "eventTagsNeverVetoExits": True,
      "stops": {
        "status": "ok" | "warning",
        "checked": int,
        "missingCount": int,
        "rows": [{symbol, key, source, hasStop, missing,
                  stopLossLine, trailingLine, stopRule, trailPct, message}],
        "missing": [...same rows...],
        "warnings": [str],
      },
      "sameKeyRebuy": {
        "active": bool,
        "sameKeyRebuy": bool,
        "pickKey": str | None,
        "lastExit": dict | None,
        "recentExits": {key: date},
        "memoryDays": int,
        "copy": str | None,
        "proposedRebuys": [{symbol, key, lastExitDate, copy,
                            sameKeyRebuy: True, deferRebuy: True}],
      },
      "warnings": [str],
      "note": str,
    }
"""

from __future__ import annotations

import re
from typing import Any

from data_sync_service.service.agent_whip import WHIP_ANNOTATION, canonical_key

# Copy Yuan asked for: advice, never a hidden veto of an already-triggered exit.
NO_STOP_MESSAGE = "该资产没有止损，需要补上"
STOP_WARNING_PREFIX = "止损缺口"
GATE_PRIORITY = "must_act_sell > same_key_rebuy"

PARKING_KEYS = frozenset({"GOLD", "OIL", "NASDAQ", "BOND10"})
ASSET_PREFIXES = ("CN:", "HK:", "ETF:", "US:")
CASH_MARKERS = ("逆回购", "现金", "货币")
CASH_TOKENS = frozenset({"REPO", "CASH", "MMF", "IDLE"})
_BARE_CODE = re.compile(r"^\d{5,6}(\.(SH|SZ|BJ))?$")


def _norm(value: Any) -> str:
    return str(value or "").strip().upper()


def _num(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def is_cash_like(
    *,
    symbol: Any = None,
    key: Any = None,
    name: Any = None,
    action: Any = None,
) -> bool:
    """True for 逆回购 / cash / idle repo — assets that must not be flagged."""
    text = " ".join(str(v) for v in (key, symbol, name, action) if v)
    if not text.strip():
        return False
    if any(marker in text for marker in CASH_MARKERS):
        return True
    tokens = [t for t in re.split(r"[^A-Z0-9\u4e00-\u9fff]+", _norm(text)) if t]
    return any(t in CASH_TOKENS for t in tokens)


def is_risky_asset(
    *,
    symbol: Any = None,
    key: Any = None,
    name: Any = None,
    action: Any = None,
) -> bool:
    """A-share / HK / US / commodity / bond ETF + stock. Cash-like is False."""
    if not (str(symbol or "").strip() or str(key or "").strip()):
        return False
    if is_cash_like(symbol=symbol, key=key, name=name, action=action):
        return False
    if _norm(key) in PARKING_KEYS:
        return True
    sym = _norm(symbol)
    if not sym:
        # A parking key without a symbol is still a real ETF leg.
        return _norm(key) in PARKING_KEYS
    if sym.startswith(ASSET_PREFIXES):
        return True
    bare = sym.removeprefix("ETF:")
    return bool(_BARE_CODE.fullmatch(bare))


def parking_rule(*, key: Any = None, symbol: Any = None) -> dict[str, Any] | None:
    """The engine trail rule for a Harbor parking leg (None = not parked).

    Reads ``harbor.TRAIL_PCT`` — never a local stop percentage.
    """
    k = _norm(key)
    if k not in PARKING_KEYS:
        try:
            k = canonical_key(key=key, symbol=symbol) or ""
        except Exception:  # noqa: BLE001 — display helper, never a gate
            k = ""
    if k not in PARKING_KEYS:
        return None
    pct: float | None
    try:
        from data_sync_service.service.harbor import TRAIL_PCT

        pct = float(TRAIL_PCT)
        rule = f"harbor_trail_{pct:g}pct"
    except Exception:  # noqa: BLE001 — copy degrades, engine constant untouched
        pct = None
        rule = "harbor_trail"
    return {"key": k, "stopRule": rule, "trailPct": pct, "engine": True}


def stop_check(
    row: dict[str, Any],
    *,
    source: str = "holding",
    parking: bool | None = None,
) -> dict[str, Any] | None:
    """One stop-coverage row; ``None`` when the asset is not risky / cash-like."""
    symbol = _norm(row.get("symbol"))
    key = _norm(row.get("key")) or None
    if not is_risky_asset(
        symbol=symbol,
        key=key,
        name=row.get("name"),
        action=row.get("action"),
    ):
        return None

    stop_loss = _num(row.get("stopLossLine"))
    trailing = _num(row.get("trailingLine"))
    stop_rule_raw = row.get("stopRule")
    stop_rule = str(stop_rule_raw).strip() if _has_value(stop_rule_raw) else None
    trail_pct = _num(row.get("trailPct"))
    has_stop = bool(stop_loss is not None or trailing is not None or stop_rule)
    if not has_stop and parking is not False:
        rule = parking_rule(key=key, symbol=symbol)
        if rule is not None:
            stop_rule = rule["stopRule"]
            trail_pct = rule["trailPct"]
            has_stop = True

    missing = not has_stop
    label = str(row.get("name") or symbol or key or "该资产")
    return {
        "symbol": symbol or None,
        "key": key,
        "source": source,
        "hasStop": has_stop,
        "missing": missing,
        "stopLossLine": stop_loss,
        "trailingLine": trailing,
        "stopRule": stop_rule,
        "trailPct": trail_pct,
        "message": f"{label}：{NO_STOP_MESSAGE}" if missing else None,
    }


def stop_coverage(
    *,
    holdings: list[dict[str, Any]] | None = None,
    proposed_buys: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Universal stop coverage over held + proposed-buy rows (advice only)."""
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for source, items in (("holding", holdings or []), ("buy", proposed_buys or [])):
        for row in items:
            if not isinstance(row, dict):
                continue
            check = stop_check(row, source=source)
            if check is None:
                continue
            dedupe = (source, str(check.get("symbol") or check.get("key") or ""))
            if dedupe in seen:
                continue
            seen.add(dedupe)
            rows.append(check)
    missing = [r for r in rows if r["missing"]]
    return {
        "status": "warning" if missing else "ok",
        "checked": len(rows),
        "missingCount": len(missing),
        "rows": rows,
        "missing": missing,
        "warnings": [str(r.get("message")) for r in missing if r.get("message")],
        "note": (
            "每个风险持仓/拟买入都必须有止损或移动止损线（停车 ETF 的 Harbor 8% trail "
            "也算）；逆回购/现金不检查。缺口只是提醒，不能否决已触发的卖出。"
        ),
    }


def rebuy_advice(
    *,
    symbol: Any = None,
    key: Any = None,
    whip: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Annotate a buy into a key that was trailed out inside the memory window."""
    recent = _recent_exits(whip)
    if not recent:
        return None
    try:
        resolved = canonical_key(key=key, symbol=symbol)
    except Exception:  # noqa: BLE001
        resolved = _norm(key) or None
    if not resolved or resolved not in recent:
        return None
    state = whip or {}
    copy = state.get("copy") or WHIP_ANNOTATION
    return {
        "symbol": _norm(symbol) or None,
        "key": resolved,
        "lastExitDate": recent[resolved],
        "copy": copy,
        "sameKeyRebuy": True,
        "deferRebuy": True,
    }


def _recent_exits(whip: dict[str, Any] | None) -> dict[str, str]:
    state = whip or {}
    context = state.get("context") or {}
    raw = context.get("recentExits") or state.get("recentExits") or {}
    out: dict[str, str] = {}
    if not isinstance(raw, dict):
        return out
    for key, value in raw.items():
        k = _norm(key)
        v = str(value or "")[:10]
        if k and len(v) == 10:
            out[k] = v
    return out


def same_key_rebuy_gate(
    whip: dict[str, Any] | None,
    *,
    proposed_buys: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Same-key rebuy state + every proposed buy that would whip the same key."""
    state = whip or {}
    context = state.get("context") or {}
    recent = _recent_exits(state)
    matches: list[dict[str, Any]] = []
    for buy in proposed_buys or []:
        if not isinstance(buy, dict):
            continue
        advice = rebuy_advice(
            symbol=buy.get("symbol"),
            key=buy.get("key"),
            whip=state,
        )
        if advice is not None:
            advice["reason"] = buy.get("reason")
            matches.append(advice)
    pick_key = _norm(context.get("pickKey") or state.get("pickKey")) or None
    same_key = bool(state.get("sameKeyRebuy"))
    return {
        "active": bool(same_key or matches),
        "sameKeyRebuy": same_key,
        "pickKey": pick_key,
        "lastExit": state.get("lastExit"),
        "recentExits": recent,
        "memoryDays": state.get("memoryDays"),
        "copy": state.get("copy"),
        "proposedRebuys": matches,
        "priority": GATE_PRIORITY,
    }


def build_risk_gates(
    *,
    whip: dict[str, Any] | None = None,
    holdings: list[dict[str, Any]] | None = None,
    proposed_buys: list[dict[str, Any]] | None = None,
    daily_adjust: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Compact, stable risk-gate block for the brief and the daily table."""
    stops = stop_coverage(holdings=holdings, proposed_buys=proposed_buys)
    rebuy = same_key_rebuy_gate(whip, proposed_buys=proposed_buys)
    warnings = list(stops["warnings"])
    for match in rebuy["proposedRebuys"]:
        label = match.get("symbol") or match.get("key")
        warnings.append(f"{label}：{match.get('copy')}")
    return {
        "available": True,
        "adviceOnly": True,
        "priority": GATE_PRIORITY,
        "eventTagsNeverVetoExits": True,
        "stops": stops,
        "sameKeyRebuy": rebuy,
        "warnings": warnings,
        "dailyAdjust": daily_adjust,
        "note": (
            "止损覆盖 + 同键回买都是建议：Must-act 卖出 > 同键回买；"
            "事件/地缘标签不能否决止损、移动止损或卖出，也不能吞掉止损缺口提醒。"
        ),
    }
