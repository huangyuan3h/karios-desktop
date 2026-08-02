"""Execution Gate: ATTACK / WEAK_ATTACK / HOLD_ONLY / DEFEND for satellite-book deployment."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from data_sync_service.service.market_sentiment import (
    CN_INDEX_TRAFFIC_LIGHT_NAMES,
    breadth_panic_active,
)
from data_sync_service.service.sector_rotation_index import (
    SRV_LEVEL_ELEVATED,
    SRV_LEVEL_EXTREME_HIGH,
    SRV_LEVEL_STABLE,
)

MODE_ATTACK = "ATTACK"
MODE_WEAK_ATTACK = "WEAK_ATTACK"
MODE_HOLD_ONLY = "HOLD_ONLY"
MODE_DEFEND = "DEFEND"

REGIME_STRONG = "Strong"
REGIME_DIVERGING = "Diverging"
REGIME_WEAK = "Weak"

# V6.3 Intraday Overflow Override thresholds
OVERFLOW_INFLOW_THRESHOLD_CNY = 500e8  # 500亿
OVERFLOW_UP_COUNT_MIN = 4000
OVERFLOW_UNLOCK_MINUTES = 14 * 60 + 30  # 14:30 Shanghai

_GREEN_SIGNALS = frozenset({"green", "light_green", "deep_green"})
_RISK_DEFEND = frozenset({"extreme_caution", "no_new_positions"})

HK_INDEX_TRAFFIC_LIGHT_NAMES = frozenset({"恒生指数", "恒生科技指数"})

_SIGNAL_RANK = {
    "deep_green": 4,
    "green": 3,
    "light_green": 3,
    "yellow": 2,
    "red": 1,
}


def _is_green(signal: str) -> bool:
    return str(signal or "").strip().lower() in _GREEN_SIGNALS


def _signal_rank(signal: str) -> int:
    return _SIGNAL_RANK.get(str(signal or "").strip().lower(), 0)


def _cn_index_signals(index_signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for sig in index_signals or []:
        if not isinstance(sig, dict):
            continue
        name = str(sig.get("name") or "").strip()
        if name in CN_INDEX_TRAFFIC_LIGHT_NAMES:
            out.append(sig)
    return out


def _hk_index_signals(index_signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for sig in index_signals or []:
        if not isinstance(sig, dict):
            continue
        name = str(sig.get("name") or "").strip()
        if name in HK_INDEX_TRAFFIC_LIGHT_NAMES:
            out.append(sig)
    return out


def _fallback_signals(index_signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [s for s in (index_signals or []) if isinstance(s, dict)][:2]


def _classify_regime(signals: list[dict[str, Any]]) -> str:
    """Strong = all signals green; Diverging = some green; Weak = none green."""
    if len(signals) < 2:
        return REGIME_WEAK
    greens = sum(1 for s in signals if _is_green(str(s.get("signal") or "")))
    if greens == len(signals):
        return REGIME_STRONG
    if greens > 0:
        return REGIME_DIVERGING
    return REGIME_WEAK


def classify_market_regime(index_signals: list[dict[str, Any]]) -> str:
    """Strong = all CN lights green; Diverging = some; else Weak."""
    cn = _cn_index_signals(index_signals)
    if len(cn) < 2:
        cn = _fallback_signals(index_signals)
    return _classify_regime(cn)


def _tighter_light(signals: list[dict[str, Any]]) -> str:
    """Return the tighter (more defensive) light from an explicit signal list."""
    if not signals:
        return "red"
    best = None
    best_rank = 99
    for sig in signals:
        raw = str(sig.get("signal") or "").strip().lower() or "red"
        rank = _signal_rank(raw)
        if rank < best_rank:
            best_rank = rank
            best = raw
    return best or "red"


def tighter_index_light(index_signals: list[dict[str, Any]]) -> str:
    """Return the tighter (more defensive) of the CN lights."""
    cn = _cn_index_signals(index_signals)
    if len(cn) < 2:
        cn = _fallback_signals(index_signals)
    return _tighter_light(cn)


def _position_range_hint_from(signals: list[dict[str, Any]], index_light: str) -> str:
    # Prefer the range from the tighter light's signal row
    for sig in signals:
        if str(sig.get("signal") or "").strip().lower() == index_light:
            pos = str(sig.get("positionRange") or "").strip()
            if pos:
                return pos
    for sig in signals:
        pos = str(sig.get("positionRange") or "").strip()
        if pos:
            return pos
    defaults = {
        "deep_green": "80%-100%",
        "green": "50%-60%",
        "light_green": "50%-60%",
        "yellow": "30%",
        "red": "0%-10%",
    }
    return defaults.get(index_light, "—")


def _position_range_hint(index_signals: list[dict[str, Any]], index_light: str) -> str:
    cn = _cn_index_signals(index_signals)
    if len(cn) < 2:
        cn = _fallback_signals(index_signals)
    return _position_range_hint_from(cn, index_light)


def _satellite_note(mode: str) -> str:
    if mode == MODE_ATTACK:
        return "允许开新仓与加仓；遵守单票上限与吊灯止盈"
    if mode == MODE_WEAK_ATTACK:
        return "极端资金流豁免；允许 5% 先锋仓试探"
    if mode == MODE_HOLD_ONLY:
        return "禁止开新仓；仅管理退出与吊灯"
    return "防守优先；禁止开新仓，优先减仓不合规持仓"


def _hk_satellite_note(mode: str) -> str:
    if mode == MODE_ATTACK:
        return "港股允许开新仓与加仓；遵守单票上限与吊灯止盈"
    if mode == MODE_WEAK_ATTACK:
        return "港股允许 5% 先锋仓试探"
    if mode == MODE_HOLD_ONLY:
        return "港股禁止开新仓；仅管理退出与吊灯"
    return "港股防守优先；禁止开新仓，优先减仓不合规持仓"


def compute_hk_gate(
    *,
    index_signals: list[dict[str, Any]] | None = None,
    risk_mode: str | None = None,
) -> dict[str, Any]:
    """
    HK execution gate: independent position budget for HK trades.

    Driven by HK index lights (恒生指数 / 恒生科技指数) plus the shared global
    risk mode. CN breadth / SRV / overflow factors are CN-specific and do not
    apply here — the HK sleeve is sized independently of the CN sleeve.
    """
    signals = list(index_signals or [])
    hk = _hk_index_signals(signals)
    if not hk:
        hk = _fallback_signals(signals)
    risk = str(risk_mode or "").strip()
    regime = _classify_regime(hk)
    index_light = _tighter_light(hk)
    reasons: list[str] = []

    defend = False
    if risk in _RISK_DEFEND:
        defend = True
        reasons.append("RISK_NO_NEW" if risk == "no_new_positions" else "RISK_EXTREME_CAUTION")
    if regime == REGIME_WEAK:
        defend = True
        reasons.append("REGIME_WEAK")

    if defend:
        mode = MODE_DEFEND
    elif regime == REGIME_DIVERGING:
        mode = MODE_HOLD_ONLY
        reasons.append("REGIME_DIVERGING")
    elif regime == REGIME_STRONG:
        mode = MODE_ATTACK
        reasons.append("REGIME_STRONG")
    else:
        mode = MODE_HOLD_ONLY

    return {
        "mode": mode,
        "allowNewEntries": mode in (MODE_ATTACK, MODE_WEAK_ATTACK),
        "marketRegime": regime,
        "indexLight": index_light,
        "riskMode": risk or None,
        "reasons": reasons,
        "positionRangeHint": _position_range_hint_from(hk, index_light),
        "satelliteNote": _hk_satellite_note(mode),
    }


def _shanghai_minutes(now: datetime | None) -> int:
    """Minutes since midnight in Asia/Shanghai."""
    if now is None:
        dt = datetime.now(tz=ZoneInfo("Asia/Shanghai"))
    elif now.tzinfo is None:
        dt = now.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
    else:
        dt = now.astimezone(ZoneInfo("Asia/Shanghai"))
    return dt.hour * 60 + dt.minute


def _overflow_inflow_yi(amount_cny: float | None) -> float | None:
    if amount_cny is None:
        return None
    try:
        return round(float(amount_cny) / 1e8, 2)
    except (TypeError, ValueError):
        return None


def compute_execution_gate(
    *,
    index_signals: list[dict[str, Any]] | None = None,
    down_count: int = 0,
    risk_mode: str | None = None,
    srv_index: dict[str, Any] | None = None,
    up_count: int = 0,
    max_sector_inflow_cny: float | None = None,
    overflow_sector: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """
    Derive ATTACK / WEAK_ATTACK / HOLD_ONLY / DEFEND from index lights, breadth,
    riskMode, SRV, and V6.3 intraday overflow override.

    SRV missing/null: does not alone force DEFEND; ATTACK still requires Stable
    when SRV level is present.

    V6.3 overflow: when base mode is DEFEND/HOLD_ONLY (and not BREADTH_PANIC /
    RISK_*), sector 1D inflow > 500亿 + upCount > 4000 + Shanghai >= 14:30
    upgrades to WEAK_ATTACK (5% pioneer sleeve).
    """
    signals = list(index_signals or [])
    srv = srv_index if isinstance(srv_index, dict) else {}
    srv_level = srv.get("level")
    srv_level_str = str(srv_level).strip() if srv_level is not None else None
    if srv_level_str == "":
        srv_level_str = None
    overlap = srv.get("overlapCount")
    overlap_count: int | None
    try:
        overlap_count = int(overlap) if overlap is not None else None
    except (TypeError, ValueError):
        overlap_count = None

    down = int(down_count or 0)
    up = int(up_count or 0)
    risk = str(risk_mode or "").strip()
    regime = classify_market_regime(signals)
    index_light = tighter_index_light(signals)
    reasons: list[str] = []

    hard_defend_reasons: list[str] = []
    if breadth_panic_active(down):
        hard_defend_reasons.append("BREADTH_PANIC")
    if risk in _RISK_DEFEND:
        hard_defend_reasons.append(
            "RISK_NO_NEW" if risk == "no_new_positions" else "RISK_EXTREME_CAUTION"
        )

    defend = False
    if hard_defend_reasons:
        defend = True
        reasons.extend(hard_defend_reasons)
    if srv_level_str == SRV_LEVEL_EXTREME_HIGH:
        defend = True
        reasons.append("SRV_EXTREME_HIGH")
    if regime == REGIME_WEAK:
        defend = True
        reasons.append("REGIME_WEAK")

    if defend:
        mode = MODE_DEFEND
    elif regime == REGIME_DIVERGING or srv_level_str == SRV_LEVEL_ELEVATED:
        mode = MODE_HOLD_ONLY
        if regime == REGIME_DIVERGING:
            reasons.append("REGIME_DIVERGING")
        if srv_level_str == SRV_LEVEL_ELEVATED:
            reasons.append("SRV_ELEVATED")
    elif regime == REGIME_STRONG and (srv_level_str is None or srv_level_str == SRV_LEVEL_STABLE):
        mode = MODE_ATTACK
        reasons.append("REGIME_STRONG")
        if srv_level_str == SRV_LEVEL_STABLE:
            reasons.append("SRV_STABLE")
        elif srv_level_str is None:
            reasons.append("SRV_UNKNOWN")
    else:
        # Strong but unexpected SRV level (should be covered by Elevated/Extreme)
        mode = MODE_HOLD_ONLY
        reasons.append("REGIME_STRONG")
        if srv_level_str:
            reasons.append(f"SRV_{srv_level_str.upper()}")

    overflow_sector_out: str | None = None
    overflow_inflow_yi: float | None = None
    try:
        inflow = float(max_sector_inflow_cny) if max_sector_inflow_cny is not None else None
    except (TypeError, ValueError):
        inflow = None
    if inflow is not None:
        overflow_inflow_yi = _overflow_inflow_yi(inflow)
        if overflow_sector:
            overflow_sector_out = str(overflow_sector).strip() or None

    # V6.3 Intraday Overflow Override → WEAK_ATTACK
    can_overflow = (
        mode in (MODE_DEFEND, MODE_HOLD_ONLY)
        and not hard_defend_reasons
        and inflow is not None
        and inflow > OVERFLOW_INFLOW_THRESHOLD_CNY
        and up > OVERFLOW_UP_COUNT_MIN
        and _shanghai_minutes(now) >= OVERFLOW_UNLOCK_MINUTES
    )
    if can_overflow:
        mode = MODE_WEAK_ATTACK
        reasons.append("INTRADAY_OVERFLOW_OVERRIDE")

    # Dedupe reasons preserving order
    seen: set[str] = set()
    uniq_reasons: list[str] = []
    for r in reasons:
        if r not in seen:
            seen.add(r)
            uniq_reasons.append(r)

    allow_new = mode in (MODE_ATTACK, MODE_WEAK_ATTACK)
    result = {
        "mode": mode,
        "allowNewEntries": allow_new,
        "marketRegime": regime,
        "indexLight": index_light,
        "srvLevel": srv_level_str,
        "srvOverlapCount": overlap_count,
        "downCount": down,
        "upCount": up,
        "riskMode": risk or None,
        "reasons": uniq_reasons,
        "positionRangeHint": _position_range_hint(signals, index_light),
        "satelliteNote": _satellite_note(mode),
        "overflowSector": overflow_sector_out,
        "overflowInflowYi": overflow_inflow_yi,
    }
    # Dual-market position budgeting: the flat top-level stays the CN gate for
    # backward compatibility; cnGate/hkGate give each market its own budget.
    result["cnGate"] = dict(result)
    result["hkGate"] = compute_hk_gate(index_signals=signals, risk_mode=risk)
    return result
