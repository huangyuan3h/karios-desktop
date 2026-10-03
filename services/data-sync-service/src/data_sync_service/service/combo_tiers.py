"""Pure functional combo-tier layer (Karios RESTRUCTURE PR1, step 1 only).

Inputs: account total assets, current tier, last-60d NAV series,
starship front-gate status.
Outputs: target tier, per-engine target weights/amounts, fuse flags,
next rebalance orders (H2j fixed ratios).

Rules (frozen, see combo_tiers_config.py for sources):
- Default attack tier A25 (40% harbor + 20% M30 + 20% B3 + 20% capacity
  starship); B/C per H2j. Starship B stays live as the ONLY baseline —
  this module never changes its params or execution chain, it only sizes
  the observation weight (0% until the front gate passes).
- Starship slot cap 20%.
- Fuse: rolling-60d -15% downgrades one tier (A->B->C->CASH); single-day
  -8% pauses new positions; single-week -5% halves; holdout -25% stops.
  Original rules unchanged.
- Starship observation front gate (until H2k lands): paper 20 fills +
  holdout recovered to within -10% + filtered valid > 0. Unmet => starship
  weight 0% and refill proportionally into base legs.
- Tier switch on month-end account total assets: A->B >= 155w, B->A <= 145w,
  B->C >= 210w, C->B <= 180w, executed next-month first trading day.
  Buffers 145-155w / 180-210w hold (no flip-flop). One step per judgement.

Purity: no DB, no network, no clock, no broker/order calls, no mutation of
inputs. All thresholds imported from combo_tiers_config (frozen).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from data_sync_service.service import combo_tiers_config as cfg

_LEGS: tuple[str, ...] = ("harbor", "m30", "b3", "starship", "cash")

_DOWNGRADE: dict[str, str] = {
    cfg.TIER_A: cfg.TIER_B,
    cfg.TIER_B: cfg.TIER_C,
    cfg.TIER_C: cfg.TIER_CASH,
    cfg.TIER_CASH: cfg.TIER_CASH,
}


def normalize_tier(tier: str | None) -> str:
    """Upper-case tier id, defaulting to A (PROMPT default attack tier A25)."""
    if tier is None:
        return cfg.TIER_A
    t = str(tier).strip().upper()
    if t in cfg.ALL_TIERS:
        return t
    raise ValueError(f"unknown tier {tier!r}, expected one of {cfg.ALL_TIERS}")


def infer_initial_tier(total_assets: float) -> str:
    """Tier for callers with no current tier (absolute bands, no buffer)."""
    if total_assets < cfg.INIT_A_MAX:
        return cfg.TIER_A
    if total_assets < cfg.INIT_B_MAX:
        return cfg.TIER_B
    return cfg.TIER_C


def resolve_asset_tier(total_assets: float, current_tier: str | None) -> str:
    """Target tier from assets + current tier (buffered, one step max)."""
    if not isinstance(total_assets, (int, float)) or not total_assets > 0:
        raise ValueError(f"total_assets must be positive, got {total_assets!r}")
    if current_tier is None:
        return infer_initial_tier(float(total_assets))
    cur = normalize_tier(current_tier)
    ta = float(total_assets)
    if cur == cfg.TIER_A:
        return cfg.TIER_B if ta >= cfg.A_TO_B else cfg.TIER_A
    if cur == cfg.TIER_B:
        if ta >= cfg.B_TO_C:
            return cfg.TIER_C
        if ta <= cfg.B_TO_A:
            return cfg.TIER_A
        return cfg.TIER_B
    if cur == cfg.TIER_C:
        return cfg.TIER_B if ta <= cfg.C_TO_B else cfg.TIER_C
    return cfg.TIER_CASH  # already terminal


def downgrade_one_tier(tier: str) -> str:
    """Fuse downgrade chain A->B->C->CASH (terminal stays)."""
    return _DOWNGRADE[normalize_tier(tier)]


def _ret(last: float, base: float) -> float | None:
    if base is None or base == 0:
        return None
    try:
        return float(last) / float(base) - 1.0
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def check_fuse(nav_60d: Sequence[float] | None) -> dict[str, Any]:
    """Fuse flags from a trailing NAV series (oldest -> newest, T-1 basis).

    Empty/None/short series => no fuse (insufficient data, fail-open for
    planning; caller should still feed real 60d NAV in production).
    - drawdown_60d = last / max(series) - 1  (rolling-60d window proxy)
    - last_day_ret = last / prev - 1
    - last_week_ret = last / nav[-6] - 1 (5 trading days; shorter series
      falls back to first point)
    Triggered when drawdown_60d <= -15% (tier downgrade). pause_new when
    last_day_ret <= -8%. halve when last_week_ret <= -5%.
    """
    empty: dict[str, Any] = {
        "triggered": False,
        "downgrade": False,
        "pause_new": False,
        "halve": False,
        "drawdown_60d": None,
        "last_day_ret": None,
        "last_week_ret": None,
        "reasons": [],
        "n": 0,
    }
    if not nav_60d:
        return dict(empty)
    try:
        nav = [float(x) for x in nav_60d]
    except (TypeError, ValueError):
        return dict(empty)
    nav = [x for x in nav if x == x]  # drop NaN
    if len(nav) < 2 or any(x <= 0 for x in nav):
        return dict({**empty, "n": len(nav)})
    last = nav[-1]
    peak = max(nav)
    drawdown = _ret(last, peak)
    last_day = _ret(last, nav[-2])
    base_w = nav[-6] if len(nav) >= 6 else nav[0]
    last_week = _ret(last, base_w)
    reasons: list[str] = []
    _eps = 1e-9  # float-border tolerance so exact -15%/-8%/-5% still trips
    downgrade = drawdown is not None and drawdown <= cfg.FUSE_DRAWDOWN_60D + _eps
    pause_new = last_day is not None and last_day <= cfg.SINGLE_DAY_STOP + _eps
    halve = last_week is not None and last_week <= cfg.WEEKLY_HALVE + _eps
    if downgrade:
        reasons.append(f"rolling-60d {drawdown:.2%} <= -15% -> downgrade one tier")
    if pause_new:
        reasons.append(f"single-day {last_day:.2%} <= -8% -> pause new positions")
    if halve:
        reasons.append(f"single-week {last_week:.2%} <= -5% -> halve positions")
    return {
        "triggered": bool(downgrade or pause_new or halve),
        "downgrade": bool(downgrade),
        "pause_new": bool(pause_new),
        "halve": bool(halve),
        "drawdown_60d": drawdown,
        "last_day_ret": last_day,
        "last_week_ret": last_week,
        "reasons": reasons,
        "n": len(nav),
    }


def parse_starship_status(
    status: bool | Mapping[str, Any] | None,
    *,
    paper20_pass: bool | None = None,
    holdout_recovered: bool | None = None,
    filtered_valid_positive: bool | None = None,
) -> dict[str, Any]:
    """Normalize the starship front-gate status (old three until H2k).

    Accepts a bool (ready directly) or a mapping with any of:
    passed / ready / paper20_pass / holdout_recovered / filtered_valid_positive.
    Explicit kwargs override mapping fields. Default (all None) => not ready
    (fail-closed 0% observation, matches 2026-10-03 paper ~4/20 unmet).
    Ready = paper20 AND holdout-recovered AND filtered-valid-positive,
    unless an explicit passed/ready bool is given.
    """
    detail: dict[str, Any] = {
        "paper20_pass": None,
        "holdout_recovered": None,
        "filtered_valid_positive": None,
    }
    explicit: bool | None = None
    if isinstance(status, bool):
        explicit = status
    elif isinstance(status, Mapping):
        for k in ("passed", "ready"):
            if k in status and status[k] is not None:
                explicit = bool(status[k])
        for k in detail:
            if k in status and status[k] is not None:
                detail[k] = bool(status[k])
    elif status is not None:
        raise ValueError(f"starship status must be bool/mapping/None, got {status!r}")
    if paper20_pass is not None:
        detail["paper20_pass"] = bool(paper20_pass)
    if holdout_recovered is not None:
        detail["holdout_recovered"] = bool(holdout_recovered)
    if filtered_valid_positive is not None:
        detail["filtered_valid_positive"] = bool(filtered_valid_positive)
    if explicit is not None:
        ready = explicit
    else:
        vals = [
            detail["paper20_pass"],
            detail["holdout_recovered"],
            detail["filtered_valid_positive"],
        ]
        ready = bool(all(v is True for v in vals)) if any(v is not None for v in vals) else False
    return {"ready": ready, "detail": detail, "explicit": explicit}


def apply_starship_gate(weights: Mapping[str, float], ready: bool) -> tuple[dict[str, float], bool]:
    """Zero starship and refill proportionally when the front gate is unmet."""
    req = {leg: float(weights.get(leg, 0.0)) for leg in _LEGS}
    star_req = req.get("starship", 0.0)
    if ready or star_req <= 0:
        return (dict(req), False)
    others = [leg for leg in _LEGS if leg != "starship"]
    denom = sum(req[leg] for leg in others)
    if denom <= 0:
        out = {leg: 0.0 for leg in _LEGS}
        out["cash"] = 1.0
        return (out, True)
    out = {}
    for leg in others:
        out[leg] = req[leg] + star_req * (req[leg] / denom)
    out["starship"] = 0.0
    return (out, True)


def base_weights(tier: str) -> dict[str, float]:
    """Frozen H2j mix for a tier (pre-gate, sums to 1.0)."""
    t = normalize_tier(tier)
    return dict(cfg.WEIGHTS_BY_TIER[t])


def plan_tier(
    total_assets: float,
    current_tier: str | None,
    nav_60d: Sequence[float] | None = None,
    starship_status: bool | Mapping[str, Any] | None = None,
    *,
    paper20_pass: bool | None = None,
    holdout_recovered: bool | None = None,
    filtered_valid_positive: bool | None = None,
) -> dict[str, Any]:
    """Full combo plan (pure): asset tier -> fuse -> gate -> amounts/orders."""
    if not isinstance(total_assets, (int, float)) or not total_assets > 0:
        raise ValueError(f"total_assets must be positive, got {total_assets!r}")
    ta = float(total_assets)
    cur = normalize_tier(current_tier) if current_tier is not None else infer_initial_tier(ta)
    asset_tier = resolve_asset_tier(ta, current_tier)
    fuse = check_fuse(nav_60d)
    target_tier = downgrade_one_tier(asset_tier) if fuse["downgrade"] else asset_tier
    requested = base_weights(target_tier)
    star = parse_starship_status(
        starship_status,
        paper20_pass=paper20_pass,
        holdout_recovered=holdout_recovered,
        filtered_valid_positive=filtered_valid_positive,
    )
    effective, refilled = apply_starship_gate(requested, star["ready"])
    # Safety: enforce the 20% starship cap even if a future config drifts.
    if effective.get("starship", 0.0) - cfg.STARSHIP_CAP > 1e-12:
        over = effective["starship"] - cfg.STARSHIP_CAP
        effective["starship"] = cfg.STARSHIP_CAP
        others = [leg for leg in _LEGS if leg != "starship"]
        denom = sum(requested.get(leg, 0.0) for leg in others) or 1.0
        for leg in others:
            effective[leg] = effective.get(leg, 0.0) + over * (requested.get(leg, 0.0) / denom)
    amounts = {leg: round(ta * w, 2) for leg, w in effective.items()}
    orders = [
        {
            "leg": leg,
            "target_weight": round(effective[leg], 6),
            "target_amount": amounts[leg],
            "note": _order_note(leg, target_tier),
        }
        for leg in _LEGS
    ]
    return {
        "current_tier": cur,
        "asset_tier": asset_tier,
        "target_tier": target_tier,
        "total_assets": ta,
        "weights_requested": {k: round(v, 6) for k, v in requested.items()},
        "weights": {k: round(v, 6) for k, v in effective.items()},
        "amounts": amounts,
        "fuse": fuse,
        "fuse_downgraded": bool(fuse["downgrade"]),
        "starship": {
            "ready": star["ready"],
            "requested_weight": round(requested.get("starship", 0.0), 6),
            "effective_weight": round(effective.get("starship", 0.0), 6),
            "refilled": refilled,
            "detail": star["detail"],
            "cap": cfg.STARSHIP_CAP,
        },
        "single_ticket_cap": cfg.SINGLE_TICKET_CAP[target_tier],
        "rebalance_orders": orders,
        "next_rebalance_rule": cfg.NEXT_REBALANCE_RULE,
        "cost_notes": {
            "rebalance": cfg.REBALANCE_COST_DESC,
            "switch": cfg.SWITCH_COST_DESC,
            "satellite": cfg.SAT_COST_DESC,
        },
    }


def _order_note(leg: str, tier: str) -> str:
    base = "monthly first trading day; ETFs whole shares; 5bp/side"
    if leg == "starship":
        return base + "; 14:30 list only, no print/limit-up-lock -> no fill (fail-closed)"
    if leg == "b3":
        return base + "; B3 OIL weight cap 10% (halve to 10% when monthly > 20%)"
    if leg == "cash":
        return base + "; money-market/REPO idle (B3-in-cash for tier B)"
    if leg in ("harbor", "m30"):
        return base + "; S-3 10 legs x 10% slots, no 1-lot constraint"
    return base
