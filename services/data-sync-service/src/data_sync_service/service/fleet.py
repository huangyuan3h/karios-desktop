"""Fleet (Jian Dui) display layer (read-only).

Named portfolio combining the three engines with the H2k revival gate
and the four defense lines. Pure functions only: no DB, no network,
no clock, no broker/order calls. 母港M30 stays the baseline (Yuan 2026-10-03);
Starship B stays observation (revival needs rolling-40-trade sum positive
AND monthly large-cap P&L positive); this module only sizes the observation
weight (read-only).

Wiring (frozen, see FLEET_report.md in karios-audit-2026-10):
- Tiers A/B/C via combo_tiers.resolve_asset_tier (buffers 155/145/210/180w,
  starting 120w A, monthly execution). Base A closed = 50% harbor +
  25% M30 + 25% B3 (A25 without starship, pro-rata).
- S-gap gate from the factor-vault revival state (H2k K2 N=40/X=75):
  rolling 40 trades percentile vs random >=75 and net>0 -> 10%,
  second confirmation -> 20%, <50 or breaker -> 0%, taken pro-rata
  from base, monthly execution, T-1 point-in-time. Caller passes the
  vault position (0/10/20, default 0 fail-closed matching 2026-10-03
  cold); this module never recomputes trades itself.
- Defense L1-L4 via combo_tiers.check_fuse (60d -15% downgrade,
  single-day -8% pause, weekly -5% halve) plus HOLDOUT_STOP -25% cash.
  Sample firing (120w long+holdout 1252d): L1 0, L2 69d, L3 20d, L4 0.
  L1/L4 never fired (free catastrophic insurance, keep); L2 costs
  ~1.4pt long for 0.2pt MDD, L3 costs ~1.2pt for 0.5pt MDD + 0.3pt
  holdout (small insurance taxes, keep as approved and flagged in UI).

Costs already in the frozen windows: satellite 32.28bp/trade,
monthly rebalance 5bp/side + 15bp/switch, 50M PIT filter + sqrt k150bp.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from data_sync_service.service import combo_tiers as ct
from data_sync_service.service import combo_tiers_config as cfg

# Frozen H2k gate params (same as factor_vault/sgap_decay).
REVIVAL_N: int = 40
REVIVAL_X: float = 75.0
REVIVAL_OFF: float = 50.0
GATE_STEPS: tuple[int, ...] = (0, 10, 20)

# Frozen fleet windows (FLEET_report.md Sec 1, 120w start, costs included).
# total/cagr/mdd/sharpe per window for the catalog + backtest page.
FLEET_WINDOWS: dict[str, dict[str, float]] = {
    "OOS2": {"total": 50.3, "cagr": 52.8, "mdd": -7.7, "sharpe": 2.24},
    "valid": {"total": 10.0, "cagr": 24.7, "mdd": -13.3, "sharpe": 1.06},
    "holdout": {"total": -8.4, "cagr": -45.8, "mdd": -9.5, "sharpe": -3.55},
    "long": {"total": 163.6, "cagr": 22.3, "mdd": -13.3, "sharpe": 1.30},
}

# Defense firing in the audit sample (1252 trading days, 120w).
DEFENSE_SAMPLE: dict[str, Any] = {
    "days": 1252,
    "L1_days": 0,
    "L2_days": 69,
    "L3_days": 20,
    "L4_days": 0,
    "recommendation": "keep-all",
    "note": (
        "L1/L4 never fired (free insurance, keep); "
        "L2 costs ~1.4pt long for 0.2pt MDD (keep, flagged); "
        "L3 costs ~1.2pt long for 0.5pt MDD + 0.3pt holdout (keep, flagged)."
    ),
}

SOURCE_LABEL = "karios-audit-2026-10/FLEET_report.md (120w, 50M PIT, monthly)"


def normalize_gate(gate: int | None) -> float:
    """Vault position 0/10/20 -> weight fraction (default 0 fail-closed)."""
    if gate is None:
        return 0.0
    try:
        g = int(gate)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"gate must be 0/10/20, got {gate!r}") from exc
    if g not in GATE_STEPS:
        raise ValueError(f"gate must be 0/10/20, got {gate!r}")
    return g / 100.0


def defense_states(nav_60d: list[float] | tuple[float, ...] | None) -> dict[str, Any]:
    """L1-L4 flags from a trailing NAV series (oldest -> newest, T-1).

    Uses combo_tiers.check_fuse for L1 (downgrade), L2 (pause_new),
    L3 (halve); L4 fires when the 60d drawdown reaches HOLDOUT_STOP
    -25% (conservative proxy for the all-time -25% cash rule: a 60d
    -25% implies all-time >=25%, so this fires a subset, never extra).
    Empty/short series => all clear (fail-open planning).
    """
    fuse = ct.check_fuse(nav_60d)
    drawdown = fuse.get("drawdown_60d")
    l4 = bool(drawdown is not None and drawdown <= cfg.HOLDOUT_STOP + 1e-9)
    return {
        "L1_downgrade": bool(fuse.get("downgrade")),
        "L2_pause_new": bool(fuse.get("pause_new")),
        "L3_halve": bool(fuse.get("halve")),
        "L4_cash": l4,
        "drawdown_60d": drawdown,
        "last_day_ret": fuse.get("last_day_ret"),
        "last_week_ret": fuse.get("last_week_ret"),
        "reasons": list(fuse.get("reasons") or [])
        + (["trailing-60d <= -25% -> cash (L4)"] if l4 else []),
        "sample": dict(DEFENSE_SAMPLE),
    }


def fleet_plan(
    total_assets: float,
    current_tier: str | None = None,
    nav_60d: list[float] | tuple[float, ...] | None = None,
    starship_gate: int | Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Read-only fleet target (tier + gate + defense -> weights).

    - total_assets: account total (month-end, T-1). Falls back to the
      frozen 120w default when None (display default, never a live feed).
    - current_tier: A | B | C (None => A attack default).
    - nav_60d: trailing NAVs for L1-L4 (None => all clear).
    - starship_gate: factor-vault position 0/10/20 (int) or mapping
      with position/ready (None => 0 fail-closed).
    Returns target tier, effective weights/amounts, gate + defense
    states, and the frozen window table for display. Never touches
    live params or orders.
    """
    if total_assets is None:
        total_assets = float(cfg.DEFAULT_TOTAL_ASSETS)
    if isinstance(starship_gate, Mapping):
        gate_pct: float
        if "position" in starship_gate and starship_gate["position"] is not None:
            gate_pct = normalize_gate(starship_gate["position"])
        elif starship_gate.get("ready") is True:
            gate_pct = 0.20
        else:
            gate_pct = 0.0
    else:
        gate_pct = normalize_gate(starship_gate)
    # Base combo plan with the starship front gate closed; then override
    # the starship weight to the vault gate (0/10/20) pro-rata.
    # ct.plan_tier applies the OLD three-condition gate (paper20 etc.);
    # pass explicit True/False only to size the base, then set gate here.
    base = ct.plan_tier(total_assets, current_tier, nav_60d, False)
    asset_tier: str = base["asset_tier"]
    fuse_downgraded: bool = bool(base["fuse_downgraded"])
    target_tier = base["target_tier"]
    requested = dict(base["weights_requested"])
    # Override starship to the vault gate pro-rata from requested non-star.
    req_star = float(requested.get("starship", 0.0))
    if abs(gate_pct - req_star) > 1e-12 and target_tier != cfg.TIER_CASH:
        non = [leg for leg in ("harbor", "m30", "b3", "cash")]
        denom = sum(float(requested.get(leg, 0.0)) for leg in non)
        effective: dict[str, float] = {}
        if denom <= 0:
            effective = {leg: 0.0 for leg in requested}
            effective["cash"] = 1.0 - gate_pct
            effective["starship"] = gate_pct
        else:
            scale = (1.0 - gate_pct) / denom
            for leg in non:
                effective[leg] = float(requested.get(leg, 0.0)) * scale
            effective["starship"] = gate_pct
    else:
        # Gate equals the requested starship (or CASH): use requested as-is.
        effective = {k: float(v) for k, v in requested.items()}
        if target_tier == cfg.TIER_CASH:
            effective = {k: float(v) for k, v in base["weights"].items()}
    # Enforce the 20% starship cap even if a future config drifts.
    if effective.get("starship", 0.0) - cfg.STARSHIP_CAP > 1e-12:
        over = effective["starship"] - cfg.STARSHIP_CAP
        effective["starship"] = cfg.STARSHIP_CAP
        non = [leg for leg in ("harbor", "m30", "b3", "cash")]
        denom = sum(float(requested.get(leg, 0.0)) for leg in non) or 1.0
        for leg in non:
            effective[leg] = effective.get(leg, 0.0) + over * (
                float(requested.get(leg, 0.0)) / denom
            )
    ta = float(total_assets)
    amounts = {leg: round(ta * w, 2) for leg, w in effective.items()}
    defense = defense_states(list(nav_60d) if nav_60d else None)
    # L4 terminal overrides everything to cash (display plan).
    if defense["L4_cash"]:
        target_tier = cfg.TIER_CASH
        effective = {leg: 0.0 for leg in effective}
        effective["cash"] = 1.0
        amounts = {leg: round(ta * w, 2) for leg, w in effective.items()}
    return {
        "current_tier": base["current_tier"],
        "asset_tier": asset_tier,
        "target_tier": target_tier,
        "total_assets": ta,
        "weights": {k: round(float(v), 6) for k, v in effective.items()},
        "amounts": amounts,
        "gate": {
            "position": int(round(gate_pct * 100)),
            "weight": round(gate_pct, 4),
            "rule": f"H2k-K2 N={REVIVAL_N}/X={REVIVAL_X}",
            "source": "factor-vault starship_b position (0 fail-closed)",
        },
        "defense": defense,
        "fuse_downgraded": fuse_downgraded,
        "single_ticket_cap": base["single_ticket_cap"],
        "next_rebalance_rule": base["next_rebalance_rule"],
        "cost_notes": base["cost_notes"],
        "windows": {k: dict(v) for k, v in FLEET_WINDOWS.items()},
        "disclaimer": (
            "母港M30 stays the baseline; Starship B stays observation "
            "(revival needs rolling-40-trade sum positive AND monthly large-cap "
            "P&L positive); fleet is research/display "
            "only (no orders). L1/L4 never fired in sample (free insurance); "
            "L2/L3 carry small insurance taxes (flagged, kept as approved)."
        ),
        "source": SOURCE_LABEL,
    }
