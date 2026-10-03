"""Frozen combo-tier config (Karios RESTRUCTURE PR1, read-only).

All numbers are FROZEN — changing any of them defines a NEW strategy and
must go through validation-gates-v2 + leave a record (RESTRUCTURE Sec 3).
This module has NO imports from broker / order / strategy-param code paths
on purpose: the combo layer only reads these constants.

Source key (Chinese comments keep the audit trail next to each value):
- H2j = H2j_capital_tiers.md (2026-10-03)
- RST = RESTRUCTURE_plan.md Sec 2.3 / Sec 3 (2026-10-03)
- PROMPT = .opencode-runs/karios_step1_prompt.md (Yuan 2026-10-03 locked decisions)
- FINAL = FINAL_REPORT.md Sec 0
- H2k = H2k_sgap_revival.md (2026-10-03)
- RECIPES = docs/modules/strategy-recipes.md
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

# Account total assets default (yuan). PROMPT Sec 3: read real account assets,
# fall back to configurable default 1.2M when no source is available.
# Env override: KARIOS_TOTAL_ASSETS (read by API/CLI, never written here).
DEFAULT_TOTAL_ASSETS: Final[float] = 1_200_000.0

# Tier ids. "CASH" is the fuse terminal state (all money-market/REPO),
# reached only via 60d-fuse from C. RST Sec 2.3: A->B->C->all-cash/REPO.
TIER_A: Final[str] = "A"
TIER_B: Final[str] = "B"
TIER_C: Final[str] = "C"
TIER_CASH: Final[str] = "CASH"
VALID_TIERS: Final[tuple[str, ...]] = (TIER_A, TIER_B, TIER_C)
ALL_TIERS: Final[tuple[str, ...]] = (TIER_A, TIER_B, TIER_C, TIER_CASH)

# Fixed mix per tier (fractions of account total assets, sum == 1.0).
# Keys: harbor (E1 pure S-3 leg), m30 (E1 packaging 70% harbor + 30% B3),
# b3 (E2 B3 5-ETF / B-leg / money-market leg), starship (E3 capacity starship),
# cash (money-market/REPO idle, incl. B3-in-cash for tier B).
# - A25: 40% harbor + 20% M30 + 20% B3 + 20% capacity starship.
#   H2j Sec 2 A25 + RST Sec 2.3 + PROMPT default attack tier A25.
# - B0: 50% harbor + 30% M30 + 20% cash (B3/money-fund inside cash, monthly;
#   starship 0% until C). H2j Sec 2 B0 + RST Sec 2.3.
# - C200: 60% M30 + 20% harbor + 10% B-leg/money-fund + 10% capacity starship.
#   H2j Sec 2 C200 + RST Sec 2.3 (FINAL 200w system as-is).
# - CASH: 100% cash. RST Sec 2.3 fuse terminal + H2j Sec 3.
WEIGHTS_A: Final[dict[str, float]] = {
    "harbor": 0.40,
    "m30": 0.20,
    "b3": 0.20,
    "starship": 0.20,
    "cash": 0.0,
}
WEIGHTS_B: Final[dict[str, float]] = {
    "harbor": 0.50,
    "m30": 0.30,
    "b3": 0.0,
    "starship": 0.0,
    "cash": 0.20,
}
WEIGHTS_C: Final[dict[str, float]] = {
    "harbor": 0.20,
    "m30": 0.60,
    "b3": 0.10,
    "starship": 0.10,
    "cash": 0.0,
}
WEIGHTS_CASH: Final[dict[str, float]] = {
    "harbor": 0.0,
    "m30": 0.0,
    "b3": 0.0,
    "starship": 0.0,
    "cash": 1.0,
}
WEIGHTS_BY_TIER: Final[dict[str, dict[str, float]]] = {
    TIER_A: dict(WEIGHTS_A),
    TIER_B: dict(WEIGHTS_B),
    TIER_C: dict(WEIGHTS_C),
    TIER_CASH: dict(WEIGHTS_CASH),
}

# Starship slot cap (fraction of combo). PROMPT locked 20% + H2j Sec 4
# (over 20% is suicide: A35 valid/holdout double-loss) + RST Sec 2.6.
STARSHIP_CAP: Final[float] = 0.20

# Tier-switch thresholds (account total assets, yuan, month-end close,
# executed on next month first trading day, T-1 basis, 5min with B-leg).
# H2j Sec 3 + RST Sec 2.3 + PROMPT locked values.
# Buffers: 145-155w and 180-210w hold current tier (no flip-flop).
A_TO_B: Final[float] = 1_550_000.0  # A->B needs >= 155w (+5w buffer)
B_TO_A: Final[float] = 1_450_000.0  # B->A needs <= 145w (-5w buffer)
B_TO_C: Final[float] = 2_100_000.0  # B->C needs >= 210w (30w buffer up)
C_TO_B: Final[float] = 1_800_000.0  # C->B needs <= 180w (30w buffer down)

# Initial-tier inference bands (only when caller passes no current tier).
# A 100-150w / B 150-200w / C 200w+. H2j Sec 2 + RST Sec 2.3.
INIT_A_MAX: Final[float] = 1_500_000.0
INIT_B_MAX: Final[float] = 2_000_000.0

# Fuse / circuit-breaker thresholds (fractions, negative = loss).
# - FUSE_DRAWDOWN_60D: forward/rolling 60d -15% downgrades one tier.
#   H2j Sec 3 + RST Sec 2.3 + PROMPT (looser than FINAL -10%).
# - SINGLE_DAY_STOP: single-day -8% pauses new positions (paper/review only).
#   FINAL Sec 0.6 + H2j Sec 3 + RST Sec 3 (e.g. 603125 -14.67% * 25% slot).
# - WEEKLY_HALVE: single-week -5% halves positions (rest to REPO).
#   H2j Sec 3 (3 same-day full losses or single-week -5% -> half) + RST Sec 3.
# - HOLDOUT_STOP: holdout extension to -25% stops everything to paper.
#   H2j Sec 3 + RST Sec 3 (current harbor -11.44% / M30 -8.83% not hit).
FUSE_DRAWDOWN_60D: Final[float] = -0.15
SINGLE_DAY_STOP: Final[float] = -0.08
WEEKLY_HALVE: Final[float] = -0.05
HOLDOUT_STOP: Final[float] = -0.25

# Starship observation-gate front conditions (OLD three, used until H2k lands).
# PROMPT locked + RST Sec 2.6 + FINAL Sec 0.6.5 / G Sec 4 / H2b Sec 2.7:
# paper 20 fills same-recipe mean > 0 AND holdout recovered to within -10%
# AND filtered valid turned positive (> 0); otherwise starship weight 0%
# and refill proportionally into base legs. Current state (2026-10-03):
# paper ~4/20 + holdout -19.1% + filtered valid -6.4% => all unmet => 0%.
PAPER_REQUIRED_N: Final[int] = 20
HOLDOUT_RECOVER_TO: Final[float] = -0.10
FILTERED_VALID_POSITIVE: Final[float] = 0.0

# Cost / execution notes (already deducted in H2j blends; shown on orders).
# H2j Sec 2 + RST Sec 2.3.
REBALANCE_COST_DESC: Final[str] = "5bp/side monthly rebalance"
SWITCH_COST_DESC: Final[str] = "15bp/switch"
SAT_COST_DESC: Final[str] = "32.28bp/trade + sqrt k150bp impact"
NEXT_REBALANCE_RULE: Final[str] = "month-end total-assets judgement, next-month first trading day"

# Per-ticket caps (fraction of combo; display guards, not order logic).
# H2j Sec 2: A 2.5% combo; B slot 10%, combo single <= 5%
# (harbor+M30 combined <= 70%, H2d Sec 5.1, FINAL Sec 0.3 note);
# C 2.5% (5w/200w), B3 OIL weight <= 10% (halve to 10% when > 20% monthly).
SINGLE_TICKET_CAP: Final[dict[str, float]] = {
    TIER_A: 0.025,
    TIER_B: 0.05,
    TIER_C: 0.025,
    TIER_CASH: 1.0,
}

# Starship frozen execution recipe (reference only; combo layer never edits).
# H2j Sec 3 + RST Sec 3 + RECIPES 52-64: S-gap > 3% + daily amp top-1/3 +
# C1 skip + body == 3 + daily turnover >= 50M filter + 4 slots
# (A35 8 slots diluted only as alternate); T+1 open buy, day-3 close sell;
# 14:30 print required (fail-closed), limit-up lock skip, limit-down frozen
# exit_skip_limit_down handling.
STARSHIP_RECIPE_DESC: Final[str] = (
    "S-gap>3% + amp-top-third + C1-3% skip + body=3 + turnover>=50M + 4 slots"
)


@dataclass(frozen=True)
class ComboConfig:
    """Single frozen instance holder (import COMBO to read everything)."""

    default_total_assets: float = DEFAULT_TOTAL_ASSETS
    starship_cap: float = STARSHIP_CAP
    a_to_b: float = A_TO_B
    b_to_a: float = B_TO_A
    b_to_c: float = B_TO_C
    c_to_b: float = C_TO_B


COMBO: Final[ComboConfig] = ComboConfig()
