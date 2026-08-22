"""TrendOK tunable recipe — 15+ knobs extracted for B-T1 (2026-08-22).

Every value matches the current hard-coded live truth (see service/trendok.py).
Live path keeps the frozen DEFAULT; experiments pass an explicit instance
through `compute_trendok_for_symbols(..., params=…)` and the backtest
`BacktestConfig.trendok_params` (walk-forward --param trendok_*).

Add a knob here ONLY when it has a business story + triple-window bar.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TrendOKParams:
    # -- V4 score weights (must sum≈1.0 for interpretability, not enforced) --
    w_ema: float = 0.40
    w_macd: float = 0.20
    w_break: float = 0.10
    w_rsi: float = 0.10
    w_vol: float = 0.20

    # -- caps / macro --
    failed_score_cap: float = 79.0
    macro_lock_down_threshold: int = 3500
    low_volume_ratio_threshold: float = 1.2
    low_volume_ratio_score_cap: float = 79.0

    # -- bonus --
    bonus_ema20_slope_5d: float = 5.0

    # -- anti-spike penalties --
    intraday_surge_threshold_pct: float = 6.0
    intraday_surge_penalty: float = 20.0
    atr_ratio_threshold: float = 0.05
    atr_penalty_scale: float = 1000.0  # (atr_ratio - thr)*scale
    volume_climax_mult: float = 3.0
    volume_climax_penalty: float = 15.0
    below_ema20_penalty: float = 30.0

    # -- volume sub-score breaks (ratio = avg5/avg30) --
    vol_break_1: float = 1.0
    vol_break_2: float = 1.2
    vol_break_3: float = 2.0
    vol_break_4: float = 3.0

    # -- industry flow deltas --
    flow_5d_top3: float = 10.0
    flow_5d_bottom5: float = -20.0
    flow_today_top3: float = 5.0
    flow_today_top4_5: float = 3.0
    flow_falloff_big_outflow: float = -15.0
    flow_absent_2d_big_outflow: float = -10.0
    flow_large_outflow: float = -1.0e8

    # -- Alpha S recovering --
    alpha_vol_mult: float = 2.5
    alpha_score_floor: float = 60.0

    def validate(self) -> None:
        # light sanity, not strict — walk-forward is the real gate
        if not 0 < self.failed_score_cap <= 100:
            raise ValueError("failed_score_cap must be (0,100]")
        if not 0 < self.low_volume_ratio_threshold <= 5:
            raise ValueError("low_volume_ratio_threshold out of range")
        if not 0 <= self.w_ema <= 1 or not 0 <= self.w_vol <= 1:
            raise ValueError("weights must be in [0,1]")
        # sum check is advisory; a lopsided recipe is allowed but logged
        total = self.w_ema + self.w_macd + self.w_break + self.w_rsi + self.w_vol
        if not 0.9 <= total <= 1.1:
            # not fatal — caller may want 0.5/0.5 degenerate for A/B tests
            pass


DEFAULT_TRENDOK_PARAMS = TrendOKParams()
DEFAULT_TRENDOK_PARAMS.validate()
