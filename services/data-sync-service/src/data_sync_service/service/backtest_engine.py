"""Backtest engine v1.5 (OPT-070) — signal replay with LIVE close logic + gates.

The engine replays the system's OWN historical signals and simulates a
satellite-book paper portfolio. The point is **same-code discipline**: entry
uses the TrendOK score the system actually recorded that day
(``watchlist_score_daily``), optionally filtered through the same entry gates
the live decision path applies (index traffic-light regime, sector fund-flow,
mainline whitelist), and every close condition is delegated to
``service.paper_trading._pick_close_reason`` — the exact function the live
paper-trading cron runs. There is no second copy of any rule.

Scope (v1.5):

- CN only. HK/ETF score rows are excluded.
- Signals: ``watchlist_score_daily`` (records start 2026-06-18). The TV
  screener universe is NOT consumed yet.
- Gates (``config.gates``, default ``full`` — matches live decision path):
  - ``none``   — v0 behaviour, score threshold only.
  - ``regime`` — index traffic-light regime must be STRONG
    (``get_index_signals(as_of_date=day)`` + ``classify_market_regime``,
    the exact functions the live gate runs).
  - ``full``   — regime AND sector fund-flow gate (whole-market SW L1
    net inflow <= 0 blocks, same rule as ``sectorOutflowBlock``) AND
    mainline gate (symbol's EM industry must be in the 5D net-inflow Top3,
    same whitelist as the live BUY mainline check).
  Gate data missing for a day → fail-closed (blocks entries), consistent
  with the live fail-closed posture. Blocked entry attempts are counted in
  ``summary.gated_blocks`` per reason.
- Entry: signal-day close, full position, score >= ``score_threshold``,
  all enabled gates passed.
- Costs: net pnl = gross - market round-trip cost (``paper_cost_model``),
  applied at close time — identical to live paper v0.2.
- Close reasons: stop_hit / target_hit / score_floor / max_hold from the
  live picker. ``pool_exit`` is OFF (no registry history → fail-open
  by design). Leftover open positions at window end close with reason
  ``end_of_window`` (engine-only, never emitted by live code).
- Look-ahead discipline: every value used on day D is recorded on or before
  D. The score injected into the close picker is the score recorded ON day
  D; missing score → fail-open (no score_floor close).

Intended use: parameter-sensitivity analysis ONLY (score threshold, hold,
stop, gates). Outputs are not a release decision basis — the paper book is.
"""

from __future__ import annotations

import logging
import statistics
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from typing import Any

from data_sync_service.db import get_connection
from data_sync_service.db.daily import fetch_ohlcv_batch_between
from data_sync_service.db.industry_fund_flow import get_dates_upto, get_rows_for_dates
from data_sync_service.service.env_label import ENV_FAN, ENV_NEUTRAL, ENV_UPTREND, ENV_WEAK
from data_sync_service.service.execution_gate import (
    REGIME_DIVERGING,
    REGIME_STRONG,
    REGIME_WEAK,
    classify_market_regime,
)
from data_sync_service.service.industry_fund_flow_read import top_by_date_from_rows
from data_sync_service.service.market_regime import get_hk_regime, get_index_signals
from data_sync_service.service.paper_cost_model import MARKETS, round_trip_cost_pct
from data_sync_service.service.paper_trading import _pick_close_reason, _resolve_ts_code

logger = logging.getLogger(__name__)

SCORE_TABLE = "watchlist_score_daily"

CLOSE_REASON_END_OF_WINDOW = "end_of_window"
CLOSE_REASON_TRAILING = "trailing_stop"
CLOSE_REASON_SWAPPED = "swapped"
CLOSE_REASON_FLOW_EXIT = "flow_exit"
CLOSE_REASON_TIME_STOP = "time_stop"  # P17: underwater N days → cut
CLOSE_REASON_DELISTED = "delisted"  # survivor-bias guard: name delisted → forced exit at last close

GATE_LEVELS = ("none", "regime", "full")

GATE_REASON_REGIME = "regime"
GATE_REASON_FLOW = "flow"
GATE_REASON_MAINLINE = "mainline"
GATE_REASON_SENTIMENT = "sentiment"
GATE_REASON_INDEX_RED = "index_red"

# OPT-103: A-share board price limits (main 10% / ChiNext+STAR 20% / BSE 30%).
# Derived from the previous session's close — no extra data source needed.
# ST 5% is not modeled (no ST flag in the daily table); HK has no limits.
def _board_limit_pct(ts: str) -> float | None:
    code = str(ts).split(".")[0]
    if code.startswith(("300", "301", "688")):
        return 0.20
    if code.startswith(("8", "4")):  # BSE
        return 0.30
    if code.startswith(("60", "00")):
        return 0.10
    return None


def _at_limit(data, ts: str, day: str, close_px: float, *, up: bool) -> bool:
    """True when ``close_px`` closed pinned at the board limit (limit-up →
    cannot buy in; limit-down → cannot sell out). qfq prices scale the ratio,
    so a 1-cent tolerance absorbs the rounding."""
    limit_pct = _board_limit_pct(ts)
    if limit_pct is None:
        return False
    series = data.closes_by_ts.get(ts)
    if not series:
        return False
    prev: float | None = None
    for d, c in series:
        if str(d) >= day:
            break
        prev = float(c)
    if prev is None or prev <= 0:
        return False
    limit_px = round(prev * (1.0 + (limit_pct if up else -limit_pct)), 2)
    if up:
        return close_px >= limit_px - 0.01
    return close_px <= limit_px + 0.01


# Live mainline momentum-breakout thresholds (hot-industry-picks.ts).
MOMENTUM_THRESHOLD_YI = 20e8
MOMENTUM_RANK_CHANGE = 10


@dataclass(frozen=True)
class BacktestConfig:
    """One simulation configuration (a point in the sensitivity grid)."""

    start_date: str
    end_date: str
    score_threshold: float = 85.0
    max_hold_days: int = 5
    stop_loss_pct: float = -5.0
    target_pnl_pct: float = 10.0
    score_floor: float = 30.0
    market: str = "CN"
    gates: str = "full"
    trailing_stop_pct: float = 0.0
    profit_trail_trigger_pct: float = 0.0
    profit_trail_pct: float = 0.0
    industry_flow_exit_days: int = 0
    mainline_top_k: int = 3
    score_confirm_days: int = 0
    position_pct: float = 0.05
    max_positions: int = 10
    rs_rank_min: float = 0.0
    diverging_scale: float = 0.0
    drawdown_circuit_pct: float = 0.0
    drawdown_circuit_window_days: int = 30
    panic_cooldown_days: int = 0
    light_red_block: bool = False
    slippage_pct: float = 0.0
    trend_score_min: float = 0.0
    swap_weak_rs_below: float = 0.0
    swap_strong_rs_at_least: float = 0.0
    swap_min_hold_days: int = 0
    swap_max_per_day: int = 0
    pyramid_trigger_pct: float = 0.0
    pyramid_add_scale: float = 0.0
    pyramid_max_adds: int = 0
    atr_size_window: int = 0
    atr_size_cap: float = 2.0
    atr_benchmark_pct: float = 2.0
    # OPT-104: per-symbol volatility-adaptive stops — when >0, the stop and
    # trailing lines become entry-time ATR% x mult instead of the fixed
    # stop_loss_pct / trailing_stop_pct (per-trade, locked at entry).
    atr_stop_mult: float = 0.0
    # OPT-105: hybrid mode — ATR line only in Strong; Weak always fixed.
    # When set, Diverging sessions also fall back to the fixed line.
    atr_stop_strong_only: bool = False
    # §19.2 D1: continuous-market-strength selector. When >0, replaces the
    # regime condition: ATR line applies when today's 0-100 strength score
    # (regime_strength_score) is >= this floor. 0 = use the regime rule.
    atr_stop_strength_min: float = 0.0
    max_per_industry: int = 0
    entry_sort: str = "score"
    min_mv: float = 0.0
    max_mv: float = 0.0
    mv_max_diverging: float = 0.0
    exclude_boards: str = ""
    board_exclude_set: frozenset[str] = frozenset()
    # Entry-price mode for a signal-day entry:
    #   close           — signal-day close (legacy default)
    #   last_hour_low   — approximate "buy the 14:00-15:00 dip" (no minute bars):
    #                     low*0.5 + close*0.5, clamped below close (A股 14:00-15:00
    #                     尾盘; HK 15:00-16:00 尾盘). Purely OHLC-based proxy.
    #   last_hour_hl    — midpoint of the last-hour proxy: (low*0.5+close*0.5 + close)/2
    #   next_open       — next-session open (signal-day close → T+1 买入)
    entry_mode: str = "close"
    # TIP-014: entry STYLE — what kind of candidate to prefer/allow on a
    # signal day, per market environment (see service/env_label.py):
    #   score     — no style filter (baseline, matches live today)
    #   momentum  — only RS >= entry_style_rs_min candidates that are NOT in a
    #               short-term pullback (5d return >= -entry_style_dip_max)
    #   dip       — only RS >= entry_style_rs_min candidates IN a short-term
    #               pullback (5d return <= -entry_style_dip_min)
    #   auto      — environment-aware: uptrend → momentum, fan → dip,
    #               weak → blocked, neutral → score (no filter)
    entry_style: str = "score"
    entry_style_rs_min: float = 0.8
    entry_style_dip_min: float = 5.0
    entry_style_dip_max: float = 3.0
    # HK auto style mapping override (TIP-014 HK experiment): HK regime
    # buckets map differently from CN (HK Strong days LOST money in valid
    # window while Diverging days were best). Format: "Strong:dip,Diverging:momentum,Weak:blocked"
    # empty (default) → HK uses the CN-style mapping (Strong→momentum,
    # Diverging→dip). Experimental only; CN always uses env_label.
    hk_style_map: str = ""
    # D2 (TIP-014 follow-up): environment-aware max-hold — positions entered
    # on an UPTREND day are force-closed after this many days (0 = off, same
    # as max_hold_days for every entry). Rationale: 主升日买入吃主升段就跑.
    # Experimental — full-window max_hold scans showed two-window conflicts
    # (hold45: valid +11.4 / OOS2 -13.5), so env-aware is the only variant
    # that could pass the three-window bar.
    max_hold_env_shorten: int = 0
    # NOTE (TIP-014, 2026-08-14): entry_style is INDEPENDENT of industry. The
    # style filters use only per-stock RS rank + 5d return — the industry
    # restriction comes from a SEPARATE layer, the dynamic mainline gate
    # (mainline_allow_by_day: 5D net-inflow Top3 industries, recomputed daily
    # — never hardcoded). 2026 buys concentrate in 电子 because the market's
    # mainline IS 电子, not because the style rules mention it.
    # TIP-014 finding #3: days with sentiment data but classified NEUTRAL
    # (not uptrend / not fan / not weak) showed 16/16 losing trades (valid
    # window, avg -6.1%) — block new entries on them. UNKNOWN days (no
    # sentiment data, e.g. pre-2026 OOS2) are NOT blocked.
    neutral_block: bool = False
    # D3 (TIP-014 follow-up): environment-aware position sizing. When set, a
    # new entry's sleeve is scaled by the factor of its ENTRY day's env label
    # (e.g. "uptrend:1.2,fan:0.8" = 12% sleeves on uptrend days, 8% on fan).
    # Unknown env / unmapped labels keep scale 1.0. Purely a leverage knob —
    # expected to shift the return/DD ratio, not the trade selection.
    env_position_scale: str = ""
    # P1 (signal pool, 2026-08-15): turtle Donchian breakout as an ADDITIVE
    # entry gate — a candidate needs close > N-day high on the entry day
    # (0 = off). Never replaces RS/score/env; verified by three windows +
    # long window before it may join the live config.
    breakout_days: int = 0
    # P2 (signal pool, 2026-08-15): volume breakout as an ADDITIVE entry
    # gate — the entry-day volume must exceed K x its 20-day average volume
    # (0 = off; 1.5 = 50% above the 20d mean). Volume is a new dimension
    # (RS/score know nothing about it) — same three-window verification bar.
    volume_breakout_mult: float = 0.0
    # P4 (signal pool, 2026-08-15): MA20 slope filter as an ADDITIVE entry
    # gate — the entry-day 20-day simple moving average must be rising by
    # at least X% over the prior 20 sessions (0 = off; 2 = MA20 up >= 2% /
    # 20 sessions). Distinct from A2's "MA alignment" state: this measures
    # the CHANGE (acceleration), not the state. Same three-window bar.
    ma_slope_min_pct: float = 0.0
    # P3 (signal pool, 2026-08-15): 200-day MA filter as an ADDITIVE entry
    # gate — the entry-day close must be at least X% ABOVE the 200-session
    # simple moving average (-1 = off; 0 = close > MA200 (state only);
    # 5 = close > MA200 x 1.05). Long-horizon trend-state filter (stock
    # level, not index — the index level is already covered by the regime
    # gate). Same three-window bar. NOTE: -1 is the OFF sentinel so that
    # 0.0 (pure state filter) is a valid ENABLED value.
    ma200_min_pct: float = -1.0
    # P5 (signal pool, 2026-08-15): dual-MA golden cross as an ADDITIVE
    # entry gate — the entry day must be within N sessions AFTER the MA5
    # crossed above MA20 (0 = off; 5 = entry allowed within 5 sessions of
    # the cross). Event-driven (cross), distinct from the A2 alignment
    # state. Same three-window bar.
    ma_cross_days: int = 0
    # P6 (signal pool, 2026-08-15): three-line MA alignment as an ADDITIVE
    # entry gate — the entry day must have MA5 > MA10 > MA20 (True = on).
    # This is the classic 三线多头排列; expected to repeat the A2
    # falsification (alignment state carries no increment) — included only
    # to close the question quickly. Same three-window bar.
    ma_aligned: bool = False
    # P7 (signal pool, 2026-08-15): short-term oversold reversal as an
    # ADDITIVE entry gate — RSI14 must be below X and the entry day must
    # close green (close > prev close) (0 = off; 30 = RSI14 < 30 + green
    # day). NOTE: the original "weak-env bottom fishing" version conflicts
    # with neutral_block (weak days are blocked — 16/16 losing trades in
    # valid); this tests the reversal filter INSIDE normal environments.
    rsi_reversal_max: float = 0.0
    # P8 (signal pool, 2026-08-15): down-day reversal as an ADDITIVE entry
    # gate — the PRIOR session must have fallen at least X% and the entry
    # day must close green (0 = off; 5 = prior -5% then green). Same
    # neutral_block caveat as P7.
    down_day_reversal_pct: float = 0.0
    # P16 (signal pool, 2026-08-15): ST/tail-risk exclusion as an ADDITIVE
    # entry gate — when True, names whose stock_basic name contains ST
    # (ST/*ST) are excluded from entries. Data is already local (name in
    # stock_basic); verified on the current baseline before it may join.
    # 2026-08-07 check: 33 of 673 score>=65 candidates were ST names.
    exclude_st: bool = False
    # P14 (signal pool, 2026-08-15): post-earnings drift (PEAD) as an
    # ADDITIVE entry gate — a candidate whose name announced a POSITIVE
    # earnings surprise (业绩预告 预增/扭亏/略增/续盈) within the last N
    # sessions is allowed at entry; names without a recent positive
    # announcement are blocked (0 = off; 30 = entry only within 30 sessions
    # of a positive forecast). Event data: db/stock_forecast (ann_date).
    pead_days: int = 0
    # P12 (signal pool, 2026-08-15): volatility-adjusted momentum as an
    # ADDITIVE entry gate — risk_adj_mom = ret(over ret_days) / stdev of
    # daily returns (over vol_days) must be >= min (0 = off). Filters for
    # names whose trend is strong PER UNIT of risk — expected to cut
    # high-vol chase entries and improve sharpe/DD rather than raw pnl.
    risk_adj_mom_ret_days: int = 0
    risk_adj_mom_vol_days: int = 60
    risk_adj_mom_min: float = 0.0
    # P11 (signal pool, 2026-08-15): industry-neutral RS / industry
    # momentum as ADDITIVE entry gates — two independent sub-filters, both
    # computed per day from the FULL daily table (equal-weighted industry
    # return over all members with a bar that day, industry mapping =
    # stock_eastmoney_industry, the same table the mainline gate uses).
    #   ind_mom_days>0        — candidate's industry must rank in the top
    #                           ind_mom_top_pct of ALL industries by that
    #                           window's average return (行业动量).
    #   ind_neutral_days>0    — candidate's window return must rank in the
    #                           top ind_neutral_rank_pct WITHIN its industry
    #                           (行业内选强 — the dimension RS rank, which is
    #                           whole-market, cannot see).
    # Window support: 20/60/120 sessions. Any other positive value disables.
    ind_mom_days: int = 0
    ind_mom_top_pct: float = 0.33
    ind_neutral_days: int = 0
    ind_neutral_rank_pct: float = 0.7
    # P10 (signal pool, 2026-08-15): 52-week-high proximity as an ADDITIVE
    # entry gate — the entry-day close must be within X% of its 250-session
    # high (0 = off; 80 = close >= 0.80 x 250d-high). CONTINUOUS state,
    # distinct from P1's discrete Donchian breakout (which requires an
    # actual new high). A2's falsified trend score included a "distance to
    # 52w high" component, so correlation with RS was pre-checked
    # (signal_p10_p9_correlation.py): candidate-pool |r| 0.19-0.26 < 0.5 —
    # ok to test; whole-market valid 0.51 is borderline, documented.
    high_52w_min_pct: float = 0.0
    # P9 (signal pool, 2026-08-15): mid-horizon cross-sectional momentum
    # as an ADDITIVE entry gate — the candidate's ret over mom_ret_days
    # (skipping the most recent mom_skip_days) must rank in the top
    # mom_rank_min of the whole market that day (0 = off; 0.5 = top 50%).
    # RS is 20d short-horizon; P9 is 120-250d mid-horizon — the correlation
    # pre-check showed |r| < 0.1, so the horizons are genuinely disjoint.
    mom_ret_days: int = 0
    mom_skip_days: int = 20
    mom_rank_min: float = 0.5
    # P17 (signal pool, 2026-08-15): portfolio-level risk controls — each
    # sub-item is tested alone (planned-doc §2 P17):
    #   min_avg_amount>0        — liquidity floor: exclude candidates whose
    #                             60-session average daily turnover (amount,
    #                             亿元) is below X. Small-cap tail defence;
    #                             data = daily.amount (already local).
    #   max_hold_unprofitable_days>0 — time stop: close a holding once it has
    #                             been open N days AND is still underwater
    #                             (net < 0). Cuts capital tie-up in flat
    #                             losers without forcing winners out early
    #                             (max_hold_days still bounds everything).
    min_avg_amount: float = 0.0
    max_hold_unprofitable_days: int = 0
    # B-T1: TrendOK recipe override — None = use live DEFAULT_TRENDOK_PARAMS.
    trendok_params: dict[str, float] | None = None

    def _env_position_scale(self, env: str | None) -> float:
        if not self.env_position_scale:
            return 1.0
        try:
            for part in self.env_position_scale.split(","):
                k, _, v = part.partition(":")
                if k.strip() == (env or ""):
                    return max(0.0, float(v))
        except ValueError:
            return 1.0
        return 1.0

    def __post_init__(self) -> None:
        if self.market not in MARKETS:
            raise ValueError(f"market must be one of {MARKETS} (got {self.market!r})")
        if self.start_date > self.end_date:
            raise ValueError("start_date must be <= end_date")
        if self.score_threshold < 0 or self.score_threshold > 100:
            raise ValueError("score_threshold must be in [0, 100]")
        if self.gates not in GATE_LEVELS:
            raise ValueError(f"gates must be one of {GATE_LEVELS} (got {self.gates!r})")
        if self.trailing_stop_pct > 0:
            raise ValueError("trailing_stop_pct must be <= 0 (0 disables, e.g. -8 = 8%% peak pullback)")
        if self.profit_trail_trigger_pct < 0:
            raise ValueError("profit_trail_trigger_pct must be >= 0 (0 disables, 10 = protect once the leg is +10%)")
        if self.profit_trail_pct > 0:
            raise ValueError("profit_trail_pct must be <= 0 (0 disables, e.g. -6 = allow only a 6%% pullback from the post-trigger peak)")
        if self.profit_trail_trigger_pct > 0 and self.profit_trail_pct == 0:
            raise ValueError("profit_trail_pct must be set when profit_trail_trigger_pct > 0")
        if self.industry_flow_exit_days < 0:
            raise ValueError("industry_flow_exit_days must be >= 0 (0 disables, 3 = exit when the holding's SW L1 industry 5d net inflow stays negative for 3 straight sessions)")
        if not 0 < self.position_pct <= 1:
            raise ValueError("position_pct must be in (0, 1]")
        if not 1 <= self.max_positions <= 100:
            raise ValueError("max_positions must be in [1, 100]")
        if not 0 <= self.rs_rank_min <= 1:
            raise ValueError("rs_rank_min must be in [0, 1] (0 disables, 0.8 = top 20% RS)")
        if not 0 <= self.diverging_scale <= 1:
            raise ValueError("diverging_scale must be in [0, 1] (0 = no entries when Diverging, 0.5 = half size)")
        if self.drawdown_circuit_pct > 0:
            raise ValueError("drawdown_circuit_pct must be <= 0 (0 disables, e.g. -5 = halt new entries when trailing 20d realized pnl < -5%)")
        if not 0 <= self.trend_score_min <= 100:
            raise ValueError("trend_score_min must be in [0, 100] (0 disables)")
        if not 0 <= self.swap_weak_rs_below <= 1:
            raise ValueError("swap_weak_rs_below must be in [0, 1] (0 disables, 0.3 = held stocks in the weakest 30% RS can be swapped out)")
        if not 0 <= self.swap_strong_rs_at_least <= 1:
            raise ValueError("swap_strong_rs_at_least must be in [0, 1] (0 disables, 0.8 = only top-20% RS candidates can replace)")
        if self.swap_min_hold_days < 0:
            raise ValueError("swap_min_hold_days must be >= 0")
        if self.swap_max_per_day < 0:
            raise ValueError("swap_max_per_day must be >= 0 (0 disables rotation)")
        if not 0 <= self.pyramid_trigger_pct <= 200:
            raise ValueError("pyramid_trigger_pct must be in [0, 200] (0 disables, 10 = add when the main leg is +10%)")
        if not 0 <= self.pyramid_add_scale <= 2:
            raise ValueError("pyramid_add_scale must be in [0, 2] (0.5 = add half the initial sleeve)")
        if not 0 <= self.pyramid_max_adds <= 5:
            raise ValueError("pyramid_max_adds must be in [0, 5] (0 disables)")
        if not 0 <= self.atr_size_window <= 120:
            raise ValueError("atr_size_window must be in [0, 120] (0 disables, 20 = size by 20-day ATR)")
        if not 1 <= self.atr_size_cap <= 5:
            raise ValueError("atr_size_cap must be in [1, 5] (2 = sleeves between 0.5x and 2x)")
        if not 0.5 <= self.atr_benchmark_pct <= 10:
            raise ValueError("atr_benchmark_pct must be in [0.5, 10] (2 = 2% daily vol gets the base sleeve)")
        if not 0 <= self.max_per_industry <= 100:
            raise ValueError("max_per_industry must be in [0, 100] (0 disables, 4 = at most 4 holdings per industry)")
        if self.entry_sort not in ("score", "score_rs", "rs"):
            raise ValueError(f"entry_sort must be one of ('score', 'score_rs', 'rs') (got {self.entry_sort!r})")
        if self.breakout_days < 0:
            raise ValueError("breakout_days must be >= 0 (0 disables, 20 = close > 20-day high required)")
        if self.volume_breakout_mult < 0:
            raise ValueError("volume_breakout_mult must be >= 0 (0 disables, 1.5 = 50%% above the 20d avg volume)")
        if self.ma_slope_min_pct < 0:
            raise ValueError("ma_slope_min_pct must be >= 0 (0 disables, 2 = MA20 rising >= 2%% over 20 sessions)")
        if self.ma200_min_pct < -1:
            raise ValueError("ma200_min_pct must be >= -1 (-1 disables; 0 = close > MA200; 5 = >= 5%% above MA200)")
        if self.ma_cross_days < 0:
            raise ValueError("ma_cross_days must be >= 0 (0 disables, 5 = entry within 5 sessions after the MA5/MA20 cross)")
        if self.rsi_reversal_max < 0:
            raise ValueError("rsi_reversal_max must be >= 0 (0 disables, 30 = RSI14 < 30 + green day required)")
        if self.down_day_reversal_pct < 0:
            raise ValueError("down_day_reversal_pct must be >= 0 (0 disables, 5 = prior session -5% then green day required)")
        if self.risk_adj_mom_ret_days < 0:
            raise ValueError("risk_adj_mom_ret_days must be >= 0 (0 disables; 120 = use 120-session return)")
        if self.pead_days < 0:
            raise ValueError("pead_days must be >= 0 (0 disables; 30 = entry only within 30 sessions of a positive forecast)")
        if self.risk_adj_mom_vol_days < 5:
            raise ValueError("risk_adj_mom_vol_days must be >= 5 (volatility window)")
        if self.risk_adj_mom_min < 0:
            raise ValueError("risk_adj_mom_min must be >= 0 (threshold on ret/vol)")
        if self.ind_mom_days not in (0, 20, 60, 120):
            raise ValueError("ind_mom_days must be 0 (off) or one of 20/60/120")
        if not 0 < self.ind_mom_top_pct <= 1:
            raise ValueError("ind_mom_top_pct must be in (0, 1] (0.33 = top third of industries)")
        if self.ind_neutral_days not in (0, 20, 60, 120):
            raise ValueError("ind_neutral_days must be 0 (off) or one of 20/60/120")
        if not 0 < self.ind_neutral_rank_pct <= 1:
            raise ValueError("ind_neutral_rank_pct must be in (0, 1] (0.7 = top 30% within industry)")
        if not 0 <= self.high_52w_min_pct <= 100:
            raise ValueError("high_52w_min_pct must be in [0, 100] (0 disables; 80 = close >= 0.80 x 250d-high)")
        if self.mom_ret_days not in (0, 60, 120, 250):
            raise ValueError("mom_ret_days must be 0 (off) or one of 60/120/250")
        if self.mom_skip_days < 0:
            raise ValueError("mom_skip_days must be >= 0 (20 = skip the most recent 20 sessions)")
        if not 0 < self.mom_rank_min <= 1:
            raise ValueError("mom_rank_min must be in (0, 1] (0.5 = top 50% whole-market momentum rank)")
        if self.min_avg_amount < 0:
            raise ValueError("min_avg_amount must be >= 0 (亿元; 0 disables)")
        if self.max_hold_unprofitable_days < 0:
            raise ValueError("max_hold_unprofitable_days must be >= 0 (0 disables; 20 = close underwater holdings after 20 days)")
        if self.entry_mode not in ("close", "last_hour_low", "last_hour_hl", "next_open"):
            raise ValueError(
                "entry_mode must be one of ('close', 'last_hour_low', 'last_hour_hl', 'next_open') "
                f"(got {self.entry_mode!r})"
            )
        if self.entry_style not in ("score", "momentum", "dip", "auto"):
            raise ValueError(
                "entry_style must be one of ('score', 'momentum', 'dip', 'auto') "
                f"(got {self.entry_style!r})"
            )
        if self.min_mv < 0 or self.max_mv < 0 or self.mv_max_diverging < 0:
            raise ValueError("min_mv / max_mv / mv_max_diverging must be >= 0 (亿元; 0 disables the bound)")
        if self.exclude_boards:
            prefixes = {p.strip() for p in self.exclude_boards.split(",") if p.strip()}
            if not all(len(p) == 3 and p.isdigit() for p in prefixes):
                raise ValueError("exclude_boards must be comma-separated 3-digit board prefixes like '300,688'")
            object.__setattr__(self, "board_exclude_set", frozenset(prefixes))


@dataclass
class BacktestTrade:
    """One simulated round trip. Fields mirror the paper_trades row shape."""

    symbol: str
    market: str
    entry_date: str
    entry_price: float
    close_date: str
    close_price: float
    gross_pnl_pct: float
    costs_pct: float
    pnl_pct: float
    holding_days: int
    close_reason: str
    score_at_entry: float | None
    position_pct: float = 0.05

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class BacktestSummary:
    """Headline stats for one configuration."""

    config: dict[str, Any]
    calendar_days: int
    trades: int
    closed: int
    open_at_end: int
    wins: int
    losses: int
    win_rate: float | None
    avg_net_pnl_pct: float | None
    avg_gross_pnl_pct: float | None
    avg_costs_pct: float | None
    max_drawdown_pct: float
    total_net_pnl_pct: float
    annual_net_pnl_pct: float
    avg_win_pct: float | None
    avg_loss_pct: float | None
    sharpe: float | None
    excess_vs_best_benchmark_pct: float
    best_benchmark: str
    by_score_bucket: dict[str, dict[str, Any]] = field(default_factory=dict)
    gated_blocks: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ---------------------------------------------------------------------------
# Data loading (one query set, reused across the sensitivity grid)
# ---------------------------------------------------------------------------


class BacktestData:
    """Windowed datasets shared by every config in a grid run."""

    def __init__(self, config: BacktestConfig) -> None:
        self.config = config
        self.calendar = _load_calendar(config.start_date, config.end_date)
        if config.trendok_params:
            from data_sync_service.service.trendok_params import DEFAULT_TRENDOK_PARAMS
            allowed = set(DEFAULT_TRENDOK_PARAMS.__dataclass_fields__.keys())
            unknown = set(config.trendok_params.keys()) - allowed
            if unknown:
                raise ValueError(f"unknown trendok_params keys: {sorted(unknown)} (allowed={sorted(allowed)})")
        self.scores_by_day: dict[str, dict[str, float]] = _load_scores(
            config.start_date, config.end_date, config.market
        )
        self._trendok_params_override = config.trendok_params
        universe = sorted({s for day in self.scores_by_day.values() for s in day})
        self.ts_codes: list[str] = []
        for u in universe:
            resolved = _resolve_ts_code(u)
            if resolved and resolved[0] == config.market:
                self.ts_codes.append(resolved[1])
        self.delist_by_ts: dict[str, str] = _load_delist_dates(set(self.ts_codes))
        self.bars_by_ts: dict[str, list[tuple[str, str, str, str, str, str]]] = {}
        if self.ts_codes:

            start_lookback = max(
                date.fromisoformat(config.start_date) - timedelta(days=TREND_LOOKBACK_DAYS),
                date(1998, 1, 1),
            ).isoformat()
            self.bars_by_ts = fetch_ohlcv_batch_between(
                self.ts_codes, start_lookback, config.end_date
            )
        # index: ts_code -> {date: close}
        self.close_by_ts_day: dict[str, dict[str, float]] = {}
        # ts_code -> ascending (date, close) series incl. lookback (A2 trendScore)
        self.closes_by_ts: dict[str, list[tuple[str, float]]] = {}
        for ts, bars in self.bars_by_ts.items():
            closes: dict[str, float] = {}
            series: list[tuple[str, float]] = []
            for bar in bars:
                try:
                    c = float(bar[4])
                except (TypeError, ValueError):
                    continue
                d = str(bar[0])
                if c > 0:
                    series.append((d, c))
                    if d >= config.start_date:
                        closes[d] = c
            series.sort(key=lambda kv: kv[0])
            self.close_by_ts_day[ts] = closes
            self.closes_by_ts[ts] = series
        self.regime_by_day = _load_regime_by_day(config, self.calendar)
        self.light_red_by_day: set[str] = set()
        if config.light_red_block and config.market == "CN":
            self.light_red_by_day = _load_light_red_days(config, self.calendar)
        self.flow_any_positive_by_day, self.mainline_allow_by_day, self.flow5d_by_day = (
            _load_flow_mainline_data(config, self.calendar)
        )
        self.industry_by_ts = _load_industries(self.ts_codes)
        self.st_ts_codes: set[str] = set()
        if config.exclude_st:
            self.st_ts_codes = _load_st_names()
        self.pead_events: dict[str, set[str]] = {}
        if config.pead_days > 0:
            from data_sync_service.db.stock_forecast import positive_forecast_dates

            lookback = (
                date.fromisoformat(config.start_date) - timedelta(days=config.pead_days * 2 + 60)
            ).isoformat()
            self.pead_events = positive_forecast_dates(lookback, config.end_date)
        self.rs_rank_by_day = _load_rs_ranks(config, self.calendar, set(self.ts_codes))
        self.sentiment_risk_by_day = _load_sentiment_risk(config)
        self.env_by_day: dict[str, str] = {}
        if config.entry_style == "auto" or config.neutral_block:
            from data_sync_service.service.env_label import load_env_by_day

            self.env_by_day = load_env_by_day(config.start_date, config.end_date)
        self.mv_by_day = _load_market_caps(config, set(self.ts_codes))
        # P17 (portfolio-level risk): 60-session average daily turnover
        # (amount, 亿元) per symbol, as-of each day. Loaded only when the
        # liquidity floor is enabled.
        self.avg_amount_by_day: dict[str, dict[str, float]] = {}
        if config.min_avg_amount > 0:
            self.avg_amount_by_day = _load_avg_amount(config, self.calendar, set(self.ts_codes))
        # P11 (industry-neutral RS / industry momentum): per-day industry
        # rank (by equal-weighted member window return, whole market) and
        # per-stock within-industry return percentile. Loaded only when one
        # of the P11 gates is enabled (both windows computed if both on).
        self.ind_industry_rank_by_day: dict[str, dict[str, float]] = {}
        self.ind_within_rank_by_day: dict[str, dict[str, float]] = {}
        if config.ind_mom_days > 0 or config.ind_neutral_days > 0:
            self.ind_industry_rank_by_day, self.ind_within_rank_by_day = _load_industry_data(
                config, self.calendar, set(self.ts_codes)
            )
        # P9 (mid-horizon cross-sectional momentum): per-day whole-market
        # percentile of ret(mom_ret_days) skipping the last mom_skip_days.
        self.mom_rank_by_day: dict[str, dict[str, float]] = {}
        if config.mom_ret_days > 0:
            self.mom_rank_by_day = _load_mom_ranks(config, self.calendar, set(self.ts_codes))
        # B-T1: params are stored for heavy recompute; auto-recompute disabled to keep <10s
    def recompute_scores_with_params(self, override: dict[str, float]) -> dict[str, dict[str, float]]:
        from data_sync_service.service.trendok import _trendok_one
        from data_sync_service.service.trendok_params import DEFAULT_TRENDOK_PARAMS, TrendOKParams
        params = TrendOKParams(**{**DEFAULT_TRENDOK_PARAMS.__dict__, **override})
        params.validate()
        out: dict[str, dict[str, float]] = {}
        sym_by_ts: dict[str, str] = {}
        # filter to symbols that ever scored >=50 (candidate pool) to keep heavy sweep <60s
        candidate_syms = set()
        for day_scores in self.scores_by_day.values():
            for sym, sc in day_scores.items():
                if sc >= 65:
                    candidate_syms.add(sym)
        # fallback to all if candidate pool too small (e.g., early windows)
        pool = candidate_syms if len(candidate_syms) >= 300 else set(s for day in self.scores_by_day.values() for s in day)
        for sym in sorted(pool):
            resolved = _resolve_ts_code(sym)
            if resolved and resolved[0] == self.config.market:
                sym_by_ts[resolved[1]] = sym
        flow_ctx_by_day: dict[str, dict] = {}
        try:
            from data_sync_service.service.trendok import _build_industry_flow_context
            for d in self.calendar:
                try:
                    flow_ctx_by_day[d] = _build_industry_flow_context(d)
                except Exception:
                    flow_ctx_by_day[d] = {"ok": False}
        except Exception:
            flow_ctx_by_day = {d: {"ok": False} for d in self.calendar}
        # full universe for fidelity (parallelized)
        from concurrent.futures import ThreadPoolExecutor
        def _score_one(args):
            ts_code, sym, day, flow_ctx, regime = args
            bars = self.bars_by_ts.get(ts_code, [])
            window = [b for b in bars if str(b[0]) <= day][-120:]
            if len(window) < 60:
                return None
            industry = self.industry_by_ts.get(ts_code)
            res = _trendok_one(symbol=sym, name=None, industry=industry, bars=window, flow_ctx=flow_ctx, market_regime=regime, params=params)
            sc = res.get("score")
            if isinstance(sc, (int, float)):
                return (sym, float(sc))
            return None
        for day in self.calendar:
            flow_ctx = flow_ctx_by_day.get(day, {"ok": False})
            regime = self.regime_by_day.get(day)
            orig_day = self.scores_by_day.get(day, {})
            # per-day filter: only recompute symbols that were plausible candidates (orig >=55) to keep sweep tractable
            candidates = [sym for sym, sc in orig_day.items() if sc >= 55]
            if not candidates:
                continue
            tasks = []
            for sym in candidates:
                resolved = _resolve_ts_code(sym)
                if not resolved or resolved[0] != self.config.market:
                    continue
                tasks.append((resolved[1], sym, day, flow_ctx, regime))
            day_scores: dict[str, float] = {}
            with ThreadPoolExecutor(max_workers=8) as ex:
                for r in ex.map(_score_one, tasks):
                    if r:
                        day_scores[r[0]] = r[1]
            if day_scores:
                out[day] = day_scores
        return out if out else self.scores_by_day


def _load_calendar(start_date: str, end_date: str) -> list[str]:
    """CN trading calendar from the daily table (distinct trade dates)."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT trade_date
                FROM daily
                WHERE trade_date >= %s AND trade_date <= %s
                ORDER BY trade_date
                """,
                (start_date, end_date),
            )
            rows = cur.fetchall()
    out: list[str] = []
    for r in rows:
        d = r[0]
        out.append(d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d))
    return out


BENCHMARK_INDEXES = [
    {"ts_code": "000001.SH", "name": "上证指数"},
    {"ts_code": "399006.SZ", "name": "创业板指"},
    {"ts_code": "000300.SH", "name": "沪深300"},
    {"ts_code": "000905.SH", "name": "中证500"},
    {"ts_code": "000688.SH", "name": "科创50"},
]


def load_benchmarks(start_date: str, end_date: str) -> list[dict[str, Any]]:
    """Window total/annual return for each benchmark index (first to last close)."""
    out: list[dict[str, Any]] = []
    with get_connection() as conn:
        with conn.cursor() as cur:
            for it in BENCHMARK_INDEXES:
                code = it["ts_code"]
                cur.execute(
                    """
                    SELECT trade_date, close FROM index_daily
                    WHERE ts_code = %s AND trade_date >= %s AND trade_date <= %s
                    ORDER BY trade_date
                    """,
                    (code, start_date, end_date),
                )
                rows = cur.fetchall()
                if len(rows) < 2:
                    continue
                start_px = float(rows[0][1])
                end_px = float(rows[-1][1])
                if start_px <= 0:
                    continue
                total = (end_px / start_px - 1.0) * 100.0
                years = max((date.fromisoformat(end_date) - date.fromisoformat(start_date)).days / 365.25, 1 / 365.25)
                out.append(
                    {
                        "ts_code": code,
                        "name": it["name"],
                        "start_date": str(rows[0][0]),
                        "end_date": str(rows[-1][0]),
                        "total_return_pct": round(total, 2),
                        "annual_pct": round(total / years, 2),
                    }
                )
    return out


def _load_scores(start_date: str, end_date: str, market: str) -> dict[str, dict[str, float]]:
    """watchlist_score_daily rows as {trade_date: {symbol: score}} for one market."""
    prefix = f"{market}:"
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT symbol, trade_date, score
                FROM {SCORE_TABLE}
                WHERE trade_date >= %s AND trade_date <= %s
                  AND score IS NOT NULL
                ORDER BY trade_date, symbol
                """,
                (start_date, end_date),
            )
            rows = cur.fetchall()
    out: dict[str, dict[str, float]] = {}
    for r in rows:
        sym = str(r[0] or "").upper()
        if not sym.startswith(prefix):
            continue
        d = r[1].strftime("%Y-%m-%d") if hasattr(r[1], "strftime") else str(r[1])
        try:
            score = float(r[2])
        except (TypeError, ValueError):
            continue
        out.setdefault(d, {})[sym] = score
    return out


def _load_delist_dates(ts_codes: set[str]) -> dict[str, str]:
    """ts_code -> delist_date (ISO) for names that have delisted.

    Survivor-bias guard: a name that is no longer listed must not be traded
    (and any open position must be force-closed at its last available close).
    Returns {} when the table is unavailable so the engine fails open.
    """
    if not ts_codes:
        return {}
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT ts_code, delist_date FROM stock_basic "
                    "WHERE delist_date IS NOT NULL",
                )
                rows = cur.fetchall()
        out: dict[str, str] = {}
        for r in rows:
            ts = str(r[0] or "")
            if ts in ts_codes:
                dd = r[1]
                out[ts] = dd.strftime("%Y-%m-%d") if hasattr(dd, "strftime") else str(dd)
        return out
    except Exception:
        return {}


def _last_close_before(data: "BacktestData", ts: str, day: str) -> float | None:
    """Last close strictly before ``day`` for ``ts`` (for delisted force-close)."""
    series = data.closes_by_ts.get(ts)
    if not series:
        return None
    res: float | None = None
    for d, c in series:
        if str(d) < day:
            res = c
        else:
            break
    return res


def _nav_for_day(
    positions: dict[str, dict[str, Any]],
    data: "BacktestData",
    day: str,
) -> float:
    """Mark-to-market of all OPEN sleeves (incl. pyramid adds) at ``day``'s
    close (raw price ratio). The realised P&L of CLOSED trades is accumulated
    separately into ``nav_cash`` by the caller, so the full equity curve is
    ``nav_cash + _nav_for_day(...)``.

    Used for honest Sharpe / MaxDD / CAGR — the previous per-close-day series
    ignored holding-period MTM and idle days → inflated Sharpe, understated DD.
    """
    mtm = 0.0
    for pos in positions.values():
        ts = pos["ts_code"]
        ep = pos.get("entry_price")
        closes = data.close_by_ts_day.get(ts)
        cp = closes.get(day) if closes else None
        ratio = (cp / ep) if (cp and ep and ep > 0) else 1.0
        mtm += pos["position_pct"] * ratio
        for a in pos.get("adds_list", []):
            aep = a.get("entry_price")
            ratio_a = (cp / aep) if (cp and aep and aep > 0) else 1.0
            mtm += a["position_pct"] * ratio_a
    return mtm


def _load_regime_by_day(config: BacktestConfig, calendar: list[str]) -> dict[str, str]:
    """As-of index traffic-light regime per trading day.

    Reuses the exact live functions (``get_index_signals`` +
    ``classify_market_regime``). Missing data → the day is absent so the
    engine's gate check fails closed.

    2026-08-10 (HK parallel line): market=HK gates on HSI/HSTECH traffic
    lights (``get_hk_regime``) — CN indexes must not drive HK entries.
    """
    out: dict[str, str] = {}
    for day in calendar:
        try:
            if config.market == "HK":
                out[day] = str(get_hk_regime(as_of_date=day).get("regime") or "")
                continue
            signals = get_index_signals(as_of_date=day, include_breadth=False)
            out[day] = classify_market_regime(signals)
        except Exception as exc:  # noqa: BLE001
            logger.warning("backtest: regime data unavailable for %s (%s)", day, exc)
    return out


CN_INDEX_LIGHT_NAMES = {"沪深300", "中证500", "创业板指"}
_LIGHT_RANK = {"deep_green": 4, "green": 3, "yellow": 2, "red": 1, "unknown": 0}


def _load_light_red_days(config: BacktestConfig, calendar: list[str]) -> set[str]:
    """CN days whose tighter index light is red (OPT-093/094).

    Same as-of replay as _load_regime_by_day (get_index_signals, no
    realtime, no breadth → no look-ahead); cached per day so it shares the
    regime loader's DB work. Backtest evidence (2026-08-12): red-light
    entries are negative EV — OOS2 win 48%→54% and valid 61%→79% when
    dropped, never a worse window. HK deliberately excluded (no separation).
    """
    out: set[str] = set()
    for day in calendar:
        try:
            signals = get_index_signals(as_of_date=day, include_breadth=False)
            lights = [
                str(s.get("signal") or "unknown")
                for s in signals
                if str(s.get("name") or "") in CN_INDEX_LIGHT_NAMES
            ]
            if lights and min(lights, key=lambda x: _LIGHT_RANK.get(x, 0)) == "red":
                out.add(day)
        except Exception as exc:  # noqa: BLE001
            logger.warning("backtest: index-light data unavailable for %s (%s)", day, exc)
    return out


def _load_flow_mainline_data(
    config: BacktestConfig,
    calendar: list[str],
) -> tuple[dict[str, bool], dict[str, set[str]], dict[str, dict[str, float]]]:
    """Per-day gate inputs from SW L1 fund-flow rows (one DB fetch).

    Returns ``(flow_any_positive_by_day, mainline_allow_by_day, flow5d_by_day)`` where:

    - ``flow_any_positive[day]`` — True when at least one SW L1 industry has
      positive net inflow that day. Mirrors the live ``sectorOutflowBlock``
      rule (all industries <= 0 blocks new entries).
    - ``mainline_allow[day]`` — industries allowed to be bought: 5D net
      inflow Top3 ∪ momentum-breakout industries (today net inflow >= 20亿
      and rank improved >= 10 vs yesterday). Mirrors the live mainline
      whitelist (hot-industry-picks.ts buildMainlineAllowSet).
    - ``flow5d_by_day[day]`` — {industry: rolling 5-session net inflow}
      (B1 flow-exit input; missing flow data day -> absent).
    """
    from datetime import timedelta

    start_early = max(date.fromisoformat(config.start_date) - timedelta(days=20), date(2024, 1, 1))
    all_dates: list[str] = []
    for day in calendar:
        for d in get_dates_upto(day, 6):
            if d not in all_dates and d >= str(start_early):
                all_dates.append(d)
    rows = get_rows_for_dates(all_dates) if all_dates else []

    by_date_rank: dict[str, dict[str, int]] = {}
    by_date_flow: dict[str, dict[str, float]] = {}
    for row in rows:
        d = str(row.get("date") or "")
        name = str(row.get("industry_name") or "").strip()
        try:
            v = float(row.get("net_inflow") or 0.0)
        except (TypeError, ValueError):
            v = 0.0
        if d and name:
            by_date_flow.setdefault(d, {})[name] = v
    for d, flows in by_date_flow.items():
        ranked = sorted(flows.items(), key=lambda kv: (-kv[1], kv[0]))
        by_date_rank[d] = {name: i + 1 for i, (name, _) in enumerate(ranked)}

    flow_any_positive: dict[str, bool] = {}
    mainline_allow: dict[str, set[str]] = {}
    for day in calendar:
        day_flows = by_date_flow.get(day)
        if day_flows:
            flow_any_positive[day] = any(v > 0 for v in day_flows.values())
        lookback = get_dates_upto(day, 5)
        top = top_by_date_from_rows(rows, lookback, top_k=config.mainline_top_k)
        allow: set[str] = set()
        for entry in top:
            if entry["date"] == day:
                allow = set(entry["top"])
        if day in by_date_rank:
            today_rank = by_date_rank[day]
            yesterday = get_dates_upto(day, 2)
            yrank = by_date_rank.get(yesterday[-1]) if yesterday else None
            if yrank:
                for name, r in today_rank.items():
                    inflow = day_flows.get(name) or 0.0
                    y_r = yrank.get(name)
                    if (
                        inflow >= MOMENTUM_THRESHOLD_YI
                        and y_r is not None
                        and (y_r - r) >= MOMENTUM_RANK_CHANGE
                    ):
                        allow.add(name)
        if day_flows:
            mainline_allow[day] = allow

    flow5d_by_day: dict[str, dict[str, float]] = {}
    for day in calendar:
        lookback = get_dates_upto(day, 5)
        if not lookback:
            continue
        five: dict[str, float] = {}
        for d in lookback:
            for name, v in by_date_flow.get(d, {}).items():
                five[name] = five.get(name, 0.0) + v
        if five:
            flow5d_by_day[day] = five
    return flow_any_positive, mainline_allow, flow5d_by_day


RS_LOOKBACK_DAYS = 20

# P11 industry-neutral RS / industry momentum: supported return windows.
IND_WINDOW_OPTIONS = (20, 60, 120)


def _load_industry_data(
    config: BacktestConfig,
    calendar: list[str],
    universe_ts: set[str],
) -> tuple[dict[str, dict[str, float]], dict[str, dict[str, float]]]:
    """P11 (signal pool): per-day industry momentum + within-industry RS.

    Industry membership = ``stock_eastmoney_industry`` (same table the
    mainline gate uses — static today-snapshot, so membership has the same
    mild look-ahead caveat as the existing mainline gate). Returns:

    - ``ind_industry_rank_by_day[day][industry]`` — percentile (0-1, 1 =
      strongest) of the industry's equal-weighted average return over the
      configured window (``config.ind_mom_days``), ranked across all
      industries with data that day. Industry momentum gate input.
    - ``ind_within_rank_by_day[day][ts]`` — percentile (0-1, 1 = strongest)
      of the stock's window return WITHIN its own industry (ranking pool =
      all stocks of that industry with a bar that day, whole market, not
      just the universe). Within-industry RS gate input.
      Only universe symbols are kept (mirrors ``_load_rs_ranks``).

    Both windows (mom / neutral) are computed when both gates are enabled;
    each uses the daily table's ``lag(close, N)`` exactly like RS ranks —
    as-of, no look-ahead.
    """
    windows = {w for w in (config.ind_mom_days, config.ind_neutral_days) if w}
    if not windows:
        return {}, {}
    from datetime import timedelta

    start_early = max(
        date.fromisoformat(config.start_date) - timedelta(days=200), date(1998, 1, 1)
    ).isoformat()
    ind_map: dict[str, str] = {}
    ind_rank_by_day: dict[str, dict[str, float]] = {}
    within_rank_by_day: dict[str, dict[str, float]] = {}
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ts_code, industry_name FROM stock_eastmoney_industry"
            )
            for ts, ind in cur.fetchall():
                if ts and ind:
                    ind_map[str(ts)] = str(ind)
            # one pass per window, kept sequential to bound memory
            for w in windows:
                cur.execute(
                    f"""
                    SELECT trade_date, ts_code,
                        (close / lag(close, {w})
                         OVER (PARTITION BY ts_code ORDER BY trade_date) - 1) * 100 AS ret
                    FROM daily
                    WHERE trade_date >= %s AND trade_date <= %s AND close > 0
                    ORDER BY trade_date
                    """,
                    (start_early, config.end_date),
                )
                day_rows: dict[str, list[tuple[str, float]]] = {}
                for d, ts, ret in cur.fetchall():
                    if ret is None:
                        continue
                    day_rows.setdefault(str(d), []).append((str(ts), float(ret)))
                for day in calendar:
                    items = day_rows.get(day)
                    if not items or len(items) < 50:
                        continue
                    # industry equal-weighted avg return
                    ind_sum: dict[str, tuple[float, int]] = {}
                    for ts, ret in items:
                        ind = ind_map.get(ts)
                        if not ind:
                            continue
                        s, n = ind_sum.get(ind, (0.0, 0))
                        ind_sum[ind] = (s + ret, n + 1)
                    if len(ind_sum) < 5:  # too thin a market to rank industries
                        continue
                    if config.ind_mom_days > 0 and w == config.ind_mom_days:
                        ind_avg = {
                            ind: s / n for ind, (s, n) in ind_sum.items() if n >= 3
                        }
                        ranked = sorted(ind_avg.items(), key=lambda kv: -kv[1])
                        total = len(ranked)
                        if total < 5:
                            continue
                        pos: dict[str, float] = {}
                        for i, (ind, _r) in enumerate(ranked, start=1):
                            pos[ind] = (total - i + 1) / total
                        ind_rank_by_day[day] = pos
                    if config.ind_neutral_days > 0 and w == config.ind_neutral_days:
                        # within-industry return percentile (whole market pool)
                        by_ind: dict[str, list[tuple[str, float]]] = {}
                        for ts, ret in items:
                            ind = ind_map.get(ts)
                            if ind:
                                by_ind.setdefault(ind, []).append((ts, ret))
                        wpos: dict[str, float] = {}
                        for _ind, members in by_ind.items():
                            if len(members) < 5:
                                continue
                            ranked = sorted(members, key=lambda kv: -kv[1])
                            total = len(ranked)
                            for i, (ts, _r) in enumerate(ranked, start=1):
                                if ts in universe_ts:
                                    wpos[ts] = (total - i + 1) / total
                        if wpos:
                            within_rank_by_day[day] = wpos
    return ind_rank_by_day, within_rank_by_day


def _load_avg_amount(
    config: BacktestConfig,
    calendar: list[str],
    universe_ts: set[str],
) -> dict[str, dict[str, float]]:
    """P17 (signal pool): per-day 60-session average daily turnover.

    ``avg_amount_by_day[day][ts]`` = mean of the 60 daily ``amount`` values
    (tushare unit: 千元 → converted to 亿元) ending at ``day``. As-of (only
    bars <= day), mirrors the daily-table lookback pattern of RS ranks.
    """
    from datetime import timedelta

    if config.min_avg_amount <= 0:
        return {}
    start_early = max(
        date.fromisoformat(config.start_date) - timedelta(days=100), date(1998, 1, 1)
    ).isoformat()
    rows: list[tuple[str, str, str]] = []
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT trade_date, ts_code, amount FROM daily
                WHERE trade_date >= %s AND trade_date <= %s AND amount > 0
                ORDER BY trade_date
                """,
                (start_early, config.end_date),
            )
            rows = cur.fetchall()
    per_ts: dict[str, list[tuple[str, float]]] = {}
    for d, ts, amt in rows:
        if ts not in universe_ts:
            continue
        per_ts.setdefault(ts, []).append((str(d), float(amt)))
    # Sliding-window mean per (ts, day): every ts advances one monotone
    # pointer over the calendar, so the total work is O(ts x calendar), not
    # O(ts x calendar x 60).
    out: dict[str, dict[str, float]] = {}
    pos: dict[str, int] = {ts: 0 for ts in per_ts}
    acc: dict[str, float] = {}
    q: dict[str, deque] = {}
    for day in calendar:
        day_vals: dict[str, float] = {}
        for ts, series in per_ts.items():
            p = pos[ts]
            n = len(series)
            while p < n and series[p][0] <= day:
                v = series[p][1]
                if ts not in q:
                    acc[ts] = 0.0
                    q[ts] = deque()
                q[ts].append(v)
                acc[ts] += v
                if len(q[ts]) > 60:
                    acc[ts] -= q[ts].popleft()
                p += 1
            pos[ts] = p
            if len(q.get(ts, ())) >= 30:  # at least half a window of data
                day_vals[ts] = round(acc[ts] / len(q[ts]) / 100000.0, 4)  # 千元 → 亿元
        if day_vals:
            out[day] = day_vals
    return out


def _load_mom_ranks(
    config: BacktestConfig,
    calendar: list[str],
    universe_ts: set[str],
) -> dict[str, dict[str, float]]:
    """P9 (signal pool): per-day whole-market momentum percentile.

    Momentum = ``close[day - skip] / close[day - skip - ret_days] - 1`` —
    the window return ending ``skip`` sessions BEFORE today (A股短期反转 →
    skip the recent window). Percentile 0-1, 1 = strongest, ranked against
    ALL stocks with a bar that day (same pool as RS ranks). Only universe
    symbols are kept. Returns {day: {symbol: percentile}}.
    """
    from datetime import timedelta

    if config.mom_ret_days <= 0:
        return {}
    skip = config.mom_skip_days
    w = config.mom_ret_days
    # need (skip + w) sessions before the day + the day itself
    start_early = max(
        date.fromisoformat(config.start_date) - timedelta(days=w + skip + 60),
        date(1998, 1, 1),
    ).isoformat()
    rows: list[tuple[str, str, float]] = []
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT trade_date, ts_code,
                    (c_skip / c_skip_w - 1) * 100 AS mom_ret FROM (
                    SELECT trade_date, ts_code,
                        LAG(close, {skip}) OVER w AS c_skip,
                        LAG(close, {skip + w}) OVER w AS c_skip_w
                    FROM daily
                    WHERE trade_date >= %s AND trade_date <= %s AND close > 0
                    WINDOW w AS (PARTITION BY ts_code ORDER BY trade_date)
                ) t
                WHERE c_skip IS NOT NULL AND c_skip_w IS NOT NULL AND c_skip_w > 0
                ORDER BY trade_date
                """,
                (start_early, config.end_date),
            )
            rows = cur.fetchall()

    out: dict[str, dict[str, float]] = {}
    day_rows: dict[str, list[tuple[str, float]]] = {}
    for d, ts, c in rows:
        day_rows.setdefault(str(d), []).append((str(ts), float(c)))
    for day in calendar:
        items = day_rows.get(day)
        if not items or len(items) < 30:
            continue
        ranked = sorted(items, key=lambda kv: -kv[1])
        total = len(ranked)
        pos: dict[str, float] = {}
        for i, (ts, _v) in enumerate(ranked, start=1):
            if ts in universe_ts:
                pos[ts] = (total - i + 1) / total
        out[day] = pos
    return out


# A2 trendScore lookback: 52-week (252 trading days ~ 365 calendar) high needs
# ~370 calendar days of bars; 400 leaves a buffer for gaps.
TREND_LOOKBACK_DAYS = 400


def _load_rs_ranks(
    config: BacktestConfig,
    calendar: list[str],
    universe_ts: set[str],
) -> dict[str, dict[str, float]]:
    """Per-day relative-strength percentile (0-1, 1 = strongest) per symbol.

    RS = 20-day return minus the CSI300 20-day return, ranked against ALL
    stocks with a bar that day (whole-market relative strength, IbD-style).
    Only universe symbols are kept; other symbols are the ranking pool.
    Returns {day: {symbol: percentile}}. Days without RS data (no bars,
    suspension, <21 bars history) are absent → the engine treats them as
    blocked (fail-closed).
    """
    from datetime import timedelta

    if config.rs_rank_min <= 0:
        return {}
    start_early = max(
        date.fromisoformat(config.start_date) - timedelta(days=40), date(1998, 1, 1)
    ).isoformat()
    rows: list[tuple[str, str, float]] = []
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT trade_date, ts_code, ret20 FROM (
                    SELECT trade_date, ts_code, close,
                        (close / lag(close, 20) OVER (PARTITION BY ts_code ORDER BY trade_date) - 1) * 100 AS ret20
                    FROM daily
                    WHERE trade_date >= %s AND trade_date <= %s AND close > 0
                ) t WHERE ret20 IS NOT NULL
                ORDER BY trade_date
                """,
                (start_early, config.end_date),
            )
            rows = cur.fetchall()
            # CSI300 benchmark 20d return (as-of, same window)
            bench: dict[str, float] = {}
            cur.execute(
                """
                SELECT trade_date, close FROM index_daily
                WHERE ts_code = '000300.SH' AND trade_date >= %s AND trade_date <= %s
                ORDER BY trade_date
                """,
                (start_early, config.end_date),
            )
            bench_rows = cur.fetchall()
    bench_close: dict[str, float] = {str(d): float(c) for d, c in bench_rows}
    bench_dates = sorted(bench_close)
    for i in range(RS_LOOKBACK_DAYS, len(bench_dates)):
        d0, d1 = bench_dates[i - RS_LOOKBACK_DAYS], bench_dates[i]
        if bench_close[d0] > 0:
            bench[d1] = (bench_close[d1] / bench_close[d0] - 1.0) * 100.0

    # group rows by day (rows are ordered by trade_date)
    out: dict[str, dict[str, float]] = {}
    day_rows: dict[str, list[tuple[str, float]]] = {}
    for d, ts, ret in rows:
        ds = str(d)
        day_rows.setdefault(ds, []).append((str(ts), float(ret)))
    for day in calendar:
        items = day_rows.get(day)
        if not items:
            continue
        b = bench.get(day)
        if b is None:
            continue
        bench_ret = b
        ranked = sorted(items, key=lambda kv: -(kv[1] - bench_ret))
        total = len(ranked)
        if total < 30:  # too thin a market to rank reliably
            continue
        pos: dict[str, float] = {}
        for i, (ts, _ret) in enumerate(ranked, start=1):
            if ts in universe_ts:
                pos[ts] = (total - i + 1) / total  # strongest = 1.0, weakest = ~0
        out[day] = pos
    return out


SENTIMENT_BLOCK_MODES = ("no_new_positions", "extreme_caution")


def _trend_score(
    rs: float | None,
    closes: list[tuple[str, float]] | None,
    as_of_day: str,
) -> float | None:
    """A2 trend-quality score (0-100) as-of ``as_of_day``.

    Three interpretable factors (each has a business story; missing inputs
    degrade to 0, so the gate fails closed):

    1. Relative strength percentile (0-1, 1 = strongest) → 40 pts.
    2. MA alignment: MA5 > MA20 > MA60 → 30 pts; partial (MA5 > MA20 or
       MA20 > MA60) → 15 pts; none → 0.
    3. Distance to 52-week high: within -5% → 30 pts; -10% → 20; -15% → 10;
       deeper → 0. Uses the longest available history when < 252 bars.
    """
    from bisect import bisect_right

    if not closes:
        return None
    dates = [d for d, _c in closes]
    idx = bisect_right(dates, as_of_day)
    if idx < 60:
        return None  # not enough history for MA60
    px = [c for _d, c in closes[:idx]]

    ma5 = sum(px[-5:]) / 5
    ma20 = sum(px[-20:]) / 20
    ma60 = sum(px[-60:]) / 60
    if ma5 > ma20 > ma60:
        mult = 1.0
    elif ma5 > ma20 or ma20 > ma60:
        mult = 0.5
    else:
        mult = 0.0

    lookback = min(idx, 252)
    high = max(px[-lookback:])
    close = px[-1]
    dist = (close - high) / high if high > 0 else -1.0
    if dist > -0.05:
        nzd = 30.0
    elif dist > -0.10:
        nzd = 20.0
    elif dist > -0.15:
        nzd = 10.0
    else:
        nzd = 0.0

    return (rs if rs is not None else 0.0) * 40.0 + 30.0 * mult + nzd

def _load_sentiment_risk(config: BacktestConfig) -> dict[str, str]:
    """Per-day sentiment risk_mode (live gate _RISK_DEFEND input).

    Missing days degrade (fail-open): sentiment data starts 2026-01-05, the
    live system did not have this gate before then.
    """
    out: dict[str, str] = {}
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT date, risk_mode FROM market_cn_sentiment_daily
                WHERE date >= %s AND date <= %s
                """,
                (config.start_date, config.end_date),
            )
            for d, mode in cur.fetchall():
                out[str(d)] = str(mode or "")
    return out


def _load_market_caps(config: BacktestConfig, ts_codes: set[str]) -> dict[str, dict[str, float]]:
    """{trade_date: {ts_code: total_mv}} in 亿元 for the windowed universe.

    Missing dates/rows degrade (fail-open): only layers the pool when the
    market-cap data is present.
    """
    out: dict[str, dict[str, float]] = {}
    if not ts_codes:
        return out
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT trade_date, ts_code, total_mv FROM stock_dailybasic
                WHERE trade_date >= %s AND trade_date <= %s
                  AND ts_code = ANY(%s) AND total_mv IS NOT NULL
                """,
                (config.start_date, config.end_date, list(ts_codes)),
            )
            for d, ts, mv in cur.fetchall():
                out.setdefault(str(d), {})[str(ts)] = float(mv) / 10000.0
    return out


def _load_industries(ts_codes: list[str]) -> dict[str, str]:
    """ts_code -> Eastmoney industry name (bulk read, mainline gate input)."""
    if not ts_codes:
        return {}
    out: dict[str, str] = {}
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ts_code, industry_name FROM stock_eastmoney_industry WHERE ts_code = ANY(%s)",
                (ts_codes,),
            )
            for ts, ind in cur.fetchall():
                if ts and ind:
                    out[str(ts)] = str(ind)
    return out


def _load_st_names() -> set[str]:
    """ts_code set of ST/*ST names (P16 tail-risk exclusion).

    stock_basic.name is the live name; a name containing ST means the name
    is ST-listed (ST / *ST / S*ST). Data is already local — zero new sync.
    """
    out: set[str] = set()
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT ts_code FROM stock_basic WHERE name LIKE '%ST%'"
                )
                for (ts,) in cur.fetchall():
                    if ts:
                        out.add(str(ts))
    except Exception:  # noqa: BLE001 — fresh DBs may lack the table
        return out
    return out


def _gate_blocked(
    config: BacktestConfig,
    data: BacktestData,
    day: str,
    ts: str,
) -> str | None:
    """Return the gate reason that blocks a new entry on ``day``, or None.

    Fail-closed: missing data for the day blocks the entry (same posture as
    the live mainline ``MAINLINE_DATA_UNAVAILABLE`` path).
    """
    if config.light_red_block and config.market == "CN" and day in data.light_red_by_day:
        return GATE_REASON_INDEX_RED
    if config.gates in ("regime", "full"):
        regime = data.regime_by_day.get(day)
        if regime != REGIME_STRONG:
            if regime == REGIME_DIVERGING and config.diverging_scale > 0:
                pass  # allowed, position scaled down at entry
            else:
                return GATE_REASON_REGIME
    if config.gates == "full":
        # Data-missing days degrade (fail-open): the live system did not have
        # fund-flow gates before 2025-12-15, so historical windows without
        # that data must replay the system's then-current capabilities.
        risk = data.sentiment_risk_by_day.get(day)
        if risk in SENTIMENT_BLOCK_MODES:
            return GATE_REASON_SENTIMENT
        if day in data.flow_any_positive_by_day and not data.flow_any_positive_by_day[day]:
            return GATE_REASON_FLOW
        allow = data.mainline_allow_by_day.get(day)
        ind = data.industry_by_ts.get(ts)
        if allow is not None and (ind is None or ind not in allow):
            return GATE_REASON_MAINLINE
    return None


@dataclass
class BacktestRun:
    """Full result of one simulation: aggregates + trade-by-trade list."""

    summary: BacktestSummary
    trades: list[BacktestTrade] = field(default_factory=list)
    positions_by_day: list[dict] = field(default_factory=list)
    # Daily account NAV after each calendar day (cash + open MTM), then one
    # terminal point after window-end forced closes. len = len(calendar) + 1.
    # Pair calendar[i] -> nav_curve[i] for sleeve / dual overlays.
    nav_curve: list[float] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Simulation
# ---------------------------------------------------------------------------


def simulate(config: BacktestConfig, data: BacktestData | None = None) -> BacktestRun:
    """Run one configuration over its window. Pure over BacktestData inputs.

    ``data`` is injectable for tests; production callers pass None and the
    windowed datasets are loaded from the DB.
    """
    if data is None:
        data = BacktestData(config)
    positions: dict[str, dict[str, Any]] = {}  # symbol -> live position state
    closed_trades: list[BacktestTrade] = []
    positions_by_day: list[dict] = []  # end-of-day holding snapshots (2026-08-11)
    nav_curve: list[float] = []  # daily account NAV (start 1.0) for Sharpe/DD/CAGR
    nav_cash: float = 1.0  # realised capital (initial units); OPEN sleeves add their MTM on top
    _rt_cost = round_trip_cost_pct(config.market) * 100.0
    _cfrac = _rt_cost / 200.0  # half of round-trip commission as entry, half as exit
    _entry_cost_frac = config.slippage_pct / 100.0 + _cfrac
    _exit_cost_frac = config.slippage_pct / 100.0 + _cfrac
    gated_blocks: dict[str, int] = defaultdict(int)
    strength_cache: dict[str, float] = {}  # §19.2 D1: day -> strength score
    last_panic_idx = -10 ** 9
    day_index = 0

    threshold = config.score_threshold
    realized_pnl_window: list[tuple[str, float]] = []  # (close_date, pnl_pct)"""

    def _circuit_halted(day: str) -> bool:
        if config.drawdown_circuit_pct >= 0:
            return False
        cutoff = (
            date.fromisoformat(day)
            - timedelta(days=max(1, int(config.drawdown_circuit_window_days)))
        ).isoformat()
        recent = [pnl for d, pnl in realized_pnl_window if d >= cutoff]
        return len(recent) >= 3 and sum(recent) <= config.drawdown_circuit_pct

    def entry_price_for(ts: str, day: str) -> float | None:
        """Entry fill price for a signal-day entry.

        Default: signal-day close (legacy, matches the live paper which fills
        at close). ``entry_mode`` variants approximate a better intraday
        fill from the OHLC bar (no minute bars needed):

        - ``last_hour_low``: low*0.5 + close*0.5 — proxy for a 尾盘 dip buy
          (A股 14:00-15:00 / HK 15:00-16:00). Always <= close.
        - ``last_hour_hl``: midpoint of that proxy and close.
        - ``next_open``: next session's open (fill on the following day).
        """
        closes = data.close_by_ts_day.get(ts)
        base = closes.get(day) if closes else None
        if base is None or base <= 0:
            return None
        mode = config.entry_mode
        if mode == "close":
            return base
        bars = data.bars_by_ts.get(ts)
        bar = None
        if bars:
            bar = next((b for b in bars if str(b[0]) == day), None)
        if mode == "next_open":
            if not bars:
                return None
            nxt = next((b for b in bars if str(b[0]) > day), None)
            if nxt is None:
                return None
            try:
                o = float(nxt[1])
            except (TypeError, ValueError):
                return None
            return o if o > 0 else None
        if bar is None:
            return base
        try:
            lo = float(bar[3])
            hi = float(bar[2])
        except (TypeError, ValueError):
            return base
        if lo <= 0 or hi < lo:
            return base
        proxy = lo * 0.5 + base * 0.5
        # A 尾盘 dip proxy that is never *above* the close (a last-hour dip
        # cannot fill worse than the close when the close is the high).
        proxy = min(proxy, base)
        if mode == "last_hour_low":
            return proxy
        # last_hour_hl: midpoint between the dip proxy and the close.
        return (proxy + base) / 2.0

    def atr_scale_for(ts: str, day: str) -> float:
        """Volatility-targeted sleeve scale: ATR% < benchmark -> bigger sleeve
        (and vice versa), clamped to [1/cap, cap]."""
        if config.atr_size_window <= 0:
            return 1.0
        bars = data.bars_by_ts.get(ts)
        if not bars:
            return 1.0
        recent = [(b, float(b[2]), float(b[3])) for b in bars if str(b[0]) <= day][-config.atr_size_window:]
        if len(recent) < max(5, config.atr_size_window // 2):
            return 1.0
        tr_sum = 0.0
        prev_close = None
        n = 0
        for _, hi, lo in recent:
            if prev_close is None:
                prev_close = hi  # first bar: approximate with its high
                continue
            tr_sum += max(hi - lo, abs(hi - prev_close), abs(lo - prev_close))
            prev_close = hi
            n += 1
        if n == 0:
            return 1.0
        atr = tr_sum / n
        px = entry_price_for(ts, day)
        if px is None or px <= 0:
            return 1.0
        atr_pct = atr / px * 100.0
        if atr_pct <= 0:
            return 1.0
        scale = config.atr_benchmark_pct / atr_pct
        return min(config.atr_size_cap, max(1.0 / config.atr_size_cap, scale))

    def atr14_pct_for(ts: str, day: str) -> float:
        """ATR14 / close x 100 at ``day`` (0.0 when bars are insufficient).

        Used by OPT-104 per-symbol stops; the value is locked at entry so a
        stop never drifts with later volatility changes."""
        bars = data.bars_by_ts.get(ts)
        if not bars:
            return 0.0
        recent = sorted(
            [b for b in bars if str(b[0]) <= day], key=lambda b: str(b[0])
        )[-15:]
        if len(recent) < 8:
            return 0.0
        trs: list[float] = []
        prev: float | None = None
        for b in recent:
            try:
                hi, lo = float(b[2]), float(b[3])
            except (TypeError, ValueError):
                continue
            if prev is None:
                prev = hi
                continue
            trs.append(max(hi - lo, abs(hi - prev), abs(lo - prev)))
            prev = hi
        if not trs:
            return 0.0
        atr = sum(trs) / len(trs)
        px = entry_price_for(ts, day)
        if px is None or px <= 0:
            return 0.0
        return atr / px * 100.0

    def ret5_for(ts: str, day: str) -> float | None:
        """5-session return % for ``ts`` at ``day`` (None when insufficient
        bars). Used by TIP-014 entry styles to separate momentum names from
        short-term pullbacks."""
        closes = data.closes_by_ts.get(ts)
        if not closes:
            return None
        idx = None
        for i, (d, _c) in enumerate(closes):
            if str(d) == day:
                idx = i
                break
        if idx is None or idx < 5:
            return None
        c_prev = closes[idx - 5][1]
        c_today = closes[idx][1]
        if c_prev <= 0:
            return None
        return (c_today / c_prev - 1.0) * 100.0

    def strength_for(day: str) -> float:
        """§19.2 D1: 0-100 continuous market-strength score for ``day``
        (regime_strength_score, cached per day). 0.0 on failure → fixed
        stops apply (fail-closed). Only called when atr_stop_strength_min > 0."""
        cached = strength_cache.get(day)
        if cached is not None:
            return cached
        from data_sync_service.service.market_regime import regime_strength_score

        try:
            val = float(regime_strength_score(as_of_date=day, market="CN")["strength"])
        except Exception:  # noqa: BLE001
            val = 0.0
        strength_cache[day] = val
        return val

    for day in data.calendar:
        day_scores = data.scores_by_day.get(day, {})
        circuit_halted = _circuit_halted(day)
        if data.sentiment_risk_by_day.get(day) in SENTIMENT_BLOCK_MODES:
            last_panic_idx = day_index
        panic_cooldown = (
            config.panic_cooldown_days > 0
            and (day_index - last_panic_idx) <= config.panic_cooldown_days
        )
        prev_day = data.calendar[day_index - 1] if day_index > 0 else None
        day_index += 1

        # 1.5) RS-rotation swaps (before entries): swap out RS-weakened held
        #      stocks for clearly-stronger candidates, so a full sleeve is not
        #      a wall against the market's strongest names (300308 case).
        swapped_syms: set[str] = set()
        if config.swap_max_per_day > 0:
            held = []
            for sym, pos in positions.items():
                if _calendar_days_between(str(pos["entry_date"]), day, data.calendar) < config.swap_min_hold_days:
                    continue
                rsv = data.rs_rank_by_day.get(day, {}).get(pos["ts_code"])
                if rsv is not None and rsv < config.swap_weak_rs_below:
                    held.append((rsv, sym, pos))
            held.sort()  # weakest RS first
            cands = []
            for sym, score in day_scores.items():
                if score < threshold:
                    continue
                if circuit_halted or panic_cooldown:
                    continue
                if sym in positions or sym in swapped_syms:
                    continue
                resolved = _resolve_ts_code(sym)
                if resolved is None or resolved[0] != config.market:
                    continue
                ts = resolved[1]
                if _gate_blocked(config, data, day, ts) is not None:
                    continue
                rsv = data.rs_rank_by_day.get(day, {}).get(ts)
                if rsv is None or rsv < config.swap_strong_rs_at_least:
                    continue
                px = entry_price_for(ts, day)
                if px is None or px <= 0:
                    continue
                regime = data.regime_by_day.get(day)
                pos_scale = 1.0 if regime == REGIME_STRONG else (
                    config.diverging_scale if regime == REGIME_DIVERGING else 0.0
                )
                cands.append((rsv, score, sym, ts, px, pos_scale))
            cands.sort(reverse=True)  # strongest RS first
            for (_rsv_w, sym_w, pos_w), cand in zip(held, cands, strict=False):
                if len(swapped_syms) >= config.swap_max_per_day:
                    break
                _, _, sym_c, ts_c, px_c, pos_scale_c = cand
                closes = data.close_by_ts_day.get(pos_w["ts_code"])
                close_px = closes.get(day) if closes else None
                if close_px is None or close_px <= 0:
                    continue
                entry_px = float(pos_w["entry_price"])
                slip = config.slippage_pct
                cost = entry_px * (1 + slip / 100.0)
                gross = (close_px * (1 - slip / 100.0) - cost) / cost * 100.0
                net = gross - _rt_cost
                realized_pnl_window.append((day, net))
                closed_trades.append(
                    BacktestTrade(
                        symbol=sym_w,
                        market=config.market,
                        entry_date=str(pos_w["entry_date"]),
                        entry_price=round(entry_px, 4),
                        close_date=day,
                        close_price=round(float(close_px), 4),
                        gross_pnl_pct=round(gross, 4),
                        costs_pct=round(_rt_cost, 4),
                        pnl_pct=round(net, 4),
                        holding_days=_calendar_days_between(str(pos_w["entry_date"]), day, data.calendar),
                        close_reason=CLOSE_REASON_SWAPPED,
                        score_at_entry=pos_w.get("score_at_entry"),
                        position_pct=float(pos_w.get("position_pct") or config.position_pct),
                    )
                )
                del positions[sym_w]
                positions[sym_c] = {
                    "symbol": sym_c,
                    "market": config.market,
                    "ts_code": ts_c,
                    "entry_date": day,
                     "entry_price": px_c,
                     "peak_price": px_c,
                     "score_at_entry": day_scores[sym_c],
                     "position_pct": config.position_pct * pos_scale_c * atr_scale_for(ts_c, day)
                     * config._env_position_scale(data.env_by_day.get(day)),
                     "industry": data.industry_by_ts.get(ts_c),
                 }
                swapped_syms.add(sym_c)

        # 1) Entries: score >= threshold, gates passed, not already held,
        #    price available. Score-desc order so a full sleeve admits the
        #    strongest candidates first (matches build_s3_candidates; the old
        #    dict iteration order was symbol-alphabetical and biased entries).
        # Entry order when the sleeve is limited: 'score' (base), 'score_rs'
        # (score * (0.5 + rs) — both matter), 'rs' (strength first).
        if config.entry_sort == "score":
            ordered_scores = sorted(day_scores.items(), key=lambda kv: -kv[1])
        else:
            rs_of = {}
            for sym in day_scores:
                resolved = _resolve_ts_code(sym)
                if resolved is not None:
                    rs_of[sym] = data.rs_rank_by_day.get(day, {}).get(resolved[1])
            if config.entry_sort == "rs":
                ordered_scores = sorted(
                    day_scores.items(),
                    key=lambda kv: (-(rs_of.get(kv[0]) or 0.0), -kv[1]),
                )
            else:  # score_rs
                ordered_scores = sorted(
                    day_scores.items(),
                    key=lambda kv: (-(kv[1] * (0.5 + (rs_of.get(kv[0]) or 0.0))), -kv[1]),
                )
        for sym, score in ordered_scores:
            if score < threshold:
                continue
            if config.score_confirm_days > 0:
                # C1 (2026-08-12 · defensive): require the score to have
                # cleared the threshold for N prior sessions too — filters
                # single-day score spikes (momentum pop) before committing.
                prev_scores = data.scores_by_day.get(prev_day or "", {})
                prev_score = prev_scores.get(sym)
                if prev_score is None or prev_score < threshold:
                    continue
            if circuit_halted:
                gated_blocks["circuit"] += 1
                continue
            if panic_cooldown:
                gated_blocks["panic_cooldown"] += 1
                continue
            # TIP-014 finding #3: block new entries on TRUE neutral days AND
            # implicit-weak days (ratio < 0.5 with only normal/caution
            # risk_mode — 16/16 losing trades in the valid window, avg
            # -6.1%). UNKNOWN days (no sentiment data) stay open.
            # (E1 conditionalization was tested 2026-08-14 and dropped — the
            # panic_cooldown 3→2 fix (E2) already resolves weak-year lockouts;
            # E1 showed no effect on the new baseline.)
            if config.neutral_block and data.env_by_day.get(day) in (ENV_NEUTRAL, ENV_WEAK):
                gated_blocks["neutral"] += 1
                continue
            if sym in positions:
                continue
            resolved = _resolve_ts_code(sym)
            if resolved is None or resolved[0] != config.market:
                continue
            ts = resolved[1]
            if config.exclude_st and ts in data.st_ts_codes:
                gated_blocks["st"] += 1
                continue
            if config.pead_days > 0:
                # P14 (signal pool): PEAD gate — the entry day must be within
                # pead_days sessions AFTER a positive earnings forecast
                # (业绩预告 预增/扭亏/略增/续盈). Names without a recent
                # positive announcement are blocked. fail-closed on missing
                # event data (no event = no drift edge).
                ev_dates = data.pead_events.get(ts)
                if not ev_dates:
                    gated_blocks["pead"] += 1
                    continue
                cal_idx = data.calendar.index(day) if day in data.calendar else None
                ok = False
                if cal_idx is not None:
                    for i in range(max(0, cal_idx - config.pead_days), cal_idx + 1):
                        if data.calendar[i] in ev_dates:
                            ok = True
                            break
                if not ok:
                    gated_blocks["pead"] += 1
                    continue
            if config.risk_adj_mom_ret_days > 0:
                # P12 (signal pool): volatility-adjusted momentum gate —
                # ret over the window / stdev of daily returns over the
                # vol window must be >= the threshold. Filters strong-but-
                # low-vol names (trend per unit of risk); a high-vol chase
                # entry fails it. Needs ret_days + vol_days of history.
                closes = data.closes_by_ts.get(ts)
                need = config.risk_adj_mom_ret_days + config.risk_adj_mom_vol_days + 2
                if closes is None or len(closes) < need:
                    gated_blocks["risk_adj_mom"] += 1
                    continue
                closes_sorted = sorted(closes, key=lambda kv: kv[0])
                idx = None
                for i, (d, _c) in enumerate(closes_sorted):
                    if str(d) == day:
                        idx = i
                        break
                if idx is None or idx < need:
                    gated_blocks["risk_adj_mom"] += 1
                    continue
                ret = closes_sorted[idx][1] / closes_sorted[idx - config.risk_adj_mom_ret_days][1] - 1.0
                rets = []
                for j in range(idx - config.risk_adj_mom_vol_days, idx):
                    prev = closes_sorted[j - 1][1]
                    if prev > 0:
                        rets.append(closes_sorted[j][1] / prev - 1.0)
                if len(rets) < config.risk_adj_mom_vol_days - 2:
                    gated_blocks["risk_adj_mom"] += 1
                    continue
                mean = sum(rets) / len(rets)
                var = sum((r - mean) ** 2 for r in rets) / len(rets)
                vol = var ** 0.5
                if vol <= 0:
                    gated_blocks["risk_adj_mom"] += 1
                    continue
                if ret / vol < config.risk_adj_mom_min:
                    gated_blocks["risk_adj_mom"] += 1
                    continue
            if config.ind_mom_days > 0:
                # P11 (signal pool): industry momentum gate — the entry
                # candidate's industry (eastmoney label, same mapping as the
                # mainline gate) must rank in the top ``ind_mom_top_pct`` of
                # ALL industries by its equal-weighted window return. "先选
                # 强势行业": distinct from the mainline gate (fund-flow Top3)
                # — this is a price-return dimension.
                ind = data.industry_by_ts.get(ts)
                ir = data.ind_industry_rank_by_day.get(day, {}).get(ind) if ind else None
                if ir is None or ir < config.ind_mom_top_pct:
                    gated_blocks["ind_mom"] += 1
                    continue
            if config.ind_neutral_days > 0:
                # P11 (signal pool): within-industry RS gate — the entry
                # candidate must rank in the top ``ind_neutral_rank_pct`` of
                # its own industry by window return. "行业内选强": the
                # whole-market RS rank cannot see this (a weak industry's
                # strongest name ranks low globally), so this is an
                # orthogonal-ish dimension.
                wr = data.ind_within_rank_by_day.get(day, {}).get(ts)
                if wr is None or wr < config.ind_neutral_rank_pct:
                    gated_blocks["ind_neutral"] += 1
                    continue
            if config.high_52w_min_pct > 0:
                # P10 (signal pool): 52-week-high proximity gate — the
                # entry-day close must be within X% of its 250-session high
                # (continuous state, NOT a discrete breakout like P1). A2's
                # falsified trend score included a "distance to 52w high"
                # component, so RS correlation was pre-checked (0.19-0.26
                # candidate-pool |r|, < 0.5) before this gate was tested.
                closes = data.closes_by_ts.get(ts)
                if closes is None:
                    gated_blocks["high52w"] += 1
                    continue
                idx = None
                for i, (d, _c) in enumerate(closes):
                    if str(d) == day:
                        idx = i
                        break
                if idx is None or idx < 250:
                    gated_blocks["high52w"] += 1
                    continue
                hi = max(c for (_d, c) in closes[idx - 250: idx])
                if hi <= 0 or (closes[idx][1] / hi) * 100.0 < config.high_52w_min_pct:
                    gated_blocks["high52w"] += 1
                    continue
            if config.mom_ret_days > 0:
                # P9 (signal pool): mid-horizon cross-sectional momentum
                # gate — the candidate's ret(mom_ret_days) skipping the last
                # mom_skip_days must rank in the top mom_rank_min of the
                # whole market. RS = 20d short horizon; P9 = 120-250d mid
                # horizon — pre-checked |r| < 0.1, horizons disjoint.
                mr = data.mom_rank_by_day.get(day, {}).get(ts)
                if mr is None or mr < config.mom_rank_min:
                    gated_blocks["mom"] += 1
                    continue
            blocked_by = _gate_blocked(config, data, day, ts)
            if blocked_by is not None:
                gated_blocks[blocked_by] += 1
                continue
            if config.rs_rank_min > 0:
                rs = data.rs_rank_by_day.get(day, {}).get(ts)
                if rs is None or rs < config.rs_rank_min:
                    gated_blocks["rs"] += 1
                    continue
            # TIP-014 entry style filter (momentum / dip / auto).
            style = config.entry_style
            if style == "auto":
                if config.market == "HK":
                    # HK has no CN sentiment data — the environment is the
                    # HSI/HSTECH regime itself. Default mapping mirrors CN
                    # (Strong→momentum, Diverging→dip, Weak→blocked); the
                    # hk_style_map override swaps it per the HK attribution
                    # (valid: Strong days lost, Diverging days best).
                    if config.hk_style_map:
                        hk_map = {}
                        for part in config.hk_style_map.split(","):
                            k, _, v = part.partition(":")
                            if k and v:
                                hk_map[k.strip()] = v.strip()
                        style = hk_map.get(data.regime_by_day.get(day) or "", "score")
                    else:
                        style = {
                            REGIME_STRONG: "momentum",
                            REGIME_DIVERGING: "dip",
                            REGIME_WEAK: "blocked",
                        }.get(data.regime_by_day.get(day) or "", "score")
                else:
                    style = {
                        ENV_UPTREND: "momentum",
                        ENV_FAN: "dip",
                        ENV_WEAK: "blocked",
                    }.get(data.env_by_day.get(day) or "", "score")
            if style in ("momentum", "dip"):
                rs = data.rs_rank_by_day.get(day, {}).get(ts)
                if rs is None or rs < config.entry_style_rs_min:
                    gated_blocks["style_rs"] += 1
                    continue
                ret5 = ret5_for(ts, day)
                if ret5 is None:
                    gated_blocks["style_ret"] += 1
                    continue
                if style == "momentum" and ret5 < -config.entry_style_dip_max:
                    gated_blocks["style_momentum"] += 1
                    continue
                if style == "dip" and ret5 > -config.entry_style_dip_min:
                    gated_blocks["style_dip"] += 1
                    continue
            elif style == "blocked":
                gated_blocks["style_blocked"] += 1
                continue
            if config.breakout_days > 0:
                # P1 (signal pool): turtle Donchian breakout gate — the
                # entry-day close must exceed the highest close of the prior
                # N sessions. ADDITIVE only: it tightens entries, never
                # loosens RS/score/env conditions.
                closes = data.closes_by_ts.get(ts)
                if closes is None:
                    gated_blocks["breakout"] += 1
                    continue
                idx = None
                for i, (d, _c) in enumerate(closes):
                    if str(d) == day:
                        idx = i
                        break
                if idx is None or idx < config.breakout_days:
                    gated_blocks["breakout"] += 1
                    continue
                prior = [c for (_d, c) in closes[idx - config.breakout_days: idx] if c is not None]
                if not prior or closes[idx][1] <= max(prior):
                    gated_blocks["breakout"] += 1
                    continue
            if config.volume_breakout_mult > 0:
                # P2 (signal pool): volume breakout gate — the entry-day
                # volume must exceed K x the 20-session average volume.
                # Volume is invisible to RS/score/env, so this is a true
                # additive dimension (and a classic A-share manipulation
                # tell: 放量突破). No prior-20-bar history -> fail-closed.
                bars = data.bars_by_ts.get(ts)
                if not bars:
                    gated_blocks["volume"] += 1
                    continue
                prior_bars = sorted(
                    [b for b in bars if str(b[0]) < day], key=lambda b: str(b[0])
                )[-20:]
                if len(prior_bars) < 20:
                    gated_blocks["volume"] += 1
                    continue
                vols = []
                for b in prior_bars:
                    try:
                        vols.append(float(b[5]))
                    except (TypeError, ValueError):
                        continue
                today_bar = next((b for b in bars if str(b[0]) == day), None)
                if len(vols) < 20 or today_bar is None:
                    gated_blocks["volume"] += 1
                    continue
                try:
                    today_vol = float(today_bar[5])
                except (TypeError, ValueError):
                    today_vol = 0.0
                avg_vol = sum(vols) / len(vols)
                if avg_vol <= 0 or today_vol <= avg_vol * config.volume_breakout_mult:
                    gated_blocks["volume"] += 1
                    continue
            if config.ma_slope_min_pct > 0:
                # P4 (signal pool): MA20 slope filter — the 20-day SMA at
                # the entry day must be >= X% above the 20-day SMA 20
                # sessions earlier (rising acceleration). Needs >= 40 closes.
                closes = data.closes_by_ts.get(ts)
                if closes is None or len(closes) < 40:
                    gated_blocks["ma_slope"] += 1
                    continue
                closes_sorted = sorted(closes, key=lambda kv: kv[0])
                idx = None
                for i, (d, _c) in enumerate(closes_sorted):
                    if str(d) == day:
                        idx = i
                        break
                if idx is None or idx < 40:
                    gated_blocks["ma_slope"] += 1
                    continue
                ma_now = sum(c for (_d, c) in closes_sorted[idx - 19: idx + 1]) / 20.0
                ma_prev = sum(c for (_d, c) in closes_sorted[idx - 39: idx - 19]) / 20.0
                if ma_prev <= 0:
                    gated_blocks["ma_slope"] += 1
                    continue
                slope_pct = (ma_now / ma_prev - 1.0) * 100.0
                if slope_pct < config.ma_slope_min_pct:
                    gated_blocks["ma_slope"] += 1
                    continue
            if config.ma200_min_pct >= 0:
                # P3 (signal pool): 200-day MA filter — entry-day close must
                # be >= X% above the 200-session SMA. Long-horizon trend
                # state (stock level; the index level is the regime gate's
                # job). Needs >= 200 closes, else fail-closed.
                closes = data.closes_by_ts.get(ts)
                if closes is None or len(closes) < 200:
                    gated_blocks["ma200"] += 1
                    continue
                closes_sorted = sorted(closes, key=lambda kv: kv[0])
                idx = None
                for i, (d, _c) in enumerate(closes_sorted):
                    if str(d) == day:
                        idx = i
                        break
                if idx is None or idx < 199:
                    gated_blocks["ma200"] += 1
                    continue
                ma200 = sum(c for (_d, c) in closes_sorted[idx - 199: idx + 1]) / 200.0
                if ma200 <= 0:
                    gated_blocks["ma200"] += 1
                    continue
                close_now = closes_sorted[idx][1]
                if (close_now / ma200 - 1.0) * 100.0 < config.ma200_min_pct:
                    gated_blocks["ma200"] += 1
                    continue
            if config.ma_cross_days > 0 or config.ma_aligned:
                # P5/P6 (signal pool): MA5/MA10/MA20 helpers from closes.
                closes = data.closes_by_ts.get(ts)
                if closes is None or len(closes) < 21:
                    gated_blocks["ma_cross"] += 1
                    continue
                closes_sorted = sorted(closes, key=lambda kv: kv[0])
                idx = None
                for i, (d, _c) in enumerate(closes_sorted):
                    if str(d) == day:
                        idx = i
                        break
                if idx is None or idx < 20:
                    gated_blocks["ma_cross"] += 1
                    continue
                ma5 = sum(c for (_d, c) in closes_sorted[idx - 4: idx + 1]) / 5.0
                ma10 = sum(c for (_d, c) in closes_sorted[idx - 9: idx + 1]) / 10.0
                ma20 = sum(c for (_d, c) in closes_sorted[idx - 19: idx + 1]) / 20.0
                if config.ma_aligned:
                    # P6: MA5 > MA10 > MA20 (three-line alignment state).
                    if not (ma5 > ma10 > ma20):
                        gated_blocks["ma_aligned"] += 1
                        continue
                if config.ma_cross_days > 0:
                    # P5: entry allowed within N sessions AFTER the golden
                    # cross (MA5 crossed above MA20). Scan back up to N
                    # sessions; a session is the cross day when the PREVIOUS
                    # session had MA5 <= MA20 and this one has MA5 > MA20.
                    crossed = False
                    for j in range(idx, max(idx - config.ma_cross_days, 20) - 1, -1):
                        ma5_j = sum(c for (_d, c) in closes_sorted[j - 4: j + 1]) / 5.0
                        ma20_j = sum(c for (_d, c) in closes_sorted[j - 19: j + 1]) / 20.0
                        ma5_jprev = sum(c for (_d, c) in closes_sorted[j - 5: j]) / 5.0
                        ma20_jprev = sum(c for (_d, c) in closes_sorted[j - 20: j]) / 20.0
                        if ma5_jprev <= ma20_jprev and ma5_j > ma20_j:
                            crossed = True
                            break
                    if not crossed:
                        gated_blocks["ma_cross"] += 1
                        continue
            if config.rsi_reversal_max > 0:
                # P7 (signal pool): RSI14 < max AND the entry day closes
                # green. RSI14 = 100 - 100/(1 + avg_gain/avg_loss) over the
                # prior 14 sessions (simple average, per classic Wilder-
                # style simple RSI used in screening tools).
                closes = data.closes_by_ts.get(ts)
                if closes is None:
                    gated_blocks["rsi_reversal"] += 1
                    continue
                closes_sorted = sorted(closes, key=lambda kv: kv[0])
                idx = None
                for i, (d, _c) in enumerate(closes_sorted):
                    if str(d) == day:
                        idx = i
                        break
                if idx is None or idx < 15:
                    gated_blocks["rsi_reversal"] += 1
                    continue
                gains = losses = 0.0
                for j in range(idx - 13, idx + 1):
                    chg = closes_sorted[j][1] - closes_sorted[j - 1][1]
                    if chg > 0:
                        gains += chg
                    else:
                        losses += abs(chg)
                rsi = 100.0
                if losses > 0:
                    rsi = 100.0 - 100.0 / (1.0 + gains / losses)
                green = closes_sorted[idx][1] > closes_sorted[idx - 1][1]
                if rsi >= config.rsi_reversal_max or not green:
                    gated_blocks["rsi_reversal"] += 1
                    continue
            if config.down_day_reversal_pct > 0:
                # P8 (signal pool): prior session fell >= X% AND the entry
                # day closes green (long-red-day reversal / 抄底反转).
                closes = data.closes_by_ts.get(ts)
                if closes is None:
                    gated_blocks["down_day_reversal"] += 1
                    continue
                closes_sorted = sorted(closes, key=lambda kv: kv[0])
                idx = None
                for i, (d, _c) in enumerate(closes_sorted):
                    if str(d) == day:
                        idx = i
                        break
                if idx is None or idx < 2:
                    gated_blocks["down_day_reversal"] += 1
                    continue
                prev_chg = (closes_sorted[idx - 1][1] / closes_sorted[idx - 2][1] - 1.0) * 100.0
                green = closes_sorted[idx][1] > closes_sorted[idx - 1][1]
                if prev_chg > -config.down_day_reversal_pct or not green:
                    gated_blocks["down_day_reversal"] += 1
                    continue
            if config.trend_score_min > 0:
                trend = _trend_score(
                    data.rs_rank_by_day.get(day, {}).get(ts),
                    data.closes_by_ts.get(ts),
                    day,
                )
                if trend is None or trend < config.trend_score_min:
                    gated_blocks["trend"] += 1
                    continue
            if config.max_per_industry > 0:
                ind = data.industry_by_ts.get(ts)
                if ind:
                    cnt = sum(
                        1
                        for p in positions.values()
                        if data.industry_by_ts.get(p["ts_code"]) == ind
                    )
                    if cnt >= config.max_per_industry:
                        gated_blocks["industry_cap"] += 1
                        continue
            if config.min_mv > 0 or config.max_mv > 0:
                mv = data.mv_by_day.get(day, {}).get(ts)
                if mv is not None:
                    if config.min_mv > 0 and mv < config.min_mv:
                        gated_blocks["mv_min"] += 1
                        continue
                    if config.max_mv > 0 and mv > config.max_mv:
                        gated_blocks["mv_max"] += 1
                        continue
            if config.min_avg_amount > 0:
                # P17 (signal pool): liquidity floor — exclude candidates
                # whose 60-session average daily turnover is below X 亿元.
                # Small-cap tail defence (thin books amplify slippage and
                # gap risk). Data missing → fail-closed (consistent with
                # the other liquidity gates).
                amt = data.avg_amount_by_day.get(day, {}).get(ts)
                if amt is None or amt < config.min_avg_amount:
                    gated_blocks["liquidity"] += 1
                    continue
            # Style-defense: in Diverging (choppy/weak) regimes exclude
            # mega-cap institutional names — 2024-25 OOS2 showed the
            # >500亿 cohort had the worst drawdowns (crowded unwind).
            if config.mv_max_diverging > 0 and data.regime_by_day.get(day) == REGIME_DIVERGING:
                mv = data.mv_by_day.get(day, {}).get(ts)
                if mv is not None and mv > config.mv_max_diverging:
                    gated_blocks["mv_diverging"] += 1
                    continue
            if config.board_exclude_set:
                code = str(sym).split(":")[-1]
                if code[:3] in config.board_exclude_set:
                    gated_blocks["board_excluded"] += 1
                    continue
            if len(positions) >= config.max_positions:
                gated_blocks["sleeve"] += 1
                continue
            if sym in swapped_syms:
                continue
            delist_d = getattr(data, "delist_by_ts", {}).get(ts)
            if delist_d is not None and delist_d <= day:
                gated_blocks["delisted"] += 1
                continue
            px = entry_price_for(ts, day)
            if px is None or px <= 0:
                continue
            # OPT-103: limit-up close = cannot buy in today (board pinned);
            # skip the signal — the engine re-evaluates next session, so a
            # still-qualified name re-enters naturally the following day.
            if config.market == "CN" and _at_limit(data, ts, day, px, up=True):
                gated_blocks["limit_up"] += 1
                continue
            regime = data.regime_by_day.get(day)
            pos_scale = 1.0 if regime == REGIME_STRONG else (
                config.diverging_scale if regime == REGIME_DIVERGING else 0.0
            )
            # E1: cash constraint — total nominal exposure capped at 100%
            eff_pct = config.position_pct * pos_scale * atr_scale_for(ts, day) * config._env_position_scale(data.env_by_day.get(day))
            if sum(p["position_pct"] for p in positions.values()) + eff_pct > 1.0 + 1e-9:
                gated_blocks["cash_cap"] += 1
                continue
            positions[sym] = {
                "symbol": sym,
                "market": config.market,
                "ts_code": ts,
                "entry_date": day,
                "entry_price": px,
                "peak_price": px,
                "score_at_entry": score,
                "industry": data.industry_by_ts.get(ts),
                "position_pct": eff_pct,
                "atr_pct": atr14_pct_for(ts, day) if config.atr_stop_mult > 0 else 0.0,
                "entry_env": data.env_by_day.get(day) if config.max_hold_env_shorten > 0 else None,
            }
            # NAV: deploy the sleeve's capital + pay entry cost up front.
            nav_cash -= eff_pct * (1.0 + _entry_cost_frac)

        # 2) Daily mark-to-market + close conditions (LIVE picker, as-of score).
        for sym in list(positions.keys()):
            pos = positions[sym]
            ts_code = pos["ts_code"]
            delist_d = getattr(data, "delist_by_ts", {}).get(ts_code)
            delisted_now = delist_d is not None and delist_d <= day
            closes = data.close_by_ts_day.get(ts_code)
            close_px = closes.get(day) if closes else None
            force_reason = None
            if close_px is None or close_px <= 0:
                if delisted_now:
                    last_close = _last_close_before(data, ts_code, day)
                    if last_close:
                        close_px = last_close
                        force_reason = CLOSE_REASON_DELISTED
                    else:
                        continue
                else:
                    # No bar today (suspension / weekend noise) — hold, retry.
                    continue
            entry_px = float(pos["entry_price"])
            slip = config.slippage_pct
            cost = entry_px * (1 + slip / 100.0)
            gross = (close_px * (1 - slip / 100.0) - cost) / cost * 100.0
            net = gross - _rt_cost
            holding = _calendar_days_between(str(pos["entry_date"]), day, data.calendar)
            score_asof = day_scores.get(sym)  # None → score_floor fails open

            if close_px > float(pos["peak_price"]):
                pos["peak_price"] = close_px

            # OPT-103: limit-down close = cannot sell today (pinned at the
            # board); roll every exit/trim decision to the next session.
            # Consecutive limit-downs roll naturally. Pyramid adds are also
            # skipped on a limit-down day (cannot buy into a board pin).
            if config.market == "CN" and _at_limit(data, pos["ts_code"], day, close_px, up=False):
                continue

            # OPT-104/105: regime-adaptive stops — Strong (and Diverging)
            # sessions use the volatility-adaptive ATR line (let winners run);
            # Weak sessions fall back to the FIXED stop/trail (cut fast, the
            # edge that the pure-ATR experiment lacked in weak markets).
            atr_pct = float(pos.get("atr_pct") or 0.0)
            regime_today = data.regime_by_day.get(day)
            if config.atr_stop_strength_min > 0:
                # §19.2 D1: continuous-strength selector replaces the regime
                # rule — ATR line when today's 0-100 strength >= the floor.
                atr_regime_ok = strength_for(day) >= config.atr_stop_strength_min
            else:
                atr_regime_ok = (
                    regime_today == REGIME_STRONG
                    if config.atr_stop_strong_only
                    else regime_today in (REGIME_STRONG, REGIME_DIVERGING)
                )
            if config.atr_stop_mult > 0 and atr_pct > 0 and atr_regime_ok:
                stop_i = -config.atr_stop_mult * atr_pct
                trail_i = -config.atr_stop_mult * atr_pct
            else:
                stop_i = config.stop_loss_pct
                trail_i = config.trailing_stop_pct

            reason = _pick_close_reason(
                t=pos,
                pnl_pct=net,
                holding_days=holding,
                registry_symbols=None,  # v0: no registry history → fail open
                score=score_asof,
                stop_loss_pct=stop_i,
                target_pnl_pct=config.target_pnl_pct,
                max_hold_days=(
                    config.max_hold_env_shorten
                    if config.max_hold_env_shorten > 0
                    and pos.get("entry_env") == ENV_UPTREND
                    else config.max_hold_days
                ),
                score_floor=config.score_floor,
            )
            if reason is None and trail_i != 0:
                peak = float(pos["peak_price"])
                if peak > 0 and (close_px - peak) / peak * 100.0 <= trail_i:
                    reason = CLOSE_REASON_TRAILING
            if (
                reason is None
                and config.max_hold_unprofitable_days > 0
                and holding >= config.max_hold_unprofitable_days
                and net < 0
            ):
                # P17 (signal pool): unprofitable time stop — a holding that
                # is still underwater after N days gets cut. The fixed stop
                # already bounds the worst case; this frees capital tied in
                # flat losers. Deliberately AFTER trailing (a winner that
                # pulled back below entry is still riding the trail).
                reason = CLOSE_REASON_TIME_STOP
            if reason is None and config.profit_trail_trigger_pct > 0 and config.profit_trail_pct < 0:
                # A6 (2026-08-12 · defensive): once the leg is past the profit
                # trigger, tighten the allowed pullback from the peak — protect
                # realized gains instead of giving 8% back on a winning leg.
                peak = float(pos["peak_price"])
                entry = float(pos["entry_price"])
                if entry > 0 and (peak - entry) / entry * 100.0 >= config.profit_trail_trigger_pct:
                    if peak > 0 and (close_px - peak) / peak * 100.0 <= config.profit_trail_pct:
                        reason = CLOSE_REASON_TRAILING
            if (
                reason is None
                and config.industry_flow_exit_days > 0
                and config.industry_flow_exit_days < 60
            ):
                # B1 (2026-08-12 · user-requested flow signal): exit when the
                # holding's SW L1 industry 5-session net inflow stays negative
                # for N straight sessions. Data starts 2025-12-15 → OOS2 has
                # no flow data (absent day = no signal, fail-open for exit).
                industry = str(pos.get("industry") or "")
                if industry:
                    streak = 0
                    for d in data.calendar:
                        if d > day:
                            break
                        f5 = data.flow5d_by_day.get(d) or {}
                        v = f5.get(industry)
                        if v is None:
                            streak = 0
                            continue
                        if v < 0:
                            streak += 1
                        else:
                            streak = 0
                        if streak >= config.industry_flow_exit_days:
                            break
                    if streak >= config.industry_flow_exit_days:
                        reason = CLOSE_REASON_FLOW_EXIT
            if reason is None and force_reason is not None:
                reason = force_reason
            if reason is not None:
                realized_pnl_window.append((day, net))
                closed_trades.append(
                    BacktestTrade(
                        symbol=sym,
                        market=config.market,
                        entry_date=str(pos["entry_date"]),
                        entry_price=round(entry_px, 4),
                        close_date=day,
                        close_price=round(float(close_px), 4),
                        gross_pnl_pct=round(gross, 4),
                        costs_pct=round(_rt_cost, 4),
                        pnl_pct=round(net, 4),
                        holding_days=holding,
                        close_reason=reason,
                        score_at_entry=pos.get("score_at_entry"),
                        position_pct=float(pos.get("position_pct") or config.position_pct),
                    )
                )
                # Pyramid add-legs exit with the main leg.
                for add in pos.get("adds_list", []):
                    add_entry = float(add["entry_price"])
                    add_cost = add_entry * (1 + slip / 100.0)
                    add_gross = (close_px * (1 - slip / 100.0) - add_cost) / add_cost * 100.0
                    # NAV: credit the realised add P&L (exit cost applied).
                    nav_cash += add["position_pct"] * (close_px / add_entry) * (1.0 - _exit_cost_frac)
                    closed_trades.append(
                        BacktestTrade(
                            symbol=sym,
                            market=config.market,
                            entry_date=str(add["entry_date"]),
                            entry_price=round(add_entry, 4),
                            close_date=day,
                            close_price=round(float(close_px), 4),
                            gross_pnl_pct=round(add_gross, 4),
                            costs_pct=round(_rt_cost, 4),
                            pnl_pct=round(add_gross - _rt_cost, 4),
                            holding_days=_calendar_days_between(str(add["entry_date"]), day, data.calendar),
                            close_reason=reason,
                            score_at_entry=pos.get("score_at_entry"),
                            position_pct=float(add.get("position_pct") or 0.0),
                        )
                    )
                # NAV: credit the realised main-leg P&L (exit cost applied).
                nav_cash += pos["position_pct"] * (close_px / entry_px) * (1.0 - _exit_cost_frac)
                del positions[sym]
                continue

            # Pyramid: add a smaller leg when the trend confirms (main leg
            # up >= trigger) — checked only when no exit fired, same day close.
            if (
                config.pyramid_max_adds > 0
                and pos.get("adds", 0) < config.pyramid_max_adds
                and gross >= config.pyramid_trigger_pct
            ):
                add_pct = float(pos.get("position_pct") or config.position_pct) * config.pyramid_add_scale
                # E1: pyramid also respects cash cap
                total_now = sum(p["position_pct"] for p in positions.values()) + sum(
                    a["position_pct"] for pp in positions.values() for a in pp.get("adds_list", [])
                )
                if total_now + add_pct > 1.0 + 1e-9:
                    gated_blocks["cash_cap_pyramid"] = gated_blocks.get("cash_cap_pyramid", 0) + 1
                else:
                    pos["adds"] = pos.get("adds", 0) + 1
                    pos.setdefault("adds_list", []).append(
                        {
                            "entry_date": day,
                            "entry_price": close_px,
                            "position_pct": add_pct,
                        }
                    )
                    # NAV: deploy the add's capital + entry cost.
                    nav_cash -= add_pct * (1.0 + _entry_cost_frac)

        # Continuous NAV (mark-to-market of all open sleeves) for honest
        # Sharpe / MaxDD / CAGR — replaces the old per-close-day proxy.
        nav_curve.append(nav_cash + _nav_for_day(positions, data, day))

        # End-of-day holding snapshot — the anchor for reconciling the real
        # paper/watchlist book against the backtest (2026-08-11). Captured
        # AFTER exits/entries/pyramids of this day, so it is "what the backtest
        # says we should be holding at the close of day".
        snapshot = [
            {
                "symbol": sym,
                "market": config.market,
                "ts_code": pos["ts_code"],
                "entry_date": str(pos["entry_date"]),
                "entry_price": round(float(pos["entry_price"]), 4),
                "score_at_entry": pos.get("score_at_entry"),
                "position_pct": round(float(pos.get("position_pct") or config.position_pct), 4),
            }
            for sym, pos in positions.items()
        ]
        positions_by_day.append({"date": day, "positions": snapshot})

    # 3) Close leftovers at the window end (engine-only reason).
    for sym, pos in list(positions.items()):
        closes = data.close_by_ts_day.get(pos["ts_code"])
        last_day = data.calendar[-1] if data.calendar else config.end_date
        final_px = None
        if closes:
            # walk back to the last day with a bar
            for d in reversed(data.calendar):
                if d in closes:
                    final_px = closes[d]
                    last_day = d
                    break
        if final_px is None or final_px <= 0:
            continue
        entry_px = float(pos["entry_price"])
        slip = config.slippage_pct
        cost = entry_px * (1 + slip / 100.0)
        gross = (final_px * (1 - slip / 100.0) - cost) / cost * 100.0
        net = gross - _rt_cost
        closed_trades.append(
            BacktestTrade(
                symbol=sym,
                market=config.market,
                entry_date=str(pos["entry_date"]),
                entry_price=round(entry_px, 4),
                close_date=last_day,
                close_price=round(float(final_px), 4),
                gross_pnl_pct=round(gross, 4),
                costs_pct=round(_rt_cost, 4),
                pnl_pct=round(net, 4),
                holding_days=_calendar_days_between(str(pos["entry_date"]), last_day, data.calendar),
                close_reason=CLOSE_REASON_END_OF_WINDOW,
                score_at_entry=pos.get("score_at_entry"),
                position_pct=float(pos.get("position_pct") or config.position_pct),
            )
        )
        for add in pos.get("adds_list", []):
            add_entry = float(add["entry_price"])
            add_cost = add_entry * (1 + slip / 100.0)
            add_gross = (final_px * (1 - slip / 100.0) - add_cost) / add_cost * 100.0
            closed_trades.append(
                BacktestTrade(
                    symbol=sym,
                    market=config.market,
                    entry_date=str(add["entry_date"]),
                    entry_price=round(add_entry, 4),
                    close_date=last_day,
                    close_price=round(float(final_px), 4),
                    gross_pnl_pct=round(add_gross, 4),
                    costs_pct=round(_rt_cost, 4),
                    pnl_pct=round(add_gross - _rt_cost, 4),
                    holding_days=_calendar_days_between(str(add["entry_date"]), last_day, data.calendar),
                    close_reason=CLOSE_REASON_END_OF_WINDOW,
                    score_at_entry=pos.get("score_at_entry"),
                    position_pct=float(add.get("position_pct") or 0.0),
                )
            )
        # NAV: credit the realised window-end P&L (main + add legs).
        nav_cash += pos["position_pct"] * (final_px / entry_px) * (1.0 - _exit_cost_frac)
        for add in pos.get("adds_list", []):
            nav_cash += add["position_pct"] * (final_px / float(add["entry_price"])) * (1.0 - _exit_cost_frac)
        # Must drop the closed position or open_at_end would count it too
        # (it counts only the positions we could not price at window end).
        del positions[sym]
    # Position dict is discarded; open_at_end = count we could not price.
    open_at_end = len(positions)
    # Final NAV point: all window-end positions are now closed and their
    # realised P&L credited to nav_cash. Append so the curve ends at the
    # realised value (otherwise closed-trade P&L would be lost from the curve).
    nav_curve.append(nav_cash)

    return BacktestRun(
        summary=_summarize(config, data, closed_trades, open_at_end, gated_blocks, nav_curve),
        trades=closed_trades,
        positions_by_day=positions_by_day,
        nav_curve=nav_curve,
    )


def _summarize(
    config: BacktestConfig,
    data: BacktestData,
    trades: list[BacktestTrade],
    open_at_end: int,
    gated_blocks: dict[str, int] | None = None,
    nav_curve: list[float] | None = None,
) -> BacktestSummary:
    closed = trades
    wins = [t for t in closed if t.pnl_pct > 0]
    losses = [t for t in closed if t.pnl_pct <= 0]
    nets = [t.pnl_pct for t in closed]
    grosses = [t.gross_pnl_pct for t in closed]
    costs = [t.costs_pct for t in closed]

    buckets: dict[str, dict[str, Any]] = {}
    for t in closed:
        s = t.score_at_entry
        bucket = ">=90" if s is not None and s >= 90 else (
            "85-90" if s is not None and s >= 85 else (
                "80-85" if s is not None and s >= 80 else (
                    "70-80" if s is not None and s >= 70 else "<70"
                )
            )
        )
        b = buckets.setdefault(
            bucket, {"trades": 0, "wins": 0, "sumNet": 0.0, "winRate": None, "avgNet": None}
        )
        b["trades"] += 1
        b["sumNet"] += t.pnl_pct
        if t.pnl_pct > 0:
            b["wins"] += 1
    for _, b in buckets.items():
        b["winRate"] = round(b["wins"] / b["trades"], 3) if b["trades"] else None
        b["avgNet"] = round(b["sumNet"] / b["trades"], 3) if b["trades"] else None
        del b["sumNet"]

    # --- Continuous-NAV metrics (honest Sharpe / MaxDD / CAGR) ---
    # Anchor at 1.0 (pre-trading capital); the window-end forced close is
    # already appended to nav_curve by the caller, so the last point is the
    # realised NAV after all exits.
    nav_full = [1.0] + (nav_curve or [1.0])
    nav_start = 1.0
    nav_end = nav_full[-1]
    daily_rets: list[float] = []
    for i in range(1, len(nav_full)):
        prev = nav_full[i - 1]
        if prev > 0:
            daily_rets.append(nav_full[i] / prev - 1.0)
    if daily_rets:
        mean_r = statistics.mean(daily_rets)
        std_r = statistics.stdev(daily_rets) if len(daily_rets) > 1 else 0.0
        sharpe_val = round(mean_r / std_r * (252 ** 0.5), 2) if std_r > 0 else None
        n_days = len(nav_curve) - 1
        cagr = (nav_end ** (252.0 / n_days) - 1.0) if n_days > 0 and nav_end > 0 else 0.0
    else:
        sharpe_val = None
        cagr = 0.0
    nav_peak = nav_curve[0] if nav_curve else 1.0
    nav_max_dd = 0.0
    for v in nav_curve:
        if v > nav_peak:
            nav_peak = v
        dd = nav_peak - v
        if dd > nav_max_dd:
            nav_max_dd = dd

    return BacktestSummary(
        config=asdict(config),
        calendar_days=len(data.calendar),
        trades=len(trades),
        closed=len(closed),
        open_at_end=open_at_end,
        wins=len(wins),
        losses=len(losses),
        win_rate=round(len(wins) / len(closed), 3) if closed else None,
        avg_net_pnl_pct=round(sum(nets) / len(nets), 3) if nets else None,
        avg_gross_pnl_pct=round(sum(grosses) / len(grosses), 3) if grosses else None,
        avg_costs_pct=round(sum(costs) / len(costs), 3) if costs else None,
        max_drawdown_pct=round(nav_max_dd * 100.0, 3),
        total_net_pnl_pct=round((nav_end / nav_start - 1.0) * 100.0, 3) if nav_curve else 0.0,
        annual_net_pnl_pct=round(cagr * 100.0, 3),
        avg_win_pct=round(sum(t.pnl_pct for t in wins) / len(wins), 3) if wins else None,
        avg_loss_pct=round(sum(t.pnl_pct for t in losses) / len(losses), 3) if losses else None,
        sharpe=sharpe_val,
        excess_vs_best_benchmark_pct=0.0,
        best_benchmark="",
        by_score_bucket=buckets,
        gated_blocks=dict(gated_blocks or {}),
    )


def _window_years(config: BacktestConfig) -> float:
    try:
        return max(
            (date.fromisoformat(config.end_date) - date.fromisoformat(config.start_date)).days / 365.25,
            1 / 365.25,
        )
    except ValueError:
        return 1.0


def _sharpe_from_closes(
    closed: list[BacktestTrade],
    config: BacktestConfig,
) -> float | None:
    """Approximate annualized Sharpe from the per-close-day return series.

    Each close day contributes pnl_pct * scale (0 on days without closes).
    This is a coarse proxy (no intraday MTM of open positions); labeled
    "approx" in the UI.
    """
    if not closed:
        return None
    import statistics

    by_day: dict[str, float] = {}
    for t in closed:
        w = float(getattr(t, "position_pct", config.position_pct))
        by_day[t.close_date] = by_day.get(t.close_date, 0.0) + t.pnl_pct * w
    days = sorted(by_day)
    rets = [by_day[d] for d in days]
    if len(rets) < 3:
        return None
    mean = statistics.mean(rets)
    stdev = statistics.stdev(rets) if len(rets) > 1 else 0.0
    if stdev <= 0:
        return None
    return round(mean / stdev * (252 ** 0.5), 2)


def with_benchmark_excess(
    summary: BacktestSummary,
    benchmarks: list[dict[str, Any]],
) -> BacktestSummary:
    """Fill excess-vs-best-benchmark fields on a summary (mutates + returns)."""
    best = max(benchmarks, key=lambda b: b.get("annual_pct") or 0.0) if benchmarks else None
    best_annual = float(best.get("annual_pct") or 0.0) if best else 0.0
    summary.best_benchmark = str(best.get("name") or "") if best else ""
    summary.excess_vs_best_benchmark_pct = round(
        float(summary.annual_net_pnl_pct or 0.0) - best_annual, 2
    )
    return summary


def run_sensitivity(configs: list[BacktestConfig]) -> list[BacktestSummary]:
    """Run a grid of configurations and return their summaries.

    Configs sharing the same (window, market) reuse one BacktestData — the
    gate datasets and price bars are loaded once per window, not per point.
    """
    groups: dict[tuple[str, str, str], list[BacktestConfig]] = defaultdict(list)
    for c in configs:
        groups[(c.start_date, c.end_date, c.market)].append(c)
    out: list[BacktestSummary] = []
    for (_start, _end, _market), group in groups.items():
        data = BacktestData(group[0])
        for c in group:
            out.append(simulate(c, data).summary)
    return out


def default_sensitivity_grid(
    start_date: str,
    end_date: str,
    gates_levels: tuple[str, ...] = ("none", "full"),
) -> list[BacktestConfig]:
    """The grid: score_threshold x max_hold_days x stop_loss_pct x gates."""
    out: list[BacktestConfig] = []
    for threshold in (70, 80, 85, 90):
        for hold in (5, 10, 20):
            for stop in (-3.0, -5.0, -8.0):
                for gates in gates_levels:
                    out.append(
                        BacktestConfig(
                            start_date=start_date,
                            end_date=end_date,
                            score_threshold=float(threshold),
                            max_hold_days=hold,
                            stop_loss_pct=stop,
                            gates=gates,
                        )
                    )
    return out


def _calendar_days_between(entry_date: str, today: str, calendar: list[str] | None = None) -> int:
    # E3 2026-08-22: use trading days when calendar is available (max_hold etc. are trading-day concepts)
    if calendar is not None:
        try:
            # calendar is sorted ascending trading dates
            # holding 0 on entry day, 1 next trading day
            if entry_date not in calendar or today not in calendar:
                # fallback to calendar diff for edge cases (weekend entry)
                e = date.fromisoformat(entry_date)
                t = date.fromisoformat(today)
                return max(0, (t - e).days)
            return max(0, calendar.index(today) - calendar.index(entry_date))
        except ValueError:
            pass
    try:
        e = date.fromisoformat(entry_date)
        t = date.fromisoformat(today)
    except ValueError:
        return 0
    return max(0, (t - e).days)
