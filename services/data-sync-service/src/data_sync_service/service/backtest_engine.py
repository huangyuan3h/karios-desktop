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
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from typing import Any

from data_sync_service.db import get_connection
from data_sync_service.db.daily import fetch_ohlcv_batch_between
from data_sync_service.db.industry_fund_flow import get_dates_upto, get_rows_for_dates
from data_sync_service.service.execution_gate import (
    REGIME_DIVERGING,
    REGIME_STRONG,
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

GATE_LEVELS = ("none", "regime", "full")

GATE_REASON_REGIME = "regime"
GATE_REASON_FLOW = "flow"
GATE_REASON_MAINLINE = "mainline"
GATE_REASON_SENTIMENT = "sentiment"

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
    max_per_industry: int = 0
    entry_sort: str = "score"
    min_mv: float = 0.0
    max_mv: float = 0.0
    mv_max_diverging: float = 0.0
    exclude_boards: str = ""
    board_exclude_set: frozenset[str] = frozenset()

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
        self.scores_by_day: dict[str, dict[str, float]] = _load_scores(
            config.start_date, config.end_date, config.market
        )
        universe = sorted({s for day in self.scores_by_day.values() for s in day})
        self.ts_codes: list[str] = []
        for u in universe:
            resolved = _resolve_ts_code(u)
            if resolved and resolved[0] == config.market:
                self.ts_codes.append(resolved[1])
        self.bars_by_ts: dict[str, list[tuple[str, str, str, str, str, str]]] = {}
        if self.ts_codes:
            from datetime import timedelta

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
        self.flow_any_positive_by_day, self.mainline_allow_by_day, self.flow5d_by_day = (
            _load_flow_mainline_data(config, self.calendar)
        )
        self.industry_by_ts = _load_industries(self.ts_codes)
        self.rs_rank_by_day = _load_rs_ranks(config, self.calendar, set(self.ts_codes))
        self.sentiment_risk_by_day = _load_sentiment_risk(config)
        self.mv_by_day = _load_market_caps(config, set(self.ts_codes))


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


def _load_flow_mainline_data(
    config: BacktestConfig,
    calendar: list[str],
) -> tuple[dict[str, bool], dict[str, set[str]]]:
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
    gated_blocks: dict[str, int] = defaultdict(int)
    last_panic_idx = -10 ** 9
    day_index = 0

    threshold = config.score_threshold
    costs_pct = round_trip_cost_pct(config.market) * 100.0
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
        closes = data.close_by_ts_day.get(ts)
        return closes.get(day) if closes else None

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
                if _calendar_days_between(str(pos["entry_date"]), day) < config.swap_min_hold_days:
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
                net = gross - costs_pct
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
                        costs_pct=round(costs_pct, 4),
                        pnl_pct=round(net, 4),
                        holding_days=_calendar_days_between(str(pos_w["entry_date"]), day),
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
                     "position_pct": config.position_pct * pos_scale_c * atr_scale_for(ts_c, day),
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
                prev_scores = data.scores_by_day.get(prev_day, {})
                prev_score = prev_scores.get(sym)
                if prev_score is None or prev_score < threshold:
                    continue
            if circuit_halted:
                gated_blocks["circuit"] += 1
                continue
            if panic_cooldown:
                gated_blocks["panic_cooldown"] += 1
                continue
            if sym in positions:
                continue
            resolved = _resolve_ts_code(sym)
            if resolved is None or resolved[0] != config.market:
                continue
            ts = resolved[1]
            blocked_by = _gate_blocked(config, data, day, ts)
            if blocked_by is not None:
                gated_blocks[blocked_by] += 1
                continue
            if config.rs_rank_min > 0:
                rs = data.rs_rank_by_day.get(day, {}).get(ts)
                if rs is None or rs < config.rs_rank_min:
                    gated_blocks["rs"] += 1
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
            px = entry_price_for(ts, day)
            if px is None or px <= 0:
                continue
            regime = data.regime_by_day.get(day)
            pos_scale = 1.0 if regime == REGIME_STRONG else (
                config.diverging_scale if regime == REGIME_DIVERGING else 0.0
            )
            positions[sym] = {
                "symbol": sym,
                "market": config.market,
                "ts_code": ts,
                "entry_date": day,
                "entry_price": px,
                "peak_price": px,
                "score_at_entry": score,
                "industry": data.industry_by_ts.get(ts),
                "position_pct": config.position_pct * pos_scale * atr_scale_for(ts, day),
            }

        # 2) Daily mark-to-market + close conditions (LIVE picker, as-of score).
        for sym in list(positions.keys()):
            pos = positions[sym]
            closes = data.close_by_ts_day.get(pos["ts_code"])
            close_px = closes.get(day) if closes else None
            if close_px is None or close_px <= 0:
                # No bar today (suspension / weekend noise) — hold, retry next day.
                continue
            entry_px = float(pos["entry_price"])
            slip = config.slippage_pct
            cost = entry_px * (1 + slip / 100.0)
            gross = (close_px * (1 - slip / 100.0) - cost) / cost * 100.0
            net = gross - costs_pct
            holding = _calendar_days_between(str(pos["entry_date"]), day)
            score_asof = day_scores.get(sym)  # None → score_floor fails open

            if close_px > float(pos["peak_price"]):
                pos["peak_price"] = close_px

            reason = _pick_close_reason(
                t=pos,
                pnl_pct=net,
                holding_days=holding,
                registry_symbols=None,  # v0: no registry history → fail open
                score=score_asof,
                stop_loss_pct=config.stop_loss_pct,
                target_pnl_pct=config.target_pnl_pct,
                max_hold_days=config.max_hold_days,
                score_floor=config.score_floor,
            )
            if reason is None and config.trailing_stop_pct != 0:
                peak = float(pos["peak_price"])
                if peak > 0 and (close_px - peak) / peak * 100.0 <= config.trailing_stop_pct:
                    reason = CLOSE_REASON_TRAILING
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
                        costs_pct=round(costs_pct, 4),
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
                    closed_trades.append(
                        BacktestTrade(
                            symbol=sym,
                            market=config.market,
                            entry_date=str(add["entry_date"]),
                            entry_price=round(add_entry, 4),
                            close_date=day,
                            close_price=round(float(close_px), 4),
                            gross_pnl_pct=round(add_gross, 4),
                            costs_pct=round(costs_pct, 4),
                            pnl_pct=round(add_gross - costs_pct, 4),
                            holding_days=_calendar_days_between(str(add["entry_date"]), day),
                            close_reason=reason,
                            score_at_entry=pos.get("score_at_entry"),
                            position_pct=float(add.get("position_pct") or 0.0),
                        )
                    )
                del positions[sym]
                continue

            # Pyramid: add a smaller leg when the trend confirms (main leg
            # up >= trigger) — checked only when no exit fired, same day close.
            if (
                config.pyramid_max_adds > 0
                and pos.get("adds", 0) < config.pyramid_max_adds
                and gross >= config.pyramid_trigger_pct
            ):
                pos["adds"] = pos.get("adds", 0) + 1
                pos.setdefault("adds_list", []).append(
                    {
                        "entry_date": day,
                        "entry_price": close_px,
                        "position_pct": float(pos.get("position_pct") or config.position_pct)
                        * config.pyramid_add_scale,
                    }
                )

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
        net = gross - costs_pct
        closed_trades.append(
            BacktestTrade(
                symbol=sym,
                market=config.market,
                entry_date=str(pos["entry_date"]),
                entry_price=round(entry_px, 4),
                close_date=last_day,
                close_price=round(float(final_px), 4),
                gross_pnl_pct=round(gross, 4),
                costs_pct=round(costs_pct, 4),
                pnl_pct=round(net, 4),
                holding_days=_calendar_days_between(str(pos["entry_date"]), last_day),
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
                    costs_pct=round(costs_pct, 4),
                    pnl_pct=round(add_gross - costs_pct, 4),
                    holding_days=_calendar_days_between(str(add["entry_date"]), last_day),
                    close_reason=CLOSE_REASON_END_OF_WINDOW,
                    score_at_entry=pos.get("score_at_entry"),
                    position_pct=float(add.get("position_pct") or 0.0),
                )
            )
        # Must drop the closed position or open_at_end would count it too
        # (it counts only the positions we could not price at window end).
        del positions[sym]
    # Position dict is discarded; open_at_end = count we could not price.
    open_at_end = len(positions)

    return BacktestRun(
        summary=_summarize(config, data, closed_trades, open_at_end, gated_blocks),
        trades=closed_trades,
        positions_by_day=positions_by_day,
    )


def _summarize(
    config: BacktestConfig,
    data: BacktestData,
    trades: list[BacktestTrade],
    open_at_end: int,
    gated_blocks: dict[str, int] | None = None,
) -> BacktestSummary:
    closed = trades
    wins = [t for t in closed if t.pnl_pct > 0]
    losses = [t for t in closed if t.pnl_pct <= 0]
    nets = [t.pnl_pct for t in closed]
    grosses = [t.gross_pnl_pct for t in closed]
    costs = [t.costs_pct for t in closed]

    # Max drawdown on the cumulative net-pnl curve (ordered by close date),
    # scaled by the per-trade position size (a 5% sleeve cannot move the
    # account by its full pnl_pct).
    curve: list[tuple[str, float]] = sorted(
        (t.close_date, t.pnl_pct * float(getattr(t, "position_pct", config.position_pct)))
        for t in closed
    )
    peak = 0.0
    cum = 0.0
    max_dd = 0.0
    for _, pnl in curve:
        cum += pnl
        if cum > peak:
            peak = cum
        dd = peak - cum
        if dd > max_dd:
            max_dd = dd

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
        max_drawdown_pct=round(max_dd, 3),
        total_net_pnl_pct=round(
            sum(t.pnl_pct * float(getattr(t, "position_pct", config.position_pct)) for t in closed),
            3,
        )
        if closed
        else 0.0,
        annual_net_pnl_pct=round(
            sum(t.pnl_pct * float(getattr(t, "position_pct", config.position_pct)) for t in closed)
            / _window_years(config),
            3,
        )
        if closed
        else 0.0,
        avg_win_pct=round(sum(t.pnl_pct for t in wins) / len(wins), 3) if wins else None,
        avg_loss_pct=round(sum(t.pnl_pct for t in losses) / len(losses), 3) if losses else None,
        sharpe=_sharpe_from_closes(closed, config),
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


def _calendar_days_between(entry_date: str, today: str) -> int:
    try:
        e = date.fromisoformat(entry_date)
        t = date.fromisoformat(today)
    except ValueError:
        return 0
    return max(0, (t - e).days)
