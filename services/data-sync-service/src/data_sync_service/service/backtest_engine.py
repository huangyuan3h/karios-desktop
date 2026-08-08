"""Backtest engine v0 (OPT-063 / L3-P2) — signal replay with LIVE close logic.

The engine replays the system's OWN historical signals and simulates a
satellite-book paper portfolio. The point is **same-code discipline**: entry
uses the TrendOK score the system actually recorded that day
(``watchlist_score_daily``), and every close condition is delegated to
``service.paper_trading._pick_close_reason`` — the exact function the live
paper-trading cron runs. There is no second copy of any rule.

Scope (v0):

- CN only. HK/ETF score rows are excluded.
- Signals: ``watchlist_score_daily`` (records start 2026-06-18). The TV
  screener universe is NOT consumed yet (v0.2 adds drawdown-window filters).
- Entry: signal-day close, full position, score >= ``score_threshold``.
- Costs: net pnl = gross - market round-trip cost (``paper_cost_model``),
  applied at close time — identical to live paper v0.2.
- Close reasons: stop_hit / target_hit / score_floor / max_hold from the
  live picker. ``pool_exit`` is OFF in v0 (no registry history → fail-open
  by design). Leftover open positions at window end close with reason
  ``end_of_window`` (engine-only, never emitted by live code).
- Look-ahead discipline: every value used on day D is recorded on or before
  D. The score injected into the close picker is the score recorded ON day
  D; missing score → fail-open (no score_floor close).

Intended use: parameter-sensitivity analysis ONLY (score threshold, max
hold, stop). Outputs are not a release decision basis — the paper book is.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Any

from data_sync_service.db import get_connection
from data_sync_service.db.daily import fetch_ohlcv_batch_between
from data_sync_service.service.paper_cost_model import MARKETS, round_trip_cost_pct
from data_sync_service.service.paper_trading import _pick_close_reason, _resolve_ts_code

logger = logging.getLogger(__name__)

SCORE_TABLE = "watchlist_score_daily"

CLOSE_REASON_END_OF_WINDOW = "end_of_window"


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

    def __post_init__(self) -> None:
        if self.market not in MARKETS:
            raise ValueError(f"market must be one of {MARKETS} (got {self.market!r})")
        if self.start_date > self.end_date:
            raise ValueError("start_date must be <= end_date")
        if self.score_threshold < 0 or self.score_threshold > 100:
            raise ValueError("score_threshold must be in [0, 100]")


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
    by_score_bucket: dict[str, dict[str, Any]] = field(default_factory=dict)

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
            self.bars_by_ts = fetch_ohlcv_batch_between(
                self.ts_codes, config.start_date, config.end_date
            )
        # index: ts_code -> {date: close}
        self.close_by_ts_day: dict[str, dict[str, float]] = {}
        for ts, bars in self.bars_by_ts.items():
            closes: dict[str, float] = {}
            for bar in bars:
                try:
                    closes[str(bar[0])] = float(bar[4])
                except (TypeError, ValueError):
                    continue
            self.close_by_ts_day[ts] = closes


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


@dataclass
class BacktestRun:
    """Full result of one simulation: aggregates + trade-by-trade list."""

    summary: BacktestSummary
    trades: list[BacktestTrade] = field(default_factory=list)


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

    threshold = config.score_threshold
    costs_pct = round_trip_cost_pct(config.market) * 100.0

    def entry_price_for(ts: str, day: str) -> float | None:
        closes = data.close_by_ts_day.get(ts)
        return closes.get(day) if closes else None

    for day in data.calendar:
        day_scores = data.scores_by_day.get(day, {})

        # 1) Entries: score >= threshold, not already held, price available.
        for sym, score in day_scores.items():
            if score < threshold:
                continue
            if sym in positions:
                continue
            resolved = _resolve_ts_code(sym)
            if resolved is None or resolved[0] != config.market:
                continue
            ts = resolved[1]
            px = entry_price_for(ts, day)
            if px is None or px <= 0:
                continue
            positions[sym] = {
                "symbol": sym,
                "market": config.market,
                "ts_code": ts,
                "entry_date": day,
                "entry_price": px,
                "score_at_entry": score,
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
            gross = (close_px - entry_px) / entry_px * 100.0
            net = gross - costs_pct
            holding = _calendar_days_between(str(pos["entry_date"]), day)
            score_asof = day_scores.get(sym)  # None → score_floor fails open

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
            if reason is not None:
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
                    )
                )
                del positions[sym]

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
        gross = (final_px - entry_px) / entry_px * 100.0
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
            )
        )
        # Must drop the closed position or open_at_end would count it too
        # (it counts only the positions we could not price at window end).
        del positions[sym]
    # Position dict is discarded; open_at_end = count we could not price.
    open_at_end = len(positions)

    return BacktestRun(
        summary=_summarize(config, data, closed_trades, open_at_end),
        trades=closed_trades,
    )


def _summarize(
    config: BacktestConfig,
    data: BacktestData,
    trades: list[BacktestTrade],
    open_at_end: int,
) -> BacktestSummary:
    closed = trades
    wins = [t for t in closed if t.pnl_pct > 0]
    losses = [t for t in closed if t.pnl_pct <= 0]
    nets = [t.pnl_pct for t in closed]
    grosses = [t.gross_pnl_pct for t in closed]
    costs = [t.costs_pct for t in closed]

    # Max drawdown on the cumulative net-pnl curve (ordered by close date).
    curve: list[tuple[str, float]] = sorted((t.close_date, t.pnl_pct) for t in closed)
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
    for bucket, b in buckets.items():
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
        by_score_bucket=buckets,
    )


def run_sensitivity(configs: list[BacktestConfig]) -> list[BacktestSummary]:
    """Run a grid of configurations and return their summaries.

    Each config gets its own BacktestData (independent windows). For a
    single-window grid, prefer building one BacktestData and calling
    :func:`simulate` per point — this helper exists for CLI ergonomics.
    """
    return [simulate(c).summary for c in configs]


def default_sensitivity_grid(start_date: str, end_date: str) -> list[BacktestConfig]:
    """The v0 grid: score_threshold x max_hold_days x stop_loss_pct."""
    out: list[BacktestConfig] = []
    for threshold in (70, 80, 85, 90):
        for hold in (5, 10, 20):
            for stop in (-3.0, -5.0, -8.0):
                out.append(
                    BacktestConfig(
                        start_date=start_date,
                        end_date=end_date,
                        score_threshold=float(threshold),
                        max_hold_days=hold,
                        stop_loss_pct=stop,
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
