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
from datetime import date
from typing import Any

from data_sync_service.db import get_connection
from data_sync_service.db.daily import fetch_ohlcv_batch_between
from data_sync_service.db.industry_fund_flow import get_dates_upto, get_rows_for_dates
from data_sync_service.service.execution_gate import REGIME_STRONG, classify_market_regime
from data_sync_service.service.industry_fund_flow_read import top_by_date_from_rows
from data_sync_service.service.market_regime import get_index_signals
from data_sync_service.service.paper_cost_model import MARKETS, round_trip_cost_pct
from data_sync_service.service.paper_trading import _pick_close_reason, _resolve_ts_code

logger = logging.getLogger(__name__)

SCORE_TABLE = "watchlist_score_daily"

CLOSE_REASON_END_OF_WINDOW = "end_of_window"

GATE_LEVELS = ("none", "regime", "full")

GATE_REASON_REGIME = "regime"
GATE_REASON_FLOW = "flow"
GATE_REASON_MAINLINE = "mainline"

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

    def __post_init__(self) -> None:
        if self.market not in MARKETS:
            raise ValueError(f"market must be one of {MARKETS} (got {self.market!r})")
        if self.start_date > self.end_date:
            raise ValueError("start_date must be <= end_date")
        if self.score_threshold < 0 or self.score_threshold > 100:
            raise ValueError("score_threshold must be in [0, 100]")
        if self.gates not in GATE_LEVELS:
            raise ValueError(f"gates must be one of {GATE_LEVELS} (got {self.gates!r})")


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
        self.regime_by_day = _load_regime_by_day(config, self.calendar)
        self.flow_any_positive_by_day, self.mainline_allow_by_day = _load_flow_mainline_data(
            config, self.calendar
        )
        self.industry_by_ts = _load_industries(self.ts_codes)


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


def _load_regime_by_day(config: BacktestConfig, calendar: list[str]) -> dict[str, str]:
    """As-of index traffic-light regime per trading day.

    Reuses the exact live functions (``get_index_signals`` +
    ``classify_market_regime``). Missing data → the day is absent so the
    engine's gate check fails closed.
    """
    out: dict[str, str] = {}
    for day in calendar:
        try:
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

    Returns ``(flow_any_positive_by_day, mainline_allow_by_day)`` where:

    - ``flow_any_positive[day]`` — True when at least one SW L1 industry has
      positive net inflow that day. Mirrors the live ``sectorOutflowBlock``
      rule (all industries <= 0 blocks new entries).
    - ``mainline_allow[day]`` — industries allowed to be bought: 5D net
      inflow Top3 ∪ momentum-breakout industries (today net inflow >= 20亿
      and rank improved >= 10 vs yesterday). Mirrors the live mainline
      whitelist (hot-industry-picks.ts buildMainlineAllowSet).
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
        top = top_by_date_from_rows(rows, lookback, top_k=3)
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
        if allow or day_flows:
            mainline_allow[day] = allow
    return flow_any_positive, mainline_allow


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
        if data.regime_by_day.get(day) != REGIME_STRONG:
            return GATE_REASON_REGIME
    if config.gates == "full":
        if not data.flow_any_positive_by_day.get(day):
            return GATE_REASON_FLOW
        allow = data.mainline_allow_by_day.get(day)
        ind = data.industry_by_ts.get(ts)
        if allow is None or ind is None or ind not in allow:
            return GATE_REASON_MAINLINE
    return None


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
    gated_blocks: dict[str, int] = defaultdict(int)

    threshold = config.score_threshold
    costs_pct = round_trip_cost_pct(config.market) * 100.0

    def entry_price_for(ts: str, day: str) -> float | None:
        closes = data.close_by_ts_day.get(ts)
        return closes.get(day) if closes else None

    for day in data.calendar:
        day_scores = data.scores_by_day.get(day, {})

        # 1) Entries: score >= threshold, gates passed, not already held,
        #    price available.
        for sym, score in day_scores.items():
            if score < threshold:
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
        summary=_summarize(config, data, closed_trades, open_at_end, gated_blocks),
        trades=closed_trades,
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
        by_score_bucket=buckets,
        gated_blocks=dict(gated_blocks or {}),
    )


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
