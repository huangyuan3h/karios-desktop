"""TIP-017 risk-state gates — breadth gate (A) + national-team gate (B).

Window computation for the pre-registered risk-state experiments
(docs/backtests/risk-state-sensors-2026-09-09.md). Windows are DETERMINISTIC
from price/flow data (no run feedback, unlike the TIP-016 A NAV watermark) —
computed once per line calendar and fed to the engine via the generic
``throttle_windows``/``throttle_scale=0`` entry-block mechanism (same path as
the TIP-016 experiments; the knob name is historical, semantics here =
"risk-off: no new entries in window").

Causality: the state for trading day ``d`` uses data STRICTLY BEFORE ``d``
(entries happen at open; the freshest known close is d-1). Missing series
history counts as below-MA / no-signal → fail-closed for triggers, and
blocks price-release (conservative).

Truth doc: the pre-registration; §10 of product-post-peak-drift-2026-09-09
for the release-dynamics lesson (release design is a first-class grid dim).
"""

from __future__ import annotations

from datetime import date, timedelta

from data_sync_service.db import get_connection

# Candidate B (national-team) fixed pool + lookback (pre-reg §2/§6.3).
BROAD_ETF_CODES: tuple[str, ...] = ("510300.SH", "510500.SH", "510510.SH", "159915.SZ")
NATIONAL_TEAM_LOOKBACK_DAYS = 400

BREADTH_SERIES: dict[str, tuple[str, str]] = {
    # asset: (table, ts_code) — index_daily / daily / global_index_daily
    "CN": ("index_daily", "000300.SH"),
    "HK": ("global_index_daily", "HSI"),
    "GOLD": ("daily", "518880.SH"),
    "OIL": ("daily", "513350.SH"),
    "NASDAQ": ("daily", "513110.SH"),
    "BOND10": ("daily", "511260.SH"),
}
PRICE_RELEASE_SERIES: tuple[str, ...] = ("CN", "HK")


def _load_close_series(table: str, ts_code: str, start: str, end: str) -> list[tuple[str, float]]:
    sql = {
        "index_daily": "SELECT trade_date, close FROM index_daily WHERE ts_code = %s AND trade_date BETWEEN %s AND %s ORDER BY trade_date",
        "daily": "SELECT trade_date, close FROM daily WHERE ts_code = %s AND trade_date BETWEEN %s AND %s ORDER BY trade_date",
        "global_index_daily": "SELECT trade_date, close FROM global_index_daily WHERE ts_code = %s AND trade_date BETWEEN %s AND %s ORDER BY trade_date",
    }[table]
    out: list[tuple[str, float]] = []
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (ts_code, start, end))
            for d, c in cur.fetchall():
                if c is not None:
                    out.append((str(d), float(c)))
    return out


def load_etf_share_series(
    codes: list[str], start: str, end: str
) -> dict[str, list[tuple[str, float]]]:
    out: dict[str, list[tuple[str, float]]] = {c: [] for c in codes}
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT trade_date, ts_code, fd_share FROM cn_etf_share "
                "WHERE ts_code = ANY(%s) AND trade_date BETWEEN %s AND %s ORDER BY trade_date",
                (codes, start, end),
            )
            for d, ts, s in cur.fetchall():
                if s is not None:
                    out[str(ts)].append((str(d), float(s)))
    return out


def load_close_series(table: str, ts_code: str, start: str, end: str) -> list[tuple[str, float]]:
    """Public wrapper over the internal close-series loader (cross-module use)."""
    return _load_close_series(table, ts_code, start, end)


def _above_flags(series: list[tuple[str, float]], calendar: list[str], ma: int) -> list[bool]:
    """Per master-calendar day: latest close ≤ d vs its MA(ma).

    Uses ALL series data points ≤ d — including pre-window lookback closes —
    so the MA is warm at window start (a regression here once made every
    window's first ~200 sessions fail-closed and manufactured year-long
    artificial risk-off windows). Insufficient history → False.
    """
    flags: list[bool] = []
    closes: list[float] = []
    ordered = sorted(series)
    pos = 0
    n = len(ordered)
    for d in calendar:
        while pos < n and ordered[pos][0] <= d:
            closes.append(ordered[pos][1])
            pos += 1
        if len(closes) < ma:
            flags.append(False)
        else:
            flags.append(closes[-1] > sum(closes[-ma:]) / ma)
    return flags


def breadth_state_by_day(
    series: dict[str, list[tuple[str, float]]],
    calendar: list[str],
    *,
    k: int,
    release: str,
    ma_window: int = 200,
    price_ma: int = 20,
    cooldown_days: int = 20,
) -> dict[str, bool]:
    """Risk-off state per master-calendar day (causal: state(d) uses data ≤ d-1).

    ``series``: BREADTH_SERIES-shaped close series. ``release``: "price"
    (either CN or HK equity index above its MA20), "breadth" (≥ k above
    MA200), "cooldown" (auto-release after cooldown_days sessions).
    """
    above: dict[str, list[bool]] = {
        name: _above_flags(series, calendar, ma_window) for name, series in series.items()
    }
    price_above: dict[str, list[bool]] = {
        name: _above_flags(series[name], calendar, price_ma)
        for name in PRICE_RELEASE_SERIES
        if name in series
    }
    state: dict[str, bool] = {}
    on = False
    triggered_at = -1
    # Data-integrity refinement (pre-reg §1 addendum): a series only counts
    # once it has ma_window real closes (OIL 513350 lists 2023-11). With more
    # than one series missing its MA warmup, the state is HELD (no trigger, no
    # release) — fail-closed here would manufacture a multi-year artificial
    # risk-off from a data vacuum instead of testing the mechanism.
    warmup: dict[str, str | None] = {
        name: (s[ma_window - 1][0] if len(s) >= ma_window else None) for name, s in series.items()
    }
    for i, d in enumerate(calendar):
        # state for day d is decided by data through calendar[i-1] (causal).
        idx = i - 1
        if idx < 0:
            state[d] = False
            continue
        avail = sum(1 for wd in warmup.values() if wd is not None and wd <= calendar[idx])
        if avail < len(series) - 1:
            state[d] = on
            continue
        breadth = sum(1 for flags in above.values() if flags[idx])
        if not on:
            if breadth < k:
                on = True
                triggered_at = i
        else:
            if release == "price":
                if any(flags[idx] for flags in price_above.values()):
                    on = False
            elif release == "breadth":
                if breadth >= k:
                    on = False
            else:  # cooldown
                if i - triggered_at >= cooldown_days:
                    on = False
        state[d] = on
    return state


def state_runs(state: dict[str, bool], calendar: list[str]) -> tuple[tuple[str, str], ...]:
    """Inclusive (first ON day, last ON day) windows — same endpoint semantics
    as backtest_engine.compute_throttle_windows (end = last day state held)."""
    windows: list[tuple[str, str]] = []
    start: str | None = None
    prev: str | None = None
    for d in calendar:
        if state.get(d):
            if start is None:
                start = d
        elif start is not None:
            windows.append((start, prev))
            start = None
        prev = d
    if start is not None and prev is not None:
        windows.append((start, prev))
    return tuple(windows)


def national_team_state_by_day(
    index_series: list[tuple[str, float]],
    share_series: dict[str, list[tuple[str, float]]],
    calendar: list[str],
    *,
    n: int = 20,
    ma_window: int = 200,
) -> dict[str, bool]:
    """Candidate B (pre-registered, single variant): CN index below MA200 AND
    broad-ETF 20-session share delta ≤ 0 → risk-off. Release: index back
    above MA200 OR share delta > 0. Causal (data ≤ d-1). Insufficient share
    history → state OFF (cannot evaluate the flow leg)."""
    idx_above = _above_flags(index_series, calendar, ma_window)
    # Per-code forward-fill FIRST, then sum — a day where one ETF's share row
    # is missing must not fake a total shrink (pre-reg pool = 4 fixed codes).
    per_code = {c: dict(s) for c, s in share_series.items()}
    all_share_days = sorted({d for s in per_code.values() for d in s})
    last_by_code: dict[str, float] = {}
    totals: dict[str, float] = {}
    for d in all_share_days:
        for c, s in per_code.items():
            if d in s:
                last_by_code[c] = s[d]
        if last_by_code:
            totals[d] = sum(last_by_code.values())
    total_days = sorted(totals)
    state: dict[str, bool] = {}
    on = False
    for i, d in enumerate(calendar):
        idx = i - 1
        if idx < 0:
            state[d] = False
            continue
        pd = calendar[idx]
        past = [x for x in total_days if x <= pd]
        if len(past) < n + 1:
            delta_ok = None  # cannot evaluate
        else:
            cur_total = totals[past[-1]]
            old_total = totals[past[-(n + 1)]]
            delta_ok = cur_total - old_total <= 0
        if not on:
            if not idx_above[idx] and delta_ok is True:
                on = True
        else:
            if idx_above[idx] or delta_ok is False:
                on = False
        state[d] = on
    return state


def national_team_state_for_calendar(
    calendar: list[str], *, lookback_days: int = NATIONAL_TEAM_LOOKBACK_DAYS
) -> dict[str, bool]:
    """CN national-team gate state per calendar day (frozen TIP-017 B).

    Loads 沪深300 closes + the 4 broad-ETF share series with a price/share
    lookback before ``calendar[0]`` so MA200 and the 20-session delta are warm
    at window start (the cold-start bug fixed in ``_above_flags``).
    """
    if not calendar:
        return {}
    start = (date.fromisoformat(calendar[0]) - timedelta(days=lookback_days)).isoformat()
    end = calendar[-1]
    index_series = _load_close_series("index_daily", "000300.SH", start, end)
    share_series = load_etf_share_series(list(BROAD_ETF_CODES), start, end)
    return national_team_state_by_day(index_series, share_series, calendar)


def national_team_state_on(as_of: str, *, lookback_days: int = NATIONAL_TEAM_LOOKBACK_DAYS) -> bool:
    """Live single-day check (causal: state uses data ≤ as_of-1). Fail-open
    (returns False) on missing data — the gate only ever pauses on a definite
    ON state. Uses the recent CN index trading days as the calendar so
    ``as_of`` is never the first entry."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT trade_date FROM index_daily WHERE ts_code = '000300.SH' "
                "AND trade_date <= %s ORDER BY trade_date DESC LIMIT 5",
                (as_of,),
            )
            days = sorted(str(r[0]) for r in cur.fetchall())
    if not days:
        return False
    state = national_team_state_for_calendar(days, lookback_days=lookback_days)
    return bool(state.get(days[-1]))
