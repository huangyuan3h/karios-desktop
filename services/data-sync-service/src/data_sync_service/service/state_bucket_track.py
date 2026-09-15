"""双子星 (Twin-Star) 卫星腿 — S-gap State-Bucket engine (service layer).

Two calibers share this engine (2026-09-11 统一后):
  frozen (next_open): 信号 T-1 收盘 → T 开盘成交 → body=3 收盘出。S-gap 研究基线，
    Timeline `strategy=state_bucket` / `recipe="frozen"` 仍用它。
  habit (same_1430): 信号/成交在 T 日 14:30 print，C1 3%，body=3 第 3 日 14:30 出，
    排序用 14:30 可得振幅（`rank_key="amp_1430"`，零前视）。**这是 Live 双子星定义**，
    Live / 审计 / paper / 产品展示（starship/starport，`recipe="habit"`）全部跑它。

  state   = S-gap (gap>3%)
  factor  = amplitude 升序取前 1/3 (bucket_q=3, 最低波33%)
  gate    = R-wide (close>MA20 占比>0.5, 当日截面)
  hold    = 3 交易日
  slots   = 4 x POSITION_PCT 0.25 (sat sleeve ~100%; 12.5% of NAV at 50/50)

Truth doc: docs/backtests/state-bucket-algo-2026-08-31.md §7
Clock unification: docs/backtests/sat/sat-clock-unify-1430-2026-09-11.md
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from typing import Any

import numpy as np
import psycopg

from data_sync_service.config import get_settings
from data_sync_service.service.paper_cost_model import round_trip_cost_pct

POSITION_PCT = 0.25
# Single source for live + backtest (OPT-172): the satellite round-trip cost is
# the CN paper-cost model (30bps), never a local copy that can drift.
COSTS_ROUNDTRIP = round_trip_cost_pct("CN")
BUCKET_Q = 3
MAX_POS = 4
BODY = 3
R_WIDE_THRESHOLD = 0.5
MIN_GAP_PCT = 0.03
WARMUP_CAL_DAYS = 120
FILL_NEXT_OPEN = "next_open"
FILL_SAME_CLOSE = "same_close"
FILL_SAME_1430 = "same_1430"
VALID_FILL_MODES = (FILL_NEXT_OPEN, FILL_SAME_CLOSE, FILL_SAME_1430)
SAME_DAY_FILL_MODES = (FILL_SAME_CLOSE, FILL_SAME_1430)
HABIT_FILL_TIMES = ("1000", "1330", "1400", "1430", "1440", "1450", "1500")

# Two frozen recipes for the satellite replay. Timeline/product callers pass the
# recipe explicitly; research scripts keep their explicit kwargs.
FROZEN_RECIPE: dict[str, Any] = {
    "skip_t1_limit": True,
    "pool_mode": "strict",
}
# Live/habit caliber (OPT-182/183 clean): 14:30 fill+exit, amp_1430 rank, C1 3%,
# gate_1430. This is the definition the frozen standalone/twin-star numbers use.
HABIT_RECIPE: dict[str, Any] = {
    "skip_t1_limit": True,
    "pool_mode": "strict",
    "max_pos": MAX_POS,
    "position_pct": POSITION_PCT,
    "body": BODY,
    "fill_mode": FILL_SAME_1430,
    "fill_hhmm": "1430",
    "exit_hhmm": "1430",
    "max_open_to_1430_pct": 0.03,
    "rank_key": "amp_1430",
    "gate_1430": True,
}


def _load_rows(start: str, end: str) -> dict[str, list[dict[str, Any]]]:
    """Load daily OHLCV rows per ts_code for [start, end] (+ no extra warmup needed:
    features only need ~20 rows; caller adds warmup by extending `start`).

    Universe = full A-share, excluding ST / BJ / delisted (docs/backtests/
    state-bucket-algo-2026-08-31.md §2). The raw daily table contains ~570k
    rows outside this universe (BJ 30% limit, ST 5% limit) which used to leak
    into S-gap candidates and distorted the backtest (fixed 2026-08-31).
    """
    s = get_settings()
    conn = psycopg.connect(s.database_url)
    cur = conn.cursor()
    cur.execute(
        "SELECT d.trade_date, d.ts_code, d.open, d.high, d.low, d.close, d.pre_close, d.amount "
        "FROM daily d JOIN stock_basic sb ON sb.ts_code = d.ts_code "
        "WHERE d.trade_date >= %s AND d.trade_date <= %s "
        "AND sb.delist_date IS NULL "
        "AND sb.name NOT LIKE '%%ST%%' "
        "AND d.ts_code NOT LIKE '%%.BJ' "
        "ORDER BY d.ts_code, d.trade_date",
        (start, end),
    )
    per_ts: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for d, ts, o, h, low, c, pc, amt in cur.fetchall():
        ds = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
        per_ts[str(ts)].append(
            {
                "date": ds,
                "open": float(o) if o is not None else None,
                "high": float(h) if h is not None else None,
                "low": float(low) if low is not None else None,
                "close": float(c) if c is not None else None,
                "pre_close": float(pc) if pc is not None else None,
                "amount": float(amt) if amt is not None else None,
            }
        )
    conn.close()
    return per_ts


def _load_mv(start: str, end: str) -> dict[str, dict[str, float]]:
    s = get_settings()
    conn = psycopg.connect(s.database_url)
    cur = conn.cursor()
    cur.execute(
        "SELECT trade_date, ts_code, total_mv FROM stock_dailybasic "
        "WHERE trade_date >= %s AND trade_date <= %s AND total_mv IS NOT NULL",
        (start, end),
    )
    out: dict[str, dict[str, float]] = defaultdict(dict)
    for d, ts, mv in cur.fetchall():
        ds = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
        out[ds][str(ts)] = float(mv) / 10000.0
    conn.close()
    return out


def _load_calendar(start: str, end: str) -> list[str]:
    """CN trading dates from ``daily`` (OPT-183: exclude the HK rows).

    ``daily`` stores CN and HK rows in one table, so an unfiltered distinct
    returns the UNION of both calendars. HK-only dates are CN holidays: the
    replay would then treat them as sessions and force-close CN positions
    with no mark (PnL zeroed). CN-only is the correct satellite calendar.
    """
    s = get_settings()
    conn = psycopg.connect(s.database_url)
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT trade_date FROM daily "
        "WHERE trade_date >= %s AND trade_date <= %s AND ts_code NOT LIKE '%%.HK' "
        "ORDER BY trade_date",
        (start, end),
    )
    cal = [
        r[0].strftime("%Y-%m-%d") if hasattr(r[0], "strftime") else str(r[0])
        for r in cur.fetchall()
    ]
    conn.close()
    return cal


def _day_features(
    per_ts: dict[str, list[dict[str, Any]]],
    mv_map: dict[str, dict[str, float]],
    cal: list[str],
    day: str,
    date_idx: dict[str, dict[str, int]],
) -> tuple[dict[str, dict[str, float]], float]:
    """Per-stock day features + market breadth (mirror of scout _day_state_fv)."""
    day_all: dict[str, dict[str, Any]] = {}
    for ts, series in per_ts.items():
        idx = date_idx.get(ts, {}).get(day, -1)
        if idx < 0 or idx < 20:
            continue
        mv = mv_map.get(day, {}).get(ts)
        if mv is None:
            continue
        cur = series[idx]
        if not cur["close"] or not cur["high"] or not cur["low"] or cur["close"] <= 0:
            continue
        amp = (cur["high"] - cur["low"]) / cur["close"]
        amts = [r["amount"] for r in series[idx - 20 : idx + 1] if r["amount"]]
        if len(amts) < 15:
            continue
        avg20 = sum(amts[:-1]) / max(len(amts) - 1, 1) if len(amts) > 1 else amts[0]
        turn = (cur["amount"] / avg20) if avg20 and avg20 > 0 else np.nan
        pc = series[idx - 1]["close"] if idx > 0 else None
        gap = (cur["open"] / pc - 1) if cur["open"] and pc and pc > 0 else np.nan
        day_all[ts] = {
            "amp": amp,
            "turn": turn,
            "gap": gap,
            "is_gap": bool(gap == gap and gap > MIN_GAP_PCT),
        }
    breadth = 0.0
    tot = 0
    above = 0
    for ts, series in per_ts.items():
        idx = date_idx.get(ts, {}).get(day, -1)
        if idx < 20 or ts not in mv_map.get(day, {}):
            continue
        closes = [r["close"] for r in series[idx - 19 : idx + 1] if r["close"]]
        if len(closes) < 20:
            continue
        tot += 1
        if series[idx]["close"] > sum(closes) / 20:
            above += 1
    if tot:
        breadth = above / tot
    return day_all, breadth


def _t1_limit_locked(
    per_ts: dict[str, list[dict[str, Any]]],
    date_idx: dict[str, dict[str, int]],
    prev_day: str,
    ts: str,
) -> bool:
    """True when ts closed at the price limit on prev_day (likely unfillable at T open).

    T-1 limit-up close usually gaps to one-word / limit-open next session;
    the backtest filling at T open would overstate returns. Executable at
    signal time (t-1 close), so it is the practical filter.
    """
    di = date_idx.get(ts, {}).get(prev_day, -1)
    if di < 0:
        return False
    r = per_ts.get(ts, [])[di]
    pc = r.get("pre_close")
    if not pc or pc <= 0:
        return False
    lim = 0.20 if str(ts).startswith(("3", "68")) else 0.10
    return float(r["close"]) >= pc * (1 + lim - 0.004)


def _limit_locked_px(px: float | None, pre_close: float | None, ts: str) -> bool:
    if not px or not pre_close or pre_close <= 0:
        return False
    lim = 0.20 if str(ts).startswith(("3", "68")) else 0.10
    return float(px) >= float(pre_close) * (1 + lim - 0.004)


def _board_limit_pct(ts: str) -> float:
    return 0.20 if str(ts).startswith(("3", "68")) else 0.10


def _same_1430_skip_reason(
    *,
    ts: str,
    px: float | None,
    open_px: float | None,
    pre_close: float | None,
    skip_t1_limit: bool,
    max_open_to_1430_pct: float | None,
    near_limit_buffer_pct: float | None,
    min_open_to_1430_pct: float | None = None,
    t1_turn: float | None = None,
    max_t1_turnover_mult: float | None = None,
) -> str | None:
    """Why this name should not fill at 14:30. None = still eligible.

    skip_t1_limit uses the frozen 40bp-under-limit lock. C1 skips names that
    already ran too far from today's open (pulse spent). C3 skips names that
    already faded too far below today's open (pulse disproved). C2 skips names
    already hugging the board limit (unfillable / buying the high). CHURN
    skips names whose T-1 session printed >X the trailing average amount
    (yesterday's distribution top; fully known at fill, no lookahead).
    Strict pool does not refill from worse ranks; idle notional stays core.
    """
    if skip_t1_limit and _limit_locked_px(px, pre_close, ts):
        return "skip_t1_limit"
    if px is None or px <= 0:
        return None
    if max_open_to_1430_pct is not None and open_px and open_px > 0:
        if (float(px) / float(open_px) - 1.0) > max_open_to_1430_pct:
            return "skip_1430_run"
    if min_open_to_1430_pct is not None and open_px and open_px > 0:
        if (float(px) / float(open_px) - 1.0) < -min_open_to_1430_pct:
            return "skip_1430_fade"
    if max_t1_turnover_mult is not None and t1_turn is not None and t1_turn == t1_turn:
        if t1_turn > max_t1_turnover_mult:
            return "skip_1430_churn"
    if near_limit_buffer_pct is not None and pre_close and pre_close > 0:
        lim = _board_limit_pct(ts)
        if float(px) >= float(pre_close) * (1.0 + lim - near_limit_buffer_pct):
            return "skip_1430_near_limit"
    return None


def _skip_kind_for_reason(reason: str) -> str:
    if reason == "skip_t1_limit":
        return "skip_t1"
    if reason == "skip_1430_run":
        return "skip_c1"
    if reason == "skip_1430_fade":
        return "skip_c3"
    if reason == "skip_1430_churn":
        return "skip_churn"
    if reason == "skip_1430_near_limit":
        return "skip_c2"
    return "skip_entry"


def _load_bar5_closes(
    start: str, end: str, times: tuple[str, ...] = ("1430",)
) -> dict[str, dict[str, dict[str, float]]]:
    """{hhmm: {ts_code: {date: close}}} from bar_5min. Empty if table missing."""
    if not times:
        return {}
    s = get_settings()
    out: dict[str, dict[str, dict[str, float]]] = {t: defaultdict(dict) for t in times}
    try:
        conn = psycopg.connect(s.database_url)
        cur = conn.cursor()
        cur.execute(
            "SELECT ts_code, trade_date, trade_time, close FROM bar_5min "
            "WHERE trade_time = ANY(%s) AND trade_date >= %s AND trade_date <= %s "
            "AND close IS NOT NULL AND close > 0",
            (list(times), start, end),
        )
        for ts, d, hhmm, c in cur.fetchall():
            key = str(hhmm)
            if key not in out:
                continue
            ds = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
            out[key][str(ts)][ds] = float(c)
        conn.close()
    except Exception:
        return {t: {} for t in times}
    return {t: dict(m) for t, m in out.items()}


def _load_1430_closes(start: str, end: str) -> dict[str, dict[str, float]]:
    """Last-hour 14:30 close per ts_code/day. Kept for tests/callers."""
    return _load_bar5_closes(start, end, ("1430",)).get("1430") or {}


def _load_bar5_hl(start: str, end: str) -> dict[str, dict[str, tuple[float, float]]]:
    """{ts_code: {date: (max_high, min_low)}} over bars with trade_time <= '1430'.

    The 14:30-knowable amplitude input (H-SAT-RANK honest live proxy). Empty if
    the table is missing or coverage is absent.
    """
    out: dict[str, dict[str, tuple[float, float]]] = defaultdict(dict)
    try:
        s = get_settings()
        conn = psycopg.connect(s.database_url)
        cur = conn.cursor()
        cur.execute(
            "SELECT ts_code, trade_date, max(high), min(low) FROM bar_5min "
            "WHERE trade_time <= '1430' AND trade_date >= %s AND trade_date <= %s "
            "AND high IS NOT NULL AND low IS NOT NULL "
            "GROUP BY ts_code, trade_date",
            (start, end),
        )
        for ts, d, h, low in cur.fetchall():
            ds = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
            out[str(ts)][ds] = (float(h), float(low))
        conn.close()
    except Exception:
        return {}
    return {t: dict(m) for t, m in out.items()}


def _intraday_px(ctx: dict[str, Any], ts: str, day: str, hhmm: str) -> float | None:
    by = ctx.get("px_by_hhmm") or {}
    px = (by.get(hhmm) or {}).get(ts, {}).get(day)
    if px is not None:
        return float(px)
    if hhmm == "1430":
        px = (ctx.get("px_1430") or {}).get(ts, {}).get(day)
        return None if px is None else float(px)
    return None


def _breadth_at_1430(ctx: dict[str, Any], day: str) -> float | None:
    """Market breadth as knowable at the 14:30 decision (H-SAT-1430 gate).

    Same formula as ``_day_features``' breadth (last price > MA20) with the
    decision-day input switched from the 15:00 close to the 14:30 print — the
    Live habit gate read the 14:20 snapshot, so the backtest proxy must not use
    the close. None when the 14:30 panel is unavailable (caller falls back).

    Basis note: the 14:30 panel is RAW (`bar_5min`), while ``daily`` was reseeded
    to qfq — the prior closes must therefore come from the raw 15:00 marks too
    (fallback to the qfq close only when a raw mark is missing).
    """
    px1430 = ctx.get("px_1430") or {}
    raw_close = (ctx.get("px_by_hhmm") or {}).get("1500") or {}
    if not px1430:
        return None
    per_ts = ctx["per_ts"]
    mv_map = ctx.get("mv_map") or {}
    date_idx = ctx["date_idx"]
    above = 0
    tot = 0
    for ts, series in per_ts.items():
        idx = date_idx.get(ts, {}).get(day, -1)
        if idx < 20 or ts not in (mv_map.get(day) or {}):
            continue
        px = (px1430.get(ts) or {}).get(day)
        if not px or px <= 0:
            continue
        prior: list[float] = []
        for r in series[idx - 19 : idx]:
            p = (raw_close.get(ts) or {}).get(str(r.get("date")))
            if p is None:
                p = r.get("close")
            if p:
                prior.append(float(p))
        if len(prior) < 19:
            continue
        tot += 1
        ma20 = (sum(prior) + float(px)) / 20.0
        if float(px) > ma20:
            above += 1
    return (above / tot) if tot else None


def _stage_labels_at(ctx: dict[str, Any], ts: str, day: str, hhmm: str) -> dict[str, str] | None:
    """Zero-lookahead stage_1430 labels at an entry/decision day.

    Prior-session closes plus the decision-day ``hhmm`` print (habit 14:30) — the
    same definition as the H-SAT-RANK ``stage_1430`` rank key. None when history
    < 61 sessions.
    """
    di = ctx["date_idx"].get(ts, {}).get(day, -1)
    series = ctx["per_ts"].get(ts)
    closes: list[float] = []
    if series and di >= 0:
        closes = [float(r["close"]) for r in series[:di] if r.get("close")]
    px = _intraday_px(ctx, ts, day, hhmm)
    if px and px > 0:
        closes.append(float(px))
    return stage_labels(closes) if len(closes) >= 61 else None


def _d3_trail_px(ctx: dict[str, Any], ts: str, day: str, pct: float) -> float | None:
    """Exit-day conditional-order trail (experiment-only, D-series).

    Walk the exit-day 5-minute bars (open → 14:30, ascending) from
    ctx["d3trail_series"][(ts, day)] as (hhmm, open, high, low, close).
    Return peak*(1-pct) at the first bar whose low prints pct below the
    running high (fill = trigger level, no extra slippage — documented
    assumption). None = never triggered or series missing → caller falls
    back to exit_hhmm/close.
    """
    series = (ctx.get("d3trail_series") or {}).get((ts, day))
    if not series:
        return None
    trig = 1.0 - float(pct)
    peak = 0.0
    for _hhmm, _o, hi, lo, _c in series:
        if hi and hi > peak:
            peak = float(hi)
        if peak > 0 and lo and float(lo) <= peak * trig:
            return peak * trig
    return None


def load_sgap_context(start: str, end: str) -> dict[str, Any]:
    """Load OHLCV/MV/calendar once; replays with different pool modes reuse this."""
    w_start = (date.fromisoformat(start) - timedelta(days=WARMUP_CAL_DAYS)).isoformat()
    per_ts = _load_rows(w_start, end)
    mv_map = _load_mv(w_start, end)
    cal = _load_calendar(w_start, end)
    date_idx = {ts: {r["date"]: i for i, r in enumerate(series)} for ts, series in per_ts.items()}
    cal_set = set(cal)
    close_by_ts: dict[str, dict[str, float]] = {}
    for ts, series in per_ts.items():
        m = {r["date"]: r["close"] for r in series if r["date"] in cal_set and r["close"]}
        if m:
            close_by_ts[ts] = m
    px_by_hhmm = _load_bar5_closes(w_start, end, HABIT_FILL_TIMES)
    return {
        "per_ts": per_ts,
        "mv_map": mv_map,
        "cal": cal,
        "date_idx": date_idx,
        "close_by_ts": close_by_ts,
        "idx_by_day": {d: i for i, d in enumerate(cal)},
        "feat_cache": {},
        "px_by_hhmm": px_by_hhmm,
        "px_1430": px_by_hhmm.get("1430") or {},
        "px_hl_1430": _load_bar5_hl(w_start, end),
    }


def _cached_day_features(
    ctx: dict[str, Any], day: str
) -> tuple[dict[str, dict[str, float]], float]:
    cache = ctx["feat_cache"]
    hit = cache.get(day)
    if hit is None:
        hit = _day_features(ctx["per_ts"], ctx["mv_map"], ctx["cal"], day, ctx["date_idx"])
        cache[day] = hit
    return hit


def _entry_pool(
    ranked: list[str],
    qn: int,
    *,
    skip_t1_limit: bool,
    pool_mode: str,
    locked: set[str],
) -> list[str]:
    """strict: top-qn then skip locked (slots may go idle).
    replace: top-qn of fillable names (same count, next-best low-amp).
    fallback: all fillable (quality dump — research only).
    """
    if not skip_t1_limit:
        return ranked[:qn]
    if pool_mode == "fallback":
        return [ts for ts in ranked if ts not in locked]
    if pool_mode == "replace":
        return [ts for ts in ranked if ts not in locked][:qn]
    return ranked[:qn]


def _audit_blotter_row(
    *,
    kind: str,
    date: str,
    ts: str,
    amp: float | None = None,
    amp_rank: int | None = None,
    skip_t1: bool = False,
    entry_date: str | None = None,
    exit_date: str | None = None,
    exit_due: str | None = None,
    pnl_pct: float | None = None,
    contrib_pct: float | None = None,
    close_reason: str | None = None,
    held_days: int | None = None,
    entry_px_src: str | None = None,
    exit_px_src: str | None = None,
) -> dict[str, Any]:
    """One sat blotter line: a fill/open, or a T-1 limit-up skip in the strict bucket.

    OPT-141: fill/open rows carry the price provenance (entryPxSrc/exitPxSrc)
    so the backtest fill-source distribution is comparable with the paper
    book's entryPxSrc/exitPxSrc counters.
    """
    return {
        "kind": kind,
        "date": date,
        "ts": ts,
        "amp": None if amp is None else round(float(amp) * 100, 2),
        "ampRank": amp_rank,
        "skipT1": bool(skip_t1),
        "entryDate": entry_date,
        "exitDate": exit_date,
        "exitDue": exit_due,
        "pnlPct": None if pnl_pct is None else round(float(pnl_pct), 2),
        "contribPct": None if contrib_pct is None else round(float(contrib_pct), 2),
        "closeReason": close_reason,
        "heldDays": held_days,
        "entryPxSrc": entry_px_src,
        "exitPxSrc": exit_px_src,
    }


def select_strict_gap_candidates(
    items: list[tuple[str, float, float]],
    locked: set[str],
    *,
    bucket_q: int = BUCKET_Q,
    top_n: int | None = None,
) -> list[tuple[str, float, float]]:
    """Live/intraday candidate list matching backtest ``pool_mode=strict``.

    Rank all S-gap names by amplitude, take the top 1/bucket_q, *then* drop
    T-1 limit-locked names. Do not refill from worse ranks (that is ``replace``,
    which lost on past_year vs strict).
    """
    ranked = sorted(items, key=lambda x: x[1])
    if not ranked:
        return []
    qn = max(1, len(ranked) // bucket_q)
    pool = [g for g in ranked[:qn] if g[0] not in locked]
    if top_n is not None:
        return pool[:top_n]
    return pool


def select_live_gap_picks(
    items: list[tuple[str, float, float]],
    locked: set[str],
    *,
    bucket_q: int = BUCKET_Q,
    top_n: int | None = None,
) -> dict[str, list[tuple[str, float, float]]]:
    """Live list: strict primary + limit-up names in the bucket + fillable swaps.

    Backtest stays ``pool_mode=strict`` (no refill). The live card shows the
    dropped limit-up names and the next fillable ranks so the user can swap.
    """
    ranked = sorted(items, key=lambda x: x[1])
    if not ranked:
        return {"primary": [], "blocked": [], "alternates": []}
    qn = max(1, len(ranked) // bucket_q)
    bucket = ranked[:qn]
    n = top_n if top_n is not None else qn
    primary = [g for g in bucket if g[0] not in locked][:n]
    blocked = [g for g in bucket if g[0] in locked][:n]
    taken = {g[0] for g in primary}
    alternates = [g for g in ranked if g[0] not in locked and g[0] not in taken][:n]
    return {"primary": primary, "blocked": blocked, "alternates": alternates}


def sat_exit_decision(
    *,
    held: int,
    body: int,
    close: float | None,
    entry: float,
    peak: float,
    protect_stop_pct: float | None = None,
    trail_after_body_pct: float | None = None,
) -> str | None:
    """Decide whether an S-gap satellite position should close today.

    Frozen live path (both optionals None): force close on the body-th hold day.
    ``protect_stop_pct`` is a hard stop vs cost, checked every day.
    ``trail_after_body_pct`` replaces the body force-close: after ``held >= body``,
    exit when close <= peak * (1 - trail). Protect wins if both fire.
    """
    if protect_stop_pct is not None and close is not None and entry > 0:
        if close <= entry * (1.0 - protect_stop_pct):
            return "protect_stop"
    if trail_after_body_pct is not None:
        if held >= body and close is not None and peak > 0:
            if close <= peak * (1.0 - trail_after_body_pct):
                return "trail_exit"
        return None
    if held >= body:
        return "body_exit"
    return None


def replay_sgap_from_context(
    ctx: dict[str, Any],
    *,
    start: str,
    end: str,
    bucket_q: int = BUCKET_Q,
    max_pos: int = MAX_POS,
    body: int = BODY,
    debug_fills: list[tuple[str, str]] | None = None,
    skip_unfillable: bool = False,
    skip_t1_limit: bool = False,
    limit_fallback: bool = False,
    pool_mode: str | None = None,
    position_pct: float = POSITION_PCT,
    protect_stop_pct: float | None = None,
    trail_after_body_pct: float | None = None,
    fill_mode: str = FILL_NEXT_OPEN,
    fill_hhmm: str = "1430",
    exit_hhmm: str | None = None,
    exit_day_trail_pct: float | None = None,
    max_open_to_1430_pct: float | None = None,
    near_limit_buffer_pct: float | None = None,
    rank_key: str | None = None,
    r_wide: float | None = None,
    gate_1430: bool = False,
    min_open_to_1430_pct: float | None = None,
    max_t1_turnover_mult: float | None = None,
    body_by_stage_tier: dict[int, int] | None = None,
    min_gap_pct: float = MIN_GAP_PCT,
) -> dict[str, Any]:
    """Replay S-gap on a preloaded context. Positions start empty at ``start``.

    fill_mode:
      next_open (frozen baseline): yesterday S-gap → today open.
      same_close (experiment): today's S-gap → today's close.
      same_1430 (LIVE habit clock, since 2026-09-11): today's S-gap → bar_5min
      close at fill_hhmm (default 1430). Live / audit / paper pass the habit
      params below; the frozen next_open callers leave them unset.

    fill_hhmm: 5-minute bar-end time used with same_1430 (habit: "1430").
    exit_hhmm: body-exit print (habit: "1430"). None = daily close (frozen).
    exit_day_trail_pct: experiment-only day-3 conditional-order trail (e.g. 0.02 =
      sell when a 5-minute bar prints 2% below the exit-day running high, else
      fall back to exit_hhmm/close). Series come from ctx["d3trail_series"].
      REJECTED (sat-exit-d3trail); Live keeps None.

    max_open_to_1430_pct: habit C1 filter (skip when 1430/open-1 > 3%), Live=0.03.
    near_limit_buffer_pct: experiment-only C2.
    min_open_to_1430_pct: experiment-only C3 fade (skip when 1430/open-1 < -X).
    max_t1_turnover_mult: experiment-only CHURN filter. Live leaves C2/C3/CHURN None.

    rank_key: S-gap ranking for same_1430. None = full-day amplitude ascending
      (the OLD lookahead key; only frozen/experiment callers use it). The LIVE
      habit key is "amp_1430" = amplitude knowable at 14:30 (max high − min low
      over bar_5min bars <= 14:30, / 14:30 print; zero lookahead). Other
      experiment keys: "gap_asc", "absrunup_asc", "stage", "stage_prev", "stage_1430".
      Names missing the fill print rank last.

    r_wide: experiment-only R-wide breadth gate (H4). None = frozen 0.5.
    gate_1430: same_1430 only — compute the R-wide breadth from the 14:30
      prints (`_breadth_at_1430`) instead of the day's 15:00 closes. The Live
      habit gate read the ~14:20 snapshot, so this is the zero-lookahead
      proxy; default False keeps the frozen engine numbers reproducible.

    body_by_stage_tier: experiment-only conditional hold. Maps a stage_1430 tier
      (0 S2&climax / 1 either / 2 neither / 3 unlabeled) to a hold length; tiers
      absent from the map fall back to ``body``. The tier is computed at entry
      with zero lookahead (previous sessions + entry-day 14:30 print). None =
      frozen uniform body. Research only (diag-sat-hold-days-2026-09-12).

    min_gap_pct: overnight-gap threshold defining the S-gap pool (default
      MIN_GAP_PCT=0.03 = the frozen definition). Research-only knob for the
      structural sensitivity scan (twin-residual-2026-09-12).
    """
    if fill_mode not in VALID_FILL_MODES:
        raise ValueError(f"fill_mode must be one of {VALID_FILL_MODES}, got {fill_mode!r}")
    fill_hhmm = str(fill_hhmm or "1430")
    if fill_mode == FILL_SAME_1430 and (len(fill_hhmm) != 4 or not fill_hhmm.isdigit()):
        raise ValueError(f"fill_hhmm must be HHMM, got {fill_hhmm!r}")
    if exit_hhmm is not None:
        exit_hhmm = str(exit_hhmm)
        if len(exit_hhmm) != 4 or not exit_hhmm.isdigit():
            raise ValueError(f"exit_hhmm must be HHMM, got {exit_hhmm!r}")
    if exit_day_trail_pct is not None:
        if fill_mode != FILL_SAME_1430:
            raise ValueError("exit_day_trail_pct requires fill_mode=same_1430")
        if not 0 < float(exit_day_trail_pct) < 1:
            raise ValueError("exit_day_trail_pct must be in (0, 1)")
    if max_open_to_1430_pct is not None or near_limit_buffer_pct is not None:
        if fill_mode != FILL_SAME_1430:
            raise ValueError("14:30 entry filters require fill_mode=same_1430")
        if max_open_to_1430_pct is not None and max_open_to_1430_pct <= 0:
            raise ValueError("max_open_to_1430_pct must be > 0")
    if min_open_to_1430_pct is not None:
        if fill_mode != FILL_SAME_1430:
            raise ValueError("14:30 entry filters require fill_mode=same_1430")
        if min_open_to_1430_pct <= 0:
            raise ValueError("min_open_to_1430_pct must be > 0")
    if max_t1_turnover_mult is not None:
        if fill_mode != FILL_SAME_1430:
            raise ValueError("14:30 entry filters require fill_mode=same_1430")
        if max_t1_turnover_mult <= 0:
            raise ValueError("max_t1_turnover_mult must be > 0")
        if near_limit_buffer_pct is not None and near_limit_buffer_pct <= 0:
            raise ValueError("near_limit_buffer_pct must be > 0")
    if rank_key is not None and fill_mode != FILL_SAME_1430:
        raise ValueError("rank_key requires fill_mode=same_1430")
    if rank_key is not None and rank_key not in (
        "gap_asc",
        "absrunup_asc",
        "stage",
        "stage_prev",
        "stage_1430",
        "amp_1430",
    ):
        raise ValueError(f"unknown rank_key {rank_key!r}")
    r_wide_threshold = R_WIDE_THRESHOLD if r_wide is None else float(r_wide)
    if not 0.0 < r_wide_threshold < 1.0:
        raise ValueError(f"r_wide must be in (0, 1), got {r_wide!r}")
    if gate_1430 and fill_mode != FILL_SAME_1430:
        raise ValueError("gate_1430 requires fill_mode=same_1430")
    if pool_mode is None:
        pool_mode = "fallback" if limit_fallback else "strict"
    clip = float(position_pct)
    if clip <= 0:
        raise ValueError("position_pct must be > 0")
    per_ts = ctx["per_ts"]
    cal = ctx["cal"]
    date_idx = ctx["date_idx"]
    close_by_ts = ctx["close_by_ts"]
    idx_by_day = ctx["idx_by_day"]
    # `daily` was reseeded to qfq while the 14:30 caliber fills on RAW bar_5min
    # prices; marking raw entries with qfq closes would fabricate P&L. Use the
    # raw 15:00 marks (same basis as the fills) for the same_1430 path.
    raw_marks = (ctx.get("px_by_hhmm") or {}).get("1500") or {}

    def _mark(ts: str, d: str) -> float | None:
        if fill_mode == FILL_SAME_1430:
            v = (raw_marks.get(ts) or {}).get(d)
            if v:
                return float(v)
        return close_by_ts.get(ts, {}).get(d)

    def _raw_ratio(ts: str, d: str) -> float | None:
        """raw / qfq close ratio for the day (None when either side is missing)."""
        v = (raw_marks.get(ts) or {}).get(d)
        q = close_by_ts.get(ts, {}).get(d)
        if v and q and float(q) > 0:
            return float(v) / float(q)
        return None

    positions: dict[str, dict[str, Any]] = {}
    realized = 0.0
    rows: list[dict[str, Any]] = []
    blotter: list[dict[str, Any]] = []
    for day in cal:
        if day < start or day > end:
            continue
        _day_all, breadth = _cached_day_features(ctx, day)
        if gate_1430:
            b1430 = _breadth_at_1430(ctx, day)
            if b1430 is not None:
                breadth = b1430
        r_wide = breadth > r_wide_threshold
        to_close: list[tuple[str, str]] = []
        for ts, p in list(positions.items()):
            ei = idx_by_day.get(p["entry_date"], -1)
            ci = idx_by_day.get(day, -1)
            held = ci - ei + 1 if ei >= 0 and ci >= 0 else 999
            cc = _mark(ts, day)
            entry = float(p.get("entry_price") or 0.0)
            peak = float(p.get("peak") or entry)
            if cc is not None and cc > peak:
                peak = float(cc)
                p["peak"] = peak
            reason = sat_exit_decision(
                held=held,
                body=(
                    int(body_by_stage_tier.get(int(p.get("stage_tier", 3)), body))
                    if body_by_stage_tier is not None
                    else body
                ),
                close=cc,
                entry=entry,
                peak=peak,
                protect_stop_pct=protect_stop_pct,
                trail_after_body_pct=trail_after_body_pct,
            )
            if reason:
                to_close.append((ts, reason))
        closed_today = [ts for ts, _ in to_close]
        for ts, reason in to_close:
            p = positions.pop(ts)
            cc = _mark(ts, day)
            exit_src = "close"
            if reason == "body_exit" and exit_day_trail_pct is not None:
                trail_px = _d3_trail_px(ctx, ts, day, float(exit_day_trail_pct))
                if trail_px is not None:
                    cc = trail_px
                    exit_src = "d3trail"
            if reason == "body_exit" and exit_src == "close" and exit_hhmm:
                intra = _intraday_px(ctx, ts, day, exit_hhmm)
                if intra is not None:
                    cc = intra
                    exit_src = f"bar_{exit_hhmm}"
            trade_ret = (cc / p["entry_price"] - 1) if cc and p["entry_price"] else 0.0
            if cc and p["entry_price"]:
                realized += (trade_ret - COSTS_ROUNDTRIP) * clip
            ei = idx_by_day.get(p["entry_date"], -1)
            ci = idx_by_day.get(day, -1)
            held = ci - ei + 1 if ei >= 0 and ci >= 0 else body
            blotter.append(
                _audit_blotter_row(
                    kind="fill",
                    date=day,
                    ts=ts,
                    amp=p.get("amp"),
                    amp_rank=p.get("amp_rank"),
                    skip_t1=False,
                    entry_date=p["entry_date"],
                    exit_date=day,
                    exit_due=p.get("exit_due"),
                    pnl_pct=trade_ret * 100 if cc and p["entry_price"] else None,
                    contrib_pct=(trade_ret - COSTS_ROUNDTRIP) * clip * 100
                    if cc and p["entry_price"]
                    else 0.0,
                    close_reason=reason,
                    held_days=held,
                    entry_px_src=p.get("entry_px_src"),
                    exit_px_src=exit_src,
                )
            )
        gap_count = 0
        strict_count = 0
        skip_t1_count = 0
        skip_c1_count = 0
        skip_c2_count = 0
        skip_c3_count = 0
        skip_churn_count = 0
        no_print_1430 = 0
        filled_today = 0
        gate_open = False
        if r_wide and day > start and day in idx_by_day and idx_by_day[day] > 0:
            if fill_mode in SAME_DAY_FILL_MODES:
                feat_all = _day_all
                lock_day = day
            else:
                prev_day = cal[idx_by_day[day] - 1]
                feat_all, _ = _cached_day_features(ctx, prev_day)
                lock_day = prev_day
            gap_stocks = [
                ts
                for ts, d in feat_all.items()
                if d.get("gap") is not None and d["gap"] == d["gap"] and d["gap"] > min_gap_pct
            ]
            if rank_key is None:
                ranked = sorted(gap_stocks, key=lambda ts: feat_all[ts]["amp"])
            elif rank_key == "gap_asc":
                ranked = sorted(gap_stocks, key=lambda ts: feat_all[ts].get("gap", 0))
            elif rank_key in ("stage", "stage_prev", "stage_1430"):
                # H-SAT-RANK: keep the amp top-qn bucket, reorder it by lifecycle
                # stage (easy risers first). Label series variants:
                #   stage       decision day's daily close (PASSed replica; lookahead)
                #   stage_prev  stops at the prior session (over-conservative)
                #   stage_1430  prior sessions + today's 14:30 print (what Live did)
                _by_amp = sorted(gap_stocks, key=lambda ts: feat_all[ts]["amp"])
                _qn = max(1, len(_by_amp) // bucket_q) if _by_amp else 0
                _mode = rank_key

                def _stage_key(
                    ts: str, _day: str = day, _m: str = _mode, _feat: dict = feat_all
                ) -> tuple:
                    di = date_idx.get(ts, {}).get(_day, -1)
                    series = per_ts.get(ts)
                    closes: list[float] = []
                    if series and di >= 0:
                        upto = di + 1 if _m == "stage" else di
                        closes = [float(r["close"]) for r in series[:upto] if r.get("close")]
                    if _m == "stage_1430":
                        px = _intraday_px(ctx, ts, _day, fill_hhmm)
                        if px and px > 0:
                            closes.append(float(px))
                    lab = stage_labels(closes) if len(closes) >= 61 else None
                    return (stage_tier(lab), _feat[ts]["amp"])

                ranked = sorted(_by_amp[:_qn], key=_stage_key) + _by_amp[_qn:]
            elif rank_key == "amp_1430":
                # Live-honest ranking: amplitude knowable at the 14:30 decision,
                # built from bar_5min bars with trade_time <= 14:30. Names without
                # a 14:30 print / intraday bars rank last (blind).
                _hl = ctx.get("px_hl_1430") or {}

                def _proxy_amp(ts: str, _day: str = day, _m: dict = _hl) -> tuple:
                    px = _intraday_px(ctx, ts, _day, "1430")
                    hl = (_m.get(ts) or {}).get(_day)
                    if not px or px <= 0 or not hl:
                        return (1, float("inf"))
                    return (0, float(hl[0] - hl[1]) / float(px))

                ranked = sorted(gap_stocks, key=_proxy_amp)
            else:  # absrunup_asc: |fill print / open - 1|, missing print ranks last

                def _abs_runup(ts: str, _day: str = day) -> float:
                    di = date_idx.get(ts, {}).get(_day, -1)
                    series = per_ts.get(ts)
                    bar = series[di] if di >= 0 and series else {}
                    open_px = (bar or {}).get("open") or 0
                    px = _intraday_px(ctx, ts, _day, fill_hhmm)
                    if not open_px or not px:
                        return float("inf")
                    return abs(float(px) / float(open_px) - 1.0)

                ranked = sorted(gap_stocks, key=_abs_runup)
            qn = max(1, len(ranked) // bucket_q) if ranked else 0
            locked_reasons: dict[str, str] = {}
            if fill_mode == FILL_SAME_1430:
                apply_1430_filters = (
                    skip_t1_limit
                    or max_open_to_1430_pct is not None
                    or near_limit_buffer_pct is not None
                    or min_open_to_1430_pct is not None
                    or max_t1_turnover_mult is not None
                )
                if apply_1430_filters:
                    for ts in ranked:
                        di_lock = date_idx.get(ts, {}).get(day, -1)
                        series_l = per_ts.get(ts)
                        bar_l = series_l[di_lock] if di_lock >= 0 and series_l else {}
                        t1_turn = None
                        if di_lock > 20 and series_l:
                            amts = [
                                r.get("amount")
                                for r in series_l[di_lock - 21 : di_lock]
                                if r.get("amount")
                            ]
                            if len(amts) >= 15 and amts[-1]:
                                avg = sum(amts[:-1]) / max(len(amts) - 1, 1)
                                t1_turn = amts[-1] / avg if avg else None
                        # The daily open/pre_close are qfq while px is raw —
                        # scale them to the raw basis (same-day ratio) before
                        # the C1 / limit checks, or the filters mis-fire.
                        _k = _raw_ratio(ts, day)
                        _raw_open = bar_l.get("open") if bar_l else None
                        _raw_pre = bar_l.get("pre_close") if bar_l else None
                        if _k:
                            if _raw_open:
                                _raw_open = float(_raw_open) * _k
                            if _raw_pre:
                                _raw_pre = float(_raw_pre) * _k
                        reason = _same_1430_skip_reason(
                            ts=ts,
                            px=_intraday_px(ctx, ts, day, fill_hhmm),
                            open_px=_raw_open,
                            pre_close=_raw_pre,
                            skip_t1_limit=skip_t1_limit,
                            max_open_to_1430_pct=max_open_to_1430_pct,
                            near_limit_buffer_pct=near_limit_buffer_pct,
                            min_open_to_1430_pct=min_open_to_1430_pct,
                            t1_turn=t1_turn,
                            max_t1_turnover_mult=max_t1_turnover_mult,
                        )
                        if reason:
                            locked_reasons[ts] = reason
            elif skip_t1_limit:
                for ts in ranked:
                    if _t1_limit_locked(per_ts, date_idx, lock_day, ts):
                        locked_reasons[ts] = "skip_t1_limit"
            locked = set(locked_reasons)
            gate_open = True
            gap_count = len(ranked)
            strict_count = qn
            bucket = ranked[:qn]
            skip_t1_count = sum(1 for ts in bucket if locked_reasons.get(ts) == "skip_t1_limit")
            skip_c1_count = sum(1 for ts in bucket if locked_reasons.get(ts) == "skip_1430_run")
            skip_c2_count = sum(
                1 for ts in bucket if locked_reasons.get(ts) == "skip_1430_near_limit"
            )
            skip_c3_count = sum(1 for ts in bucket if locked_reasons.get(ts) == "skip_1430_fade")
            skip_churn_count = sum(
                1 for ts in bucket if locked_reasons.get(ts) == "skip_1430_churn"
            )
            for ts in bucket:
                reason = locked_reasons.get(ts)
                if not reason:
                    continue
                feat = feat_all.get(ts) or {}
                blotter.append(
                    _audit_blotter_row(
                        kind=_skip_kind_for_reason(reason),
                        date=day,
                        ts=ts,
                        amp=feat.get("amp"),
                        amp_rank=ranked.index(ts) + 1,
                        skip_t1=reason == "skip_t1_limit",
                        close_reason=reason,
                        contrib_pct=0.0,
                    )
                )
            pool = _entry_pool(
                ranked, qn, skip_t1_limit=skip_t1_limit, pool_mode=pool_mode, locked=locked
            )
            ei_today = idx_by_day.get(day, -1)
            for ts in pool:
                if ts in positions or len(positions) >= max_pos:
                    continue
                if ts in locked:
                    continue
                series = per_ts.get(ts)
                di = date_idx.get(ts, {}).get(day, -1)
                if di < 0 or not series:
                    continue
                bar = series[di]
                if fill_mode == FILL_SAME_1430:
                    px = _intraday_px(ctx, ts, day, fill_hhmm)
                    entry_src = f"bar_{fill_hhmm}"
                    if not px or px <= 0:
                        # OPT-143: missing intraday print → no fill (conservative).
                        # Counted here (not a blotter row) so coverage gaps are visible.
                        no_print_1430 += 1
                        continue
                elif fill_mode == FILL_SAME_CLOSE:
                    px = bar.get("close")
                    entry_src = "close"
                else:
                    px = bar.get("open")
                    entry_src = "open"
                if px and px > 0:
                    if skip_unfillable or fill_mode == FILL_SAME_1430:
                        pc = bar.get("pre_close")
                        lim = 0.20 if str(ts).startswith(("3", "68")) else 0.10
                        if fill_mode == FILL_SAME_1430:
                            one_word = False
                            # px is raw; scale the qfq pre_close to the raw basis
                            # before the limit-up guard (mixed basis = false skips).
                            _kf = _raw_ratio(ts, day)
                            if _kf and pc:
                                pc = float(pc) * _kf
                        elif fill_mode == FILL_SAME_CLOSE:
                            one_word = bar["high"] == bar["low"] == bar["close"]
                        else:
                            one_word = bar["high"] == bar["low"] == bar["open"]
                        if pc and pc > 0 and (one_word or px >= pc * (1 + lim - 0.004)):
                            continue
                    feat = feat_all.get(ts) or {}
                    tier: int | None = None
                    eff_body = body
                    if body_by_stage_tier is not None:
                        tier = stage_tier(_stage_labels_at(ctx, ts, day, fill_hhmm))
                        eff_body = int(body_by_stage_tier.get(tier, body))
                    exit_due = (
                        cal[ei_today + eff_body - 1]
                        if ei_today >= 0 and ei_today + eff_body - 1 < len(cal)
                        else end
                    )
                    positions[ts] = {
                        "entry_date": day,
                        "entry_price": px,
                        "entry_px_src": entry_src,
                        "peak": px,
                        "amp": feat.get("amp"),
                        "amp_rank": ranked.index(ts) + 1,
                        "exit_due": exit_due,
                        "stage_tier": tier,
                        "body": eff_body,
                    }
                    filled_today += 1
                    if debug_fills is not None:
                        debug_fills.append((day, ts))
        mtm = 0.0
        for ts, p in positions.items():
            cc = _mark(ts, day)
            mtm += clip * (cc / p["entry_price"]) if cc and p["entry_price"] else clip
        nav = 1.0 + realized + (mtm - len(positions) * clip)
        sat_active = len(positions) > 0 or len(closed_today) > 0
        sat_slots = len(positions) + len(closed_today)
        idle_slots = max(0, max_pos - sat_slots)
        cash = 1.0 + realized - len(positions) * clip
        cash_share = min(1.0, max(0.0, cash / nav)) if nav > 0 else 0.0
        rows.append(
            {
                "date": day,
                "satNav": round(nav, 6),
                "satNavReturnPct": round((nav - 1) * 100, 2),
                "cashShare": round(cash_share, 4),
                "satPositions": len(positions),
                "satSlots": sat_slots,
                "satActive": sat_active,
                "gapCount": gap_count,
                "strictCount": strict_count,
                "skipT1Count": skip_t1_count,
                "skipC1Count": skip_c1_count,
                "skipC2Count": skip_c2_count,
                "skipC3Count": skip_c3_count,
                "skipChurnCount": skip_churn_count,
                "skipNoPrint1430": no_print_1430,
                "filledToday": filled_today,
                "idleSlots": idle_slots,
                "satCapacity": max_pos,
                "gateOpen": gate_open,
            }
        )
    last_day = end
    for d in reversed(cal):
        if start <= d <= end:
            last_day = d
            break
    open_positions: list[dict[str, Any]] = []
    for ts, p in positions.items():
        cc = _mark(ts, last_day)
        ei = idx_by_day.get(p["entry_date"], -1)
        ci = idx_by_day.get(last_day, -1)
        held = ci - ei + 1 if ei >= 0 and ci >= 0 else 0
        p_body = int(p.get("body", body))
        days_left = max(0, p_body - held)
        # Prefer the entry-time due date when the window calendar ends before it
        # (entry-time uses the requested `end`, so an in-flight leg shows the real
        # next session instead of being clamped to the last loaded data day).
        exit_due = (
            cal[ei + p_body - 1]
            if ei >= 0 and ei + p_body - 1 < len(cal)
            else p.get("exit_due")
        )
        open_positions.append(
            {
                "ts": ts,
                "entryDate": p["entry_date"],
                "entryPrice": round(float(p["entry_price"]), 4) if p["entry_price"] else None,
                "close": round(float(cc), 4) if cc else None,
                "heldDays": held,
                "daysLeft": days_left,
                "exitDue": exit_due,
                "pnlPct": round((cc / p["entry_price"] - 1) * 100, 2)
                if cc and p["entry_price"]
                else None,
            }
        )
    for ts, p in list(positions.items()):
        cc = _mark(ts, last_day)
        if cc and p["entry_price"]:
            realized += ((cc / p["entry_price"] - 1) - COSTS_ROUNDTRIP) * clip
        ei = idx_by_day.get(p["entry_date"], -1)
        ci = idx_by_day.get(last_day, -1)
        held = ci - ei + 1 if ei >= 0 and ci >= 0 else 0
        trade_ret = (cc / p["entry_price"] - 1) if cc and p["entry_price"] else None
        blotter.append(
            _audit_blotter_row(
                kind="open",
                date=last_day,
                ts=ts,
                amp=p.get("amp"),
                amp_rank=p.get("amp_rank"),
                skip_t1=False,
                entry_date=p["entry_date"],
                exit_due=p.get("exit_due"),
                pnl_pct=None if trade_ret is None else trade_ret * 100,
                contrib_pct=None if trade_ret is None else trade_ret * clip * 100,
                close_reason="open",
                held_days=held,
                entry_px_src=p.get("entry_px_src"),
                exit_px_src=None,
            )
        )
    final_nav = 1.0 + realized
    if rows:
        rows[-1]["satNav"] = round(final_nav, 6)
        rows[-1]["satNavReturnPct"] = round((final_nav - 1) * 100, 2)
    peak = 1.0
    max_dd = 0.0
    for r in rows:
        nav = r["satNav"]
        peak = max(peak, nav)
        if peak > 0:
            max_dd = max(max_dd, (peak - nav) / peak)
    skip_n = sum(1 for b in blotter if b.get("kind") == "skip_t1")
    skip_c1_n = sum(1 for b in blotter if b.get("kind") == "skip_c1")
    skip_c2_n = sum(1 for b in blotter if b.get("kind") == "skip_c2")
    skip_c3_n = sum(1 for b in blotter if b.get("kind") == "skip_c3")
    skip_churn_n = sum(1 for b in blotter if b.get("kind") == "skip_churn")
    fill_n = sum(1 for b in blotter if b.get("kind") == "fill")
    close_reasons: dict[str, int] = {}
    held_days: list[int] = []
    fill_src_entry: dict[str, int] = {}
    fill_src_exit: dict[str, int] = {}
    for b in blotter:
        if b.get("kind") != "fill":
            continue
        reason = str(b.get("closeReason") or "unknown")
        close_reasons[reason] = close_reasons.get(reason, 0) + 1
        hd = b.get("heldDays")
        if hd is not None:
            held_days.append(int(hd))
        es = str(b.get("entryPxSrc") or "unknown")
        fill_src_entry[es] = fill_src_entry.get(es, 0) + 1
        xs = str(b.get("exitPxSrc") or "unknown")
        fill_src_exit[xs] = fill_src_exit.get(xs, 0) + 1
    return {
        "rows": rows,
        "openPositions": open_positions,
        "blotter": blotter,
        "summary": {
            "satPct": round((final_nav - 1) * 100, 2),
            "satMaxDdPct": round(max_dd * 100, 1),
            "skipT1Count": skip_n,
            "skipC1Count": skip_c1_n,
            "skipC2Count": skip_c2_n,
            "skipC3Count": skip_c3_n,
            "skipChurnCount": skip_churn_n,
            "fillCount": fill_n,
            "closeReasons": close_reasons,
            "avgHeldDays": round(sum(held_days) / len(held_days), 2) if held_days else 0.0,
            "fillSrc": {"entry": fill_src_entry, "exit": fill_src_exit},
            "skipNoPrint1430": sum(int(r.get("skipNoPrint1430") or 0) for r in rows),
        },
        "pool_mode": pool_mode,
        "position_pct": clip,
        "max_pos": max_pos,
        "satCapacity": max_pos,
        "fill_mode": fill_mode,
        "fill_hhmm": fill_hhmm if fill_mode == FILL_SAME_1430 else None,
        "exit_hhmm": exit_hhmm,
        "exit_day_trail_pct": exit_day_trail_pct,
        "max_open_to_1430_pct": max_open_to_1430_pct,
        "near_limit_buffer_pct": near_limit_buffer_pct,
        "min_open_to_1430_pct": min_open_to_1430_pct,
        "max_t1_turnover_mult": max_t1_turnover_mult,
        "rank_key": rank_key,
        "r_wide": r_wide_threshold,
    }


def build_sgap_timeline(
    *,
    start: str,
    end: str,
    bucket_q: int = BUCKET_Q,
    max_pos: int = MAX_POS,
    body: int = BODY,
    debug_fills: list[tuple[str, str]] | None = None,
    skip_unfillable: bool = False,
    skip_t1_limit: bool = False,
    limit_fallback: bool = False,
    pool_mode: str | None = None,
    position_pct: float = POSITION_PCT,
    protect_stop_pct: float | None = None,
    trail_after_body_pct: float | None = None,
    fill_mode: str = FILL_NEXT_OPEN,
    fill_hhmm: str = "1430",
    exit_hhmm: str | None = None,
    max_open_to_1430_pct: float | None = None,
    near_limit_buffer_pct: float | None = None,
    rank_key: str | None = None,
    gate_1430: bool = False,
    body_by_stage_tier: dict[int, int] | None = None,
    min_gap_pct: float = MIN_GAP_PCT,
) -> dict[str, Any]:
    """Replay S-gap satellite NAV (daily rows for UI) over [start, end].

    Returns {rows: [{date, satNav, satNavReturnPct, satPositions, satSlots, satActive}],
             openPositions: [...], summary: {...}}.
    skip_t1_limit: drop candidates that closed limit-up on T-1 (executable口径).
    pool_mode: strict | replace | fallback (limit_fallback=True aliases fallback).
    Frozen baseline callers keep fill_mode=next_open and the rest None.
    Live habit callers pass fill_mode="same_1430", fill_hhmm="1430",
    exit_hhmm="1430", max_open_to_1430_pct=0.03, rank_key="amp_1430",
    gate_1430=True.
    protect_stop_pct / trail_after_body_pct remain experiment-only.
    """
    ctx = load_sgap_context(start, end)
    return replay_sgap_from_context(
        ctx,
        start=start,
        end=end,
        bucket_q=bucket_q,
        max_pos=max_pos,
        body=body,
        debug_fills=debug_fills,
        skip_unfillable=skip_unfillable,
        skip_t1_limit=skip_t1_limit,
        limit_fallback=limit_fallback,
        pool_mode=pool_mode,
        position_pct=position_pct,
        protect_stop_pct=protect_stop_pct,
        trail_after_body_pct=trail_after_body_pct,
        fill_mode=fill_mode,
        fill_hhmm=fill_hhmm,
        exit_hhmm=exit_hhmm,
        max_open_to_1430_pct=max_open_to_1430_pct,
        near_limit_buffer_pct=near_limit_buffer_pct,
        rank_key=rank_key,
        gate_1430=gate_1430,
        body_by_stage_tier=body_by_stage_tier,
        min_gap_pct=min_gap_pct,
    )


def sgap_to_timeline_rows(sat: dict[str, Any]) -> dict[str, Any]:
    """Adapt ``build_sgap_timeline`` output to Timeline API shape (standalone leg)."""
    rows: list[dict[str, Any]] = []
    for r in sat.get("rows") or []:
        nav = float(r.get("satNav") or 1.0)
        ret_pct = r.get("satNavReturnPct")
        if ret_pct is None:
            ret_pct = round((nav - 1.0) * 100, 2)
        rows.append(
            {
                "date": r["date"],
                "pick": "S-GAP",
                "pickTs": "",
                "navSingle": nav,
                "navMulti": nav,
                "navSingleReturnPct": ret_pct,
                "navMultiReturnPct": ret_pct,
                "satNav": nav,
                "satNavReturnPct": ret_pct,
                "satPositions": int(r.get("satPositions") or 0),
                "satSlots": int(r.get("satSlots") or r.get("satPositions") or 0),
                "cashShare": r.get("cashShare"),
                "satActive": bool(r.get("satActive")) if "satActive" in r else None,
                "gapCount": r.get("gapCount"),
                "strictCount": r.get("strictCount"),
                "skipT1Count": r.get("skipT1Count"),
                "filledToday": r.get("filledToday"),
                "idleSlots": r.get("idleSlots"),
                "satCapacity": int(r.get("satCapacity") or MAX_POS),
                "gateOpen": r.get("gateOpen"),
            }
        )
    summary = sat.get("summary") or {}
    sat_pct = float(summary.get("satPct") or 0.0)
    sat_dd = float(summary.get("satMaxDdPct") or 0.0)
    return {
        "ok": True,
        "mode": "state_bucket_sgap",
        "strategy": "状态分桶 S-gap (可执行)",
        "rows": rows,
        "satCapacity": int(sat.get("satCapacity") or MAX_POS),
        "summary": {
            "fusedPct": round(sat_pct, 2),
            "corePct": None,
            "basePct": None,
            # engine keeps the drawdown magnitude; Timeline convention is negative
            "maxDdFusedPct": round(-abs(sat_dd), 1),
            "satPct": round(sat_pct, 2),
            "satMaxDdPct": round(sat_dd, 1),
        },
        "openPositions": sat.get("openPositions") or [],
        "blotter": sat.get("blotter") or [],
        "opportunity": False,
        "note": (
            "Standalone S-gap leg (bucket_q=3, 4 slots x 25%, body=3, R-wide). "
            "Executable口径: skip_t1_limit=True "
            "(涨停可能买不进 → 不假设开盘能成交; 机会双子星同口径)."
        ),
    }


def compose_parked_rows(
    rows: list[dict[str, Any]],
    sleeve_ret_by_day: dict[str, float],
    *,
    cost_bps: float = 5.0,
) -> dict[str, Any]:
    """Starship v2 composition (H-SAT-IDLE `A2_true`, user-approved 2026-09-15).

    ``port_ret_t = sat_ret_t + w_t * sleeve_ret_t - cost * |w_t - w_{t-1}|`` with
    ``w_t = cashShare_{t-1}`` (causal, true engine cash share: the frozen engine
    uses fixed 25% clips so idle cash grows as NAV compounds).

    Pure function (no DB) — returns ``{"rows": [{date, parkedNav,
    parkedReturnPct, parkedWeight}], "summary": {...}}``.
    """
    out_rows: list[dict[str, Any]] = []
    if not rows:
        return {"rows": [], "summary": {}}
    nav = 1.0
    prev_w = 0.0
    peak = 1.0
    max_dd = 0.0
    for i, r in enumerate(rows):
        w = 0.0
        if i > 0:
            w = min(1.0, max(0.0, float(rows[i - 1].get("cashShare") or 0.0)))
        r_sat = 0.0
        if i > 0:
            prev_nav = float(rows[i - 1].get("satNav") or 0.0)
            cur_nav = float(r.get("satNav") or 0.0)
            r_sat = cur_nav / prev_nav - 1.0 if prev_nav > 0 else 0.0
        r_sleeve = float(sleeve_ret_by_day.get(str(r["date"]), 0.0))
        cost = cost_bps / 1e4 * abs(w - prev_w)
        nav *= 1.0 + r_sat + w * r_sleeve - cost
        peak = max(peak, nav)
        if peak > 0:
            max_dd = max(max_dd, (peak - nav) / peak)
        out_rows.append(
            {
                "date": r["date"],
                "parkedNav": round(nav, 6),
                "parkedReturnPct": round((nav - 1) * 100, 2),
                "parkedWeight": round(w, 4),
            }
        )
        prev_w = w
    return {
        "rows": out_rows,
        "summary": {
            "parkedPct": round((nav - 1) * 100, 2),
            "parkedMaxDdPct": round(max_dd * 100, 1),
            "avgParkedWeight": round(
                sum(r["parkedWeight"] for r in out_rows) / len(out_rows), 3
            ),
        },
    }


def parked_blotter(
    recs: list[dict[str, Any]],
    closes: dict[str, dict[str, float]],
    *,
    names: dict[str, str] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    """Buy/sell audit trail for the parked sleeve (starship v2).

    Events come from the canonical ``harbor.parking_replay`` records: the engine
    fills at ``prev`` close (the 14:30 caliber's close proxy), so the trade date
    is the record's ``prev``. A trail exit sells to cash (REPO) with no same-day
    re-entry; an alias switch (NASDAQ 513100<->513110) is a sell+buy pair with
    ``reason="rotate"``. Prices are the same close map the replay used.
    """
    names = names or {}
    events: list[dict[str, Any]] = []
    held_key: str | None = None
    held_ts: str | None = None
    since: str | None = None

    def price(ts: str, day: str) -> float | None:
        v = (closes.get(ts) or {}).get(day)
        return round(float(v), 3) if v else None

    def push(day: str, kind: str, key: str | None, ts: str, reason: str) -> None:
        if not ts:
            return
        events.append(
            {
                "date": day,
                "kind": kind,
                "key": key or ts,
                "name": names.get(key or "", key or ts),
                "ts": ts,
                "price": price(ts, day),
                "reason": reason,
            }
        )

    for rec in recs:
        day = str(rec.get("prev") or "")
        want_key = rec.get("want_key")
        want_ts = rec.get("want_ts")
        if rec.get("trail_exit") and held_ts:
            push(day, "sell", held_key, held_ts, "trail")
            held_key = held_ts = since = None
        elif held_ts != want_ts:
            if held_ts:
                push(day, "sell", held_key, held_ts, "rotate" if want_ts else "cash")
            if want_ts:
                push(day, "buy", want_key, want_ts, "rotate" if held_ts else "entry")
            held_key, held_ts = want_key, want_ts
            since = day if want_ts else None
    if not held_ts:
        return events, None
    last_day = str(recs[-1].get("date") or "") if recs else ""
    return events, {
        "key": held_key,
        "name": names.get(held_key or "", held_key or ""),
        "ts": held_ts,
        "since": since,
        "price": price(held_ts, last_day),
    }


def apply_parked_display(out: dict[str, Any], *, cost_bps: float = 5.0) -> dict[str, Any]:
    """Overlay starship v2 (satellite + parked idle cash) onto a Timeline result.

    Display fields (``navSingle*``/``navMulti*``) switch to the parked series;
    ``satNav``/``satNavReturnPct`` stay standalone so the satellite-leg detail
    panel and all blend legs (starport/twin_star) keep the frozen v1 numbers.
    Adds ``parkedBlotter``/``parkedHeld`` so the audit trail records the sleeve's
    ETF buys/sells (e.g. 买黄金), not just the satellite book.
    """
    from data_sync_service.service.harbor import (
        COST,
        NAMES,
        load_etf_closes,
        parking_replay,
    )

    rows = out.get("rows") or []
    if not rows:
        return out
    dates = [str(r["date"]) for r in rows]
    closes = load_etf_closes()
    recs = parking_replay(closes, dates, idle_by_day=None)
    sleeve_ret_by_day = {
        str(rec["date"]): float(rec["parking_ret"]) - COST * int(rec["sides"]) for rec in recs
    }
    parked = compose_parked_rows(rows, sleeve_ret_by_day, cost_bps=cost_bps)
    by_day = {r["date"]: r for r in parked["rows"]}
    for r in rows:
        p = by_day.get(str(r["date"]))
        if not p:
            continue
        r["parkedNav"] = p["parkedNav"]
        r["parkedReturnPct"] = p["parkedReturnPct"]
        r["parkedWeight"] = p["parkedWeight"]
        r["navSingle"] = p["parkedNav"]
        r["navMulti"] = p["parkedNav"]
        r["navSingleReturnPct"] = p["parkedReturnPct"]
        r["navMultiReturnPct"] = p["parkedReturnPct"]
    events, held = parked_blotter(recs, closes, names=NAMES)
    if held is not None:
        held["weight"] = parked["rows"][-1]["parkedWeight"]
    summary = out.setdefault("summary", {})
    summary["parkedPct"] = parked["summary"]["parkedPct"]
    summary["parkedMaxDdPct"] = parked["summary"]["parkedMaxDdPct"]
    summary["parkedAvgWeight"] = parked["summary"]["avgParkedWeight"]
    summary["parkedTrades"] = len(events)
    summary["parkedTrailExits"] = sum(1 for e in events if e["reason"] == "trail")
    summary["fusedPct"] = parked["summary"]["parkedPct"]
    summary["maxDdFusedPct"] = -abs(parked["summary"]["parkedMaxDdPct"])
    out["parkedBlotter"] = events
    out["parkedHeld"] = held
    out["mode"] = "starship_parked"
    out["note"] = (
        "星舰 v2 = 卫星 standalone + 闲置现金停 ETF 停车场（H-SAT-IDLE A2_true，"
        "真实现金权重 causal T-1，5bps/边）。展示/回测口径；Live=港湾，前置=paper 3/20+授权。"
    )
    return out


def build_state_bucket_timeline(
    *, start: str, end: str, recipe: str = "frozen", parked_display: bool = False
) -> dict[str, Any]:
    """Product Timeline entry for the standalone state-bucket S-gap strategy.

    ``recipe="frozen"`` = next_open research baseline (legacy state_bucket view);
    ``recipe="habit"`` = Live 14:30 caliber (starship/starport product views).
    ``parked_display`` = starship v2 overlay (idle cash parked in the ETF sleeve).
    """
    kwargs = HABIT_RECIPE if recipe == "habit" else FROZEN_RECIPE
    sat = build_sgap_timeline(start=start, end=end, **kwargs)
    out = sgap_to_timeline_rows(sat)
    out["start"] = start
    out["end"] = end
    if parked_display:
        out = apply_parked_display(out)
    return out


# --- Lifecycle stage labels (H-SAT-RANK, PASS 2026-09-11) ---------------------
# Single source of truth for stage buckets. Backtest scripts
# (diag_sat_stage, compare_sat_stage/rank) and the live intraday push
# must all use these — never reimplement the buckets elsewhere.


def stage_labels(closes: list[float]) -> dict[str, str] | None:
    """Trailing-only lifecycle labels; last close = entry/decision day.

    Returns None when history < 61 sessions. Buckets:
      dd60: at-high (<2% off 60d high) | shallow (2-8%) | deep (>8%)
      rally20: neg | mid (0-15%) | blasted (>15%)
      blast: fresh<=10d | mid11-40d | old/never (days since trailing 20d +25%)
      wein: S2-advance | S3-distrib | S4-decline | S1-base (P vs MA20/60)
      runup5: cool (<3%) | warm (3-10%) | climax (>10%)
    """
    n = len(closes)
    if n < 61:
        return None
    c = closes[-1]
    h60 = max(closes[-60:])
    dd60 = 1 - c / h60 if h60 else 0.0
    r20 = c / closes[-21] - 1
    r5 = c / closes[-6] - 1
    before = closes[:-1]  # strictly before decision day for blast scan
    blast = None
    for i in range(len(before) - 20, -1, -1):
        seg = before[max(0, i - 19) : i + 1]
        if len(seg) == 20 and seg[-1] / seg[0] - 1 >= 0.25:
            blast = (len(before) - 1) - i
            break
    ma20 = sum(closes[-20:]) / 20
    ma60 = sum(closes[-60:]) / 60
    ma20_prev = sum(closes[-21:-1]) / 20
    slope_up = ma20 >= ma20_prev
    if c > ma20 > ma60 and slope_up:
        wein = "S2-advance"
    elif ma20 > ma60:
        wein = "S3-distrib"
    elif c < ma20 < ma60 and not slope_up:
        wein = "S4-decline"
    else:
        wein = "S1-base"
    return {
        "dd60": "at-high" if dd60 < 0.02 else ("shallow" if dd60 <= 0.08 else "deep"),
        "rally20": "neg" if r20 < 0 else ("mid" if r20 <= 0.15 else "blasted"),
        "blast": "fresh<=10d"
        if blast is not None and blast <= 10
        else ("mid11-40d" if blast is not None and blast <= 40 else "old/never"),
        "wein": wein,
        "runup5": "cool" if r5 < 0.03 else ("warm" if r5 <= 0.10 else "climax"),
    }


def stage_tier(labels: dict[str, str] | None) -> int:
    """H-SAT-RANK order key tier: 0 = S2&climax, 1 = either, 2 = neither.

    Unlabeled (short history) ranks last (3) — same as the validated replay.
    """
    if not labels:
        return 3
    s2 = labels.get("wein") == "S2-advance"
    cx = labels.get("runup5") == "climax"
    return 0 if (s2 and cx) else (1 if (s2 or cx) else 2)


def stage_rank_key(ts: str, labels: dict[str, str] | None, amp: float) -> tuple:
    """Sort key for the primary bucket: easy risers first, amp tiebreak."""
    return (stage_tier(labels), amp if amp == amp else float("inf"))
