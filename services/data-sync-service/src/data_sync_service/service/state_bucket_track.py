"""双子星 (Twin-Star) 卫星腿 — S-gap State-Bucket engine (service layer).

S-gap 单态卫星 (frozen R12 / core_satellite_frozen_2026-08-31.json):
  state   = S-gap (gap>3%)
  factor  = amplitude 升序取前 1/3 (bucket_q=3, 最低波33%)
  gate    = R-wide (close>MA20 占比>0.5, 当日截面)
  entry   = T 日 open (信号取 T-1 状态), 滑点 0.15% 单边并入 COSTS_ROUNDTRIP
  hold    = 3 交易日, close 出, 0.3% 往返
  slots   = 15 x POSITION_PCT 0.10 (切片内名义最高 150%)

Truth doc: docs/backtests/state-bucket-algo-2026-08-31.md §7
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from typing import Any

import numpy as np
import psycopg

from data_sync_service.config import get_settings

POSITION_PCT = 0.10
COSTS_ROUNDTRIP = 0.003
BUCKET_Q = 3
MAX_POS = 15
BODY = 3
R_WIDE_THRESHOLD = 0.5
WARMUP_CAL_DAYS = 120


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
    s = get_settings()
    conn = psycopg.connect(s.database_url)
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT trade_date FROM daily WHERE trade_date >= %s AND trade_date <= %s "
        "ORDER BY trade_date",
        (start, end),
    )
    cal = [r[0].strftime("%Y-%m-%d") if hasattr(r[0], "strftime") else str(r[0]) for r in cur.fetchall()]
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
        amts = [r["amount"] for r in series[idx - 20: idx + 1] if r["amount"]]
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
            "is_gap": bool(gap == gap and gap > 0.03),
        }
    breadth = 0.0
    tot = 0
    above = 0
    for ts, series in per_ts.items():
        idx = date_idx.get(ts, {}).get(day, -1)
        if idx < 20 or ts not in mv_map.get(day, {}):
            continue
        closes = [r["close"] for r in series[idx - 19: idx + 1] if r["close"]]
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
) -> dict[str, Any]:
    """Replay S-gap satellite NAV (daily rows for UI) over [start, end].

    Returns {rows: [{date, satNav, satNavReturnPct, satPositions}], summary: {...}}.
    debug_fills: optional out-list collecting (entry_day, ts) — for tests/audit.
    skip_unfillable: when True, entries whose T-day open cannot actually be
    filled (one-word limit board open==high==low, or open at the price limit)
    are skipped — an audit of how much the backtest overstates returns when
    the satellite cannot buy the candidate.
    """
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
    idx_by_day = {d: i for i, d in enumerate(cal)}
    positions: dict[str, dict[str, Any]] = {}
    realized = 0.0
    rows: list[dict[str, Any]] = []
    for day in cal:
        if day < start:
            continue
        day_all, breadth = _day_features(per_ts, mv_map, cal, day, date_idx)
        r_wide = breadth > R_WIDE_THRESHOLD
        to_close = []
        for ts, p in list(positions.items()):
            ei = idx_by_day.get(p["entry_date"], -1)
            ci = idx_by_day.get(day, -1)
            held = ci - ei + 1 if ei >= 0 and ci >= 0 else 999
            if held >= body:
                to_close.append(ts)
        for ts in to_close:
            p = positions.pop(ts)
            cc = close_by_ts.get(ts, {}).get(day)
            if cc and p["entry_price"]:
                realized += ((cc / p["entry_price"] - 1) - COSTS_ROUNDTRIP) * POSITION_PCT
        if r_wide and day > start and day in idx_by_day and idx_by_day[day] > 0:
            prev_day = cal[idx_by_day[day] - 1]
            prev_all, _ = _day_features(per_ts, mv_map, cal, prev_day, date_idx)
            gap_stocks = [ts for ts, d in prev_all.items() if d["is_gap"]]
            if skip_t1_limit:
                gap_stocks = [
                    ts
                    for ts in gap_stocks
                    if not _t1_limit_locked(per_ts, date_idx, prev_day, ts)
                ]
            ranked = sorted(gap_stocks, key=lambda ts: prev_all[ts]["amp"])
            qn = max(1, len(ranked) // bucket_q)
            for ts in ranked[:qn]:
                if ts in positions or len(positions) >= max_pos:
                    continue
                series = per_ts.get(ts)
                di = date_idx.get(ts, {}).get(day, -1)
                open_px = series[di]["open"] if di >= 0 else None
                if open_px and open_px > 0:
                    if skip_unfillable:
                        cur = series[di]
                        pc = cur.get("pre_close")
                        lim = 0.20 if str(ts).startswith(("3", "68")) else 0.10
                        if pc and pc > 0 and (
                            cur["high"] == cur["low"] == cur["open"]  # one-word board
                            or open_px >= pc * (1 + lim - 0.004)
                        ):
                            continue
                    positions[ts] = {"entry_date": day, "entry_price": open_px}
                    if debug_fills is not None:
                        debug_fills.append((day, ts))
        mtm = 0.0
        for ts, p in positions.items():
            cc = close_by_ts.get(ts, {}).get(day)
            mtm += POSITION_PCT * (cc / p["entry_price"]) if cc and p["entry_price"] else POSITION_PCT
        nav = 1.0 + realized + (mtm - len(positions) * POSITION_PCT)
        rows.append(
            {
                "date": day,
                "satNav": round(nav, 6),
                "satNavReturnPct": round((nav - 1) * 100, 2),
                "satPositions": len(positions),
            }
        )
    last_day = cal[-1]
    for ts, p in list(positions.items()):
        cc = close_by_ts.get(ts, {}).get(last_day)
        if cc and p["entry_price"]:
            realized += ((cc / p["entry_price"] - 1) - COSTS_ROUNDTRIP) * POSITION_PCT
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
    return {
        "rows": rows,
        "summary": {
            "satPct": round((final_nav - 1) * 100, 2),
            "satMaxDdPct": round(max_dd * 100, 1),
        },
    }