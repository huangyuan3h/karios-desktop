"""双子星 (Twin-Star) 卫星腿 — S-gap State-Bucket engine (service layer).

S-gap 单态卫星 (frozen R12 / core_satellite_frozen_2026-08-31.json):
  state   = S-gap (gap>3%)
  factor  = amplitude 升序取前 1/3 (bucket_q=3, 最低波33%)
  gate    = R-wide (close>MA20 占比>0.5, 当日截面)
  entry   = T 日 open (信号取 T-1 状态), 滑点 0.15% 单边并入 COSTS_ROUNDTRIP
  hold    = 3 交易日, close 出, 0.3% 往返
  slots   = 4 x POSITION_PCT 0.25 (sat sleeve ~100%; 12.5% of NAV at 50/50)

Truth doc: docs/backtests/state-bucket-algo-2026-08-31.md §7
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from typing import Any

import numpy as np
import psycopg

from data_sync_service.config import get_settings

POSITION_PCT = 0.25
COSTS_ROUNDTRIP = 0.003
BUCKET_Q = 3
MAX_POS = 4
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
    return {
        "per_ts": per_ts,
        "mv_map": mv_map,
        "cal": cal,
        "date_idx": date_idx,
        "close_by_ts": close_by_ts,
        "idx_by_day": {d: i for i, d in enumerate(cal)},
        "feat_cache": {},
    }


def _cached_day_features(ctx: dict[str, Any], day: str) -> tuple[dict[str, dict[str, float]], float]:
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
) -> dict[str, Any]:
    """Replay S-gap on a preloaded context. Positions start empty at ``start``."""
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
    positions: dict[str, dict[str, Any]] = {}
    realized = 0.0
    rows: list[dict[str, Any]] = []
    for day in cal:
        if day < start or day > end:
            continue
        _day_all, breadth = _cached_day_features(ctx, day)
        r_wide = breadth > R_WIDE_THRESHOLD
        to_close = []
        for ts, p in list(positions.items()):
            ei = idx_by_day.get(p["entry_date"], -1)
            ci = idx_by_day.get(day, -1)
            held = ci - ei + 1 if ei >= 0 and ci >= 0 else 999
            if held >= body:
                to_close.append(ts)
        closed_today = list(to_close)
        for ts in to_close:
            p = positions.pop(ts)
            cc = close_by_ts.get(ts, {}).get(day)
            if cc and p["entry_price"]:
                realized += ((cc / p["entry_price"] - 1) - COSTS_ROUNDTRIP) * clip
        if r_wide and day > start and day in idx_by_day and idx_by_day[day] > 0:
            prev_day = cal[idx_by_day[day] - 1]
            prev_all, _ = _cached_day_features(ctx, prev_day)
            gap_stocks = [ts for ts, d in prev_all.items() if d["is_gap"]]
            ranked = sorted(gap_stocks, key=lambda ts: prev_all[ts]["amp"])
            qn = max(1, len(ranked) // bucket_q)
            locked = {
                ts
                for ts in ranked
                if skip_t1_limit and _t1_limit_locked(per_ts, date_idx, prev_day, ts)
            }
            pool = _entry_pool(
                ranked, qn, skip_t1_limit=skip_t1_limit, pool_mode=pool_mode, locked=locked
            )
            for ts in pool:
                if ts in positions or len(positions) >= max_pos:
                    continue
                if ts in locked:
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
                            cur["high"] == cur["low"] == cur["open"]
                            or open_px >= pc * (1 + lim - 0.004)
                        ):
                            continue
                    positions[ts] = {"entry_date": day, "entry_price": open_px}
                    if debug_fills is not None:
                        debug_fills.append((day, ts))
        mtm = 0.0
        for ts, p in positions.items():
            cc = close_by_ts.get(ts, {}).get(day)
            mtm += clip * (cc / p["entry_price"]) if cc and p["entry_price"] else clip
        nav = 1.0 + realized + (mtm - len(positions) * clip)
        sat_active = len(positions) > 0 or len(closed_today) > 0
        sat_slots = len(positions) + len(closed_today)
        rows.append(
            {
                "date": day,
                "satNav": round(nav, 6),
                "satNavReturnPct": round((nav - 1) * 100, 2),
                "satPositions": len(positions),
                "satSlots": sat_slots,
                "satActive": sat_active,
            }
        )
    last_day = end
    for d in reversed(cal):
        if start <= d <= end:
            last_day = d
            break
    open_positions: list[dict[str, Any]] = []
    for ts, p in positions.items():
        cc = close_by_ts.get(ts, {}).get(last_day)
        ei = idx_by_day.get(p["entry_date"], -1)
        ci = idx_by_day.get(last_day, -1)
        held = ci - ei + 1 if ei >= 0 and ci >= 0 else 0
        days_left = max(0, body - held)
        exit_due = cal[ei + body - 1] if ei >= 0 and ei + body - 1 < len(cal) else last_day
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
        cc = close_by_ts.get(ts, {}).get(last_day)
        if cc and p["entry_price"]:
            realized += ((cc / p["entry_price"] - 1) - COSTS_ROUNDTRIP) * clip
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
        "openPositions": open_positions,
        "summary": {
            "satPct": round((final_nav - 1) * 100, 2),
            "satMaxDdPct": round(max_dd * 100, 1),
        },
        "pool_mode": pool_mode,
        "position_pct": clip,
        "max_pos": max_pos,
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
) -> dict[str, Any]:
    """Replay S-gap satellite NAV (daily rows for UI) over [start, end].

    Returns {rows: [{date, satNav, satNavReturnPct, satPositions, satSlots, satActive}],
             openPositions: [...], summary: {...}}.
    skip_t1_limit: drop candidates that closed limit-up on T-1 (executable口径).
    pool_mode: strict | replace | fallback (limit_fallback=True aliases fallback).
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
                "satActive": bool(r.get("satActive")) if "satActive" in r else None,
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
        "summary": {
            "fusedPct": round(sat_pct, 2),
            "corePct": None,
            "basePct": None,
            "maxDdFusedPct": round(sat_dd, 1),
            "satPct": round(sat_pct, 2),
            "satMaxDdPct": round(sat_dd, 1),
        },
        "openPositions": sat.get("openPositions") or [],
        "opportunity": False,
        "note": (
            "Standalone S-gap leg (bucket_q=3, 4 slots x 25%, body=3, R-wide). "
            "Executable口径: skip_t1_limit=True "
            "(涨停可能买不进 → 不假设开盘能成交; 机会双子星同口径)."
        ),
    }


def build_state_bucket_timeline(*, start: str, end: str) -> dict[str, Any]:
    """Product Timeline entry for the standalone state-bucket S-gap strategy."""
    sat = build_sgap_timeline(start=start, end=end, skip_t1_limit=True, pool_mode="strict")
    out = sgap_to_timeline_rows(sat)
    out["start"] = start
    out["end"] = end
    return out
