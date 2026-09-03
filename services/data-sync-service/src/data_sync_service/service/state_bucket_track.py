"""双子星 (Twin-Star) 卫星腿 — S-gap State-Bucket engine (service layer).

S-gap 单态卫星 (frozen R12 / core_satellite_frozen_2026-08-31.json):
  state   = S-gap (gap>3%)
  factor  = amplitude 升序取前 1/3 (bucket_q=3, 最低波33%)
  gate    = R-wide (close>MA20 占比>0.5, 当日截面)
  entry   = T 日 open (信号取 T-1 状态), 滑点 0.15% 单边并入 COSTS_ROUNDTRIP
            fill_mode=next_open is frozen. same_close is experiment-only
            (today's S-gap, fill at today's close; Live ~14:30 proxy).
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
FILL_NEXT_OPEN = "next_open"
FILL_SAME_CLOSE = "same_close"
VALID_FILL_MODES = (FILL_NEXT_OPEN, FILL_SAME_CLOSE)


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
) -> dict[str, Any]:
    """One sat blotter line: a fill/open, or a T-1 limit-up skip in the strict bucket."""
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
) -> dict[str, Any]:
    """Replay S-gap on a preloaded context. Positions start empty at ``start``.

    fill_mode:
      next_open (frozen): yesterday S-gap → today open.
      same_close (experiment): today's S-gap → today's close. Proxies Live
      ~14:30 until bar_5min coverage is complete. Do not pass from Live/UI.
    """
    if fill_mode not in VALID_FILL_MODES:
        raise ValueError(f"fill_mode must be one of {VALID_FILL_MODES}, got {fill_mode!r}")
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
    blotter: list[dict[str, Any]] = []
    for day in cal:
        if day < start or day > end:
            continue
        _day_all, breadth = _cached_day_features(ctx, day)
        r_wide = breadth > R_WIDE_THRESHOLD
        to_close: list[tuple[str, str]] = []
        for ts, p in list(positions.items()):
            ei = idx_by_day.get(p["entry_date"], -1)
            ci = idx_by_day.get(day, -1)
            held = ci - ei + 1 if ei >= 0 and ci >= 0 else 999
            cc = close_by_ts.get(ts, {}).get(day)
            entry = float(p.get("entry_price") or 0.0)
            peak = float(p.get("peak") or entry)
            if cc is not None and cc > peak:
                peak = float(cc)
                p["peak"] = peak
            reason = sat_exit_decision(
                held=held,
                body=body,
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
            cc = close_by_ts.get(ts, {}).get(day)
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
                )
            )
        gap_count = 0
        strict_count = 0
        skip_t1_count = 0
        filled_today = 0
        gate_open = False
        if r_wide and day > start and day in idx_by_day and idx_by_day[day] > 0:
            if fill_mode == FILL_SAME_CLOSE:
                feat_all = _day_all
                lock_day = day
            else:
                prev_day = cal[idx_by_day[day] - 1]
                feat_all, _ = _cached_day_features(ctx, prev_day)
                lock_day = prev_day
            gap_stocks = [ts for ts, d in feat_all.items() if d["is_gap"]]
            ranked = sorted(gap_stocks, key=lambda ts: feat_all[ts]["amp"])
            qn = max(1, len(ranked) // bucket_q) if ranked else 0
            locked = {
                ts
                for ts in ranked
                if skip_t1_limit and _t1_limit_locked(per_ts, date_idx, lock_day, ts)
            }
            gate_open = True
            gap_count = len(ranked)
            strict_count = qn
            bucket = ranked[:qn]
            skip_t1_count = sum(1 for ts in bucket if ts in locked)
            for ts in bucket:
                if ts not in locked:
                    continue
                feat = feat_all.get(ts) or {}
                blotter.append(
                    _audit_blotter_row(
                        kind="skip_t1",
                        date=day,
                        ts=ts,
                        amp=feat.get("amp"),
                        amp_rank=ranked.index(ts) + 1,
                        skip_t1=True,
                        close_reason="skip_t1_limit",
                        contrib_pct=0.0,
                    )
                )
            pool = _entry_pool(
                ranked, qn, skip_t1_limit=skip_t1_limit, pool_mode=pool_mode, locked=locked
            )
            ei_today = idx_by_day.get(day, -1)
            exit_due = (
                cal[ei_today + body - 1]
                if ei_today >= 0 and ei_today + body - 1 < len(cal)
                else end
            )
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
                px = bar.get("close") if fill_mode == FILL_SAME_CLOSE else bar.get("open")
                if px and px > 0:
                    if skip_unfillable:
                        pc = bar.get("pre_close")
                        lim = 0.20 if str(ts).startswith(("3", "68")) else 0.10
                        one_word = (
                            bar["high"] == bar["low"] == bar["close"]
                            if fill_mode == FILL_SAME_CLOSE
                            else bar["high"] == bar["low"] == bar["open"]
                        )
                        if pc and pc > 0 and (
                            one_word or px >= pc * (1 + lim - 0.004)
                        ):
                            continue
                    feat = feat_all.get(ts) or {}
                    positions[ts] = {
                        "entry_date": day,
                        "entry_price": px,
                        "peak": px,
                        "amp": feat.get("amp"),
                        "amp_rank": ranked.index(ts) + 1,
                        "exit_due": exit_due,
                    }
                    filled_today += 1
                    if debug_fills is not None:
                        debug_fills.append((day, ts))
        mtm = 0.0
        for ts, p in positions.items():
            cc = close_by_ts.get(ts, {}).get(day)
            mtm += clip * (cc / p["entry_price"]) if cc and p["entry_price"] else clip
        nav = 1.0 + realized + (mtm - len(positions) * clip)
        sat_active = len(positions) > 0 or len(closed_today) > 0
        sat_slots = len(positions) + len(closed_today)
        idle_slots = max(0, max_pos - sat_slots)
        rows.append(
            {
                "date": day,
                "satNav": round(nav, 6),
                "satNavReturnPct": round((nav - 1) * 100, 2),
                "satPositions": len(positions),
                "satSlots": sat_slots,
                "satActive": sat_active,
                "gapCount": gap_count,
                "strictCount": strict_count,
                "skipT1Count": skip_t1_count,
                "filledToday": filled_today,
                "idleSlots": idle_slots,
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
    fill_n = sum(1 for b in blotter if b.get("kind") == "fill")
    close_reasons: dict[str, int] = {}
    held_days: list[int] = []
    for b in blotter:
        if b.get("kind") != "fill":
            continue
        reason = str(b.get("closeReason") or "unknown")
        close_reasons[reason] = close_reasons.get(reason, 0) + 1
        hd = b.get("heldDays")
        if hd is not None:
            held_days.append(int(hd))
    return {
        "rows": rows,
        "openPositions": open_positions,
        "blotter": blotter,
        "summary": {
            "satPct": round((final_nav - 1) * 100, 2),
            "satMaxDdPct": round(max_dd * 100, 1),
            "skipT1Count": skip_n,
            "fillCount": fill_n,
            "closeReasons": close_reasons,
            "avgHeldDays": round(sum(held_days) / len(held_days), 2) if held_days else 0.0,
        },
        "pool_mode": pool_mode,
        "position_pct": clip,
        "max_pos": max_pos,
        "fill_mode": fill_mode,
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
) -> dict[str, Any]:
    """Replay S-gap satellite NAV (daily rows for UI) over [start, end].

    Returns {rows: [{date, satNav, satNavReturnPct, satPositions, satSlots, satActive}],
             openPositions: [...], summary: {...}}.
    skip_t1_limit: drop candidates that closed limit-up on T-1 (executable口径).
    pool_mode: strict | replace | fallback (limit_fallback=True aliases fallback).
    protect_stop_pct / trail_after_body_pct / fill_mode: experiment-only;
    live UI must leave fill_mode at next_open and the stops at None.
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
                "gapCount": r.get("gapCount"),
                "strictCount": r.get("strictCount"),
                "skipT1Count": r.get("skipT1Count"),
                "filledToday": r.get("filledToday"),
                "idleSlots": r.get("idleSlots"),
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
        "summary": {
            "fusedPct": round(sat_pct, 2),
            "corePct": None,
            "basePct": None,
            "maxDdFusedPct": round(sat_dd, 1),
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


def build_state_bucket_timeline(*, start: str, end: str) -> dict[str, Any]:
    """Product Timeline entry for the standalone state-bucket S-gap strategy."""
    sat = build_sgap_timeline(start=start, end=end, skip_t1_limit=True, pool_mode="strict")
    out = sgap_to_timeline_rows(sat)
    out["start"] = start
    out["end"] = end
    return out
