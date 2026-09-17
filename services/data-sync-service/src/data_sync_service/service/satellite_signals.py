"""Live 14:30 satellite signal list (OPT-186 slice 1, read-only).

Assembles the habit-candidate panel for ONE decision day from the stored
panel (``daily`` rows + ``bar_5min`` prints ``<= 14:30``), reusing the frozen
replay helpers so the list is identical to what the backtest replay computes
for the same day::

    gap filter (MIN_GAP_PCT) -> amp_1430 rank -> top-1/BUCKET_Q bucket ->
    locked reasons (skip_t1 / C1 / C2 + fillable guard) -> strict pool ->
    R-wide gate breadth (gate_1430 basis)

Frozen habit recipe (mirrors ``_sat_book`` in eval_sat_idle_parking):
4 slots x 25%, C1 3%, skip_t1 on, C2/C3/churn off, strict pool, gate 0.5.

Fills price basis: 14:30 raw print (FILL_SAME_1430, user-confirmed
2026-09-16); paper fills add 30bps RT on top (paper layer, slice 2).

Availability: the endpoint only serves days whose panel is complete
(``daily`` row + 14:30 prints present). Intraday-today panels depend on the
live 5-min writer; when absent the response is ``decisionAvailable: false``
with an explicit reason (never a partial list).
"""

from __future__ import annotations

from typing import Any

from data_sync_service.service.state_bucket_track import (
    BUCKET_Q,
    MIN_GAP_PCT,
    R_WIDE_THRESHOLD,
    _board_limit_pct,
    _breadth_at_1430,
    _cached_day_features,
    _entry_pool,
    _intraday_px,
    _same_1430_skip_reason,
    _skip_kind_for_reason,
    load_sgap_context,
)

FILL_HHMM = "1430"
COST_RT_BPS = 30.0  # satellite round-trip cost applied by the paper layer

# Frozen habit recipe (parity with _sat_book; change = new prereg).
RECIPE: dict[str, Any] = {
    "min_gap_pct": MIN_GAP_PCT,
    "bucket_q": BUCKET_Q,
    "max_slots": 4,
    "slot_pct": 0.25,
    "skip_t1_limit": True,
    "max_open_to_1430_pct": 0.03,
    "near_limit_buffer_pct": None,
    "min_open_to_1430_pct": None,
    "max_t1_turnover_mult": None,
    "pool_mode": "strict",
    "gate_threshold": R_WIDE_THRESHOLD,
    "fill_hhmm": FILL_HHMM,
    "fill_basis": "14:30 raw print + 30bps RT (paper)",
}


def _raw_ratio(ctx: dict[str, Any], ts: str, day: str) -> float | None:
    """raw / qfq close ratio for the day (mirrors the replay closure).

    Raw side = bar_5min 15:00 mark, qfq side = daily close. None when either
    side is missing (caller treats missing ratio as no scaling).
    """
    raw_marks = (ctx.get("px_by_hhmm") or {}).get("1500") or {}
    v = (raw_marks.get(ts) or {}).get(day)
    q = (ctx.get("close_by_ts") or {}).get(ts, {}).get(day)
    if v and q and float(q) > 0:
        return float(v) / float(q)
    return None


def _t1_turnover(
    per_ts: dict[str, list[dict[str, Any]]],
    date_idx: dict[str, dict[str, int]],
    ts: str,
    day: str,
) -> float | None:
    """T-1 session amount vs trailing average (mirrors the replay block)."""
    di = date_idx.get(ts, {}).get(day, -1)
    series = per_ts.get(ts)
    if di is None or di <= 20 or not series:
        return None
    amts = [r.get("amount") for r in series[di - 21 : di] if r.get("amount")]
    if len(amts) >= 15 and amts[-1]:
        avg = sum(amts[:-1]) / max(len(amts) - 1, 1)
        return amts[-1] / avg if avg else None
    return None


def _fillable_at_print(
    *,
    ts: str,
    px: float | None,
    bar: dict[str, Any],
    raw_ratio: float | None,
) -> tuple[bool, str | None]:
    """Same fillability guard as the replay fill block (limit-lock at print).

    px is raw; scale the qfq pre_close to the raw basis first (mixed basis =
    false skips). Missing print or limit-locked print = unfillable.
    """
    if not px or px <= 0:
        return False, "no_print_1430"
    pc = bar.get("pre_close")
    if raw_ratio and pc:
        pc = float(pc) * raw_ratio
    lim = _board_limit_pct(ts)
    if pc and pc > 0 and px >= float(pc) * (1.0 + lim - 0.004):
        return False, "limit_locked_at_print"
    return True, None


def satellite_signals_for_day(
    ctx: dict[str, Any], day: str, *, today: str | None = None,
) -> dict[str, Any]:
    """Habit candidate panel for one decision day (pure; ctx injected).

    Returns ``decisionAvailable: False`` (with reason) when the day's panel
    is incomplete instead of a partial list.

    Deliberate difference vs replay rows: the replay only ranks names when
    its gate block runs, so replay ``gap_count`` is 0 on gate-closed days;
    this panel ALWAYS ranks (gate-closed days still show what the setups
    were — ``wouldFill`` is all false there). Gate decision itself matches
    the replay exactly (14:30 breadth, else close-basis breadth on
    historical days; live edge without breadth = unavailable).
    """
    from datetime import date as _date

    if today is None:
        today = _date.today().isoformat()
    if day not in (ctx.get("idx_by_day") or {}):
        return {"ok": True, "date": day, "decisionAvailable": False,
                "reason": "day not in trading calendar"}
    per_ts = ctx.get("per_ts") or {}
    date_idx = ctx.get("date_idx") or {}
    px1430_day = {ts: m.get(day) for ts, m in (ctx.get("px_1430") or {}).items() if m.get(day)}
    if not px1430_day:
        return {"ok": True, "date": day, "decisionAvailable": False,
                "reason": "no 14:30 prints for day (live panel not yet synced)"}
    feat_all, close_breadth = _cached_day_features(ctx, day)
    if not feat_all:
        return {"ok": True, "date": day, "decisionAvailable": False,
                "reason": "no daily features for day (daily row not yet synced)"}
    breadth = _breadth_at_1430(ctx, day)
    if breadth is None:
        if day >= today:
            return {"ok": True, "date": day, "decisionAvailable": False,
                    "reason": "14:30 breadth uncomputable (panel incomplete)"}
        breadth = close_breadth  # historical parity path (replay fallback)

    gap_stocks = [
        ts for ts, d in feat_all.items()
        if d.get("gap") is not None and d["gap"] == d["gap"] and d["gap"] > RECIPE["min_gap_pct"]
    ]
    hl = ctx.get("px_hl_1430") or {}

    def _proxy_amp(ts: str) -> tuple[int, float]:
        px = _intraday_px(ctx, ts, day, FILL_HHMM)
        h_l = (hl.get(ts) or {}).get(day)
        if not px or px <= 0 or not h_l:
            return (1, float("inf"))
        return (0, float(h_l[0] - h_l[1]) / float(px))

    ranked = sorted(gap_stocks, key=_proxy_amp)
    qn = max(1, len(ranked) // RECIPE["bucket_q"]) if ranked else 0

    entries: dict[str, dict[str, Any]] = {}
    locked: dict[str, str] = {}
    for ts in ranked:
        di = date_idx.get(ts, {}).get(day, -1)
        series = per_ts.get(ts)
        bar = series[di] if di >= 0 and series else {}
        px = _intraday_px(ctx, ts, day, FILL_HHMM)
        k = _raw_ratio(ctx, ts, day)
        raw_open = bar.get("open") if bar else None
        raw_pre = bar.get("pre_close") if bar else None
        if k:
            if raw_open:
                raw_open = float(raw_open) * k
            if raw_pre:
                raw_pre = float(raw_pre) * k
        reason = _same_1430_skip_reason(
            ts=ts, px=px, open_px=raw_open, pre_close=raw_pre,
            skip_t1_limit=RECIPE["skip_t1_limit"],
            max_open_to_1430_pct=RECIPE["max_open_to_1430_pct"],
            near_limit_buffer_pct=RECIPE["near_limit_buffer_pct"],
            min_open_to_1430_pct=RECIPE["min_open_to_1430_pct"],
            t1_turn=_t1_turnover(per_ts, date_idx, ts, day),
            max_t1_turnover_mult=RECIPE["max_t1_turnover_mult"],
        )
        if reason:
            locked[ts] = reason
        fillable, fill_note = _fillable_at_print(ts=ts, px=px, bar=bar or {}, raw_ratio=k)
        gap = (feat_all.get(ts) or {}).get("gap")
        amp_key = _proxy_amp(ts)
        entries[ts] = {
            "ts": ts,
            "gapPct": round(float(gap) * 100, 2) if gap == gap else None,
            "amp1430Pct": round(amp_key[1] * 100, 2) if amp_key[0] == 0 else None,
            "px1430": round(float(px), 4) if px and px > 0 else None,
            "skipReason": reason,
            "skipKind": _skip_kind_for_reason(reason) if reason else None,
            "fillable": fillable,
            "unfillableReason": fill_note if not fillable else None,
        }

    pool = _entry_pool(ranked, qn, skip_t1_limit=True, pool_mode="strict", locked=set(locked))
    gate_open = bool(breadth is not None and breadth > RECIPE["gate_threshold"])
    for rank, ts in enumerate(ranked, start=1):
        e = entries[ts]
        e["ampRank"] = rank
        e["inBucket"] = rank <= qn
        e["wouldFill"] = bool(
            gate_open and rank <= qn and not e["skipReason"] and e["fillable"]
            and ts in pool
        )
    return {
        "ok": True,
        "date": day,
        "decisionAvailable": True,
        "gateOpen": gate_open,
        "breadth1430": round(float(breadth), 4) if breadth is not None else None,
        "gapCount": len(ranked),
        "bucketSize": qn,
        "poolSize": len(pool),
        "ranked": [entries[ts] for ts in ranked],
        "recipe": {k: v for k, v in RECIPE.items()},
        "basis": ("gap>3% (daily open/prev close) -> amp_1430 asc -> top-1/3 bucket -> "
                  "skip_t1/C1/C2 + fillable guard -> strict pool; fills at 14:30 raw "
                  "print + 30bps RT (paper); R-wide gate 0.5 on 14:30 breadth"),
    }


def load_live_satellite_signals(day: str) -> dict[str, Any]:
    """Load context (warmup included) and compute the panel (DB reads only)."""
    from data_sync_service.service.state_bucket_track import HABIT_CTX_TIMES

    ctx = load_sgap_context(day, day, times=HABIT_CTX_TIMES)
    return satellite_signals_for_day(ctx, day)
