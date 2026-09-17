"""Hysteresis parking sleeve (H-SLEEVE-TUNE H2 adoption, research-only).
Starship-idle parking variant of the canonical Harbor rule (mom60+MA200
argmax, trail8, REPO fallback): rotate to a new pick ONLY when its mom60
beats the held leg's by >= ``band`` (default 0.02 = H2, user-approved
2026-09-16). Kills noise-sized leadership rotations (median gap 0.61pt).

Single source for the starship parked leg (Timeline + paper + catalog).
Harbor Live keeps canonical ``harbor.parking_replay`` — this module never
touches it. Pick ranking itself is borrowed (``harbor.pick_parking``); only
the stateful rotate/trail loop is variant-specific. Record shape matches
``parking_replay`` exactly (``date/prev/pick_key/pick_ts/want_key/want_ts/
parking_ret/sides/trail_exit/cooldown_active``) so all downstream readers
(``parked_blotter``, ``compose_parked_rows``, recon) work unchanged.
"""

from __future__ import annotations

import bisect
from typing import Any

from data_sync_service.service.harbor import LOOKBACK, TRAIL_PCT, pick_parking

HYST_BAND = 0.02  # H2: rotate only on >= 2pt mom60 leadership (approved 2026-09-16)


def _held_mom(closes: dict[str, dict[str, float]], ts: str, prev: str) -> float | None:
    """Held leg's own mom60 (per-TS, same formula as the pick ranking)."""
    mp = closes.get(ts) or {}
    ds = sorted(mp)
    i = bisect.bisect_right(ds, prev) - 1
    if i < LOOKBACK or not mp[ds[i - LOOKBACK]]:
        return None
    return mp[ds[i]] / mp[ds[i - LOOKBACK]] - 1.0


def hysteresis_parking_replay(
    etf_close: dict[str, dict[str, float]],
    calendar: list[str],
    *,
    band: float = HYST_BAND,
    trail_pct: float = TRAIL_PCT,
) -> list[dict[str, Any]]:
    """Canonical parking loop + hysteresis gate on rotations.

    Identical to ``harbor.parking_replay`` except the rotate step requires
    ``want.mom60 - held.mom60 >= band`` (missing mom data fails open =
    rotate, status-quo bias). Trail exits, REPO fallback, sides accounting
    and peak resets are unchanged.

    Blocked rotation semantics (prereg 2026-09-16): the machine ONLY adds a
    "skip rotation" branch — a blocked switch keeps the incumbent leg
    untouched (``pick_*`` stays, ``sides == 0``, peak keeps trailing). It
    must never sell to cash: conflating "challenger not good enough" with
    "no candidate at all" turned every blocked day into a REPO gap
    (bug found in the 2026-09-17 audit; frozen H2 numbers were rerun).
    """
    sessions = {d for mp in etf_close.values() for d in mp}
    cal = [d for d in calendar if d in sessions]
    closes = etf_close
    out: list[dict[str, Any]] = []
    held_key: str | None = None
    held_ts: str | None = None
    peak = 0.0
    for i in range(1, len(cal)):
        day, prev = cal[i], cal[i - 1]
        want = pick_parking(etf_close, prev)
        want_key = want["key"] if want else None
        want_ts = want["ts"] if want else None
        sides = 0
        parking_ret = 0.0
        trail_exit = False
        # 1) trail the held leg at the prev close (exit -> REPO that day)
        if held_ts is not None:
            c = (closes.get(held_ts) or {}).get(prev)
            if c:
                peak = max(peak, c)
                if peak > 0 and c < peak * (1.0 - trail_pct / 100.0):
                    held_key = held_ts = None
                    peak = 0.0
                    sides += 1
                    trail_exit = True
        # 2) rotate on a TS change, gated by the hysteresis band. A blocked
        # rotation keeps the incumbent leg (prereg semantics): no sell, no
        # REPO gap, peak keeps trailing. ``want_*`` records the effective
        # target (= held when blocked) so downstream blotter readers see no
        # rotation; ``hyst_blocked`` marks the day for audits.
        blocked = False
        if not trail_exit and (held_key != want_key or held_ts != want_ts):
            if want is not None:
                hm = _held_mom(closes, held_ts, prev) if held_ts else None
                gap = (want["mom60"] / 100.0 - hm) if hm is not None else None
                blocked = gap is not None and gap < band
            if blocked:
                want_key, want_ts = held_key, held_ts
            else:
                sides += int(held_ts is not None) + int(want_ts is not None)
                if want_ts is not None:
                    held_key, held_ts = want_key, want_ts
                    peak = (closes.get(held_ts) or {}).get(prev) or 0.0
                else:
                    held_key, held_ts = None, None
                    peak = 0.0
        # 3) day return of the (possibly new) held leg
        if held_ts is not None:
            c0 = (closes.get(held_ts) or {}).get(prev)
            c1 = (closes.get(held_ts) or {}).get(day)
            parking_ret = c1 / c0 - 1.0 if c0 and c1 else 0.0
        out.append(
            {
                "date": day,
                "prev": prev,
                "pick_key": held_key or "REPO",
                "pick_ts": held_ts or "GC001",
                "want_key": want_key,
                "want_ts": want_ts,
                "parking_ret": parking_ret,
                "sides": sides,
                "trail_exit": trail_exit,
                "cooldown_active": False,
                "hyst_blocked": blocked,
            }
        )
    return out


def blend_harbor_h2_timeline(
    harbor_result: dict[str, Any],
    *,
    closes: dict[str, dict[str, float]] | None = None,
    band: float = HYST_BAND,
    cost: float = 0.0005,
) -> dict[str, Any]:
    """Harbor timeline with the idle-parking leg switched to H2 hysteresis.

    Research/validation line (H-HARBOR-H2, 条件PASS待 paper). Recomputes ONLY
    the parking leg from a built Harbor timeline: engine NAV (``navBase``)
    and idle fractions (``idlePct``) are reused untouched; ``navSingle`` /
    ``navMulti`` and all parked fields/blotter/held/summary become H2.
    Harbor Live, homeport/starport bases and frozen evals are unaffected
    (they never call this function).
    """
    from data_sync_service.service.harbor import NAMES, load_etf_closes
    from data_sync_service.service.state_bucket_track import parked_blotter

    out = dict(harbor_result)
    rows = [dict(r) for r in (harbor_result.get("rows") or [])]
    if not rows:
        return out
    px = closes if closes is not None else load_etf_closes()
    dates = [str(r["date"]) for r in rows]
    prev0 = str(rows[0].get("prev") or "")
    cal = ([prev0] if prev0 else []) + dates
    recs = hysteresis_parking_replay(px, cal, band=band)
    rec_by_day = {str(r["date"]): r for r in recs}
    assert len(recs) == len(rows), (len(recs), len(rows))
    nav = 1.0
    peak = 1.0
    max_dd = 0.0
    trades = 0
    trail_exits = 0
    for i, r in enumerate(rows):
        rec = rec_by_day.get(str(r["date"])) or {}
        hret = float(rec.get("parking_ret") or 0.0)
        sides = int(rec.get("sides") or 0)
        idle = float(r.get("idlePct") or 0.0) / 100.0
        base = float(r.get("navBase") or 1.0)
        prev_base = float(rows[i - 1].get("navBase") or 1.0) if i > 0 else 1.0
        r_eng = base / prev_base - 1.0 if prev_base else 0.0
        nav *= 1.0 + r_eng + idle * (hret - cost * sides)
        peak = max(peak, nav)
        max_dd = max(max_dd, (peak - nav) / peak if peak > 0 else 0.0)
        if sides:
            trades += 1
        if rec.get("trail_exit"):
            trail_exits += 1
        r["pick"] = rec.get("pick_key")
        r["pickTs"] = rec.get("pick_ts")
        r["parkedSides"] = sides
        r["parkedRetPct"] = round(hret * 100, 2)
        r["parkedTrail"] = bool(rec.get("trail_exit"))
        r["parkingPct"] = round(idle * hret * 100, 2)
        r["navSingle"] = round(nav, 6)
        r["navMulti"] = round(nav, 6)
        r["navSingleReturnPct"] = round((nav - 1) * 100, 2)
        r["navMultiReturnPct"] = round((nav - 1) * 100, 2)
    events, held = parked_blotter(recs, px, names=NAMES)
    if held is not None and rows:
        held["weight"] = round(float(rows[-1].get("idlePct") or 0.0) / 100.0, 4)
    out["rows"] = rows
    out["parkedBlotter"] = events
    out["parkedHeld"] = held
    out["mode"] = "harbor_h2"
    out["strategy"] = "港湾H2"
    out["note"] = (
        "港湾H2验证线（H-HARBOR-H2，条件PASS待 paper）：S-3 核心冻结，闲置停车换 "
        "H2 迟滞套筒。非产品档；Live=港湾 canonical。"
    )
    out["summary"] = {
        **(out.get("summary") or {}),
        "fusedPct": round((nav - 1) * 100, 2),
        "maxDdFusedPct": round(max_dd * 100, 1),
        "parkingTrades": trades,
        "trailExits": trail_exits,
        "parkedTrades": len(events),
        "parkedTrailExits": trail_exits,
    }
    return out
