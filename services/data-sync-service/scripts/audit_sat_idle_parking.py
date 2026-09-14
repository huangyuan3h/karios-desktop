#!/usr/bin/env python3
"""Audit A2 (satellite idle cash -> ETF parking sleeve): clock/logic + MDD attribution.

Read-only. Rebuilds the daily A2 frame for one window and answers:
  1. clock: w_t = f(pos_{t-1}); sleeve pick/trail on prev close; extra-lag sensitivity
  2. alignment: stale sleeve days (sat calendar vs ETF sessions), zero-return artifacts
  3. max drawdown episodes: peak/trough, satellite vs parking contribution, held ETFs
  4. costs: transfer friction and sleeve-internal sides

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/audit_sat_idle_parking.py --window long
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from eval_sat_idle_parking import (  # noqa: E402
    SLOT_PCT,
    _align_day_nav,
    _compose,
    _idle_prev,
    _rets,
    _sat_book,
)
from eval_twin_star_parking import COST, _load_etf_closes  # noqa: E402
from run_walk_forward import WINDOWS  # noqa: E402

from data_sync_service.service.harbor import parking_replay  # noqa: E402


def _episodes(nav: list[float], dates: list[str], top: int = 5) -> list[dict]:
    """All drawdown episodes, deepest first (peak -> trough -> recovery)."""
    peak = nav[0]
    peak_i = 0
    eps: list[dict] = []
    for i, v in enumerate(nav):
        if v > peak:
            peak, peak_i = v, i
            continue
        depth = (peak - v) / peak * 100 if peak else 0.0
        if depth > 0.5:
            eps.append({"peak_i": peak_i, "trough_i": i, "depth": depth})
    # keep only local maxima of depth per peak segment, then top-N by depth
    best: dict[int, dict] = {}
    for e in eps:
        k = e["peak_i"]
        if k not in best or e["depth"] > best[k]["depth"]:
            best[k] = e
    out = sorted(best.values(), key=lambda e: -e["depth"])[:top]
    for e in out:
        e["peak_date"] = dates[e["peak_i"]]
        e["trough_date"] = dates[e["trough_i"]]
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--window", default="long", choices=list(WINDOWS))
    ap.add_argument("--top", type=int, default=6)
    args = ap.parse_args()
    s, e = WINDOWS[args.window]
    print(f"=== audit {args.window} ({s}~{e}) ===", flush=True)

    px = _load_etf_closes()
    sat = _sat_book(s, e)
    dates, sat_nav, pos = sat["dates"], sat["nav"], sat["pos"]
    n = len(dates)

    recs = parking_replay(px, dates, idle_by_day=None)
    rec_by_day = {str(r["date"]): r for r in recs}
    day_nav = {dates[0]: 1.0} if dates else {}
    nav = 1.0
    sides_tot = 0
    for r in recs:
        nav *= 1.0 + float(r["parking_ret"]) - COST * int(r["sides"])
        day_nav[str(r["date"])] = nav
        sides_tot += int(r["sides"])
    sleeve_nav = _align_day_nav(day_nav, dates)
    sleeve_r = _rets(sleeve_nav)
    sat_r = _rets(sat_nav)

    w = _idle_prev(pos, lag=True)
    w2 = [0.0] * n
    for t in range(2, n):
        w2[t] = min(1.0, max(0.0, 1.0 - SLOT_PCT * pos[t - 2]))
    w0 = _idle_prev(pos, lag=False)

    a2 = _compose(sat_nav, w, sleeve_nav, 5.0)
    a2_nocost = _compose(sat_nav, w, sleeve_nav, 0.0)
    a2_lag2 = _compose(sat_nav, w2, sleeve_nav, 5.0)
    a2_sameday = _compose(sat_nav, w0, sleeve_nav, 5.0)

    def _total(x: list[float]) -> float:
        return round((x[-1] / x[0] - 1) * 100, 1)

    print(
        f"  totals: sat {_total(sat_nav)} | A2 {_total(a2)} | A2_nocost {_total(a2_nocost)} "
        f"| A2_lag2 {_total(a2_lag2)} | A2_sameday {_total(a2_sameday)}"
    )

    # --- clock / alignment checks
    sess = {d for mp in px.values() for d in mp}
    missing = [d for d in dates if d not in sess]
    stale = [i for i in range(1, n) if sleeve_r[i] == 0.0 and i != 0]
    stale_held = [
        i for i in stale if (rec_by_day.get(dates[i]) or {}).get("pick_ts") not in (None, "", "GC001")
    ]
    stale_by_ts: dict[str, int] = {}
    for i in stale_held:
        ts = str((rec_by_day.get(dates[i]) or {}).get("pick_ts"))
        stale_by_ts[ts] = stale_by_ts.get(ts, 0) + 1
    big_moves = sorted(range(1, n), key=lambda i: -abs(sleeve_r[i]))[:3]
    print(
        f"  align: sat days {n} | not in ETF sessions {len(missing)} | sleeve 0-return days "
        f"{len(stale)} (of which still holding an ETF {len(stale_held)}: {stale_by_ts})"
    )
    for i in big_moves:
        print(
            f"    sleeve big day {dates[i]} r={sleeve_r[i] * 100:+.2f}% "
            f"held={(rec_by_day.get(dates[i]) or {}).get('pick_ts')} "
            f"prev={(rec_by_day.get(dates[i]) or {}).get('prev')}"
        )
    # sleeve picks vs. satellite position changes (same-day leak check)
    w_changes = sum(1 for i in range(1, n) if abs(w[i] - w[i - 1]) > 1e-9)
    print(
        f"  clock: w changes {w_changes} days | max|Δw| {max((abs(w[i] - w[i - 1]) for i in range(1, n)), default=0):.2f} "
        f"| mean|Δw| {float(np.mean([abs(w[i] - w[i - 1]) for i in range(1, n)])) * 100:.1f}%/d "
        f"| sleeve sides {sides_tot} (avg {sides_tot / max(1, n - 1):.2f}/d)"
    )
    print(
        f"  lag sensitivity: A2 {_total(a2)} vs lag2 {_total(a2_lag2)} vs same-day {_total(a2_sameday)} "
        f"(Δ = {_total(a2_lag2) - _total(a2):+.1f} / {_total(a2_sameday) - _total(a2):+.1f})"
    )

    # --- drawdown episodes
    print(f"\n  top {args.top} A2 drawdown episodes (peak->trough):")
    for ep in _episodes(a2, dates, args.top):
        i0, i1 = ep["peak_i"], ep["trough_i"]
        sat_seg = sat_nav[i0 : i1 + 1]
        sl_seg = sleeve_nav[i0 : i1 + 1]
        sat_dd = (max(sat_seg) - sat_nav[i1]) / max(sat_seg) * 100 if i1 > i0 else 0.0
        sl_dd = (max(sl_seg) - sleeve_nav[i1]) / max(sl_seg) * 100 if i1 > i0 else 0.0
        park_contrib = sum(w[i] * sleeve_r[i] for i in range(i0 + 1, i1 + 1)) * 100
        sat_contrib = sum(sat_r[i] for i in range(i0 + 1, i1 + 1)) * 100
        cost = sum(5.0 / 1e4 * abs(w[i] - w[i - 1]) for i in range(i0 + 1, i1 + 1)) * 100
        picks: dict[str, int] = {}
        for i in range(i0 + 1, i1 + 1):
            ts = (rec_by_day.get(dates[i]) or {}).get("pick_ts") or "REPO"
            picks[ts] = picks.get(ts, 0) + 1
        both_down = sum(
            1 for i in range(i0 + 1, i1 + 1) if sat_r[i] < 0 and w[i] * sleeve_r[i] < 0
        )
        print(
            f"  - {ep['peak_date']} -> {ep['trough_date']}  A2 {ep['depth']:.1f}% "
            f"(sat {sat_dd:.1f}% / sleeve {sl_dd:.1f}%) | Σ sat {sat_contrib:+.1f}pt "
            f"parking {park_contrib:+.1f}pt cost {cost:.1f}pt | both-down days {both_down} "
            f"| held {picks}"
        )

    deep = _episodes(a2, dates, 1)[0]
    i0, i1 = deep["peak_i"], deep["trough_i"]
    print(f"\n  deepest episode daily slice ({dates[i0]} -> {dates[i1]}), worst 20 days by A2 ret:")
    idx = sorted(range(i0 + 1, i1 + 1), key=lambda i: a2[i] / a2[i - 1] if a2[i - 1] else 0)[:20]
    for i in sorted(idx):
        rec = rec_by_day.get(dates[i]) or {}
        print(
            f"    {dates[i]} pos={pos[i - 1]}->{pos[i]} w={w[i]:.2f} sat={sat_r[i] * 100:+6.2f}% "
            f"sleeve={sleeve_r[i] * 100:+6.2f}% held={rec.get('pick_ts')} "
            f"port={(a2[i] / a2[i - 1] - 1) * 100:+6.2f}%"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
