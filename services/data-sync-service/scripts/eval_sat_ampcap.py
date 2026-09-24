#!/usr/bin/env python3
"""H-SAT-AMP-CAP replay: tighten the satellite to 'gap-and-hold' names.

Arms (prereg docs/designs/h-sat-amp-cap-prereg-2026-09-24.md):
  base  bucket_q=3 (Live)
  A     bucket_q=4
  B     max_amp_1430_pct=0.010

Composite = Starport B (satellite + cashShare(T-1) x 3-leg inverse-vol park).
Read-only. Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/eval_sat_ampcap.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from eval_sat_idle_parking import SLOT_PCT, _compose, _true_cash_share  # noqa: E402
from eval_starship_b import PARK_B, blend_b, load_raw  # noqa: E402
from eval_twin_star_parking import _stats  # noqa: E402
from run_walk_forward import WINDOWS  # noqa: E402

from data_sync_service.service.state_bucket_track import (  # noqa: E402
    FILL_SAME_1430,
    load_sgap_context,
    replay_sgap_from_context,
)

BASE = dict(
    skip_t1_limit=True,
    pool_mode="strict",
    max_pos=4,
    position_pct=SLOT_PCT,
    body=3,
    fill_mode=FILL_SAME_1430,
    fill_hhmm="1430",
    exit_hhmm="1430",
    max_open_to_1430_pct=0.03,
    rank_key="amp_1430",
    gate_1430=True,
)
ARMS = {
    "base": {},
    "A_bq4": {"bucket_q": 4},
    "B_cap1.0": {"max_amp_1430_pct": 0.010},
}
WINS = {k: WINDOWS[k] for k in ("OOS2", "train", "valid", "long")}


def main() -> int:
    print("H-SAT-AMP-CAP replay (Starport B composite)\n", flush=True)
    ls, le = WINS["long"]
    ctx = load_sgap_context(ls, le)
    park_sm = {ts: load_raw(ts) for ts in PARK_B}

    res: dict[str, dict] = {}
    series: dict[str, tuple[list[str], list[float]]] = {}
    for wname, (s, e) in WINS.items():
        sat: dict[str, dict] = {}
        for arm, extra in ARMS.items():
            r = replay_sgap_from_context(ctx, start=s, end=e, **BASE, **extra)
            rows = r["rows"]
            dates = [str(x["date"]) for x in rows]
            sat_nav = [float(x.get("satNav") or 1.0) for x in rows]
            calB = sorted({d for d in park_sm[PARK_B[0]] if d in set(dates)})
            parkB = blend_b(park_sm, calB)
            byd = dict(zip(calB, parkB, strict=True))
            last, navB = 1.0, []
            for d in dates:
                last = byd.get(d, last)
                navB.append(last)
            cash = _true_cash_share(r, from_rows=True)
            w = [0.0] + cash[:-1]
            n = min(len(sat_nav), len(navB), len(w))
            nav = _compose(sat_nav[:n], w[:n], navB[:n], 5.0)
            st = _stats(nav)
            fills = [
                b
                for b in (r.get("blotter") or [])
                if b.get("kind") == "fill" and b.get("contribPct") is not None
            ]
            contrib = np.array([float(b["contribPct"]) for b in fills])
            wins = contrib[contrib > 0]
            sat[arm] = {
                **st,
                "fills": len(fills),
                "win": 100 * float((contrib > 0).mean()) if contrib.size else 0.0,
                "win_mean": float(wins.mean()) if wins.size else 0.0,
            }
            if wname == "long":
                series[arm] = (dates, nav)
        res[wname] = sat
        line = f"  {wname:<6}"
        for arm in ARMS:
            m = sat[arm]
            line += (
                f" | {arm:<9} {m['total_pct']:+7.1f} cagr{m['cagr']:5.1f} dd{m['max_dd']:+5.1f} "
                f"sr{m['sharpe']:4.2f} fills{m['fills']:>4} win{m['win']:>3.0f}% wm{m['win_mean']:+.2f}"
            )
        print(line, flush=True)

    def summ(arm):
        tot3 = sum(res[w][arm]["total_pct"] for w in ("OOS2", "train", "valid"))
        return tot3

    print("\nK1 3-window total sum: base", f"{summ('base'):+.1f}", "A", f"{summ('A_bq4'):+.1f}", "B", f"{summ('B_cap1.0'):+.1f}")
    for arm in ("A_bq4", "B_cap1.0"):
        k1 = summ(arm) >= summ("base") and all(
            res[w][arm]["total_pct"] >= res[w]["base"]["total_pct"] - 5 for w in ("OOS2", "train", "valid")
        )
        lb, la = res["long"]["base"], res["long"][arm]
        k2 = (
            la["total_pct"] >= lb["total_pct"] - 5
            and la["max_dd"] <= lb["max_dd"] + 1
            and la["sharpe"] >= lb["sharpe"] - 0.15
        )
        k3 = (
            res["long"][arm]["win"] >= res["long"]["base"]["win"] - 3
            and res["long"][arm]["win_mean"] >= 0.8 * res["long"]["base"]["win_mean"]
        )
        print(f"  {arm}: K1 {'ok' if k1 else 'FAIL'} K2 {'ok' if k2 else 'FAIL'} K3 {'ok' if k3 else 'FAIL'} -> {'PASS' if (k1 and k2 and k3) else 'REJECT'}")

    # CAGR / risk trade summary + calendar-year long NAV
    print("\nTRADE SUMMARY (long, Starport B composite)")
    b, v = res["long"]["base"], res["long"]["B_cap1.0"]
    print(
        f"  base: CAGR {b['cagr']:.1f}%  total {b['total_pct']:+.1f}  MDD {b['max_dd']:.1f}  "
        f"Sharpe {b['sharpe']:.2f}  Calmar {(b['cagr'] / -b['max_dd'] if b['max_dd'] else 0):.2f}"
    )
    print(
        f"  B   : CAGR {v['cagr']:.1f}%  total {v['total_pct']:+.1f}  MDD {v['max_dd']:.1f}  "
        f"Sharpe {v['sharpe']:.2f}  Calmar {(v['cagr'] / -v['max_dd'] if v['max_dd'] else 0):.2f}"
    )
    print(
        f"  Δ   : CAGR {v['cagr'] - b['cagr']:+.1f}pt  Sharpe {v['sharpe'] - b['sharpe']:+.2f}  "
        f"MDD {v['max_dd'] - b['max_dd']:+.1f}pt"
    )

    print("\nCALENDAR-YEAR total % (long)")
    print(f"  {'year':<6} {'base':>9} {'B_cap1.0':>9}")
    bd, bn = series["base"]
    vd, vn = series["B_cap1.0"]

    def yr_ret(dates: list[str], navv: list[float], y: str) -> float | None:
        idxs = [i for i, d in enumerate(dates) if d[:4] == y]
        if len(idxs) < 2:
            return None
        base_i = idxs[0] - 1 if idxs[0] > 0 else 0
        return 100 * (navv[idxs[-1]] / navv[base_i] - 1)

    for y in sorted({d[:4] for d in bd}):
        rb, rv = yr_ret(bd, bn, y), yr_ret(vd, vn, y)
        print(
            f"  {y:<6} {rb if rb is not None else float('nan'):>+8.1f}% "
            f"{rv if rv is not None else float('nan'):>+8.1f}%"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
