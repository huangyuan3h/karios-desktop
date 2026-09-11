"""DH-2 honest beta scan (no look-ahead) — find a real, tradable, positive beta.

Universe (all selected by PRIOR-month 60d avg amount, as-of) x vol target.
EW = equal weight, monthly rebalance, costs 10bps/side. 2007-2026 annualized.

Usage:
    PYTHONPATH=src python3 scripts/incubate/dh_v2_honest.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # scripts/
from strategy_a1_voltarget import build_panel, run, stats  # noqa: E402

UNIVERSES = [
    ("EW all", 10 ** 9, 0.0),
    ("EW floor top50%", 10 ** 9, 0.50),
    ("EW floor top30%", 10 ** 9, 0.70),
    ("top500", 500, 0.0),
    ("top300", 300, 0.0),
    ("top200", 200, 0.0),
]
VOLS = [0.0, 0.30, 0.25, 0.20, 0.15, 0.10]


def main() -> int:
    mo = build_panel()
    print(f"panel rows={len(mo)} months={mo['ym'].nunique()} (prior-month liquidity, no look-ahead)\n")
    header = f"{'universe':16s}" + "".join(f"{('vt'+str(int(v*100)) if v else 'no-vt'):>18s}" for v in VOLS)
    print(header)
    for name, topn, floor in UNIVERSES:
        cells = []
        for v in VOLS:
            r = run(mo, topn=topn, target_vol=v, cap=1.0, cost_side=0.001, liq_floor=floor)
            s = stats(r, name)
            cells.append(f"{s['cagr']:+5.1f}/{s['maxdd']:4.0f}/{s['sharpe']:4.2f}")
        print(f"{name:16s}" + "".join(f"{c:>18s}" for c in cells))
    # detail + sub-periods for the sensible picks
    from strategy_a1_voltarget import SUB_PERIODS
    print("\n=== detail (CAGR / vol / DD / Sharpe / avgExp) ===")
    for name, topn, floor in UNIVERSES:
        for v in (0.0, 0.20, 0.15):
            r = run(mo, topn=topn, target_vol=v, cap=1.0, cost_side=0.001, liq_floor=floor)
            s = stats(r, name)
            print(f"  {name:16s} vt{int(v*100):>2d}%  {s['cagr']:+5.1f}%/yr vol{s['vol']:5.1f}% "
                  f"DD{s['maxdd']:6.1f}% sr{s['sharpe']:5.2f} exp{s['avg_exp']:.2f} turn{s['avg_turn']:.2f}")
    print("\n=== sub-periods (EW all, vt0/20/15) ===")
    for lo, hi, sp in SUB_PERIODS:
        line = []
        for v in (0.0, 0.20, 0.15):
            r = run(mo, topn=10 ** 9, target_vol=v, cap=1.0, cost_side=0.001)
            r = r[(r["ym"].astype(str) >= lo) & (r["ym"].astype(str) < hi)]
            cum = (1 + r["ret"]).cumprod()
            line.append(f"vt{int(v*100):>2d} x{cum.iloc[-1]:.2f}/{((cum/cum.cummax()-1).min()*100):.0f}%")
        print(f"  {sp:12s} " + "  ".join(line))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
