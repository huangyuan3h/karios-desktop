"""W1 platform scan — is 15% vol target / top300 a platform or a spike?

Structure parameters (S4) get a small grid, not a diagnostic. Builds the
monthly panel once, then sweeps target-vol x top-N x leverage cap and
reports CAGR/vol/DD/Sharpe over 2007-2026. Read-only.

Usage:
    PYTHONPATH=src python3 scripts/incubate/w1_scan.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # scripts/ for import
from strategy_a1_voltarget import build_panel, run, stats  # noqa: E402


def main() -> int:
    mo = build_panel()
    print(f"panel rows={len(mo)} months={mo['ym'].nunique()}\n")

    topns = [200, 300, 500]
    vols = [0.10, 0.12, 0.15, 0.18, 0.20]
    print("=== cap=1.5 (with leverage) ===")
    header = "topN\\vol  " + "  ".join(f"{v:.0%}" for v in vols)
    print(header)
    for topn in topns:
        cells = []
        for v in vols:
            r = run(mo, topn=topn, target_vol=v, cap=1.5, cost_side=0.001)
            s = stats(r, "")
            cells.append(f"c{s['cagr']:+.1f}/d{s['maxdd']:.0f}/s{s['sharpe']:.2f}")
        print(f"  {topn:4d}  " + "  ".join(cells))

    print("\n=== cap=1.0 (no leverage) ===")
    print(header)
    for topn in topns:
        cells = []
        for v in vols:
            r = run(mo, topn=topn, target_vol=v, cap=1.0, cost_side=0.001)
            s = stats(r, "")
            cells.append(f"c{s['cagr']:+.1f}/d{s['maxdd']:.0f}/s{s['sharpe']:.2f}")
        print(f"  {topn:4d}  " + "  ".join(cells))

    # no-vt reference
    for topn in topns:
        r = run(mo, topn=topn, target_vol=0.0, cap=1.0, cost_side=0.001)
        s = stats(r, "")
        print(f"\nno-vt top{topn}: c{s['cagr']:+.1f}%/yr vol {s['vol']}% DD {s['maxdd']}% sr {s['sharpe']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
