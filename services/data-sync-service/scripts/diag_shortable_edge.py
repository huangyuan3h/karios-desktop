#!/usr/bin/env python3
"""Stage 2b gate: does the edge survive inside the shortable subset?

Frozen rule (see designs/hedge-twin-short-2026-09-05.md §6): PRIMARY subset
= row-exists shortable (same def as stage 2). PASS iff BOTH halves
(2023-2024 vs 2025-01~2025-08) show P(profit) >= 80% AND mean net > +5%.
rqmcl>0-only is DESCRIPTIVE (no gate). Entry = next open (v0.2 replay spec),
cost 0.6% + (borrow ignored at this gate; replay adds 8% annual).

Read-only vs Postgres. Saves nothing.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from audit_shortable_overlap import _load_margin, _shortable  # noqa: E402
from repro_scoop_short import _load_bars, detect  # noqa: E402


def main() -> int:
    print("loading bars + margin ...", flush=True)
    per_ts = _load_bars()
    margin = _load_margin()
    halves: dict[str, dict[str, list]] = {
        "2023-2024": {"all": [], "shortable": [], "rqmcl": []},
        "2025H1-H2": {"all": [], "shortable": [], "rqmcl": []},
    }
    for ts, s in per_ts.items():
        last_t = len(s["d"]) - 1
        rows = margin.get(ts)
        for sig in detect(s):
            if not (sig["ret60"] > 0.40 and sig["vr"] > 1.2):
                continue
            d = sig["date"]
            if d < "2023-01-01" or d > "2025-08-31":
                continue
            t = sig["t"]
            if t + 21 > last_t:
                continue
            entry = float(s["o"][t + 1])
            if entry <= 0:
                continue
            target, stop = sig["target"], sig["stop"]
            h, l, c = s["h"], s["l"], s["c"]
            done = None
            for j in range(t + 2, t + 22):
                ht = l[j] <= target
                hs = h[j] >= stop
                if ht and hs:
                    done = (entry - stop) / entry * 100 - 0.6
                    break
                if ht:
                    done = (entry - target) / entry * 100 - 0.6
                    break
                if hs:
                    done = (entry - stop) / entry * 100 - 0.6
                    break
            if done is None:
                done = (entry - c[t + 21]) / entry * 100 - 0.6
            half = "2023-2024" if d < "2025-01-01" else "2025H1-H2"
            halves[half]["all"].append(done)
            ok, rq = _shortable(rows, d) if rows else (False, False)
            if ok:
                halves[half]["shortable"].append(done)
            if rq:
                halves[half]["rqmcl"].append(done)
    print("\n| half | all P/n/mean | shortable P/n/mean | rqmcl>0 P/n/mean |")
    print("|------|--------------|--------------------|--------------------|")
    gate_ok = True
    for half in ("2023-2024", "2025H1-H2"):
        cells = []
        for k in ("all", "shortable", "rqmcl"):
            v = halves[half][k]
            p = float(np.mean([1.0 if x > 0 else 0.0 for x in v])) * 100 if v else 0.0
            m = float(np.mean(v)) if v else 0.0
            cells.append(f"{p:.1f}%/{len(v)}/{m:+.2f}")
            if k == "shortable" and not (p >= 80.0 and m > 5.0):
                gate_ok = False
        print(f"| {half} | " + " | ".join(cells) + " |")
    print()
    print("STAGE-2b VERDICT:", "PASS (edge survives shortable)" if gate_ok else "FAIL — close v0.2 too")
    return 0 if gate_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
