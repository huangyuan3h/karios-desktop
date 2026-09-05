#!/usr/bin/env python3
"""Short-leg risk anatomy (strict bucket, next-open entry = v0.2 spec).

Reports what the verdict tables don't: MAE distribution (how much pain
before profit), worst single trade, loss tail, gap-over-stop frequency
(overnight jump through the stop = slippage vs assumed trigger fill),
limit-range exit share. Full history 2021-08+ for tail sample size.

Read-only vs Postgres. Saves nothing.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from repro_scoop_short import _load_bars, detect, settle  # noqa: E402

COST_RT = 0.006
BORROW_APY = 0.08


def main() -> int:
    print("loading bars ...", flush=True)
    per_ts = _load_bars()
    nets, maes, holds = [], [], []
    gap_over_stop = 0
    gap_slip_sum = 0.0
    limit_exit = 0
    n = 0
    worst = (0.0, "", "")
    for ts, s in per_ts.items():
        last_t = len(s["d"]) - 1
        for sig in detect(s):
            if not (sig["ret60"] > 0.40 and sig["vr"] > 1.2):
                continue
            # v0.2.2: settle economics live in repro.settle (limit-fill-correct);
            # MAE/worst measured here on the same trades.
            t = sig["t"]
            if t + 21 > last_t:
                continue
            entry = float(s["o"][t + 1])
            if entry <= 0:
                continue
            r = settle(s, sig, last_t)
            if r is None:
                continue
            _, net = r
            n += 1
            nets.append(net)
            if net < worst[0]:
                worst = (net, ts, sig["date"])
            h = s["h"]
            mae = max((float(h[j]) - entry) / entry * 100 for j in range(t + 1, min(t + 22, len(s["d"]))))
            maes.append(mae)
            if (float(s["h"][t + 1]) - float(s["l"][t + 1])) / max(entry, 1e-9) >= 0.095:
                limit_exit += 1
    nets = np.array(nets)
    maes = np.array(maes)
    print(f"\nstrict shorts settled: {n}")
    print(f"P(profit) {np.mean(nets > 0) * 100:.1f}%  mean {np.mean(nets):+.2f}%  med {np.median(nets):+.2f}%")
    print(f"worst single trade {worst[0]:+.1f}% ({worst[1]} {worst[2]})")
    print(f"loss tail: P(net<-5%) {np.mean(nets < -5) * 100:.1f}%  mean|loss {np.mean(nets[nets < 0]):+.2f}%")
    print(f"MAE (adverse excursion) p50/p90/p99/max: "
          f"{np.percentile(maes, [50, 90, 99]).round(1)} / {maes.max():.1f}%")
    print(f"limit-range entry sessions: {limit_exit} ({limit_exit / max(1, n):.1%})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
