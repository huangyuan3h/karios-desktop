"""F4 diagnostic (P0-11): leverage safety — pre-registered, single hypothesis.

Hypothesis H4: stocks with below-industry-median debt ratios have higher
60d forward returns than high-leverage peers (fragility first dies in
bear/tight-credit regimes — defensive gate candidate for S-3).

Signal: lev_safe = industry-median debt ratio - own (higher = safer).
F3 (accruals) skipped: same source as F2 whose secondary metric showed no
incremental info (see fin-f2-cashconv-2026-09-10 §5).

PASS bar to open a replay (frozen before seeing numbers):
  1. mean quarterly RankIC(lev_safe, fwd60) > 0
  2. Q5-Q1(lev_safe) > 0 in >= 60% of quarters
  3. |corr(lev_safe rank, mom60 rank)| < 0.5 every quarter (RS-independence)
Otherwise the direction closes with no grid search.

PiT: ann_date; entry first trading day after. Universe: non-financials.
Horizons 20/60 trading days.

Usage:
    PYTHONPATH=src python3 scripts/diag_fin_f4.py
"""

from __future__ import annotations

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


def _spearman(xs: list[float], ys: list[float]) -> float | None:
    n = len(xs)
    if n < 10:
        return None

    def ranks(v: list[float]) -> list[float]:
        order = sorted(range(n), key=lambda i: v[i])
        r = [0.0] * n
        i = 0
        while i < n:
            j = i
            while j + 1 < n and v[order[j + 1]] == v[order[i]]:
                j += 1
            avg = (i + j) / 2 + 1
            for k in range(i, j + 1):
                r[order[k]] = avg
            i = j + 1
        return r

    rx, ry = ranks(xs), ranks(ys)
    mx, my = sum(rx) / n, sum(ry) / n
    num = sum((rx[i] - mx) * (ry[i] - my) for i in range(n))
    dx = math.sqrt(sum((rx[i] - mx) ** 2 for i in range(n)))
    dy = math.sqrt(sum((ry[i] - my) ** 2 for i in range(n)))
    return num / (dx * dy) if dx and dy else None


def main() -> int:
    import pandas as pd

    from data_sync_service.service.fin_panel import (
        leverage_panel,
        load_price_map,
        load_trade_calendar,
        shift_return,
    )

    panel = leverage_panel()
    panel = panel[~panel["is_fin"] & panel["lev_safe"].notna()].copy()
    print(f"panel rows={len(panel)} stocks={panel['ts_code'].nunique()} "
          f"quarters={panel['end_date'].nunique()}")

    cal, idx_of = load_trade_calendar()
    px = load_price_map()
    panel["sig_date"] = panel["ann_date"].dt.strftime("%Y-%m-%d").map(
        lambda a: next((d for d in cal if d >= a), None)
    )
    panel = panel[panel["sig_date"].notna()].copy()
    panel["fwd60"] = [shift_return(px, cal, idx_of, t, d, 60)
                      for t, d in zip(panel["ts_code"], panel["sig_date"])]
    panel["mom60"] = [shift_return(px, cal, idx_of, t, d, -60)
                      for t, d in zip(panel["ts_code"], panel["sig_date"])]

    rows = []
    for end, g in panel.groupby("end_date"):
        g = g[g["fwd60"].notna() & g["mom60"].notna()].copy()
        if len(g) < 100:
            continue
        lo, hi = g["lev_safe"].quantile([0.01, 0.99])
        g["sw"] = g["lev_safe"].clip(lo, hi)
        ric = _spearman(g["sw"].tolist(), g["fwd60"].tolist())
        g["q"] = pd.qcut(g["sw"], 5, labels=False, duplicates="drop")
        qm = g.groupby("q")["fwd60"].mean()
        spread = (qm.iloc[-1] - qm.iloc[0]) if len(qm) == 5 else None
        mc = _spearman(g["sw"].rank().tolist(), g["mom60"].rank().tolist())
        lo_r = g[g["below_ind"]]["fwd60"].mean()
        hi_r = g[~g["below_ind"]]["fwd60"].mean()
        rows.append({
            "end": str(end)[:10], "n": len(g),
            "rankIC60": round(ric, 4) if ric is not None else None,
            "Q5-Q1": round(spread, 3) if spread is not None else None,
            "corr_mom": round(mc, 3) if mc is not None else None,
            "lowLev": round(lo_r, 3), "highLev": round(hi_r, 3),
        })
    res = pd.DataFrame(rows)
    print(res.to_string(index=False))
    ok1 = (res["rankIC60"].mean() > 0) if len(res) else False
    ok2 = ((res["Q5-Q1"] > 0).mean() >= 0.6) if len(res) else False
    ok3 = bool((res["corr_mom"].abs() < 0.5).all()) if len(res) else False
    print(f"\nquarters={len(res)} meanIC={res['rankIC60'].mean():.4f} "
          f"posQ={(res['Q5-Q1'] > 0).mean():.0%} max|corr|={res['corr_mom'].abs().max():.3f}")
    print(f"PASS bar: IC>0 [{ok1}]  Q-spread>=60% [{ok2}]  |corr|<0.5 [{ok3}]")
    print("VERDICT:", "OPEN replay" if (ok1 and ok2 and ok3) else "CLOSE direction, no grid")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
