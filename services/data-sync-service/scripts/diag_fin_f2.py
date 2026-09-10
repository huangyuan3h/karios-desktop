"""F2 diagnostic (P0-11): cash-conversion quality — pre-registered, single hypothesis.

Hypothesis H2: stocks with operating-cash-flow / net-income (TTM) > 0.5 have
higher 60d forward returns than low-conversion peers (profit is accruable,
cash is harder to fake — defensive gate candidate for S-3).

Primary metric: ccr (requires NI-TTM > 0; coverage loss reported, not hidden).
Secondary (descriptive only, no verdict weight): Sloan-style accruals.

PASS bar to open a replay (frozen before seeing numbers):
  1. mean quarterly RankIC(ccr, fwd60) > 0
  2. Q5-Q1(ccr) > 0 in >= 60% of quarters
  3. |corr(ccr rank, mom60 rank)| < 0.5 every quarter (RS-independence)
Otherwise the direction closes with no grid search.

PiT: knowable at max(income ann_date, cashflow ann_date); entry first trading
day after. Universe: non-financials. Horizons 20/60 trading days.

Usage:
    PYTHONPATH=src python3 scripts/diag_fin_f2.py
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
        cashconv_panel,
        load_price_map,
        load_trade_calendar,
        shift_return,
    )

    panel = cashconv_panel()
    base = panel[~panel["is_fin"]].copy()
    print(f"panel rows={len(base)} stocks={base['ts_code'].nunique()} "
          f"quarters={base['end_date'].nunique()}")
    ccr = base[base["ccr"].notna()].copy()
    print(f"ccr coverage: {len(ccr)}/{len(base)} = {len(ccr)/len(base):.1%} "
          f"(NI-TTM>0 filter loss reported per 覆盖律)")

    cal, idx_of = load_trade_calendar()
    px = load_price_map()
    ccr["sig_date"] = ccr["ann_date"].dt.strftime("%Y-%m-%d").map(
        lambda a: next((d for d in cal if d >= a), None)
    )
    ccr = ccr[ccr["sig_date"].notna()].copy()
    # winsorize 1/99 per cohort later; raw here
    ccr["fwd60"] = [shift_return(px, cal, idx_of, t, d, 60)
                    for t, d in zip(ccr["ts_code"], ccr["sig_date"])]
    ccr["mom60"] = [shift_return(px, cal, idx_of, t, d, -60)
                    for t, d in zip(ccr["ts_code"], ccr["sig_date"])]

    rows = []
    for end, g in ccr.groupby("end_date"):
        g = g[g["fwd60"].notna() & g["mom60"].notna()].copy()
        if len(g) < 100:
            continue
        lo, hi = g["ccr"].quantile([0.01, 0.99])
        g["ccrw"] = g["ccr"].clip(lo, hi)
        ric = _spearman(g["ccrw"].tolist(), g["fwd60"].tolist())
        g["q"] = pd.qcut(g["ccrw"], 5, labels=False, duplicates="drop")
        qm = g.groupby("q")["fwd60"].mean()
        spread = (qm.iloc[-1] - qm.iloc[0]) if len(qm) == 5 else None
        mc = _spearman(g["ccrw"].rank().tolist(), g["mom60"].rank().tolist())
        up = g[g["ccr"] > 0.5]["fwd60"].mean()
        dn = g[g["ccr"] <= 0.5]["fwd60"].mean()
        ga = g[g["accruals"].notna()].copy()
        rica = _spearman((-ga["accruals"]).tolist(), ga["fwd60"].tolist())
        rows.append({
            "end": str(end)[:10], "n": len(g),
            "rankIC60": round(ric, 4) if ric is not None else None,
            "Q5-Q1": round(spread, 3) if spread is not None else None,
            "corr_mom": round(mc, 3) if mc is not None else None,
            "ccr>0.5": round(up, 3), "ccr<=0.5": round(dn, 3),
            "IC_-accr": round(rica, 4) if rica is not None else None,
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
