"""G2 diagnostic (P0-11): lev_safe, size-neutral — pre-registered, single hypothesis.

Hypothesis H-G2: within size quintiles, low-leverage (industry-relative 
debt-ratio) stocks have higher 60d forward returns (F4's weak raw signal was
possibly drowned by the size effect, e.g. 2024Q2 small-cap rally).

Method: per quarter cohort, split into circ_mv quintiles; RankIC and Q5-Q1
computed within each size bucket, pooled equal-weight across buckets.
Verdict on pooled numbers only — no bucket picking (that would be a grid).

PASS bar to open a replay (frozen before seeing numbers):
  1. pooled mean RankIC > 0
  2. pooled Q5-Q1 > 0 in >= 60% of quarters
  3. |corr| < 0.5 (pooled mom-corr; signal-size corr reported for context)
Otherwise the direction closes with no grid search.

PiT: ann_date; universe non-financials (same as F4).

Usage:
    PYTHONPATH=src python3 scripts/diag_fin_g2.py
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
        roe_ttm_panel,
        load_mv_map,
        load_price_map,
        load_trade_calendar,
        mv_asof,
        shift_return,
    )

    panel = roe_ttm_panel()
    panel = panel[~panel["is_fin"] & panel["roe_ttm"].notna()].copy()
    cal, idx_of = load_trade_calendar()
    px = load_price_map()
    mv = load_mv_map("circ_mv")
    panel["sig_date"] = panel["ann_date"].dt.strftime("%Y-%m-%d").map(
        lambda a: next((d for d in cal if d >= a), None)
    )
    panel = panel[panel["sig_date"].notna()].copy()
    panel["mv"] = [mv_asof(mv, cal, idx_of, t, d)
                   for t, d in zip(panel["ts_code"], panel["sig_date"])]
    panel["fwd60"] = [shift_return(px, cal, idx_of, t, d, 60)
                      for t, d in zip(panel["ts_code"], panel["sig_date"])]
    panel["mom60"] = [shift_return(px, cal, idx_of, t, d, -60)
                      for t, d in zip(panel["ts_code"], panel["sig_date"])]

    rows = []
    for end, g in panel.groupby("end_date"):
        g = g[g["fwd60"].notna() & g["mom60"].notna() & g["mv"].notna()].copy()
        if len(g) < 250:
            continue
        lo, hi = g["roe_ttm"].quantile([0.01, 0.99])
        g["sw"] = g["roe_ttm"].clip(lo, hi)
        g["size_q"] = pd.qcut(g["mv"], 5, labels=False, duplicates="drop")
        if g["size_q"].nunique() < 5:
            continue
        ics, spreads, mcs = [], [], []
        for _, b in g.groupby("size_q"):
            if len(b) < 50:
                continue
            ric = _spearman(b["sw"].tolist(), b["fwd60"].tolist())
            b = b.copy()
            b["q"] = pd.qcut(b["sw"], 5, labels=False, duplicates="drop")
            qm = b.groupby("q")["fwd60"].mean()
            sp = (qm.iloc[-1] - qm.iloc[0]) if len(qm) == 5 else None
            mc = _spearman(b["sw"].rank().tolist(), b["mom60"].rank().tolist())
            if ric is not None:
                ics.append(ric)
            if sp is not None:
                spreads.append(sp)
            if mc is not None:
                mcs.append(abs(mc))
        if not ics or not spreads:
            continue
        sc = _spearman(g["sw"].rank().tolist(), g["mv"].rank().tolist())
        rows.append({
            "end": str(end)[:10], "n": len(g),
            "pIC": round(sum(ics) / len(ics), 4),
            "pQ5-Q1": round(sum(spreads) / len(spreads), 3),
            "p|corr|": round(max(mcs), 3) if mcs else None,
            "corr_size": round(sc, 3) if sc is not None else None,
        })
    res = pd.DataFrame(rows)
    print(res.to_string(index=False))
    ok1 = (res["pIC"].mean() > 0) if len(res) else False
    ok2 = ((res["pQ5-Q1"] > 0).mean() >= 0.6) if len(res) else False
    ok3 = bool((res["p|corr|"] < 0.5).all()) if len(res) else False
    print(f"\nquarters={len(res)} pooledIC={res['pIC'].mean():.4f} "
          f"posQ={(res['pQ5-Q1'] > 0).mean():.0%} maxp|corr|={res['p|corr|'].max():.3f}")
    print(f"PASS bar: pIC>0 [{ok1}]  pQ>=60% [{ok2}]  |corr|<0.5 [{ok3}]")
    print("VERDICT:", "OPEN replay" if (ok1 and ok2 and ok3) else "CLOSE direction, no grid")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
