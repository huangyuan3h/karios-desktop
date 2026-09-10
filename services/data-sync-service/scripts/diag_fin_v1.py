"""V1 diagnostic (P0-12): slow value — pre-registered, single hypothesis.

Hypothesis H-V1: the P18 value composite (EP/BP/SP/FCF equal-weight rank)
predicts 120/250-trading-day forward returns; top-decile buy-hold beats the
equal-weight cohort. Rationale: value is a 12-month factor — P18 tested it
at the wrong (60d) horizon. This is a NEW-strategy probe (slow sleeve), NOT
an S-3 gate: benchmark is equal-weight buy-hold, never the S-3 baseline.

PASS bar to incubate a standalone sleeve (frozen before seeing numbers):
  1. pooled size-neutral mean RankIC(comp, fwd120) > 0
  2. pooled size-neutral mean RankIC(comp, fwd250) > 0
  3. pooled D10-D1(comp, fwd250) > 0 in >= 60% of quarters
  4. top-decile fwd250 beats equal-weight cohort mean in >= 60% of quarters
All four, else CLOSE with no grid search. No weight tuning (equal-weight
legs and 0.5/0.5 carried over frozen from P18).

Boundary: 250d forward prices end 2026-09 → last full cohort ~2025Q1.

Usage:
    PYTHONPATH=src python3 scripts/diag_fin_v1.py
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
        load_mv_map,
        load_price_map,
        load_trade_calendar,
        mv_asof,
        shift_return,
        value_panel,
    )

    panel = value_panel()
    base = panel[~panel["is_fin"]].copy()
    legs = ["n_income_attr_p_sq_ttm", "total_revenue_sq_ttm",
            "total_hldr_eqy_inc_min_int", "free_cashflow_sq_ttm"]
    full = base[[c for c in legs]].notna().all(axis=1)
    print(f"4-leg coverage: {full.sum()}/{len(base)} = {full.mean():.1%}")
    panel = base[full].copy()

    cal, idx_of = load_trade_calendar()
    px = load_price_map()
    tmv = load_mv_map("total_mv")
    cmv = load_mv_map("circ_mv")
    panel["sig_date"] = panel["ann_date"].dt.strftime("%Y-%m-%d").map(
        lambda a: next((d for d in cal if d >= a), None)
    )
    panel = panel[panel["sig_date"].notna()].copy()
    panel["tmv"] = [mv_asof(tmv, cal, idx_of, t, d)
                    for t, d in zip(panel["ts_code"], panel["sig_date"])]
    panel["cmv"] = [mv_asof(cmv, cal, idx_of, t, d)
                    for t, d in zip(panel["ts_code"], panel["sig_date"])]
    panel = panel[panel["tmv"].notna() & (panel["tmv"] > 0)].copy()
    panel["EP"] = panel["n_income_attr_p_sq_ttm"] / panel["tmv"]
    panel["BP"] = panel["total_hldr_eqy_inc_min_int"] / panel["tmv"]
    panel["SP"] = panel["total_revenue_sq_ttm"] / panel["tmv"]
    panel["FCFP"] = panel["free_cashflow_sq_ttm"] / panel["tmv"]
    for h in (120, 250):
        panel[f"fwd{h}"] = [shift_return(px, cal, idx_of, t, d, h)
                            for t, d in zip(panel["ts_code"], panel["sig_date"])]

    rows = []
    for end, g in panel.groupby("end_date"):
        g = g[g["fwd250"].notna() & g["fwd120"].notna() & g["cmv"].notna()].copy()
        if len(g) < 250:
            continue
        for c in ("EP", "BP", "SP", "FCFP"):
            lo, hi = g[c].quantile([0.01, 0.99])
            g[c + "w"] = g[c].clip(lo, hi)
        g["comp"] = g[[c + "w" for c in ("EP", "BP", "SP", "FCFP")]].rank().mean(axis=1)
        g["size_q"] = pd.qcut(g["cmv"], 5, labels=False, duplicates="drop")
        if g["size_q"].nunique() < 5:
            continue
        i120, i250, spreads, tops, ews = [], [], [], [], []
        for _, b in g.groupby("size_q"):
            if len(b) < 50:
                continue
            a = _spearman(b["comp"].tolist(), b["fwd120"].tolist())
            c = _spearman(b["comp"].tolist(), b["fwd250"].tolist())
            b = b.copy()
            b["d"] = pd.qcut(b["comp"], 10, labels=False, duplicates="drop")
            dm = b.groupby("d")["fwd250"].mean()
            if len(dm) == 10:
                spreads.append(dm.iloc[-1] - dm.iloc[0])
                tops.append(dm.iloc[-1])
                ews.append(b["fwd250"].mean())
            if a is not None:
                i120.append(a)
            if c is not None:
                i250.append(c)
        if not i120 or not i250 or not spreads:
            continue
        rows.append({
            "end": str(end)[:10], "n": len(g),
            "pIC120": round(sum(i120) / len(i120), 4),
            "pIC250": round(sum(i250) / len(i250), 4),
            "pD10-D1": round(sum(spreads) / len(spreads), 2),
            "top-vs-ew": round(sum(tops) / len(tops) - sum(ews) / len(ews), 2),
        })
    res = pd.DataFrame(rows)
    print(res.to_string(index=False))
    ok1 = (res["pIC120"].mean() > 0) if len(res) else False
    ok2 = (res["pIC250"].mean() > 0) if len(res) else False
    ok3 = ((res["pD10-D1"] > 0).mean() >= 0.6) if len(res) else False
    ok4 = ((res["top-vs-ew"] > 0).mean() >= 0.6) if len(res) else False
    print(f"\nquarters={len(res)} mIC120={res['pIC120'].mean():.4f} "
          f"mIC250={res['pIC250'].mean():.4f} posD={(res['pD10-D1'] > 0).mean():.0%} "
          f"topBeatEW={(res['top-vs-ew'] > 0).mean():.0%}")
    print(f"PASS bar: IC120>0 [{ok1}]  IC250>0 [{ok2}]  D-spread>=60% [{ok3}]  top>EW>=60% [{ok4}]")
    print("VERDICT:", "INCUBATE sleeve" if (ok1 and ok2 and ok3 and ok4) else "CLOSE direction, no grid")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
