"""P18 diagnostic (P0-11): value x momentum composite — pre-registered, single hypothesis.

Hypothesis H-P18: composite = 0.5 * value_rank + 0.5 * mom60_rank predicts
60d forward returns. value_rank = mean rank of EP / BP / SP / FCF-yield
(equal-weight, frozen — the spec's 0.3/0.7 variant is NOT run; zero-grid).
This is the literature-backed shape (quality/value + momentum, size-neutral),
unlike the weak-weak patchwork rejected in F/G rounds.

Honest boundary: no dividend-yield leg (no local dividend table).

PASS bar to open a replay (frozen before seeing numbers):
  1. pooled size-neutral mean RankIC(composite, fwd60) > 0
  2. pooled Q5-Q1(composite) > 0 in >= 60% of quarters
  3. |corr(value_rank, mom60_rank)| < 0.5 (value leg independent of momentum;
     composite-mom corr is mechanical ~0.7, reported only descriptively)
Otherwise the direction closes with no grid search. Mom-only IC reported
descriptively (is value adding anything?).

PiT: max ann_date of three statements; ratios use total_mv asof signal date.
Universe: non-financials, all four value legs non-null (coverage reported).

Usage:
    PYTHONPATH=src python3 scripts/diag_fin_p18.py
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
    print(f"panel rows={len(base)} stocks={base['ts_code'].nunique()}")
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
    panel["fwd60"] = [shift_return(px, cal, idx_of, t, d, 60)
                      for t, d in zip(panel["ts_code"], panel["sig_date"])]
    panel["mom60"] = [shift_return(px, cal, idx_of, t, d, -60)
                      for t, d in zip(panel["ts_code"], panel["sig_date"])]

    rows = []
    for end, g in panel.groupby("end_date"):
        g = g[g["fwd60"].notna() & g["mom60"].notna() & g["cmv"].notna()].copy()
        if len(g) < 250:
            continue
        for c in ("EP", "BP", "SP", "FCFP"):
            lo, hi = g[c].quantile([0.01, 0.99])
            g[c + "w"] = g[c].clip(lo, hi)
        g["value"] = g[[c + "w" for c in ("EP", "BP", "SP", "FCFP")]].rank().mean(axis=1)
        g["momr"] = g["mom60"].rank()
        g["comp"] = 0.5 * g["value"].rank() + 0.5 * g["momr"]
        g["size_q"] = pd.qcut(g["cmv"], 5, labels=False, duplicates="drop")
        if g["size_q"].nunique() < 5:
            continue
        ics, spreads, vcs = [], [], []
        mom_ics, val_ics = [], []
        for _, b in g.groupby("size_q"):
            if len(b) < 50:
                continue
            ric = _spearman(b["comp"].tolist(), b["fwd60"].tolist())
            b = b.copy()
            b["q"] = pd.qcut(b["comp"], 5, labels=False, duplicates="drop")
            qm = b.groupby("q")["fwd60"].mean()
            sp = (qm.iloc[-1] - qm.iloc[0]) if len(qm) == 5 else None
            vc = _spearman(b["value"].tolist(), b["mom60"].tolist())
            mi = _spearman(b["momr"].tolist(), b["fwd60"].tolist())
            vi = _spearman(b["value"].tolist(), b["fwd60"].tolist())
            if ric is not None:
                ics.append(ric)
            if sp is not None:
                spreads.append(sp)
            if vc is not None:
                vcs.append(abs(vc))
            if mi is not None:
                mom_ics.append(mi)
            if vi is not None:
                val_ics.append(vi)
        if not ics or not spreads:
            continue
        rows.append({
            "end": str(end)[:10], "n": len(g),
            "pIC": round(sum(ics) / len(ics), 4),
            "pQ5-Q1": round(sum(spreads) / len(spreads), 3),
            "p|v-m|": round(max(vcs), 3) if vcs else None,
            "momIC": round(sum(mom_ics) / len(mom_ics), 4) if mom_ics else None,
            "valIC": round(sum(val_ics) / len(val_ics), 4) if val_ics else None,
        })
    res = pd.DataFrame(rows)
    print(res.to_string(index=False))
    ok1 = (res["pIC"].mean() > 0) if len(res) else False
    ok2 = ((res["pQ5-Q1"] > 0).mean() >= 0.6) if len(res) else False
    ok3 = bool((res["p|v-m|"] < 0.5).all()) if len(res) else False
    print(f"\nquarters={len(res)} pooledIC={res['pIC'].mean():.4f} "
          f"posQ={(res['pQ5-Q1'] > 0).mean():.0%} maxp|v-m|={res['p|v-m|'].max():.3f} "
          f"momOnlyIC={res['momIC'].mean():.4f} valOnlyIC={res['valIC'].mean():.4f}")
    print(f"PASS bar: pIC>0 [{ok1}]  pQ>=60% [{ok2}]  |v-m|<0.5 [{ok3}]")
    print("VERDICT:", "OPEN replay" if (ok1 and ok2 and ok3) else "CLOSE direction, no grid")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
