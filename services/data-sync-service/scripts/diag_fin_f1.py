"""F1 diagnostic (P0-11): ROE-TTM > industry median — pre-registered, single hypothesis.

Hypothesis H1: stocks with above-industry-median ROE-TTM have higher 60d
forward returns than below-median peers (defensive gate candidate for S-3).

PASS bar to open a replay (frozen before seeing numbers):
  1. mean quarterly RankIC(roe_ttm, fwd60) > 0
  2. Q5-Q1 quintile spread > 0 in >= 60% of quarters
  3. |corr(roe rank, mom60 rank)| < 0.5 every window (RS-independence)
Otherwise the direction closes with no grid search.

PiT: signal knowable at ann_date; entry at first trading day after ann_date.
Universe: non-financials (comp_type='1') with industry + prices. Horizons
20/60 trading days (slow variable; satellite 3d never touches this).

Usage:
    PYTHONPATH=src python3 scripts/diag_fin_f1.py
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

    from data_sync_service.db import get_connection
    from data_sync_service.service.fin_panel import roe_ttm_panel

    panel = roe_ttm_panel()
    panel = panel[~panel["is_fin"] & panel["roe_ttm"].notna()].copy()
    print(f"panel rows={len(panel)} stocks={panel['ts_code'].nunique()} "
          f"quarters={panel['end_date'].nunique()}")

    with get_connection() as conn:
        cal = pd.read_sql(
            "SELECT DISTINCT trade_date FROM daily ORDER BY trade_date", conn,
            parse_dates=["trade_date"],
        )["trade_date"].dt.strftime("%Y-%m-%d").tolist()
        idx_of = {d: i for i, d in enumerate(cal)}
        closes = pd.read_sql(
            "SELECT ts_code, trade_date, close FROM daily", conn,
            parse_dates=["trade_date"],
        )
    closes["trade_date"] = closes["trade_date"].dt.strftime("%Y-%m-%d")
    px: dict[str, dict[str, float]] = {}
    for ts, d, c in zip(closes["ts_code"], closes["trade_date"], closes["close"]):
        if c and c > 0:
            px.setdefault(ts, {})[d] = float(c)
    del closes

    def fwd(ts: str, base: str, horizon: int) -> float | None:
        i = idx_of.get(base)
        if i is None or i + horizon >= len(cal):
            return None
        days = px.get(ts)
        if not days:
            return None
        c0, c1 = days.get(base), days.get(cal[i + horizon])
        if not c0 or not c1:
            return None
        return (c1 / c0 - 1.0) * 100.0

    panel["sig_date"] = panel["ann_date"].dt.strftime("%Y-%m-%d").map(
        lambda a: next((d for d in cal if d >= a), None)
    )
    panel = panel[panel["sig_date"].notna()].copy()
    panel["fwd20"] = [fwd(t, d, 20) for t, d in zip(panel["ts_code"], panel["sig_date"])]
    panel["fwd60"] = [fwd(t, d, 60) for t, d in zip(panel["ts_code"], panel["sig_date"])]

    def back(ts: str, base: str, horizon: int) -> float | None:
        i = idx_of.get(base)
        if i is None or i - horizon < 0:
            return None
        days = px.get(ts)
        if not days:
            return None
        c0, c1 = days.get(cal[i - horizon]), days.get(base)
        if not c0 or not c1:
            return None
        return (c1 / c0 - 1.0) * 100.0

    panel["mom60"] = [back(t, d, 60) for t, d in zip(panel["ts_code"], panel["sig_date"])]

    rows = []
    for end, g in panel.groupby("end_date"):
        g = g[g["fwd60"].notna() & g["mom60"].notna()].copy()
        if len(g) < 100:
            continue
        ric = _spearman(g["roe_ttm"].tolist(), g["fwd60"].tolist())
        g["q"] = pd.qcut(g["roe_ttm"], 5, labels=False, duplicates="drop")
        qm = g.groupby("q")["fwd60"].mean()
        spread = (qm.iloc[-1] - qm.iloc[0]) if len(qm) == 5 else None
        mc = _spearman(
            g["roe_ttm"].rank().tolist(), g["mom60"].rank().tolist()
        )
        up = g[g["above_ind"]]["fwd60"].mean()
        dn = g[~g["above_ind"]]["fwd60"].mean()
        rows.append({
            "end": str(end)[:10], "n": len(g),
            "rankIC60": round(ric, 4) if ric is not None else None,
            "Q5-Q1": round(spread, 3) if spread is not None else None,
            "corr_mom": round(mc, 3) if mc is not None else None,
            "above": round(up, 3), "below": round(dn, 3),
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
