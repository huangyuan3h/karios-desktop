"""A1 diagnostic (P0-12): aggregate earnings regime — pre-registered, single hypothesis.

Hypothesis H-A1: market-wide attributable-NI TTM YoY (monthly PiT snapshots)
predicts CSI300 forward 60-session returns (allocation-layer regime variable,
NOT a stock-selection gate — different game from F/G/P18/V).

Method: each month-end M (A-share session), per stock take latest announced
single-quarter NI (ann <= M), sum its trailing-4-quarter TTM; aggregate YoY
= agg_ttm(M)/agg_ttm(M-12M) - 1 (both PiT). Forward: 000300.SH fwd60/fwd120.

PASS bar to open regime-gate work (frozen; n~60 monthly points, weak-evidence
bar disclosed): corr(agg_yoy, fwd60) > 0.2 AND Q4-Q1(fwd60) > 0.
120d descriptive only. Else CLOSE.

Usage:
    PYTHONPATH=src python3 scripts/diag_fin_a1.py
"""

from __future__ import annotations

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


def _pearson(xs: list[float], ys: list[float]) -> float | None:
    n = len(xs)
    if n < 10:
        return None
    mx, my = sum(xs) / n, sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    dx = math.sqrt(sum((x - mx) ** 2 for x in xs))
    dy = math.sqrt(sum((y - my) ** 2 for y in ys))
    return num / (dx * dy) if dx and dy else None


def main() -> int:
    import pandas as pd

    from data_sync_service.db import get_connection
    from data_sync_service.service.fin_panel import ni_sq_panel

    ni = ni_sq_panel()
    ni["ann_s"] = ni["ann_date"].dt.strftime("%Y-%m-%d")
    ni["end_s"] = ni["end_date"].dt.strftime("%Y-%m-%d")
    with get_connection() as conn:
        idx = pd.read_sql(
            "SELECT trade_date, close FROM index_daily WHERE ts_code='000300.SH' "
            "ORDER BY trade_date", conn, parse_dates=["trade_date"],
        )
    idx["d"] = idx["trade_date"].dt.strftime("%Y-%m-%d")
    idx = idx[idx["close"] > 0].reset_index(drop=True)
    closes = dict(zip(idx["d"], idx["close"].astype(float)))
    cal = idx["d"].tolist()
    pos = {d: i for i, d in enumerate(cal)}
    # month-ends that are A-share sessions
    months = sorted({d[:7] for d in cal})
    snaps = []
    for m in months:
        ds = [d for d in cal if d.startswith(m)]
        if ds:
            snaps.append(ds[-1])
    snaps = [s for s in snaps if "2021-01-01" <= s <= "2026-09-01"]
    # per-stock ascending (end, ann, ni) for pointer walk
    per_ts: dict[str, list[tuple[str, str, float]]] = {}
    for r in ni.itertuples():
        v = r.n_income_attr_p_sq
        if v is None or v != v:
            continue
        per_ts.setdefault(str(r.ts_code), []).append((r.end_s, r.ann_s, float(v)))
    for v in per_ts.values():
        v.sort()

    def agg_ttm(snap: str) -> float | None:
        total, n = 0.0, 0
        for rows in per_ts.values():
            known = [x for x in rows if x[1] <= snap]
            if len(known) < 4:
                continue
            total += sum(x[2] for x in known[-4:])
            n += 1
        return total if n >= 1000 else None

    agg = [(s, agg_ttm(s)) for s in snaps]
    agg = [(s, v) for s, v in agg if v]
    print(f"snapshots={len(agg)} range={agg[0][0]}..{agg[-1][0]}")
    rows = []
    for k, (s, v) in enumerate(agg):
        back = [(s2, v2) for s2, v2 in agg[:k]
                if s2[:4] == str(int(s[:4]) - 1) and s2[4:] == s[4:]]
        if not back:
            continue
        yoy = v / back[-1][1] - 1
        i = pos.get(s)
        if i is None:
            continue
        f60 = f120 = None
        if i + 60 < len(cal):
            c1 = closes.get(cal[i + 60])
            if c1:
                f60 = c1 / closes[s] - 1
        if i + 120 < len(cal):
            c1 = closes.get(cal[i + 120])
            if c1:
                f120 = c1 / closes[s] - 1
        if f60 is not None:
            rows.append({"m": s, "yoy": round(yoy * 100, 2),
                         "fwd60": round(f60 * 100, 2),
                         "fwd120": round(f120 * 100, 2) if f120 is not None else None})
    res = pd.DataFrame(rows)
    print(res.to_string(index=False))
    g = res[res["fwd60"].notna()].copy()
    corr = _pearson(g["yoy"].tolist(), g["fwd60"].tolist())
    g["q"] = pd.qcut(g["yoy"], 4, labels=False, duplicates="drop")
    qm = g.groupby("q")["fwd60"].mean()
    spread = (qm.iloc[-1] - qm.iloc[0]) if len(qm) == 4 else None
    g2 = res[res["fwd120"].notna()].copy()
    corr120 = _pearson(g2["yoy"].tolist(), g2["fwd120"].tolist())
    print(f"\nn={len(g)} corr60={corr:.3f} Q4-Q1={spread:.2f} corr120={corr120:.3f}")
    ok1 = (corr is not None and corr > 0.2)
    ok2 = (spread is not None and spread > 0)
    print(f"PASS bar: corr60>0.2 [{ok1}]  Q4-Q1>0 [{ok2}] (n~{len(g)}, weak-evidence bar)")
    print("VERDICT:", "OPEN regime work" if (ok1 and ok2) else "CLOSE direction, no grid")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
