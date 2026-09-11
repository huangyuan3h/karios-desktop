"""D1 forward diagnostic (P0-12): hot-rank attention — descriptive only.

Hypothesis H-D1: hotter stocks (smaller rank) earn higher 20/60d forward
returns (slow attention diffusion). DESCRIPTIVE verdict only — single window
(valid, 2026-03+) can never clear the Live bar per discipline; a pass opens
daily incremental collection + re-test in a year, nothing more.

Method: weekly Wednesday snapshots (A-share sessions), rank -> cross-sectional
hotness percentile (1 = hottest). Battery: RankIC(hot, fwd20/60), Q5-Q1,
top100 vs universe mean. Overlap with mom60 re-checked on full panel.

Pass (descriptive): IC20>0 AND IC60>0 AND Q-spread60>0 -> collect daily.
Else shelve D1.

Usage:
    PYTHONPATH=src python3 scripts/diag_hot_rank_fwd.py
"""

from __future__ import annotations

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


def _spearman(xs, ys):
    n = len(xs)
    if n < 10:
        return None

    def ranks(v):
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

    rx, ry = ranks(list(xs)), ranks(list(ys))
    mx, my = sum(rx) / n, sum(ry) / n
    num = sum((rx[i] - mx) * (ry[i] - my) for i in range(n))
    dx = math.sqrt(sum((x - mx) ** 2 for x in rx))
    dy = math.sqrt(sum((y - my) ** 2 for y in ry))
    return num / (dx * dy) if dx and dy else None


def main() -> int:
    import pandas as pd

    from data_sync_service.db import get_connection
    from data_sync_service.service.fin_panel import (
        load_price_map,
        load_trade_calendar,
        shift_return,
    )

    with get_connection() as conn:
        rank = pd.read_sql(
            "SELECT ts_code, trade_date, rank FROM cn_hot_rank", conn,
            parse_dates=["trade_date"],
        )
    rank["d"] = rank["trade_date"].dt.strftime("%Y-%m-%d")
    cal, idx_of = load_trade_calendar()
    # A-share sessions only (mixed-calendar trap, V2 §3)
    with get_connection() as conn:
        ad = pd.read_sql(
            "SELECT DISTINCT trade_date FROM daily WHERE ts_code LIKE '%.SH' "
            "OR ts_code LIKE '%.SZ' OR ts_code LIKE '%.BJ'", conn,
            parse_dates=["trade_date"],
        )["trade_date"].dt.strftime("%Y-%m-%d").tolist()
    aset = set(ad)
    px = load_price_map()
    # weekly Wednesday snapshots in valid window with forward room
    snaps = [d for d in sorted(aset)
             if "2026-03-01" <= d and pd.Timestamp(d).weekday() == 2]
    snaps = [s for s in snaps if idx_of.get(s, 0) + 60 < len(cal)]
    print(f"snapshots={len(snaps)} {snaps[0] if snaps else None}..{snaps[-1] if snaps else None}")
    rmap = {(t, d): r for t, d, r in zip(rank["ts_code"], rank["d"], rank["rank"])}
    rows = []
    for s in snaps:
        day = [(t, r) for (t, d), r in rmap.items() if d == s and r and r > 0]
        if len(day) < 1000:
            continue
        hot = {t: 1.0 - (r - 1) / 6000.0 for t, r in day}
        f20, f60, mom = {}, {}, {}
        for t in hot:
            a = shift_return(px, cal, idx_of, t, s, 20)
            b = shift_return(px, cal, idx_of, t, s, 60)
            c = shift_return(px, cal, idx_of, t, s, -60)
            if a is not None and b is not None and c is not None:
                f20[t], f60[t], mom[t] = a, b, c
        if len(f60) < 1000:
            continue
        ts = list(f60)
        ic20 = _spearman([hot[t] for t in ts], [f20[t] for t in ts])
        ic60 = _spearman([hot[t] for t in ts], [f60[t] for t in ts])
        mc = _spearman([hot[t] for t in ts], [mom[t] for t in ts])
        q = pd.qcut([hot[t] for t in ts], 5, labels=False, duplicates="drop")
        qm = pd.Series([f60[t] for t in ts]).groupby(q).mean()
        spread = (qm.iloc[-1] - qm.iloc[0]) if len(qm) == 5 else None
        top = sorted(ts, key=lambda t: -hot[t])[:100]
        rows.append({"snap": s, "n": len(ts),
                     "IC20": round(ic20, 4) if ic20 is not None else None,
                     "IC60": round(ic60, 4) if ic60 is not None else None,
                     "Q5-Q1": round(spread, 3) if spread is not None else None,
                     "top100": round(sum(f60[t] for t in top) / len(top), 2),
                     "univ": round(sum(f60.values()) / len(f60), 2),
                     "corr_mom": round(mc, 3) if mc is not None else None})
    res = pd.DataFrame(rows)
    print(res.to_string(index=False))
    ok1 = (res["IC20"].mean() > 0) if len(res) else False
    ok2 = (res["IC60"].mean() > 0) if len(res) else False
    ok3 = ((res["Q5-Q1"] > 0).mean() >= 0.6) if len(res) else False
    print(f"\nweeks={len(res)} mIC20={res['IC20'].mean():.4f} "
          f"mIC60={res['IC60'].mean():.4f} posQ={(res['Q5-Q1'] > 0).mean():.0%} "
          f"max|corr|={res['corr_mom'].abs().max():.3f}")
    print(f"Descriptive bar: IC20>0 [{ok1}]  IC60>0 [{ok2}]  Q>=60% [{ok3}]")
    print("VERDICT:", "COLLECT daily" if (ok1 and ok2 and ok3) else "SHELVE D1")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
