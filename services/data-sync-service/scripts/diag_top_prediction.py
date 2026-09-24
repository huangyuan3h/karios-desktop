#!/usr/bin/env python3
"""Diagnostic: can a top be predicted ("逃顶")? The 2026-09 oil case as the probe.

Two questions:
  A. Is the top even reachable in A-share hours? Decompose the drop into overnight
     gap (open/prev_close) vs intraday (close/open). QDII/commodity ETFs consume US
     overnight info; if the decline is all gap, no A-share-time rule can escape it.
  B. Does any observable predict a "top" (>=8% drawdown within 10 sessions)?
     Base rate + AUC + forward-return-by-quintile, no lookahead (predictors use <=t).

Read-only. Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/diag_top_prediction.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_sync_service.config import get_settings  # noqa: E402

ASSETS = {
    "518880.SH": "GOLD 黄金",
    "513350.SH": "OIL 油气QDII",
    "513110.SH": "NASDAQ 纳指",
    "511260.SH": "BOND10 国债",
    "000300.SH": "沪深300",
}
DD_THRESH = -0.08
HORIZON = 10


def _load(ts: str) -> dict:
    with psycopg.connect(get_settings().database_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT trade_date, open, high, low, close, amount FROM daily WHERE ts_code=%s ORDER BY trade_date",
            (ts,),
        )
        rows = cur.fetchall()
        if not rows:
            cur.execute(
                "SELECT trade_date, open, high, low, close, NULL FROM index_daily WHERE ts_code=%s ORDER BY trade_date",
                (ts,),
            )
            rows = cur.fetchall()
    return {
        "dates": [r[0].isoformat() for r in rows],
        "open": np.array([float(r[1]) if r[1] is not None else np.nan for r in rows]),
        "close": np.array([float(r[4]) if r[4] is not None else np.nan for r in rows]),
        "amount": np.array([float(r[5]) if r[5] is not None else np.nan for r in rows]),
    }


def _rsi(c: np.ndarray, n: int = 14) -> np.ndarray:
    d = np.diff(c, prepend=c[0])
    up = np.where(d > 0, d, 0.0)
    dn = np.where(d < 0, -d, 0.0)
    au = np.full_like(c, np.nan); ad = np.full_like(c, np.nan)
    if len(c) > n:
        au[n] = up[1:n + 1].mean(); ad[n] = dn[1:n + 1].mean()
        for i in range(n + 1, len(c)):
            au[i] = (au[i - 1] * (n - 1) + up[i]) / n
            ad[i] = (ad[i - 1] * (n - 1) + dn[i]) / n
    rs = au / np.where(ad == 0, np.nan, ad)
    return 100 - 100 / (1 + rs)


def _ma(c: np.ndarray, n: int) -> np.ndarray:
    out = np.full_like(c, np.nan)
    cs = np.cumsum(np.nan_to_num(c))
    for i in range(n - 1, len(c)):
        out[i] = (cs[i] - (cs[i - n] if i >= n else 0.0)) / n
    return out


def _auc(score: np.ndarray, label: np.ndarray) -> float:
    m = ~np.isnan(score)
    s, y = score[m], label[m]
    if y.sum() == 0 or y.sum() == len(y):
        return float("nan")
    r = np.argsort(np.argsort(s)) + 1
    n1, n0 = y.sum(), len(y) - y.sum()
    return (r[y == 1].sum() - n1 * (n1 + 1) / 2) / (n1 * n0)


def _gap_decomp(d: dict, label: str) -> None:
    c, o = d["close"], d["open"]
    n = len(c)
    gap = np.full(n, np.nan); intr = np.full(n, np.nan); tot = np.full(n, np.nan)
    for i in range(1, n):
        if c[i - 1] > 0 and o[i] > 0:
            gap[i] = o[i] / c[i - 1] - 1
            intr[i] = c[i] / o[i] - 1
            tot[i] = c[i] / c[i - 1] - 1
    # large down days
    big = tot <= -0.02
    print(f"  {label}: big-down days(<=-2%) n={int(big.sum())}  "
          f"gap mean={100*np.nanmean(gap[big]):+.2f}%  intraday mean={100*np.nanmean(intr[big]):+.2f}%  "
          f"|gap|/|total| share={100*np.nansum(np.abs(gap[big]))/(np.nansum(np.abs(gap[big]))+np.nansum(np.abs(intr[big]))):.0f}%")
    # the 2026-09 oil episode
    dates = d["dates"]
    if "2026-09-16" in dates:
        i0, i1 = dates.index("2026-09-16"), dates.index("2026-09-23")
        g = np.nansum(gap[i0 + 1:i1 + 1]); it = np.nansum(intr[i0 + 1:i1 + 1])
        print(f"      2026-09-16->09-23: total={100*(c[i1]/c[i0]-1):+.2f}%  "
              f"cum gap={100*g:+.2f}%  cum intraday={100*it:+.2f}%  gap share={100*abs(g)/(abs(g)+abs(it)):.0f}%")


def main() -> int:
    print("=" * 90)
    print("A. Gap vs intraday on big down days (can A-share hours escape?)")
    for ts, label in ASSETS.items():
        _gap_decomp(_load(ts), label)

    print("\n" + "=" * 90)
    print(f"B. Top predictability: top = >= {int(-DD_THRESH*100)}% drawdown within {HORIZON} sessions from close(t)")
    for ts, label in ASSETS.items():
        d = _load(ts)
        c = d["close"]
        n = len(c)
        ma20, ma60 = _ma(c, 20), _ma(c, 60)
        rsi = _rsi(c)
        ret = np.diff(c, prepend=c[0]) / np.where(c == 0, np.nan, c)
        vol20 = np.array([np.nanstd(ret[max(0, i - 19):i + 1]) for i in range(n)])
        std20 = np.array([np.nanstd(c[max(0, i - 19):i + 1]) for i in range(n)])
        amt20 = np.array([np.nanmean(d["amount"][max(0, i - 19):i + 1]) for i in range(n)])
        preds = {
            "mom20": np.array([c[i] / c[i - 20] - 1 if i >= 20 else np.nan for i in range(n)]),
            "mom60": np.array([c[i] / c[i - 60] - 1 if i >= 60 else np.nan for i in range(n)]),
            "rsi14": rsi,
            "dist_ma20": c / ma20 - 1,
            "dist_ma60": c / ma60 - 1,
            "pctb20": (c - (ma20 - 2 * std20)) / np.where(4 * std20 == 0, np.nan, 4 * std20),
            "vol20": vol20,
            "volratio": d["amount"] / np.where(amt20 == 0, np.nan, amt20),
        }
        dd_fwd = np.full(n, np.nan); fwd = np.full(n, np.nan)
        for i in range(n):
            j = min(i + HORIZON, n - 1)
            if j > i:
                dd_fwd[i] = np.nanmin(c[i + 1:j + 1]) / c[i] - 1
                fwd[i] = c[j] / c[i] - 1
        top = (dd_fwd <= DD_THRESH).astype(float)
        top[np.isnan(dd_fwd)] = np.nan
        base = np.nanmean(top)
        print(f"\n  {label} {ts}  n={n}  base rate of top={100*base:.1f}%")
        rows = []
        for name, p in preds.items():
            a = _auc(p, np.nan_to_num(top))
            m = np.isfinite(p) & np.isfinite(fwd)
            if m.sum() < 50:
                continue
            qs = np.nanquantile(p[m], [0.2, 0.4, 0.6, 0.8])
            bins = np.digitize(p[m], qs)
            qf = [np.nanmean(fwd[m][bins == q]) for q in range(5)]
            rows.append((name, a, qf))
        for name, a, qf in rows:
            qs = " ".join(f"{100*x:+5.1f}" for x in qf)
            print(f"    {name:<9} AUC={a:+.3f}  fwd10 by Q1..Q5: {qs}")
        # the oil 2026-09-16 peak row
        if "2026-09-16" in d["dates"]:
            i = d["dates"].index("2026-09-16")
            vals = {k: (v[i] if k in ("rsi14", "volratio") else 100 * v[i]) for k, v in preds.items()}
            print(f"    2026-09-16 peak values: " + ", ".join(f"{k}={v:+.1f}" for k, v in vals.items())
                  + f"  fwd10={100*fwd[i]:+.1f}%  dd_fwd={100*dd_fwd[i]:+.1f}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
