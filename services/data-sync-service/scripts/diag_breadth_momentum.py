#!/usr/bin/env python3
"""Diagnostic: is the R-wide breadth gate's *rate of change* related to market up/down?

The satellite 14:30 gate is `breadth1430 > 0.5` (share of the day's universe whose
close is above its own MA20). Hypothesis: the gate's rate of change (Δbreadth)
relates to market direction.

We rebuild the daily close-basis breadth over the satellite universe (full A-share
minus ST / BJ / delisted; optionally mv-gated to match the engine from 2021) and
measure:
  1. contemporaneous corr(Δbreadth_t, r_t)
  2. predictive corr(Δbreadth_t, r_{t+1..t+5})   (leading signal?)
  3. forward return by Δbreadth quintile / sign
  4. gate state (b>0.5) and gate transitions vs forward returns

Read-only. Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/diag_breadth_momentum.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_sync_service.config import get_settings  # noqa: E402

INDEXES = {"000300.SH": "沪深300", "000905.SH": "中证500", "399006.SZ": "创业板指"}
HORIZONS = (1, 2, 3, 5, 10)


def _breadth_sql(*, mv_gated: bool) -> str:
    extra = ""
    if mv_gated:
        extra = (
            " AND EXISTS (SELECT 1 FROM stock_dailybasic b "
            "WHERE b.ts_code = d.ts_code AND b.trade_date::date = d.trade_date AND b.total_mv IS NOT NULL)"
        )
    return f"""
    SELECT t.trade_date,
           AVG(CASE WHEN t.close > t.ma20 THEN 1.0 ELSE 0.0 END) AS breadth,
           COUNT(*) AS n
    FROM (
        SELECT d.ts_code, d.trade_date, d.close,
               AVG(d.close) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date
                                  ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
               COUNT(*)     OVER (PARTITION BY d.ts_code ORDER BY d.trade_date
                                  ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS n20
        FROM daily d JOIN stock_basic sb ON sb.ts_code = d.ts_code
        WHERE d.ts_code NOT LIKE '%.BJ' AND d.ts_code NOT LIKE '%.HK'
          AND sb.delist_date IS NULL AND sb.name NOT LIKE '%ST%'{extra}
    ) t
    WHERE t.n20 = 20
    GROUP BY t.trade_date ORDER BY t.trade_date
    """


def _load_breadth(*, mv_gated: bool) -> list[tuple[str, float, int]]:
    with psycopg.connect(get_settings().database_url) as conn, conn.cursor() as cur:
        cur.execute("SET statement_timeout = 600000")
        cur.execute(_breadth_sql(mv_gated=mv_gated))
        return [(r[0].isoformat(), float(r[1]), int(r[2])) for r in cur.fetchall()]


def _load_index(ts: str) -> dict[str, float]:
    with psycopg.connect(get_settings().database_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT trade_date, close FROM index_daily WHERE ts_code=%s ORDER BY trade_date", (ts,))
        return {r[0].isoformat(): float(r[1]) for r in cur.fetchall()}


def _corr(a: np.ndarray, b: np.ndarray) -> float:
    if len(a) < 5 or a.std() == 0 or b.std() == 0:
        return float("nan")
    return float(np.corrcoef(a, b)[0, 1])


def _fwd_ret(px: dict[str, float], days: list[str], i: int, n: int) -> float | None:
    j = i + n
    if i < 0 or j >= len(days):
        return None
    a, b = px.get(days[i]), px.get(days[j])
    return (b / a - 1.0) if a and b and a > 0 else None


def _report(label: str, rows: list[tuple[str, float, int]]) -> None:
    if not rows:
        return
    dates = [d for d, _, _ in rows]
    b = np.array([v for _, v, _ in rows])
    db = np.diff(b, prepend=b[0])
    db[0] = np.nan
    print(f"\n{'='*84}\n{label}   days={len(rows)}  {dates[0]}..{dates[-1]}  "
          f"breadth mean={100*b.mean():.1f}%  min={100*b.min():.1f}% max={100*b.max():.1f}%")
    for ts, iname in INDEXES.items():
        px = _load_index(ts)
        same = np.full(len(dates), np.nan)
        for i in range(1, len(dates)):
            a, c = px.get(dates[i - 1]), px.get(dates[i])
            if a and c and a > 0:
                same[i] = c / a - 1.0
        print(f"  {iname}({ts})")
        m = ~np.isnan(db) & ~np.isnan(same)
        print(f"    contemporaneous corr(db_t, r_t)      = {_corr(db[m], same[m]):+.3f}")
        for n in HORIZONS:
            fwd = np.array([(_fwd_ret(px, dates, i, n) or np.nan) for i in range(len(dates))])
            mask = ~np.isnan(db) & ~np.isnan(fwd)
            print(f"    predictive     corr(db_t, r_t+{n:<2d})   = {_corr(db[mask], fwd[mask]):+.3f}  "
                  f"(corr(b_t,r_t+{n}) = {_corr(b[mask], fwd[mask]):+.3f})")
        # quintiles of db -> next-5d mean return
        fwd5 = np.array([(_fwd_ret(px, dates, i, 5) or np.nan) for i in range(len(dates))])
        mask = ~np.isnan(db) & ~np.isnan(fwd5)
        d2, f2 = db[mask], fwd5[mask]
        qs = np.quantile(d2, [0.2, 0.4, 0.6, 0.8])
        bins = np.digitize(d2, qs)
        print("    forward 5d return by Δbreadth quintile (Q1=most negative .. Q5=most positive):")
        for q in range(5):
            v = f2[bins == q]
            if len(v):
                print(f"      Q{q+1}: mean{100*v.mean():+6.2f}%  hit{100*np.mean(v>0):3.0f}%  n{len(v)}")
        # gate state
        for state, sel in (("b>0.5 (gate open)", b > 0.5), ("b<=0.5 (gate closed)", b <= 0.5)):
            v = fwd5[sel & ~np.isnan(fwd5)]
            if len(v):
                print(f"    {state:<22} fwd5 mean{100*v.mean():+6.2f}%  hit{100*np.mean(v>0):3.0f}%  n{len(v)}")
        # gate transitions
        cross_up = (b[1:] > 0.5) & (b[:-1] <= 0.5)
        cross_dn = (b[1:] <= 0.5) & (b[:-1] > 0.5)
        for name, sel in (("cross UP 0.5", cross_up), ("cross DOWN 0.5", cross_dn)):
            idxs = np.where(sel)[0] + 1
            v = [(_fwd_ret(px, dates, i, 5) or np.nan) for i in idxs]
            v = [x for x in v if not np.isnan(x)]
            if v:
                print(f"    {name:<16} fwd5 mean{100*np.mean(v):+6.2f}%  hit{100*np.mean(np.array(v)>0):3.0f}%  n{len(v)}")


def main() -> int:
    print("computing close-basis breadth (full A-share ex-ST/BJ/delisted)...", flush=True)
    _report("breadth (full A-share, 2007+)", _load_breadth(mv_gated=False))
    print("\ncomputing mv-gated breadth (engine universe, 2021+)...", flush=True)
    _report("breadth (mv-gated = engine gate universe, 2021+)", _load_breadth(mv_gated=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
