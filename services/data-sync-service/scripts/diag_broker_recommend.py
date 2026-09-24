#!/usr/bin/env python3
"""Read-only diagnostic: do monthly broker top-picks (券商金股) have forward edge?

Mechanism (written before running, per first-principles §四 S1):
  Broker monthly picks are a *published consensus attention* signal. Prior
  attention/text experiments in this repo all came back as "追高税"
  (cn_hot_rank SHELVE, research-coverage REJECT, alpha-radar REJECT).
  Predicted death: #2 共线/被注意力维度吸收, or #4 regime-不一致.

Test (no param scan, frozen once):
  - entry = first trading day of the pick month, open; exit = first trading
    day of the next month, open (executable next-open convention).
  - benchmark = equal-weight all CN A-share (.SH/.SZ, non-ST, non-BJ) same window.
  - report total/mean excess, hit rate, by year, and by consensus tier
    (# distinct brokers recommending the same stock in that month).

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/diag_broker_recommend.py
"""

from __future__ import annotations

import csv
import logging
import statistics
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_sync_service.db import get_connection  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DATA = Path(__file__).resolve().parents[1] / "data"
PICKS = DATA / "altdata" / "broker_recommend.csv"
INDEX_CODE = "000905.SH"  # 中证500


def _month_bounds() -> dict[str, tuple[str, str]]:
    """first/last trading day per YYYYMM from the index calendar."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT trade_date FROM index_daily WHERE ts_code = %s ORDER BY trade_date",
                (INDEX_CODE,),
            )
            days = [str(r[0])[:10] for r in cur.fetchall()]
    bounds: dict[str, tuple[str, str]] = {}
    for d in days:
        m = d[:7].replace("-", "")
        if m not in bounds:
            bounds[m] = (d, d)
        else:
            bounds[m] = (bounds[m][0], d)
    return bounds


def _next_month(m: str) -> str:
    y, mm = int(m[:4]), int(m[4:6])
    mm += 1
    if mm > 12:
        y, mm = y + 1, 1
    return f"{y:04d}{mm:02d}"


def main() -> int:
    bounds = _month_bounds()
    months = sorted(bounds)
    wanted_dates: set[str] = set()
    for m in months:
        wanted_dates.add(bounds[m][0])
        nm = _next_month(m)
        if nm in bounds:
            wanted_dates.add(bounds[nm][0])
    dates = sorted(wanted_dates)
    logger.info("months=%d anchor dates=%d", len(months), len(dates))

    # Load open prices on anchor dates for all CN A-shares (non-ST, non-BJ).
    prices: dict[str, dict[str, float]] = defaultdict(dict)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT d.ts_code, d.trade_date, d.open
                FROM daily d
                JOIN stock_basic b ON b.ts_code = d.ts_code
                WHERE d.trade_date = ANY(%s)
                  AND (d.ts_code LIKE %s OR d.ts_code LIKE %s)
                  AND b.name NOT LIKE %s
                """,
                (dates, "%.SH", "%.SZ", "%ST%"),
            )
            for ts, dt, op in cur.fetchall():
                if op is None or float(op) <= 0:
                    continue
                prices[ts][str(dt)[:10]] = float(op)

    picks: dict[str, list[tuple[str, int]]] = defaultdict(list)
    with PICKS.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            picks[row["month"]].append((row["ts_code"],))

    # benchmark return per month (equal-weight open->open)
    bench: dict[str, float] = {}
    for m in months:
        nm = _next_month(m)
        if nm not in bounds:
            continue
        d0, d1 = bounds[m][0], bounds[nm][0]
        rets = [
            prices[ts][d1] / prices[ts][d0] - 1.0
            for ts in prices
            if d0 in prices[ts] and d1 in prices[ts]
        ]
        if rets:
            bench[m] = statistics.fmean(rets)

    excess_by_month: dict[str, float] = {}
    excess_by_year: dict[str, list[float]] = defaultdict(list)
    excess_by_tier: dict[int, list[float]] = defaultdict(list)
    all_excess: list[float] = []
    all_picks = 0

    for m in months:
        nm = _next_month(m)
        if nm not in bounds or m not in bench:
            continue
        d0, d1 = bounds[m][0], bounds[nm][0]
        counts: dict[str, int] = defaultdict(int)
        for (ts,) in picks.get(m, []):
            counts[ts] += 1
        if not counts:
            continue
        month_ex: list[float] = []
        for ts, n in counts.items():
            if d0 in prices.get(ts, {}) and d1 in prices.get(ts, {}):
                r = prices[ts][d1] / prices[ts][d0] - 1.0
                ex = r - bench[m]
                month_ex.append(ex)
                all_excess.append(ex)
                excess_by_year[m[:4]].append(ex)
                excess_by_tier[min(n, 3)].append(ex)
                all_picks += 1
        if month_ex:
            excess_by_month[m] = statistics.fmean(month_ex)

    if not all_excess:
        logger.error("no overlap between picks and price data")
        return 1

    print("\n=== broker_recommend read-only diagnostic (open->open monthly excess vs EW A-share) ===")
    print(f"picks evaluated: {all_picks}  months: {len(excess_by_month)}")
    print(f"mean excess : {statistics.fmean(all_excess)*100:+.2f}%/month")
    print(f"median      : {statistics.median(all_excess)*100:+.2f}%")
    hit = sum(1 for x in all_excess if x > 0) / len(all_excess)
    print(f"hit rate    : {hit*100:.1f}%")
    print(f"months > 0  : {sum(1 for x in excess_by_month.values() if x>0)}/{len(excess_by_month)}")

    print("\nby year (mean excess / n):")
    for y in sorted(excess_by_year):
        v = excess_by_year[y]
        print(f"  {y}: {statistics.fmean(v)*100:+.2f}%  n={len(v)}")

    print("\nby consensus tier (# brokers picking the same stock):")
    for t in sorted(excess_by_tier):
        v = excess_by_tier[t]
        label = f"{t}+" if t == 3 else str(t)
        print(f"  tier {label}: {statistics.fmean(v)*100:+.2f}%  n={len(v)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())