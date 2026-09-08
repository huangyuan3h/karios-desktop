#!/usr/bin/env python3
"""Research coverage event study (pre-reg: docs/designs/research-coverage-prereg-2026-09-07.md).

E1 first-coverage day per stock (created_at CST) -> next-open entry, 10/20d
market-relative forwards. E2 coverage intensity (>=2 vs 1 reports first week).
Excluded by construction: alpha_score (recomputed with today's close =
lookahead), ratings (94.5% bullish, zero sells), target upside (n=131 thin).
Read-only vs Postgres. Prints tables + K1/K2 verdict, saves nothing.
"""

from __future__ import annotations

import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_sync_service.config import get_settings  # noqa: E402

COSTS_ROUNDTRIP = 0.003
CUTOFF_LEFT_CENSORED = "2026-08-05"


def net_relative(gross: float, market: float) -> float:
    return (gross - COSTS_ROUNDTRIP) - market


def batch_clustered_mean(by_batch: dict[str, list[float]]) -> tuple[float, float, int]:
    means = [sum(v) / len(v) for v in by_batch.values() if v]
    n = len(means)
    if not n:
        return float("nan"), float("nan"), 0
    m = sum(means) / n
    var = sum((x - m) ** 2 for x in means) / n
    return m, (var / n) ** 0.5, n


def _conn():
    return psycopg.connect(get_settings().database_url)


def load_first_coverages() -> list[dict]:
    """First created-report day per stock + reports within 7 calendar days.

    First-week counts are computed from rows only (no future peeking beyond
    the birth week); total-count grouping would leak later coverage.
    """
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT stock_code, market,
                   (created_at AT TIME ZONE 'Asia/Shanghai')::date AS d
            FROM research_reports
            WHERE stock_code ~ '^[0-9]{6}$'
              AND market IN ('SHANGHAI', 'SHENZHEN')
            """
        )
        by_stock: dict[tuple[str, str], list[str]] = defaultdict(list)
        for code, market, d in cur.fetchall():
            by_stock[(str(code), str(market))].append(str(d))
    out = []
    for (code, market), days in by_stock.items():
        first = min(days)
        # birth-week window: date arithmetic on ISO strings is calendar-safe
        from datetime import date as _date

        d0 = _date.fromisoformat(first)
        n_wk1 = sum(
            1 for x in days if (_date.fromisoformat(x) - d0).days <= 6
        )
        out.append({"code": code, "market": market, "day": first, "count": n_wk1})
    return out


def load_sessions(start: str, end: str) -> list[str]:
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT cal_date FROM trade_calendar
            WHERE exchange = 'SSE' AND is_open = 1 AND cal_date BETWEEN %s AND %s
            ORDER BY cal_date
            """,
            (start, end),
        )
        return [str(r[0]) for r in cur.fetchall()]


def load_bars(ts_codes: list[str], start: str, end: str) -> dict[str, dict[str, tuple]]:
    out: dict[str, dict[str, tuple]] = defaultdict(dict)
    if not ts_codes:
        return out
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT ts_code, trade_date, open, close FROM daily
            WHERE ts_code = ANY(%s) AND trade_date BETWEEN %s AND %s
            """,
            (ts_codes, start, end),
        )
        for ts, td, o, c in cur.fetchall():
            if o and c:
                out[str(ts)][str(td)] = (float(o), float(c))
    return out


def market_mean(entry: str, exit_: str) -> float | None:
    with _conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT AVG(d2.close / d1.open - 1)
            FROM daily d1 JOIN daily d2
              ON d1.ts_code = d2.ts_code
             AND d1.trade_date = %s AND d2.trade_date = %s
             AND (d1.ts_code LIKE '%%.SH' OR d1.ts_code LIKE '%%.SZ')
            WHERE d1.open > 0 AND d2.close > 0
            """,
            (entry, exit_),
        )
        v = cur.fetchone()[0]
        return float(v) if v is not None else None


def to_ts(code: str, market: str) -> str:
    suffix = "SH" if market == "SHANGHAI" else "SZ"
    return f"{code}.{suffix}"


def evaluate(rows: list[dict], label: str) -> None:
    if not rows:
        print(f"{label}: n=0")
        return
    # Absolute calendar baseline so every birth gets full forward room.
    sessions = load_sessions("2026-06-01", date.today().isoformat())
    sidx = {d: i for i, d in enumerate(sessions)}
    rows = [r for r in rows if r["day"] in sidx]
    ts_codes = sorted({to_ts(r["code"], r["market"]) for r in rows})
    first_entry = sessions[sidx[min(r["day"] for r in rows)] + 1]
    bars = load_bars(ts_codes, first_entry, sessions[-1])
    mkt_cache: dict[tuple[str, str], float | None] = {}
    by_h: dict[int, list[float]] = defaultdict(list)
    by_h_batch: dict[int, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for r in rows:
        if r["day"] not in sidx or sidx[r["day"]] + 1 >= len(sessions):
            continue
        entry = sessions[sidx[r["day"]] + 1]
        b0 = bars.get(to_ts(r["code"], r["market"]), {}).get(entry)
        if not b0:
            continue
        for h in (10, 20):
            if sidx[r["day"]] + 1 + h >= len(sessions):
                continue  # per-horizon completeness (as-of safe)
            exit_ = sessions[sidx[r["day"]] + 1 + h]
            b1 = bars.get(to_ts(r["code"], r["market"]), {}).get(exit_)
            if not b1:
                continue
            key = (entry, exit_)
            if key not in mkt_cache:
                mkt_cache[key] = market_mean(entry, exit_)
            mkt = mkt_cache[key]
            if mkt is None:
                continue
            rel = net_relative(b1[1] / b0[0] - 1, mkt)
            by_h[h].append(rel)
            by_h_batch[h][r["day"]].append(rel)
    for h in (10, 20):
        v = by_h[h]
        if not v:
            print(f"{label} h={h}: n=0")
            continue
        hit = sum(1 for x in v if x > 0) / len(v) * 100
        bm, se, nb = batch_clustered_mean(by_h_batch[h])
        print(f"{label} h={h}: n={len(v)} mean={sum(v) / len(v) * 100:+.2f}% "
              f"hit={hit:.1f}% batches={nb} batch_mean={bm * 100:+.2f}% se={se * 100:.2f}")


def main() -> None:
    covs = load_first_coverages()
    print(f"stocks={len(covs)}")
    main_sample = [c for c in covs if c["day"] > CUTOFF_LEFT_CENSORED]
    print(f"--- E1 first coverage (excl left-censored {CUTOFF_LEFT_CENSORED}, "
          f"n={len(main_sample)}) ---")
    evaluate(main_sample, "E1")
    print("--- sensitivity: incl 08-05 cohort ---")
    evaluate(covs, "E1sens")
    hi = [c for c in main_sample if c["count"] >= 2]
    lo1 = [c for c in main_sample if c["count"] == 1]
    print(f"--- E2 intensity: >=2 (n={len(hi)}) vs ==1 (n={len(lo1)}) ---")
    evaluate(hi, "E2hi")
    evaluate(lo1, "E2lo")


if __name__ == "__main__":
    main()
