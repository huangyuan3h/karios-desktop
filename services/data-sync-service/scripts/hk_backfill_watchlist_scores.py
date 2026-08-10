"""Backfill HK watchlist_score_daily — HK parallel strategy line (2026-08-10).

Universe: HK tickers ranked by 60-day trading volume (top ``--top``) — a
zero-external-dependency proxy for the Hang Seng Composite (~500 names; the
official constituents API is not reliably reachable from this network).
Union with any HK symbols already in the watchlist registry.

Score: reuses the LIVE pure function ``_trendok_one`` with bars truncated at
each target date (as-of). No sector flow context: HK has no SW-L1/EM board
mapping, and the HK gate is regime-only (score + RS + HSI/HSTECH regime).

Regime: ``get_hk_regime`` — HSI/HSTECH traffic lights from ``macro_daily``
(source="db.macro_daily", fully offline). CN indexes do NOT drive HK gates.

Usage:
    .venv/bin/python scripts/hk_backfill_watchlist_scores.py
    .venv/bin/python scripts/hk_backfill_watchlist_scores.py --start 2024-08-01 --end 2026-08-07 --top 500
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from bisect import bisect_right
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_sync_service.db import get_connection  # noqa: E402
from data_sync_service.db.daily import fetch_ohlcv_batch_between  # noqa: E402
from data_sync_service.db.watchlist_automation import (  # noqa: E402
    list_registry,
    upsert_score_daily,
)
from data_sync_service.service.market_regime import get_hk_regime  # noqa: E402
from data_sync_service.service.trendok import _symbol_to_ts_code, _trendok_one  # noqa: E402

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("hk_backfill_scores")


def _load_hk_universe(end: str, top_n: int) -> list[tuple[str, str]]:
    """[(symbol, ts_code)] — HK vol top N + registry HK union."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ts_code FROM daily
                WHERE ts_code LIKE '%%.HK' AND vol > 0 AND trade_date >= %s
                GROUP BY ts_code ORDER BY sum(vol) DESC LIMIT %s
                """,
                (end, top_n),
            )
            ts_set = {r[0] for r in cur.fetchall()}
    finally:
        conn.close()
    for row in list_registry():
        sym = str(row.get("symbol") or "").upper()
        if sym.startswith("HK:"):
            parsed = _symbol_to_ts_code(sym)
            if parsed:
                ts_set.add(parsed[2])
    out = []
    for ts in sorted(ts_set):
        code = ts.split(".")[0].zfill(5)
        out.append((f"HK:{code}", ts))
    return out


def _load_hk_calendar(start: str, end: str) -> list[str]:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT trade_date FROM daily
                WHERE ts_code LIKE '%%.HK' AND trade_date >= %s AND trade_date <= %s
                ORDER BY trade_date
                """,
                (start, end),
            )
            return [str(r[0]) for r in cur.fetchall()]
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill HK watchlist_score_daily")
    parser.add_argument("--start", default="2024-08-01")
    parser.add_argument("--end", default="2026-08-07")
    parser.add_argument("--top", type=int, default=500)
    args = parser.parse_args()

    t0 = time.time()
    calendar = _load_hk_calendar(args.start, args.end)
    universe = _load_hk_universe(args.end, args.top)
    print(f"calendar days: {len(calendar)}  HK universe: {len(universe)}")
    if not universe:
        print("no HK universe; nothing to do")
        return

    from datetime import date, timedelta

    start = max(date.fromisoformat(args.start) - timedelta(days=240), date(2020, 1, 1)).isoformat()
    bars_by_ts = fetch_ohlcv_batch_between(sorted(ts for _sym, ts in universe), start, args.end)
    for _ts, bars in bars_by_ts.items():
        bars.sort(key=lambda b: str(b[0]))
    ts_dates: dict[str, list[str]] = {
        _ts: [str(b[0]) for b in _bars] for _ts, _bars in bars_by_ts.items()
    }
    print(f"bar coverage: {sum(len(b) for b in bars_by_ts.values())} rows for {len(bars_by_ts)} ts_codes")

    total_rows = 0
    scored = 0
    regime_counts: dict[str, int] = {}
    for day in calendar:
        day_start = time.time()
        regime = str(get_hk_regime(as_of_date=day).get("regime") or "Unknown")
        regime_counts[regime] = regime_counts.get(regime, 0) + 1
        day_rows: list[dict] = []
        for sym, ts in universe:
            dates = ts_dates.get(ts)
            if not dates:
                continue
            idx = bisect_right(dates, day)
            if idx < 60:
                continue  # not enough history as of that day (score needs EMA60+)
            bars = bars_by_ts[ts][:idx]
            row = _trendok_one(
                symbol=sym,
                name=None,
                industry=None,
                bars=bars,
                flow_ctx=None,
                market_regime=regime,
                inst_summary=None,
                buy_seats_by_key=None,
                resolve_stoploss=None,
                index_20d_ret=None,
                is_alpha_s=False,
                is_held=False,
            )
            score = row.get("score")
            if score is None:
                continue
            scored += 1
            total_rows += 1
            day_rows.append(
                {"symbol": sym, "trade_date": day, "score": float(score), "industry": None}
            )
        if day_rows:
            upsert_score_daily(day_rows)
        print(
            f"  {day}  rows={len(day_rows):4d} regime={regime:9s} "
            f"({time.time() - day_start:.1f}s)"
        )

    print(
        f"\ndone: {total_rows} rows, {scored} scores, regimes={regime_counts}, "
        f"wall={time.time() - t0:.0f}s"
    )


if __name__ == "__main__":
    main()
