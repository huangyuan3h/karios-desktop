"""Backfill watchlist_score_daily for dates before the score table's start
(2026-06-18) — OPT-071.

Smart universe selection (not the full ~8600-stock market):

- CN symbols that appeared >= min_appearances times in TV screener snapshots
  captured before ``end_date`` (they were "selected/noticed" back then), plus
- all CN symbols already in watchlist_score_daily (they have score history),
- all CN symbols in the current watchlist_registry.

Score computation reuses the LIVE pure function ``_trendok_one`` with bars
truncated at each target date (as-of). No network calls: the HK index
on-demand fetch is patched to fail fast (CN traffic-light regime is what the
gate needs and it is computed from DB ``index_daily``).

Usage:
    .venv/bin/python scripts/backfill_watchlist_scores.py
    .venv/bin/python scripts/backfill_watchlist_scores.py --start 2026-03-02 --end 2026-06-17
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from bisect import bisect_right
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

# --- Patch: HK index on-demand fetch is network-only and fails offline;
# it does not affect the CN regime used by the backtest gate. -------------
import data_sync_service.service.market_regime as _mr  # noqa: E402

_mr.fetch_hk_index_on_demand = lambda series_id: ({}, None)  # type: ignore[assignment]

from data_sync_service.db import get_connection  # noqa: E402
from data_sync_service.db.daily import fetch_ohlcv_batch_between  # noqa: E402
from data_sync_service.db.index_daily import fetch_last_closes_upto  # noqa: E402
from data_sync_service.db.watchlist_automation import upsert_score_daily  # noqa: E402
from data_sync_service.service.market_regime import get_market_regime  # noqa: E402
from data_sync_service.service.trendok import (  # noqa: E402
    _build_industry_flow_context,
    _ema,
    _lookup_em_industry_boards,
    _lookup_stock_basic,
    _symbol_to_ts_code,
    _trendok_one,
)
from data_sync_service.service.watchlist_funnel_health import (  # noqa: E402
    _normalize_screener_symbol,
)

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("backfill_scores")


def _noop_resolve_stoploss(ts_code: str, newly_computed: float, as_of_date: str | None, *, is_held: bool, use_stored: bool = True):
    return newly_computed, False


def _load_calendar(start: str, end: str) -> list[str]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT trade_date FROM daily WHERE trade_date >= %s AND trade_date <= %s ORDER BY trade_date",
                (start, end),
            )
            return [str(r[0]) for r in cur.fetchall()]


def _load_universe(end: str, min_appearances: int) -> set[str]:
    """CN symbols noticed before ``end``: TV snapshot hits + score history + registry."""
    universe: set[str] = set()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT captured_at, payload
                FROM tv_screener_snapshots
                WHERE captured_at < %s
                """,
                (end + "T00:00:00",),
            )
            from collections import Counter

            counts: Counter[str] = Counter()
            for _captured_at, payload in cur.fetchall():
                rows = payload.get("rows") if isinstance(payload, dict) else None
                for r in rows or []:
                    if not isinstance(r, dict):
                        continue
                    sym = _normalize_screener_symbol(r.get("Ticker") or r.get("Symbol"))
                    if sym:
                        counts[sym] += 1
            for sym, n in counts.items():
                if sym.startswith("CN:") and n >= min_appearances:
                    universe.add(sym)
            cur.execute(
                "SELECT DISTINCT symbol FROM watchlist_score_daily WHERE symbol LIKE 'CN:%'"
            )
            for (sym,) in cur.fetchall():
                universe.add(str(sym))
            cur.execute(
                "SELECT DISTINCT symbol FROM watchlist_registry WHERE symbol LIKE 'CN:%'"
            )
            for (sym,) in cur.fetchall():
                universe.add(str(sym))
    return universe


def _index_20d(day: str) -> tuple[float | None, bool]:
    closes = fetch_last_closes_upto("000300.SH", day, days=25)
    if len(closes) < 21:
        return None, False
    ret = (closes[-1][1] - closes[-21][1]) / closes[-21][1] * 100.0
    ema = _ema([c for _, c in closes], 20)
    down = len(ema) >= 2 and ema[-1] < ema[-2]
    return ret, down


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill watchlist_score_daily (OPT-071)")
    parser.add_argument("--start", default="2026-03-02", help="First trade date to backfill (default 2026-03-02).")
    parser.add_argument("--end", default="2026-06-17", help="Last trade date to backfill (default 2026-06-17).")
    parser.add_argument("--min-appearances", type=int, default=2, help="Min TV snapshot appearances (default 2).")
    args = parser.parse_args()

    t0 = time.time()
    calendar = _load_calendar(args.start, args.end)
    universe = _load_universe(args.end, args.min_appearances)
    print(f"calendar days: {len(calendar)}  universe CN symbols: {len(universe)}")

    # Resolve symbols -> ts_codes via the live mapper, keep CN only.
    sym_to_ts: dict[str, str] = {}
    for sym in sorted(universe):
        parsed = _symbol_to_ts_code(sym)
        if parsed and parsed[0] == "CN":
            sym_to_ts[sym] = parsed[2]
    if not sym_to_ts:
        print("no CN symbols in universe; nothing to do")
        return
    print(f"resolved CN ts_codes: {len(sym_to_ts)}")

    # One big bar fetch for the whole window + lookback.
    from datetime import date, timedelta

    start = max(date.fromisoformat(args.start) - timedelta(days=240), date(2024, 1, 1)).isoformat()
    bars_by_ts = fetch_ohlcv_batch_between(sorted(set(sym_to_ts.values())), start, args.end)
    for _ts, bars in bars_by_ts.items():
        bars.sort(key=lambda b: str(b[0]))
    ts_dates: dict[str, list[str]] = {
        _ts: [str(b[0]) for b in _bars] for _ts, _bars in bars_by_ts.items()
    }
    print(f"bar coverage: {sum(len(b) for b in bars_by_ts.values())} rows for {len(bars_by_ts)} ts_codes")

    by_name, by_tushare = _lookup_stock_basic(list(sym_to_ts.values()))
    by_em = _lookup_em_industry_boards(list(sym_to_ts.values()))

    total_rows = 0
    rows_ge_70 = 0
    rows_ge_85 = 0
    scored = 0
    regime_counts: dict[str, int] = {}
    for day in calendar:
        day_start = time.time()
        flow_ctx = _build_industry_flow_context(day)
        regime = str(get_market_regime(as_of_date=day, include_breadth=False).get("regime") or "Unknown")
        regime_counts[regime] = regime_counts.get(regime, 0) + 1
        idx_ret, idx_ema_down = _index_20d(day)
        day_rows: list[dict] = []
        for sym, ts in sym_to_ts.items():
            dates = ts_dates.get(ts)
            if not dates:
                continue
            idx = bisect_right(dates, day)
            if idx < 60:
                continue  # not enough history as of that day (score needs EMA60+)
            bars = bars_by_ts[ts][:idx]
            industry = by_em.get(ts) or by_tushare.get(ts)
            row = _trendok_one(
                symbol=sym,
                name=by_name.get(ts),
                industry=industry,
                tushare_industry=by_tushare.get(ts),
                em_industry=by_em.get(ts),
                bars=bars,
                flow_ctx=flow_ctx,
                market_regime=regime,
                inst_summary=None,
                buy_seats_by_key=None,
                resolve_stoploss=_noop_resolve_stoploss,
                index_20d_ret=idx_ret,
                index_ema20_down=idx_ema_down,
                is_alpha_s=False,
                is_held=False,
            )
            score = row.get("score")
            if score is None:
                continue
            scored += 1
            total_rows += 1
            if score >= 70:
                rows_ge_70 += 1
            if score >= 85:
                rows_ge_85 += 1
            day_rows.append(
                {"symbol": sym, "trade_date": day, "score": float(score), "industry": industry}
            )
        if day_rows:
            upsert_score_daily(day_rows)
        print(
            f"  {day}  rows={len(day_rows):4d} regime={regime:9s} "
            f"({time.time() - day_start:.1f}s)"
        )
        sys.stdout.flush()

    print(f"\nregime distribution: {regime_counts}")
    print(
        f"done in {time.time() - t0:.0f}s: {total_rows} rows upserted "
        f"(score>=70: {rows_ge_70}, score>=85: {rows_ge_85}, scored cells: {scored})"
    )


if __name__ == "__main__":
    main()
