"""Verify HK daily adj consistency — Tencent qfq vs tushare raw mixing.

The HK daily table is written by two sources: tushare (raw, first-time
backfill) and Tencent ifzq (qfq-adjusted, incremental daily sync). If a
stock paid a dividend / split between the two segments, the qfq close
series is scaled and the boundary shows a jump that the raw series does
not have — this breaks trend/RS indicators in backtests.

Method: for a set of high-dividend HK tickers, pull the FULL Tencent qfq
history (paged) and diff close prices against the DB row-by-row. Any
mismatch date marks a raw-vs-qfq inconsistency boundary.

Usage:
    PYTHONPATH=src python3 scripts/hk_adj_consistency_check.py [--tickers 00823.HK,00005.HK] [--since 2023-01-01]
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta

sys.path.insert(0, "src")

from data_sync_service.service.hk_daily_tx import (  # noqa: E402
    _IDX_CLOSE,
    _IDX_DATE,
    _IDX_HIGH,
    _IDX_OPEN,
    _fetch_kline_page,
    _ts_code_to_tx,
)

# High-dividend / frequent corporate-action tickers are most likely to expose
# a raw-vs-qfq boundary jump.
_DEFAULT_TICKERS = [
    "00823.HK",  # Link REIT — pays dividends multiple times a year
    "00005.HK",  # HSBC — interim/final dividends
    "00884.HK",  # CIFI — high-yield, frequent actions
    "00011.HK",  # Hang Seng Bank
    "00027.HK",  # Galaxy Entertainment
    "00941.HK",  # China Mobile — big dividends
    "01810.HK",  # Xiaomi
    "02318.HK",  # Ping An
    "01398.HK",  # ICBC
    "00388.HK",  # HKEX
]


def _fetch_full_qfq(ts_code: str, since: date) -> dict[str, tuple[float, float, float]]:
    """Paged fetch of Tencent qfq bars on/after since → {date: (open, close, high)}."""
    symbol = _ts_code_to_tx(ts_code)
    if symbol is None:
        return {}
    out: dict[str, tuple[float, float, float]] = {}
    window_end = date.today()
    while True:
        rows = _fetch_kline_page(symbol, since, window_end, count=1000)
        if not rows:
            break
        for row in rows:
            raw_date = row[_IDX_DATE]
            try:
                d = date.fromisoformat(str(raw_date).strip())
            except ValueError:
                continue
            try:
                out[d.isoformat()] = (
                    float(row[_IDX_OPEN]),
                    float(row[_IDX_CLOSE]),
                    float(row[_IDX_HIGH]),
                )
            except (TypeError, ValueError):
                continue
        if len(rows) < 1000:
            break
        oldest = rows[0][_IDX_DATE]
        try:
            prev_day = date.fromisoformat(str(oldest)) - timedelta(days=1)
        except ValueError:
            break
        if prev_day < since:
            break
        window_end = prev_day
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tickers", help="comma-separated ticker list (default: sample set)")
    parser.add_argument("--since", default="2023-01-01", help="start date (default 2023-01-01)")
    args = parser.parse_args()

    tickers = [t.strip() for t in (args.tickers or ",".join(_DEFAULT_TICKERS)).split(",") if t.strip()]
    since = date.fromisoformat(args.since)

    import psycopg

    from data_sync_service.config import get_settings

    conn = psycopg.connect(get_settings().database_url)
    cur = conn.cursor()

    any_bad = False
    for ts_code in tickers:
        cur.execute(
            "SELECT trade_date, open, close, high FROM daily WHERE ts_code=%s AND trade_date>=%s ORDER BY trade_date",
            (ts_code, since.isoformat()),
        )
        db_rows = {(str(r[0])): (float(r[1]), float(r[2]), float(r[3])) for r in cur.fetchall()}
        tx_rows = _fetch_full_qfq(ts_code, since)
        if not db_rows or not tx_rows:
            print(f"{ts_code}: skip (db={len(db_rows)} tx={len(tx_rows)})")
            continue

        common = sorted(set(db_rows) & set(tx_rows))
        mismatches = []
        for d in common:
            db_open, db_close, db_high = db_rows[d]
            tx_open, tx_close, tx_high = tx_rows[d]
            for label, db_v, tx_v in (("open", db_open, tx_open), ("close", db_close, tx_close)):
                if abs(tx_v) < 1e-9:
                    continue
                diff_pct = abs(db_v - tx_v) / tx_v * 100.0
                if diff_pct > 0.01:
                    mismatches.append((d, label, db_v, tx_v, diff_pct))
                    break

        if mismatches:
            any_bad = True
            print(f"{ts_code}: db={len(db_rows)} common={len(common)} MISMATCH={len(mismatches)} days")
            for d, label, db_v, tx_v, diff_pct in mismatches[:8]:
                print(f"    {d} {label}: db={db_v:.4f} tx_qfq={tx_v:.4f} diff={diff_pct:.2f}%")
        else:
            print(f"{ts_code}: db={len(db_rows)} common={len(common)} consistent (no qfq boundary)")

    conn.close()
    print("\nRESULT:", "MIXED ADJ CONFIRMED — needs qfq re-seed" if any_bad else "all consistent")
    return 1 if any_bad else 0


if __name__ == "__main__":
    sys.exit(main())
