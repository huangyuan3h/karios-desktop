"""Reseed all HK daily bars from Tencent ifzq qfq (adjusted) prices.

Problem: the `daily` table for HK tickers mixes tushare RAW prices (first
backfill, dividend days show artificial gaps) with Tencent qfq (adjusted,
recent days only). Trend indicators (EMA/RSI/RS) in backtests misread
dividend gaps as price crashes → systematic signal distortion.

Fix: re-pull the FULL Tencent qfq series per HK ticker (paged, ~640 rows
per page, walking backwards) and upsert-OVERWRITE every bar on/after
`--since`. Existing rows are replaced (ON CONFLICT DO UPDATE), rows newer
than `--since` that Tencent does not serve are left untouched.

Pacing: 0.2s between tickers × ~2800 tickers ≈ 15–20 min for the full run.
Resume: `--limit` for smoke tests; interrupted runs restart per ticker
since writes are idempotent.

Usage:
    PYTHONPATH=src python3 scripts/hk_reseed_qfq.py --limit 20          # smoke test
    PYTHONPATH=src python3 scripts/hk_reseed_qfq.py --since 2022-06-01  # full sweep
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date, timedelta

sys.path.insert(0, "src")

from data_sync_service.db.daily import upsert_from_dataframe  # noqa: E402
from data_sync_service.service.hk_daily_tx import (  # noqa: E402
    _IDX_AMOUNT,
    _IDX_CLOSE,
    _IDX_DATE,
    _IDX_HIGH,
    _IDX_LOW,
    _IDX_OPEN,
    _IDX_VOL,
    _fetch_kline_page,
    _ts_code_to_tx,
)

_PAGE_SIZE = 640  # Tencent ifzq hard cap per request.
_AMOUNT_UNIT = 10_000.0
_DELAY_S = 0.2
_PROGRESS_EVERY = 100

_UPSERT_COLS = [
    "ts_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "change",
    "pct_chg",
    "vol",
    "amount",
]


def fetch_full_qfq(ts_code: str, since: date) -> list[dict[str, object]]:
    """Paged Tencent qfq fetch for one ticker → upsert-ready dict list (ascending)."""
    symbol = _ts_code_to_tx(ts_code)
    if symbol is None:
        raise ValueError(f"cannot map {ts_code} to tencent symbol")
    raw_rows: list[list[object]] = []
    window_end = date.today()
    while True:
        rows = _fetch_kline_page(symbol, since, window_end, count=_PAGE_SIZE)
        if not rows:
            break
        raw_rows = list(rows) + raw_rows  # ascending; earlier pages prepend
        if len(rows) < _PAGE_SIZE:
            break
        oldest = rows[0][_IDX_DATE]
        try:
            prev_day = date.fromisoformat(str(oldest).strip()) - timedelta(days=1)
        except ValueError:
            break
        if prev_day < since:
            break
        window_end = prev_day

    out: list[dict[str, object]] = []
    prev_close: float | None = None
    for row in raw_rows:
        try:
            d = date.fromisoformat(str(row[_IDX_DATE]).strip())
            o = float(row[_IDX_OPEN])
            c = float(row[_IDX_CLOSE])
            h = float(row[_IDX_HIGH])
            lo = float(row[_IDX_LOW])
            v = float(row[_IDX_VOL])
            amt_raw = row[_IDX_AMOUNT]
            amt = float(amt_raw) * _AMOUNT_UNIT if amt_raw is not None else None
        except (TypeError, ValueError):
            continue
        pre_close = prev_close
        change_val = None
        pct_chg = None
        if pre_close is not None and pre_close != 0:
            change_val = round(c - pre_close, 6)
            pct_chg = round(change_val / pre_close * 100.0, 6)
        out.append({
            "ts_code": ts_code,
            "trade_date": d.isoformat(),
            "open": o,
            "high": h,
            "low": lo,
            "close": c,
            "pre_close": pre_close,
            "change": change_val,
            "pct_chg": pct_chg,
            "vol": v,
            "amount": amt,
        })
        prev_close = c
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since", default="2022-06-01", help="reseed start date (default 2022-06-01)")
    parser.add_argument("--limit", type=int, default=0, help="only reseed first N tickers (smoke test)")
    parser.add_argument("--tickers", help="comma-separated explicit ticker list")
    args = parser.parse_args()
    since = date.fromisoformat(args.since)

    import psycopg
    from data_sync_service.config import get_settings

    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
        if not tickers[0].endswith(".HK"):
            tickers = [f"{t}.HK" if t.isdigit() else t for t in tickers]
    else:
        conn = psycopg.connect(get_settings().database_url)
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT ts_code FROM daily WHERE ts_code LIKE '%.HK' ORDER BY ts_code")
        tickers = [r[0] for r in cur.fetchall()]
        conn.close()
    if args.limit and args.limit > 0:
        tickers = tickers[: args.limit]

    total_rows = 0
    failed: list[str] = []
    empty: list[str] = []
    for i, ts_code in enumerate(tickers, start=1):
        try:
            rows = fetch_full_qfq(ts_code, since)
        except Exception as exc:  # noqa: BLE001
            failed.append((ts_code, str(exc)))
            continue
        if not rows:
            empty.append(ts_code)
            continue
        import pandas as pd  # type: ignore[import-not-found]

        df = pd.DataFrame(rows, columns=_UPSERT_COLS)
        updated = upsert_from_dataframe(df)
        total_rows += updated
        if i % _PROGRESS_EVERY == 0:
            print(f"progress {i}/{len(tickers)} updated={total_rows} failed={len(failed)}", flush=True)
        if _DELAY_S > 0 and i < len(tickers):
            time.sleep(_DELAY_S)

    print(f"\nDONE: {len(tickers)} tickers, {total_rows} rows upserted")
    print(f"failed: {len(failed)}")
    for ts_code, err in failed[:10]:
        print(f"  {ts_code}: {err}")
    print(f"empty (no tencent data since {since}): {len(empty)}")
    if empty:
        print("  " + ", ".join(empty[:20]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
