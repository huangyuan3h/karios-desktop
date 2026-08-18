"""Reseed all CN A-share daily bars from Tencent ifzq fqkline (qfq) prices.

Problem (mirror of hk_reseed_qfq): the `daily` table for CN A-shares stores
tushare RAW prices — dividend/ex-right days show artificial gaps that trend
indicators (EMA/RSI/RS/stop-loss) misread as crashes. 2025-08-01+ audit:
4625 ex-div days across 3483 symbols, 794 days with a >=5% artificial drop
across 775 symbols — ~15% of the universe is polluted.

Fix: re-pull the FULL Tencent qfq series per CN A-share (paged, 640 rows
per page, walking backwards) and upsert-OVERWRITE every bar on/after
`--since`. Existing rows are replaced (ON CONFLICT DO UPDATE); rows newer
than `--since` that Tencent does not serve are left untouched.

Tencent CN kline row format differs from HK: [date, open, CLOSE, high, low,
vol] — CLOSE is at index 2 (HK: open, high, low, close). amount is not
served by the CN endpoint; the existing amount column is preserved.

Pacing: 0.15s between tickers × ~5200 tickers ≈ 30–50 min for the full run.
Resume: `--limit` for smoke tests; interrupted runs restart per ticker
since writes are idempotent.

Usage:
    PYTHONPATH=src python3 scripts/cn_reseed_qfq_tx.py --limit 5          # smoke test
    PYTHONPATH=src python3 scripts/cn_reseed_qfq_tx.py --tickers 300246,300750
    PYTHONPATH=src python3 scripts/cn_reseed_qfq_tx.py --since 2023-01-01 # full sweep
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from datetime import date, timedelta
from typing import Any
from urllib.request import Request

sys.path.insert(0, "src")

from data_sync_service.config import get_settings  # noqa: E402

_PAGE_SIZE = 640
_DELAY_S = 0.6
_PROGRESS_EVERY = 100
_KLINE_ENDPOINTS = [
    "https://ifzq.gtimg.cn/appstock/app/fqkline/get",
    "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get",
]

# Tencent CN kline row: [date, open, CLOSE, high, low, vol]
_IDX_DATE = 0
_IDX_OPEN = 1
_IDX_CLOSE = 2
_IDX_HIGH = 3
_IDX_LOW = 4
_IDX_VOL = 5

_A_SHARE_RE = re.compile(r"^(6\d{5}\.SH|(0|3)\d{5}\.SZ)$")


def _ts_code_to_tx(ts_code: str) -> str | None:
    """CN ts_code → Tencent symbol (sh600000 / sz000001)."""
    m = _A_SHARE_RE.match(ts_code)
    if not m:
        return None
    ticker = ts_code.split(".")[0]
    return f"sh{ticker}" if ts_code.endswith(".SH") else f"sz{ticker}"


def _fetch_kline_page(symbol: str, start: str, end: str, count: int) -> list[list[Any]]:
    import json
    import urllib.request
    from urllib.error import HTTPError

    # macOS Python reads the SYSTEM proxy (ClashX 127.0.0.1:7890) via
    # _scproxy regardless of env — its flaky node hangs requests. Force
    # direct connections for the Tencent kline host.
    direct_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    last_err: Exception | None = None
    for endpoint in _KLINE_ENDPOINTS:
        url = f"{endpoint}?param={symbol},day,{start},{end},{count},qfq"
        for attempt in range(3):
            try:
                req = Request(
                    url,
                    headers={
                        "User-Agent": "Mozilla/5.0",
                        "Referer": "https://gu.qq.com/",
                    },
                )
                with direct_opener.open(req, timeout=20) as resp:
                    payload = resp.read().decode("utf-8")
                data = json.loads(payload)
                node = (data.get("data") or {}).get(symbol) or {}
                rows = node.get("qfqday") or node.get("day") or []
                return rows
            except HTTPError as exc:
                last_err = exc
                if exc.code == 501:  # WAF / anti-bot — cool down and retry
                    time.sleep(20 * (attempt + 1))
                    continue
                break
            except Exception as exc:  # noqa: BLE001
                last_err = exc
                break
    raise last_err or RuntimeError(f"all endpoints failed for {symbol}")


def fetch_full_qfq(ts_code: str, since: str) -> list[dict[str, object]]:
    """Paged Tencent qfq fetch for one CN ticker → upsert-ready dicts (ascending)."""
    symbol = _ts_code_to_tx(ts_code)
    if symbol is None:
        raise ValueError(f"cannot map {ts_code} to tencent symbol")
    raw_rows: list[list[Any]] = []
    window_end = date.today()
    while True:
        rows = _fetch_kline_page(symbol, since, window_end.isoformat(), _PAGE_SIZE)
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
        if prev_day < date.fromisoformat(since):
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
        })
        prev_close = c
    return out


_UPSERT_SQL_OHLCV = """
INSERT INTO daily (ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (ts_code, trade_date) DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    pre_close = EXCLUDED.pre_close,
    change = EXCLUDED.change,
    pct_chg = EXCLUDED.pct_chg,
    vol = EXCLUDED.vol
"""


def upsert_ohlcv(rows: list[dict[str, object]]) -> int:
    """Overwrite OHLCV for one symbol's qfq bars; amount/adj_factor untouched."""
    from data_sync_service.db.daily import ensure_table

    ensure_table()
    params = [
        (
            str(r["ts_code"]),
            str(r["trade_date"]),
            float(r["open"]),
            float(r["high"]),
            float(r["low"]),
            float(r["close"]),
            r["pre_close"] if r["pre_close"] is not None else None,
            r["change"] if r["change"] is not None else None,
            r["pct_chg"] if r["pct_chg"] is not None else None,
            float(r["vol"]),
        )
        for r in rows
    ]
    if not params:
        return 0
    from data_sync_service.db import get_connection

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(_UPSERT_SQL_OHLCV, params)
        conn.commit()
    return len(params)


def already_qfq(ts_code: str, rows: list[dict[str, object]]) -> bool:
    """True when the WHOLE fetched qfq series already matches the DB.

    Comparing only the last few rows is WRONG: qfq(latest) == raw(latest),
    so a symbol whose dividend happened weeks ago looks "already adjusted"
    while its older rows are still raw (2026-08-11 incident: gap audit
    showed only 794→764 of the >=5% artificial drops removed).
    """
    from data_sync_service.db import get_connection

    if not rows:
        return False
    pairs = [(str(r["trade_date"]), float(r["close"])) for r in rows]
    dates = [p[0] for p in pairs]
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT trade_date, close FROM daily WHERE ts_code=%s AND trade_date = ANY(%s)",
                (ts_code, dates),
            )
            db = {str(r[0]): float(r[1]) for r in cur.fetchall()}
    return all(abs(db.get(d, -1) - c) < 0.01 for d, c in pairs)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--since", default="2023-01-01", help="reseed start date (default 2023-01-01)")
    parser.add_argument("--limit", type=int, default=0, help="only reseed first N tickers (smoke test)")
    parser.add_argument("--tickers", help="comma-separated explicit ticker list")
    args = parser.parse_args()
    since = date.fromisoformat(args.since)

    import psycopg

    if args.tickers:
        tickers = []
        for t in args.tickers.split(","):
            t = t.strip().upper()
            if not t:
                continue
            if "." not in t and t.isdigit() and len(t) == 6:
                t = f"{t}.SH" if t.startswith("6") else f"{t}.SZ"
            tickers.append(t)
    else:
        conn = psycopg.connect(get_settings().database_url)
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT ts_code FROM daily WHERE ts_code ~ '^(6\\d{5}\\.SH|(0|3)\\d{5}\\.SZ)$' ORDER BY ts_code"
        )
        tickers = [r[0] for r in cur.fetchall()]
        conn.close()
    if args.limit and args.limit > 0:
        tickers = tickers[: args.limit]
    print(f"tickers to reseed: {len(tickers)} since {since}")

    total_rows = 0
    stats_done = 0
    failed: list[tuple[str, str]] = []
    empty: list[str] = []
    t0 = time.time()
    for i, ts_code in enumerate(tickers, start=1):
        try:
            rows = fetch_full_qfq(ts_code, since.isoformat())
        except Exception as exc:  # noqa: BLE001
            failed.append((ts_code, str(exc)))
            continue
        if not rows:
            empty.append(ts_code)
            continue
        if already_qfq(ts_code, rows):
            stats_done += 1
        else:
            updated = upsert_ohlcv(rows)
            total_rows += updated
        if i % _PROGRESS_EVERY == 0:
            print(
                f"progress {i}/{len(tickers)} updated={total_rows} "
                f"done_before={stats_done} failed={len(failed)} elapsed={time.time() - t0:.0f}s",
                flush=True,
            )
        if _DELAY_S > 0 and i < len(tickers):
            time.sleep(_DELAY_S)

    print(f"\nDONE: {len(tickers)} tickers, {total_rows} rows upserted in {time.time() - t0:.0f}s")
    print(f"failed: {len(failed)}")
    for ts_code, err in failed[:10]:
        print(f"  {ts_code}: {err}")
    print(f"empty (no tencent data since {since}): {len(empty)}")
    if empty:
        print("  " + ", ".join(empty[:20]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
