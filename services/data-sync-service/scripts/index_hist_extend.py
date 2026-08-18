#!/usr/bin/env python3
"""Backfill CN index history (index_daily) from Tencent kline for window extension.

index_daily only has 2023-01-03+ (tushare start). The walk-forward extension
to 2021-01 needs the 5 regime indices (SH 000001/000300/000688/000905, SZ
399006) before that date. Tencent kline serves full index history, same
direct-opener trick as cn_reseed_qfq_tx.py (macOS _scproxy → ClashX hang).

Usage:
  PYTHONPATH=src python3 scripts/index_hist_extend.py [--since 2021-01-01]
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.request
from datetime import date
from pathlib import Path
from urllib.request import Request

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_sync_service.db import get_connection  # noqa: E402

# ts_code → Tencent symbol (indices, no qfq)
_INDICES = {
    "000001.SH": "sh000001",
    "000300.SH": "sh000300",
    "000688.SH": "sh000688",
    "000905.SH": "sh000905",
    "399006.SZ": "sz399006",
}

_PAGE = 640
_KLINE_ENDPOINTS = [
    "https://ifzq.gtimg.cn/appstock/app/fqkline/get",
    "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get",
]

_INSERT_SQL = """
INSERT INTO index_daily (ts_code, trade_date, open, high, low, close, vol)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (ts_code, trade_date) DO UPDATE SET
    open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
    close = EXCLUDED.close, vol = EXCLUDED.vol
"""


def _fetch_all(symbol: str, since: str, end: str) -> list[list]:
    direct_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    rows: list[list] = []
    window_end = end
    while True:
        last_err = None
        for ep in _KLINE_ENDPOINTS:
            url = f"{ep}?param={symbol},day,{since},{window_end},{_PAGE},"
            try:
                req = Request(url, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://gu.qq.com/"})
                with direct_opener.open(req, timeout=20) as resp:
                    payload = resp.read().decode("utf-8")
                node = (json.loads(payload).get("data") or {}).get(symbol) or {}
                page = node.get("day") or node.get("qfqday") or []
                if not page:
                    return rows
                rows = list(page) + rows
                if len(page) < _PAGE:
                    return rows
                window_end = str(page[0][0])
                break
            except Exception as exc:  # noqa: BLE001
                last_err = exc
                time.sleep(2)
        else:
            raise last_err or RuntimeError(f"all endpoints failed for {symbol}")
        time.sleep(0.4)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--since", default="2021-01-01")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    since = date.fromisoformat(args.since)
    end = date.today().isoformat()

    total = 0
    for ts_code, sym in _INDICES.items():
        rows = _fetch_all(sym, since, end)
        print(f"{ts_code}: {len(rows)} rows  {rows[0][0]} .. {rows[-1][0]}", flush=True)
        if args.dry_run or not rows:
            continue
        params = []
        for r in rows:
            try:
                params.append(
                    (
                        ts_code,
                        str(r[0]),
                        float(r[1]), float(r[3]), float(r[4]), float(r[2]),
                        float(r[5]),
                    )
                )
            except (TypeError, ValueError, IndexError):
                continue
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(_INSERT_SQL, params)
            conn.commit()
        total += len(params)
        time.sleep(0.6)
    print(f"DONE: {total} index rows inserted (since {since})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
