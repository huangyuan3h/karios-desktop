#!/usr/bin/env python3
"""Build per-IPO post-listing performance from local `daily` (打新 sell returns).

For each new_share row, read daily bars from issue_date (listing day) and compute:
  d0_ret      : listing-day close / issue price - 1
  sell_ret    : 主板 -> first day close not at the +10% cap (开板); 注册制(30x/688)
                -> listing-day close (first 5 days uncapped)
  hold20_ret  : close on 20th session / price - 1
  sessions_to_board : sessions until 开板 (main board)

Output: data/ipo/ipo_perf.csv (gitignored).

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/build_ipo_perf.py
"""

from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parents[1]
IN = ROOT / "data" / "ipo" / "new_share.csv"
OUT = ROOT / "data" / "ipo" / "ipo_perf.csv"


def _board(code: str) -> str:
    p = code.split(".")[0]
    if p.startswith(("688", "689")):
        return "star"      # 科创 注册制
    if p.startswith(("300", "301")):
        return "chinext"   # 创业 注册制
    if p.startswith(("600", "601", "603", "605")):
        return "shmain"
    if p.startswith(("000", "001", "002", "003")):
        return "szmain"
    if p.startswith(("8", "4", "920", "92")):
        return "bj"
    return "other"


def main() -> int:
    from data_sync_service.config import get_settings

    ipos = list(csv.DictReader(IN.open()))
    codes = [r["ts_code"] for r in ipos]
    bars: dict[str, list[tuple[str, float, float]]] = defaultdict(list)
    with psycopg.connect(get_settings().database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ts_code, trade_date, close, pct_chg FROM daily "
                "WHERE ts_code = ANY(%s) ORDER BY ts_code, trade_date",
                (codes,),
            )
            for ts, d, c, p in cur.fetchall():
                if c is None:
                    continue
                bars[str(ts)].append((str(d), float(c), float(p) if p is not None else 0.0))

    rows_out = []
    miss = 0
    for r in ipos:
        ts = r["ts_code"]
        b = bars.get(ts) or []
        try:
            price = float(r["price"])
        except ValueError:
            miss += 1
            continue
        issue = r["issue_date"]
        iso = f"{issue[:4]}-{issue[4:6]}-{issue[6:8]}" if len(issue) == 8 else issue
        fwd = [x for x in b if x[0] >= iso][:45]
        if not fwd or price <= 0:
            miss += 1
            continue
        # guard: first bar must be near the true listing day (else daily coverage gap)
        from datetime import date as _date
        try:
            gap = (_date.fromisoformat(fwd[0][0]) - _date.fromisoformat(iso)).days
        except ValueError:
            gap = 99
        if gap > 10:
            miss += 1
            continue
        bd = _board(ts)
        reg = bd in ("star", "chinext")
        d0 = fwd[0]
        d0_ret = d0[1] / price - 1.0
        # sell return
        if reg:
            sell_ret, sess = d0_ret, 1
        else:
            sell_ret, sess = None, None
            for i, (d, c, p) in enumerate(fwd):
                if i == 0:
                    continue
                if p < 9.8:  # 开板 (no longer at +10% cap)
                    sell_ret, sess = c / price - 1.0, i + 1
                    break
            if sell_ret is None:  # never opened within window
                sell_ret, sess = fwd[-1][1] / price - 1.0, len(fwd)
        hold20 = (fwd[19][1] / price - 1.0) if len(fwd) >= 20 else None
        rows_out.append({
            "ts_code": ts, "name": r["name"], "board": bd,
            "ipo_date": r["ipo_date"], "issue_date": issue,
            "price": price, "ballot": r["ballot"], "limit_amount": r["limit_amount"],
            "funds": r["funds"], "list_date": d0[0],
            "d0_ret": round(d0_ret * 100, 2),
            "sell_ret": round(sell_ret * 100, 2),
            "sessions_to_board": sess,
            "hold20_ret": round(hold20 * 100, 2) if hold20 is not None else "",
        })

    with OUT.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows_out[0].keys()))
        w.writeheader()
        w.writerows(rows_out)
    print(f"ipos in={len(ipos)} out={len(rows_out)} missing={miss} -> {OUT}")
    neg = sum(1 for x in rows_out if float(x["sell_ret"]) < 0)
    print(f"sell_ret<0 (破发): {neg}/{len(rows_out)} = {100*neg/len(rows_out):.1f}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
