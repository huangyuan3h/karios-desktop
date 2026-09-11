"""CN hot-rank backfill — akshare stock_hot_rank_detail_em per A-share stock.

~366d history per stock (weekend rows included), ~2.5s/call. Reruns are
idempotent; fresh stocks (rows within `fresh_days`) are skipped for resume.
BJ stocks excluded (detail endpoint takes SH/SZ codes only).
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


def _load_root_env() -> None:
    root = Path(__file__).resolve().parents[3] / ".env"
    if not root.exists():
        return
    with open(root) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            if k in ("DATABASE_URL",):
                os.environ.setdefault(k, v.strip().strip('"').strip("'"))


def _retry(fn, tries=4, base=2.0):
    last = None
    for i in range(tries):
        try:
            return fn()
        except Exception as e:
            last = e
            if i < tries - 1:
                time.sleep(base * (i + 1))
    raise last  # type: ignore[misc]


def main() -> int:
    _load_root_env()
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--offset", type=int, default=0)
    ap.add_argument("--sleep", type=float, default=0.4)
    ap.add_argument("--fresh-days", type=int, default=3)
    ap.add_argument("--no-skip-fresh", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    log = logging.getLogger("hot_rank")

    import re

    import akshare as ak  # type: ignore[import-not-found]

    from data_sync_service.db import cn_hot_rank, get_connection

    cn_hot_rank.ensure_table()
    pat = re.compile(r"^(60[0138]|68[89]|00[0123]|30[01])\d{3}\.(SH|SZ)$")
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT ts_code FROM stock_basic WHERE ts_code LIKE '%.SH' "
                        "OR ts_code LIKE '%.SZ' ORDER BY ts_code")
            codes = [str(r[0]) for r in cur.fetchall() if r[0]]
    codes = [c for c in codes if pat.match(c)]
    codes = codes[args.offset:]
    if args.limit:
        codes = codes[:args.limit]
    log.info("universe=%d", len(codes))
    skipped = 0
    if not args.no_skip_fresh:
        cutoff = (date.today() - timedelta(days=args.fresh_days)).isoformat()
        fresh = cn_hot_rank.fresh_codes(cutoff)
        before = len(codes)
        codes = [c for c in codes if c not in fresh]
        skipped = before - len(codes)
        log.info("skipped_fresh=%d cutoff=%s", skipped, cutoff)

    done, failed, rows = 0, [], 0
    for i, ts_code in enumerate(codes):
        code, mkt = ts_code.split(".")
        sym = ("SZ" if mkt == "SZ" else "SH") + code
        try:
            df = _retry(lambda: ak.stock_hot_rank_detail_em(symbol=sym))
            batch = []
            if df is not None and not df.empty:
                for r in df.itertuples():
                    batch.append({
                        "ts_code": ts_code,
                        "trade_date": getattr(r, "时间", None),
                        "rank": getattr(r, "排名", None),
                        "new_fans": getattr(r, "新晋粉丝", None),
                        "iron_fans": getattr(r, "铁杆粉丝", None),
                    })
            if batch:
                rows += cn_hot_rank.upsert_rows(batch)
            done += 1
        except Exception as e:
            log.warning("hot_rank %s failed: %s", ts_code, str(e)[:120])
            failed.append(ts_code)
            time.sleep(2.0)
        if (i + 1) % 50 == 0:
            log.info("progress %d/%d done=%d rows=%d failed=%d",
                     i + 1, len(codes), done, rows, len(failed))
        time.sleep(args.sleep)
    print(f"universe={len(codes) + skipped} skipped={skipped} stocks={done} "
          f"rows={rows} failed={len(failed)}")
    if failed:
        print("failed sample:", failed[:10])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
