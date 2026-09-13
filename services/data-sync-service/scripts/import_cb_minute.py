#!/usr/bin/env python3
"""Aggregate the purchased CB 1-minute archive into daily CB bars.

Source (user-provided, stays on disk): ~/Downloads/1分钟_按月归档/<YYYY-MM>/<YYYYMMDD>_1min.zip
Each daily zip holds one CSV per convertible bond (early months hold only one
bond; by 2024-25 the full market ~300-400 bonds/day). CSV columns:
  时间,代码,名称,开盘价,收盘价,最高价,最低价,成交量,成交额,涨幅,振幅

Output: data/cb/cb_daily.csv (gitignored) with
  date,ts_code,open,high,low,close,volume,amount

Read-only w.r.t. the live system. Research input for the CB 条款/估值 line
(P0-13 B1); no DB table until/unless adopted.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/import_cb_minute.py --start 2024-06-01 --end 2026-08-07
"""

from __future__ import annotations

import argparse
import csv
import sys
import zipfile
from collections import defaultdict
from datetime import date
from pathlib import Path

DEFAULT_ROOT = Path.home() / "Downloads" / "1分钟_按月归档"
OUT_DIR = Path(__file__).resolve().parents[1] / "data" / "cb"
COLS = ("时间", "代码", "名称", "开盘价", "收盘价", "最高价", "最低价", "成交量", "成交额", "涨幅", "振幅")


def _iter_zips(root: Path, start: str, end: str):
    for month in sorted(p for p in root.iterdir() if p.is_dir()):
        for z in sorted(month.glob("*_1min.zip")):
            ds = z.stem.split("_")[0]
            if len(ds) != 8 or not ds.isdigit():
                continue
            d = f"{ds[:4]}-{ds[4:6]}-{ds[6:]}"
            if start <= d <= end:
                yield d, z


def _agg_csv(raw: bytes) -> tuple[float, float, float, float, float, float] | None:
    text = raw.decode("utf-8-sig", errors="replace")
    open_px = high = low = close = None
    vol = amt = 0.0
    for i, line in enumerate(text.splitlines()):
        if i == 0 and line.startswith("时间"):
            continue
        parts = line.split(",")
        if len(parts) < 9:
            continue
        try:
            o, c, h, lo = float(parts[3]), float(parts[4]), float(parts[5]), float(parts[6])
            v, a = float(parts[7] or 0), float(parts[8] or 0)
        except ValueError:
            continue
        if o > 0 and open_px is None:
            open_px = o
        if h > 0:
            high = h if high is None else max(high, h)
        if lo > 0:
            low = lo if low is None else min(low, lo)
        if c > 0:
            close = c  # last positive close
        vol += v
        amt += a
    if open_px is None or high is None or low is None or close is None:
        return None
    return open_px, high, low, close, vol, amt


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--root", default=str(DEFAULT_ROOT))
    ap.add_argument("--start", default="2024-06-01")
    ap.add_argument("--end", default="2026-08-07")
    ap.add_argument("--out", default="cb_daily.csv")
    args = ap.parse_args()
    root = Path(args.root).expanduser()
    if not root.is_dir():
        print(f"ERROR: archive root not found: {root}", file=sys.stderr)
        return 1

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUT_DIR / args.out
    total_rows = 0
    coverage: dict[str, set[str]] = defaultdict(set)
    n_zip = 0
    with out.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["date", "ts_code", "open", "high", "low", "close", "volume", "amount"])
        for d, z in _iter_zips(root, args.start, args.end):
            n_zip += 1
            with zipfile.ZipFile(z) as zf:
                for name in zf.namelist():
                    if not name.endswith(".csv"):
                        continue
                    ts = name[:-4]
                    agg = _agg_csv(zf.read(name))
                    if agg is None:
                        continue
                    w.writerow([d, ts, *agg])
                    total_rows += 1
                    coverage[d].add(ts)
            if n_zip % 50 == 0:
                print(f"  {n_zip} zips ... last {d} rows={total_rows}", flush=True)

    days = sorted(coverage)
    print(f"\ndone: {n_zip} daily zips, {total_rows} CB-day rows -> {out}")
    if days:
        by_year: dict[str, list[int]] = defaultdict(list)
        for d in days:
            by_year[d[:4]].append(len(coverage[d]))
        print("coverage (median bonds/day):")
        for y in sorted(by_year):
            v = sorted(by_year[y])
            print(f"  {y}: {len(v)} days, median {v[len(v) // 2]} bonds/day")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
