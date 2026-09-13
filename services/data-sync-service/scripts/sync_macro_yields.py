#!/usr/bin/env python3
"""Sync CN/US treasury yields (akshare bond_zh_us_rate).

Output: data/macro/us_yields.csv  date,cn2y,cn10y,us2y,us10y

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/sync_macro_yields.py
"""

from __future__ import annotations

from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "data" / "macro" / "us_yields.csv"
COLMAP = {"日期": "date", "中国国债收益率2年": "cn2y", "中国国债收益率10年": "cn10y",
          "美国国债收益率2年": "us2y", "美国国债收益率10年": "us10y"}


def main() -> int:
    import akshare as ak

    df = ak.bond_zh_us_rate()
    df = df.rename(columns=COLMAP)
    keep = [c for c in COLMAP.values() if c in df.columns]
    df = df[keep].copy()
    df["date"] = df["date"].astype(str)
    df = df.dropna(subset=["us2y"])
    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT, index=False)
    print(f"rows={len(df)} {df['date'].min()}~{df['date'].max()} -> {OUT}")
    print("cols:", list(df.columns))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
