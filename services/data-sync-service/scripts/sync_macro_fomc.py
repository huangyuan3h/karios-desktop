#!/usr/bin/env python3
"""Sync US FOMC rate decisions (akshare macro_bank_usa_interest_rate).

Output: data/macro/fomc.csv  date,rate,forecast,prev,action
  action = hike | cut | hold  (rate vs prev)

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/sync_macro_fomc.py
"""

from __future__ import annotations

from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "data" / "macro" / "fomc.csv"


def main() -> int:
    import akshare as ak

    df = ak.macro_bank_usa_interest_rate()
    df = df.rename(columns={"商品": "item", "日期": "date", "今值": "rate", "预测值": "forecast", "前值": "prev"})
    df["date"] = df["date"].astype(str)
    df = df.dropna(subset=["rate"])

    def act(r):
        if r["prev"] != r["prev"]:  # NaN
            return "hold"
        if r["rate"] > r["prev"]:
            return "hike"
        if r["rate"] < r["prev"]:
            return "cut"
        return "hold"

    df["action"] = df.apply(act, axis=1)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    df[["date", "rate", "forecast", "prev", "action"]].to_csv(OUT, index=False)
    sub = df[df["date"] >= "2021-01-01"]
    print(f"fomc rows={len(df)} (2021+: {len(sub)}) -> {OUT}")
    print("2021+ action counts:", dict(sub["action"].value_counts()))
    print("last date:", df["date"].max())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
