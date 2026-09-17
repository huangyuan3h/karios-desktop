#!/usr/bin/env python3
"""H-TAIL-1 pre-registered diagnostic: buy shrinking-volume down days at the
close, sell next open; lower-shadow pattern arm.

Signal T (T-data only): D = close < prev_close; V = vol < vol_ma20 (20d);
P = lower-shadow share (min(open,close)-low)/(high-low) >= 0.5 with high > low.
Arms: A0 = D&V, A1 = D&V&P, control C = D&~V (same execution).
Execution: buy T close -> sell T+1 open, 30bp round trip, 5-trading-day
per-symbol cooldown. Eras 2010-2013/2014-2017/2018-2021/2022-2026.

Known optimistic bias (declared in prereg): limit-down opens that cannot be
sold are still priced at the open. A negative result is therefore conclusive;
a positive one would need stk_limit handling before any further claim.

Read-only, zero grid.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/diag_tail_reversal.py --save-report
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg

ROOT = Path(__file__).resolve().parents[1]
REPORT_DIR = ROOT / "data" / "backtest_reports"
COST = 0.0030
ERAS = [("2010-2013", 2010, 2013), ("2014-2017", 2014, 2017),
        ("2018-2021", 2018, 2021), ("2022-2026", 2022, 2026)]
COOLDOWN = 5
SHADOW_MIN = 0.5


def _features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.reset_index(drop=True).sort_values(["ts_code", "trade_date"]).reset_index(drop=True)
    g = df.groupby("ts_code", sort=False)
    df["prev_close"] = g["close"].shift(1)
    df["ret1"] = df["close"] / df["prev_close"] - 1.0
    df["vol_ma20"] = g["vol"].rolling(20, min_periods=20).mean().reset_index(level=0, drop=True)
    rng = df["high"] - df["low"]
    df["shadow"] = (df[["open", "close"]].min(axis=1) - df["low"]) / rng.replace(0.0, np.nan)
    df["D"] = df["ret1"] < 0
    df["V"] = df["vol"] < df["vol_ma20"]
    df["P"] = (df["shadow"] >= SHADOW_MIN) & (rng > 0) & df["shadow"].notna()
    df["sig_a0"] = df["D"] & df["V"]
    df["sig_a1"] = df["D"] & df["V"] & df["P"]
    df["sig_c"] = df["D"] & ~df["V"]
    df["open_next"] = g["open"].shift(-1)
    df["r_gross"] = df["open_next"] / df["close"] - 1.0
    df["r_net"] = df["r_gross"] - COST
    return df


def _dedup(df: pd.DataFrame, col: str) -> pd.Series:
    kept = pd.Series(False, index=df.index)
    for _ts, sub in df[df[col]].groupby("ts_code", sort=False):
        last = -10**9
        for idx, pos in zip(sub.index, sub["pos"], strict=False):
            if pos >= last + COOLDOWN:
                kept.loc[idx] = True
                last = pos
    return kept


def _stats(s: pd.Series) -> dict:
    s = s.dropna()
    n = int(len(s))
    if not n:
        return {"n": 0, "mean": None, "win": None}
    return {
        "n": n,
        "mean": round(100 * float(s.mean()), 4),
        "win": round(100 * float((s > 0).mean()), 2),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    ap.add_argument("--out", default="tail_reversal.json")
    args = ap.parse_args()

    from data_sync_service.config import get_settings

    with psycopg.connect(get_settings().database_url) as conn:
        stocks = pd.read_sql(
            """
            SELECT d.ts_code, d.trade_date, d.open, d.high, d.low, d.close, d.vol, d.amount
            FROM daily d JOIN stock_basic sb ON sb.ts_code = d.ts_code
            WHERE d.trade_date >= '2010-01-01' AND d.vol IS NOT NULL AND d.vol > 0
              AND d.close IS NOT NULL AND d.open IS NOT NULL
              AND d.high IS NOT NULL AND d.low IS NOT NULL
              AND (d.ts_code LIKE '%.SH' OR d.ts_code LIKE '%.SZ') AND d.ts_code NOT LIKE '%.BJ'
              AND COALESCE(sb.market,'') NOT IN ('ETF','HK','北交所')
              AND sb.name NOT LIKE '%ST%' AND sb.name NOT LIKE '%退%'
              AND (sb.list_date IS NULL OR sb.list_date <= (d.trade_date - INTERVAL '400 days'))
              AND d.amount >= 30000
            ORDER BY d.ts_code, d.trade_date
            """,
            conn,
            dtype={"ts_code": str},
        )
    print(f"stock rows {len(stocks)}", flush=True)

    stocks["year"] = pd.to_datetime(stocks["trade_date"]).dt.year
    stocks = _features(stocks)
    stocks["pos"] = stocks.groupby("ts_code", sort=False).cumcount()
    # Drop rows whose exit (next open) is unavailable.
    stocks = stocks[stocks["open_next"].notna()].reset_index(drop=True)
    for col in ("sig_a0", "sig_a1", "sig_c"):
        stocks[col] = _dedup(stocks, col)

    result: dict = {
        "tag": "tail-reversal-2026-09-16",
        "prereg": "docs/designs/tail-reversal-prereg-2026-09-16.md",
        "cost": COST,
        "eras": {},
    }
    k1_marks, k2_marks = [], []
    for label, y0, y1 in ERAS:
        sub = stocks[(stocks["year"] >= y0) & (stocks["year"] <= y1)]
        a0 = sub[sub["sig_a0"]]["r_net"]
        a1 = sub[sub["sig_a1"]]["r_net"]
        c = sub[sub["sig_c"]]["r_net"]
        s0, s1, sc = _stats(a0), _stats(a1), _stats(c)
        gross0 = round(100 * float(a0.add(COST).mean()), 4) if len(a0) else None
        row = {"a0": s0, "a1": s1, "control": sc, "a0_gross": gross0,
               "a1_minus_a0": (round(s1["mean"] - s0["mean"], 4)
                               if s1["mean"] is not None and s0["mean"] is not None else None)}
        result["eras"][label] = row
        thin = s0["n"] < 200
        k1_marks.append(False if thin else bool(s0["mean"] is not None and s0["mean"] > 0))
        k2_marks.append(False if thin else bool(row["a1_minus_a0"] is not None and row["a1_minus_a0"] > 0))
        print(
            f"  {label:<10} a0 n={s0['n']:>6} mean={s0['mean']} win={s0['win']} (gross {gross0})"
            f" | a1 n={s1['n']:>6} mean={s1['mean']} win={s1['win']}"
            f" | ctrl n={sc['n']:>6} mean={sc['mean']}"
            f" | a1-a0 {row['a1_minus_a0']}{' THIN' if thin else ''}",
            flush=True,
        )

    full = {k: _stats(stocks[stocks[v]]["r_net"])
            for k, v in (("a0", "sig_a0"), ("a1", "sig_a1"), ("control", "sig_c"))}
    result["full"] = full
    k1 = sum(k1_marks) >= 3 and (full["a0"]["mean"] or -9) > 0
    k2 = (sum(k2_marks) >= 3 and full["a1"]["mean"] is not None
          and full["a0"]["mean"] is not None and full["a1"]["mean"] - full["a0"]["mean"] > 0)
    verdict = ("OPEN next档" if (k1 and k2)
               else "REJECT（不扫定义/不换形态）")
    result["verdict"] = {"k1": k1, "k1_marks": k1_marks, "k2": k2, "k2_marks": k2_marks,
                         "call": verdict}
    print("\n## H-TAIL-1 verdict")
    print(f"  full a0 {full['a0']} | a1 {full['a1']} | control {full['control']}")
    print(f"  K1 base>0 eras {k1_marks} -> {'pass' if k1 else 'FAIL'}"
          f" | K2 pattern增量 eras {k2_marks} -> {'pass' if k2 else 'FAIL'}")
    print(f"  => {verdict}")

    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        out = REPORT_DIR / args.out
        out.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str),
                       encoding="utf-8")
        print(f"\nsaved {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
