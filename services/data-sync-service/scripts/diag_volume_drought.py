#!/usr/bin/env python3
"""H-VOL-1 pre-registered diagnostic: volume drought after a long decline.

State D(t): close < MA200 and ret250 < 0. Drought V(t): vol(t) <= min(vol(t-249..t)).
Signal = first D&V day in a 20-trading-day cluster. Entry T+1 open, exit T+N close,
net 30bp. Control = same-state D days without a signal (same entry/exit).

Index eras 2005-2010/2011-2015/2016-2020/2021-2026; stock eras 2010-2013/2014-2017/
2018-2021/2022-2026. Prereg: docs/designs/volume-drought-prereg-2026-09-15.md
Read-only, zero grid.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/diag_volume_drought.py --save-report
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg

ROOT = Path(__file__).resolve().parents[1]
REPORT_DIR = ROOT / "data" / "backtest_reports"
COST = 0.0030
NS = (20, 60, 120)
INDEX_ERAS = [("2005-2010", 2005, 2010), ("2011-2015", 2011, 2015),
              ("2016-2020", 2016, 2020), ("2021-2026", 2021, 2026)]
STOCK_ERAS = [("2010-2013", 2010, 2013), ("2014-2017", 2014, 2017),
              ("2018-2021", 2018, 2021), ("2022-2026", 2022, 2026)]
INDEXES = ("000300.SH", "000001.SH", "000905.SH")
AMOUNT_FLOOR_QIAN = 30_000.0  # 0.3亿元
COOLDOWN = 20


def _features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.reset_index(drop=True).sort_values(["ts_code", "trade_date"]).reset_index(drop=True)
    g = df.groupby("ts_code", sort=False)
    df["ma200"] = g["close"].rolling(200, min_periods=200).mean().reset_index(level=0, drop=True)
    df["ret250"] = g["close"].transform(lambda s: s / s.shift(250) - 1.0)
    df["vmin250"] = g["vol"].rolling(250, min_periods=250).min().reset_index(level=0, drop=True)
    df["D"] = (df["close"] < df["ma200"]) & (df["ret250"] < 0) & df["ma200"].notna()
    df["V"] = df["vol"] <= df["vmin250"]
    df["sig"] = df["D"] & df["V"]
    df["open_next"] = g["open"].shift(-1)
    for n in NS:
        df[f"close_f{n}"] = g["close"].shift(-(1 + n))
    return df


def _cluster_dedup(df: pd.DataFrame) -> pd.Series:
    """First signal per 20-trading-day cluster (per ts_code)."""
    kept = pd.Series(False, index=df.index)
    for _ts, sub in df[df["sig"]].groupby("ts_code", sort=False):
        last = -10**9
        for idx, pos in zip(sub.index, sub["pos"], strict=False):
            if pos >= last + COOLDOWN:
                kept.loc[idx] = True
                last = pos
    return kept


def _ret(df: pd.DataFrame, n: int) -> pd.Series:
    return df[f"close_f{n}"] / df["open_next"] - 1.0 - COST


def _era_table(df: pd.DataFrame, eras: list[tuple[str, int, int]]) -> dict:
    out: dict = {}
    for label, y0, y1 in eras:
        sub = df[(df["year"] >= y0) & (df["year"] <= y1)]
        sig = sub[sub["signal"]]
        base = sub[sub["D"] & ~sub["signal"]]
        row = {"n_sig": int(len(sig)), "n_base": int(len(base))}
        for n in NS:
            rs = sig[f"r{n}"].dropna()
            rb = base[f"r{n}"].dropna()
            row[f"sig{n}"] = round(100 * float(rs.mean()), 2) if len(rs) else None
            row[f"base{n}"] = round(100 * float(rb.mean()), 2) if len(rb) else None
            row[f"ex{n}"] = (
                round(100 * (float(rs.mean()) - float(rb.mean())), 2) if len(rs) and len(rb) else None
            )
        out[label] = row
    sub = df
    sig = sub[sub["signal"]]
    base = sub[sub["D"] & ~sub["signal"]]
    full = {"n_sig": int(len(sig))}
    for n in NS:
        rs, rb = sig[f"r{n}"].dropna(), base[f"r{n}"].dropna()
        full[f"ex{n}"] = (
            round(100 * (float(rs.mean()) - float(rb.mean())), 2) if len(rs) and len(rb) else None
        )
    out["full"] = full
    return out


def _k(table: dict, eras: list[tuple[str, int, int]], n: int = 60, min_n: int = 20) -> tuple[bool, list]:
    marks = []
    for label, _y0, _y1 in eras:
        row = table.get(label, {})
        ex = row.get(f"ex{n}")
        marks.append(bool(ex is not None and row.get("n_sig", 0) >= min_n and ex > 0))
    return sum(marks) >= 3 and (table["full"].get(f"ex{n}") or -9) > 0, marks


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    ap.add_argument("--ns", default="20,60,120", help="holding horizons, comma list (default 20,60,120)")
    ap.add_argument("--out", default="volume_drought.json")
    args = ap.parse_args()
    global NS
    NS = tuple(int(x) for x in args.ns.split(",") if x.strip())
    print(f"horizons N={NS}", flush=True)

    from data_sync_service.config import get_settings

    with psycopg.connect(get_settings().database_url) as conn:
        idx = pd.read_sql(
            "SELECT ts_code, trade_date, open, close, vol FROM index_daily "
            "WHERE ts_code IN ('000300.SH','000001.SH','000905.SH') AND trade_date >= '2005-01-01' "
            "AND vol IS NOT NULL AND vol > 0 ORDER BY ts_code, trade_date",
            conn,
            dtype={"ts_code": str},
        )
        stocks = pd.read_sql(
            """
            SELECT d.ts_code, d.trade_date, d.open, d.close, d.vol, d.amount, d.adj_factor
            FROM daily d JOIN stock_basic sb ON sb.ts_code = d.ts_code
            WHERE d.trade_date >= '2010-01-01' AND d.vol IS NOT NULL AND d.vol > 0
              AND d.close IS NOT NULL AND d.open IS NOT NULL AND d.adj_factor IS NOT NULL
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
    print(f"index rows {len(idx)}, stock rows {len(stocks)}", flush=True)

    result: dict = {
        "tag": "volume-drought-2026-09-15",
        "prereg": "docs/designs/volume-drought-prereg-2026-09-15.md",
        "index": {},
        "stocks": {},
    }

    # --- index ---
    idx = idx.reset_index(drop=True)
    idx["year"] = pd.to_datetime(idx["trade_date"]).dt.year
    for code in INDEXES:
        sub = _features(idx[idx["ts_code"] == code].copy())
        sub["pos"] = sub.groupby("ts_code", sort=False).cumcount()
        sub["signal"] = _cluster_dedup(sub)
        for n in NS:
            sub[f"r{n}"] = _ret(sub, n)
        table = _era_table(sub, INDEX_ERAS)
        k, marks = _k(table, INDEX_ERAS)
        result["index"][code] = {"eras": table, "k1": k, "marks": marks}
        print(f"\n## {code} (fwd60 excess vs same-state, eras {marks}, K1={'pass' if k else 'FAIL'})")
        for label, row in table.items():
            if label == "full":
                continue
            cells = " ".join(
                f"ex{n} {row.get(f'ex{n}')}(sig {row.get(f'sig{n}')} base {row.get(f'base{n}')})"
                for n in NS
            )
            print(f"  {label:<10} n={row.get('n_sig'):>5} {cells}")

    # --- stocks ---
    stocks["year"] = pd.to_datetime(stocks["trade_date"]).dt.year
    stocks = _features(stocks)
    stocks["pos"] = stocks.groupby("ts_code", sort=False).cumcount()
    stocks["signal"] = _cluster_dedup(stocks)
    for n in NS:
        stocks[f"r{n}"] = _ret(stocks, n)
    table = _era_table(stocks, STOCK_ERAS)
    k2, marks2 = _k(table, STOCK_ERAS)
    # K3: drop deepest-decline decile by ret250 among signals
    sig = stocks[stocks["signal"]]
    cut = float(sig["ret250"].quantile(0.10))
    keep = stocks[~(stocks["signal"] & (stocks["ret250"] <= cut))]
    table_k3 = _era_table(keep, STOCK_ERAS)
    k3, marks3 = _k(table_k3, STOCK_ERAS)
    result["stocks"] = {
        "eras": table,
        "k2": k2,
        "marks": marks2,
        "deep_decile_cut": round(cut, 4),
        "eras_ex_deep": table_k3,
        "k3": k3,
        "marks_k3": marks3,
    }
    print(f"\n## stocks (fwd60 excess vs same-state, eras {marks2}, K2={'pass' if k2 else 'FAIL'})")
    for label, row in table.items():
        if label == "full":
            continue
        cells = " ".join(
            f"ex{n} {row.get(f'ex{n}')}(sig {row.get(f'sig{n}')} base {row.get(f'base{n}')})" for n in NS
        )
        print(f"  {label:<10} n={row.get('n_sig'):>6} {cells}")
    print(f"\n## K3 ex-deepest-decile (ret250 <= {cut:.2f}, eras {marks3}, K3={'pass' if k3 else 'FAIL'})")
    for label, row in table_k3.items():
        print(f"  {label:<10} n={row.get('n_sig'):>6} ex60 {row.get('ex60')}")

    k1 = result["index"]["000300.SH"]["k1"]
    verdict = (
        "OPEN: drought carries incremental info"
        if (k1 and k2 and k3)
        else "REJECT / family killed (oversold/low-vol proxy)"
    )
    print("\n## H-VOL-1 verdict\n")
    print(f"  K1 index 000300 -> {'pass' if k1 else 'FAIL'} | K2 stocks -> {'pass' if k2 else 'FAIL'}"
          f" | K3 ex-deep -> {'pass' if k3 else 'FAIL'}")
    print(f"  => {verdict}")
    result["verdict"] = {"k1": k1, "k2": k2, "k3": k3, "call": verdict}

    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        out = REPORT_DIR / args.out
        out.write_text(
            json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
        )
        print(f"\nsaved {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
