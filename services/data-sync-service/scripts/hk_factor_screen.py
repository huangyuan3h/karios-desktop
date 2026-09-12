#!/usr/bin/env python3
"""HK-market L0 screen for Alpha101 + GTJA191 (read-only).

Purpose: test whether the A-share verdict ("price-volume family alive but
cost-killed, no new axis") is A-specific or reproduces on H-shares.

Data notes (HK, from `daily` where ts_code like '%.HK'):
  * NO adj_factor -> raw prices used; symbols with any |1d ret|>60% in the
    slice are dropped (split/fraud proxy).
  * vwap = amount / volume (both raw); volume := amount (HKD dollar volume).
  * no HK industry map -> groups 'UNKNOWN' (IndNeutralize == cross-section demean).
  * no HK cap -> cap = NaN (A56/GTJA056 become NO-DATA).
  * benchmark = HSI (global_index_daily).

Usage:
  PYTHONPATH=src python3 scripts/hk_factor_screen.py --windows OOS2,train,valid --cost 0.004
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import alpha101_screen as a101  # noqa: E402
import gtja191_screen as g191  # noqa: E402

HK_LIQ_HKD = 10_000_000.0  # 20d avg daily turnover floor (HKD)
HK_MIN_PRICE = 1.0
LOOKBACK_DAYS = a101.LOOKBACK_DAYS
FWD_BUFFER_DAYS = a101.FWD_BUFFER_DAYS
REPORT_DIR = a101.REPORT_DIR


def _join(recs, wins, key, fmt):
    return "/".join(
        format(recs[w][key], fmt) if recs[w].get(key) is not None else "na" for w in wins
    )


def load_hk_panel(start: str, end: str):
    from data_sync_service.db import get_connection

    load_start = (date.fromisoformat(start) - timedelta(days=LOOKBACK_DAYS)).isoformat()
    load_end = (date.fromisoformat(end) + timedelta(days=FWD_BUFFER_DAYS)).isoformat()
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT ts_code FROM stock_basic WHERE market='HK' "
            "AND (name IS NULL OR (name NOT ILIKE '%%ETF%%' AND name NOT ILIKE '%%基金%%'))"
        )
        universe = {str(r[0]) for r in cur.fetchall()}
        cur.execute(
            """
            SELECT trade_date, ts_code, open, high, low, close, vol, amount
            FROM daily WHERE trade_date >= %s AND trade_date <= %s
              AND ts_code LIKE '%%.HK' ORDER BY trade_date
            """,
            (load_start, load_end),
        )
        rows = cur.fetchall()
    df = pd.DataFrame(
        rows, columns=["trade_date", "ts_code", "open", "high", "low", "close", "vol", "amount"]
    )
    df = df[df["ts_code"].isin(universe)]
    for col in ["open", "high", "low", "close", "vol", "amount"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df[(df["vol"] > 0) & (df["amount"] > 0) & (df["close"] > 0)]
    df["trade_date"] = df["trade_date"].astype(str)
    print(f"  HK daily rows in universe: {len(df)}", flush=True)

    def wide(col):
        return df.pivot(index="trade_date", columns="ts_code", values=col).sort_index()

    panel = {
        "open": wide("open"), "high": wide("high"), "low": wide("low"),
        "close": wide("close"),
    }
    panel["volume"] = wide("amount")  # dollar volume
    panel["amount"] = panel["volume"]
    panel["vwap"] = wide("amount") / wide("vol")
    panel["prev_close"] = panel["close"].shift(1)
    panel["returns"] = panel["close"] / panel["prev_close"] - 1
    panel["cap"] = pd.DataFrame(np.nan, index=panel["close"].index, columns=panel["close"].columns)

    # drop split/fraud proxy: any |1d ret| > 60% in slice
    rets = panel["returns"]
    bad = rets.columns[(rets.abs() > 0.6).any(axis=0)]
    if len(bad):
        panel = {k: v.drop(columns=list(bad)) for k, v in panel.items()}
        print(f"  dropped {len(bad)} symbols with |1d ret|>60%", flush=True)

    close = panel["close"]
    avg20 = panel["amount"].rolling(20, min_periods=10).mean()
    tradable = (avg20 >= HK_LIQ_HKD) & (close >= HK_MIN_PRICE) & close.notna()
    win_dates = [d for d in close.index if start <= d <= end]

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT to_char(trade_date,'YYYY-MM-DD') d, open, close FROM global_index_daily "
            "WHERE ts_code='HSI' AND close>0"
        )
        bm = cur.fetchall()
    bm = pd.DataFrame(bm, columns=["d", "open", "close"]).set_index("d")
    benchmark = {"open": bm["open"].reindex(close.index).astype(float),
                 "close": bm["close"].reindex(close.index).astype(float)}
    return panel, tradable, win_dates, benchmark


def run_window(name, start, end, cost, which):
    print(f"[{name}] {start}..{end} cost={cost}", flush=True)
    t0 = time.time()
    panel, tradable, win_dates, bm = load_hk_panel(start, end)
    close = panel["close"]
    fwd = {h: a101._forward_returns(close, h) for h in a101.HORIZONS}
    groups = pd.Series("UNKNOWN", index=close.columns)
    libs = {}
    if which in ("all", "101"):
        libs.update(a101.alpha101_lib(panel, groups))
    if which in ("all", "191"):
        libs.update(g191.gtja_lib(panel, bm))
    a101.COST_ROUNDTRIP = cost
    n_days = len(win_dates)
    print(f"[{name}] days {n_days}, assets {close.shape[1]}, factors {len(libs)} "
          f"(load {round(time.time()-t0,1)}s)", flush=True)
    out = {}
    for aname, afac in libs.items():
        out[aname] = {str(h): a101.evaluate(afac, fwd[h], tradable, win_dates) for h in a101.HORIZONS}
        del afac
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--windows", default="OOS2,train,valid")
    ap.add_argument("--cost", type=float, default=0.004, help="roundtrip cost (0.004=40bp)")
    ap.add_argument("--which", default="all", choices=["all", "101", "191"])
    ap.add_argument("--json", default="")
    args = ap.parse_args()
    wins = [w.strip() for w in args.windows.split(",") if w.strip()]

    allw = {}
    for w in wins:
        if w not in a101.WINDOWS:
            print(f"unknown window {w}", file=sys.stderr)
            return 2
        allw[w] = run_window(w, *a101.WINDOWS[w], args.cost, args.which)

    verdict = a101._verdict(allw)
    counts: dict[str, int] = {}
    for v in verdict.values():
        counts[v["verdict"]] = counts.get(v["verdict"], 0) + 1
    payload = {
        "generated_at": __import__("datetime").datetime.now(
            __import__("datetime").UTC
        ).isoformat(),
        "market": "HK", "windows": wins, "cost_roundtrip": args.cost,
        "universe": f"HK ex-ETF/基金, 20d avg amount>={HK_LIQ_HKD/1e6:.0f}M HKD, price>={HK_MIN_PRICE}",
        "counts": counts, "verdict": verdict, "raw": allw,
    }
    out_path = Path(args.json) if args.json else REPORT_DIR / f"hk_factor_screen_{int(args.cost*1e4)}bp_latest.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    print(f"\nreport -> {out_path}\ncounts: {counts}\n")

    rows = []
    for a, rec in verdict.items():
        irs = [abs(x) for x in rec.get("ic_irs", []) if x is not None]
        rows.append((float(np.median(irs)) if irs else -1.0, a, rec))
    rows.sort(reverse=True, key=lambda x: x[0])
    print("| factor | ver | ICIR O/T/V | IC O/T/V | mono | net O/T/V |")
    print("|--------|-----|-----------|----------|------|-----------|")
    for _s, a, rec in rows[:30]:
        recs = rec["windows"]
        print(f"| {a} | {rec['verdict']} | {_join(recs, wins, 'ic_ir', '+.2f')} | "
              f"{_join(recs, wins, 'ic_mean', '+.3f')} | "
              f"{rec.get('mono_count')}/3 | {_join(recs, wins, 'net_spread', '+.4f')} |")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
