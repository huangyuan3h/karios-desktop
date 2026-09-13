#!/usr/bin/env python3
"""H-STABLE pre-registered diagnostic: standalone multi-asset stable core.

5-asset universe (沪深300/中证500/黄金/纳指/十年国债), monthly rebalance,
trailing-60d risk weights (shifted, no look-ahead). Compares fixed structures
B0..B5 over 2021-2026. Prereg & thresholds:
docs/designs/stable-core-prereg-2026-09-12.md

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/diag_stable_core.py --save-report
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
ETF = ROOT / "data" / "etf" / "etf_daily.csv"
REPORT_DIR = ROOT / "data" / "backtest_reports"
CODES = ["510300.SH", "510500.SH", "518880.SH", "513100.SH", "511260.SH"]
BOND = "511260.SH"
EQUITY_GOLD = ["510300.SH", "510500.SH", "518880.SH", "513100.SH"]
WINDOWS = {"OOS2": ("2024-08-01", "2025-08-01"), "train": ("2025-08-01", "2026-02-01"),
           "valid": ("2026-03-01", "2026-08-07"), "long": ("2021-01-04", "2026-08-07")}
COST = 0.0005
VT_TARGET = 0.08
LABELS = {"B0": "300 买持", "B1": "60/40", "B2": "等权", "B3": "波动率倒数",
          "B4": "B3+波动率目标8%", "B5": "B3+趋势过滤"}


def _iso(d: str) -> str:
    return f"{d[:4]}-{d[4:6]}-{d[6:8]}" if len(d) == 8 and d.isdigit() else d


def _load() -> pd.DataFrame:
    rows = []
    import csv
    with ETF.open() as fh:
        for r in csv.DictReader(fh):
            if r["ts_code"] in CODES:
                rows.append({"date": _iso(r["trade_date"]), "code": r["ts_code"], "px": float(r["close_adj"])})
    df = pd.DataFrame(rows).pivot(index="date", columns="code", values="px").sort_index()
    return df[CODES].dropna()


def _weights(structure: str, ret_hist: pd.DataFrame, px_hist: pd.DataFrame, prev_w: pd.Series) -> pd.Series:
    cols = list(ret_hist.columns)
    if structure == "B0":
        w = pd.Series(0.0, index=cols)
        w["510300.SH"] = 1.0
        return w
    if structure == "B1":
        w = pd.Series(0.0, index=cols)
        w["510300.SH"], w[BOND] = 0.6, 0.4
        return w
    if structure == "B2":
        return pd.Series(1.0 / len(cols), index=cols)
    # B3/B4/B5: inverse-vol base
    sd = ret_hist.std().replace(0, np.nan)
    inv = 1.0 / sd
    if structure == "B5":
        keep = []
        for c in cols:
            if c == BOND:
                keep.append(c)
                continue
            p = px_hist[c].dropna()
            if len(p) < 200 or p.iloc[-1] > p.iloc[-200:].mean():  # short history -> fail-open
                keep.append(c)
        inv = inv[keep]
    w = (inv / inv.sum()).reindex(cols).fillna(0.0)
    if structure == "B4":
        cov = ret_hist.cov() * 242.0
        wv = w.values
        pvol = float(np.sqrt(wv @ cov.reindex(index=cols, columns=cols).values @ wv))
        scale = min(1.0, VT_TARGET / pvol) if pvol > 0 else 1.0
        w = w * scale
    return w


def _nav(df: pd.DataFrame, structure: str) -> pd.Series:
    dates = list(df.index)
    ret = df.pct_change().fillna(0.0)
    # rebalance on first trading day of each month
    rebal = {}
    seen = set()
    for d in dates:
        m = d[:7]
        if m not in seen:
            seen.add(m)
            rebal[d] = True
    w = pd.Series(1.0 / len(CODES), index=CODES) if structure != "B0" else pd.Series([1 if c == "510300.SH" else 0 for c in CODES], index=CODES)
    nav = [1.0]
    for i, d in enumerate(dates):
        if i == 0:
            continue
        # mark existing weights by returns
        r = ret.iloc[i]
        pnl = float((w * r).sum())
        nav.append(nav[-1] * (1.0 + pnl))
        w = w * (1.0 + r)
        if w.sum() > 0:
            w = w / w.sum()
        if rebal.get(d) and i >= 60:
            hist_ret = ret.iloc[i - 60:i]
            hist_px = df.iloc[max(0, i - 200):i]
            w_new = _weights(structure, hist_ret, hist_px, w).reindex(CODES).fillna(0.0)
            turnover = float((w_new - w).abs().sum())
            cost = COST * turnover
            nav[-1] *= (1.0 - cost)
            w = w_new
    return pd.Series(nav, index=dates)


def _metrics(nav: pd.Series, s: str, e: str) -> dict:
    sl = nav[(nav.index >= s) & (nav.index <= e)]
    if len(sl) < 5:
        return {"n": len(sl)}
    r = sl.pct_change().dropna()
    years = len(sl) / 242.0
    cagr = (sl.iloc[-1] / sl.iloc[0]) ** (1 / years) - 1 if sl.iloc[0] > 0 else None
    vol = float(r.std() * np.sqrt(242))
    sharpe = float(r.mean() / r.std() * np.sqrt(242)) if r.std() > 0 else None
    cum = sl / sl.cummax()
    mdd = float(cum.min() - 1.0)
    return {"n": len(sl), "cagr": round(100 * cagr, 2) if cagr is not None else None,
            "vol": round(100 * vol, 2), "sharpe": round(sharpe, 2) if sharpe is not None else None,
            "mdd": round(100 * mdd, 2)}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    df = _load()
    print(f"panel {df.index[0]} -> {df.index[-1]}  assets {list(df.columns)}  rows {len(df)}")
    navs = {b: _nav(df, b) for b in LABELS}

    print("\n## long window (CAGR% / vol% / Sharpe / MDD%)")
    res: dict[str, dict] = {}
    for b, lbl in LABELS.items():
        res[b] = {w: _metrics(navs[b], *WINDOWS[w]) for w in WINDOWS}
        m = res[b]["long"]
        print(f"  {b} {lbl:<16} {m['cagr']}/{m['vol']}/{m['sharpe']}/{m['mdd']}")

    print("\n## by window (Sharpe / MDD)")
    for w in WINDOWS:
        line = "  ".join(f"{b}:{res[b][w].get('sharpe')}/{res[b][w].get('mdd')}" for b in LABELS)
        print(f"  {w:<7} {line}")

    b0 = res["B0"]["long"]
    usable = []
    for b in LABELS:
        m = res[b]["long"]
        if (m.get("sharpe") or -9) >= 1.0 and (m.get("cagr") or -9) >= 5.0 and (m.get("mdd") or -9) >= -20.0 \
                and (m.get("sharpe") or -9) > (b0.get("sharpe") or 9) and (m.get("mdd") or 9) > (b0.get("mdd") or -9):
            usable.append(b)
    print(f"\n## H-STABLE verdict: usable structures = {usable if usable else 'NONE'}")
    print(f"   (long: Sharpe>=1.0 & CAGR>=5% & MDD>=-20% & beats B0 on Sharpe+MDD; B0 "
          f"{b0.get('sharpe')}/{b0.get('mdd')})")

    payload = {"tag": "stable-core-2026-09-12",
               "prereg": "docs/designs/stable-core-prereg-2026-09-12.md",
               "universe": CODES, "windows": WINDOWS, "results": res, "usable": usable,
               "as_of": datetime.now(UTC).isoformat(timespec="seconds")}
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        (REPORT_DIR / "stable_core_2026-09-12.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print("\nsaved report")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
