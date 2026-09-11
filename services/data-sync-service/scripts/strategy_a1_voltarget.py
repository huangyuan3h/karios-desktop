"""A1 v0 — risk-managed A-share beta (P0-12 new-strategy incubator).

Long-hold-safe, no industry mapping (survivorship-free), no timing gate.
Universe = top-N most liquid A-shares (60d avg amount), equal weight,
monthly rebalance; exposure scaled by a 15% annualized vol target using
trailing 60d realized market vol. This is risk-managed beta, NOT alpha.

Evaluation over 2007-2026 (multi-cycle), with transaction costs and a
no-leverage variant. Own baseline = equal-weight all-liquid universe.

Usage:
    PYTHONPATH=src python3 scripts/strategy_a1_voltarget.py --topn 300 --target-vol 0.15
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import numpy as np
import pandas as pd

SUB_PERIODS = [
    ("2007", "2010", "07-09 GFC"),
    ("2010", "2015", "10-14"),
    ("2015", "2016", "2015 crash"),
    ("2016", "2019", "16-18 value"),
    ("2019", "2022", "19-21 bull"),
    ("2022", "2027", "22-26"),
]


def build_panel(liq_min: float = 0.0) -> pd.DataFrame:
    from data_sync_service.db import get_connection

    with get_connection() as conn:
        px = pd.read_sql(
            "SELECT ts_code, trade_date, close, pct_chg, amount FROM daily "
            "WHERE trade_date >= '2006-01-01' AND close > 0 "
            "AND (ts_code LIKE '%.SH' OR ts_code LIKE '%.SZ' OR ts_code LIKE '%.BJ')",
            conn, parse_dates=["trade_date"],
        )
    px = px[px["ts_code"].str[0].isin(list("0368489"))].sort_values(["ts_code", "trade_date"])
    px["ym"] = px["trade_date"].dt.to_period("M")
    px["r"] = px["pct_chg"] / 100
    px["liq60"] = px.groupby("ts_code")["amount"].transform(
        lambda s: s.rolling(60, min_periods=30).mean())
    mo = px.groupby(["ts_code", "ym"]).agg(
        first=("close", "first"), last=("close", "last"), liq=("liq60", "last"),
    ).reset_index()
    mo["ret"] = mo["last"] / mo["first"] - 1
    # FIX (2026-09-11): use PRIOR month's liquidity for selection. The original
    # used the SAME month's end-of-month liq60 to trade that same month — a
    # look-ahead that inflated the top-liquidity basket by ~+20pt/yr. Selection
    # must be knowable at the rebalance date.
    mo = mo.sort_values(["ts_code", "ym"])
    mo["liq"] = mo.groupby("ts_code")["liq"].shift(1)
    mstart = px.groupby("ym")["trade_date"].min()
    mret = px.groupby("trade_date")["r"].mean()
    mvol = (mret.rolling(60, min_periods=30).std() * np.sqrt(252)).rename("mvol")
    ms = pd.DataFrame({"ym": mstart.index, "td": mstart.values}).merge(
        mvol, left_on="td", right_index=True)
    mo = mo.merge(ms[["ym", "mvol"]], on="ym", how="left")
    if liq_min > 0:
        mo = mo[mo["liq"] >= liq_min]
    return mo


def run(mo: pd.DataFrame, topn: int, target_vol: float, cap: float,
        cost_side: float, liq_floor: float = 0.0) -> pd.DataFrame:
    months = sorted(mo["ym"].unique())
    rows = []
    prev_set: set[str] = set()
    for ym in months:
        d = mo[(mo["ym"] == ym) & mo["ret"].notna() & mo["liq"].notna()]
        if len(d) < 200:
            continue
        if liq_floor > 0:
            d = d[d["liq"] >= d["liq"].quantile(liq_floor)]
        sub = d.nlargest(min(topn, len(d)), "liq")
        cur = set(sub["ts_code"])
        turn = 1.0 - (len(cur & prev_set) / max(len(cur), 1)) if prev_set else 1.0
        prev_set = cur
        gross = sub["ret"].mean()
        mv = d["mvol"].iloc[0]
        if target_vol <= 0:
            exp = 1.0
        else:
            exp = min(cap, max(0.2, target_vol / mv)) if mv and mv > 0 else 1.0
        cost = turn * 2 * cost_side * exp
        rows.append({"ym": ym, "ret": gross * exp - cost, "gross": gross * exp,
                     "ew": d["ret"].mean(), "exp": exp, "turn": turn})
    return pd.DataFrame(rows)


def stats(r: pd.DataFrame, label: str) -> dict:
    cp = float((1 + r["ret"]).prod())
    ce = float((1 + r["ew"]).prod())
    yrs = len(r) / 12
    vol = float(r["ret"].std() * np.sqrt(12))
    cum = (1 + r["ret"]).cumprod()
    dd = float((cum / cum.cummax() - 1).min())
    return {
        "label": label, "nav": round(cp, 3), "cagr": round((cp ** (1 / yrs) - 1) * 100, 1),
        "vol": round(vol * 100, 1), "maxdd": round(dd * 100, 1),
        "sharpe": round((r["ret"].mean() * 12) / vol, 2) if vol else None,
        "ew_nav": round(ce, 3), "avg_exp": round(float(r["exp"].mean()), 2),
        "avg_turn": round(float(r["turn"].mean()), 2),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--topn", type=int, default=200)
    ap.add_argument("--target-vol", type=float, default=0.10)
    ap.add_argument("--cost-side", type=float, default=0.001)
    ap.add_argument("--liq-min", type=float, default=0.0)
    args = ap.parse_args()

    t0 = time.time()
    mo = build_panel(args.liq_min)
    print(f"panel built ({time.time() - t0:.0f}s), rows={len(mo)} months={mo['ym'].nunique()}")

    variants = {
        f"top{args.topn} + vt{args.target_vol:.0%} cap1.5": dict(cap=1.5),
        f"top{args.topn} + vt{args.target_vol:.0%} cap1.0 (no lev)": dict(cap=1.0),
        f"top{args.topn} (no vt)": dict(cap=1.0, target_vol=0.0),
        "EW all (base)": dict(cap=1.0, target_vol=0.0, topn=10**9),
    }
    report: dict = {"params": vars(args), "variants": [], "sub_periods": {}}
    for label, over in variants.items():
        kwargs = dict(topn=args.topn, target_vol=args.target_vol, cap=1.5,
                      cost_side=args.cost_side)
        kwargs.update(over)
        r = run(mo, **kwargs)
        s = stats(r, label)
        s["gross_nav"] = round(float((1 + r["gross"]).prod()), 3)
        report["variants"].append(s)
        print(f"{label:34s} NAV x{s['nav']:.2f} (gross x{s['gross_nav']:.2f}) "
              f"{s['cagr']:+.1f}%/yr vol {s['vol']}% DD {s['maxdd']}% sr {s['sharpe']} "
              f"avgExp {s['avg_exp']} turn {s['avg_turn']} | EW x{s['ew_nav']:.2f}")

    print("\nsub-periods (NAV multiple / maxDD):")
    for lo, hi, name in SUB_PERIODS:
        report["sub_periods"][name] = {}
        line = []
        for label, over in variants.items():
            kwargs = dict(topn=args.topn, target_vol=args.target_vol, cap=1.5,
                          cost_side=args.cost_side)
            kwargs.update(over)
            r = run(mo, **kwargs)
            r = r[(r["ym"].astype(str) >= lo) & (r["ym"].astype(str) < hi)]
            cum = (1 + r["ret"]).cumprod()
            nav = float(cum.iloc[-1]) if len(cum) else 1.0
            dd = float((cum / cum.cummax() - 1).min() * 100) if len(cum) else 0.0
            report["sub_periods"][name][label] = {"nav": round(nav, 2), "dd": round(dd, 1)}
            line.append(f"{label.split(' ')[0]} x{nav:.2f}/DD{dd:.0f}%")
        print(f"  {name:12s} " + "  ".join(line))

    out = Path(__file__).resolve().parents[1] / "data/backtest_reports/a1_voltarget_report.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2, default=str))
    print(f"\nreport -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
