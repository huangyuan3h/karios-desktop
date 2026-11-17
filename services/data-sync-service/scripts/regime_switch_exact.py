#!/usr/bin/env python3
"""Exact regime-switch rerun on exported daily NAVs (read-only).

Reads data/backtest_reports/regime_navs/daily_navs.csv (from
scripts/export_regime_navs.py) and 000300 from index_daily, then runs:
  - 2-state: 300 > MA200 -> h2a25, else starport
  - E: bull -> h2a25, bear -> b3, chop -> starport
  - D: bull -> h2a25, bear -> m30, chop -> starport
plus path block bootstrap and regime Markov MC.

No orders, no Live writes.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT_DEFAULT = ROOT / "data" / "backtest_reports" / "regime_navs"


def load_index(start: str, end: str) -> pd.Series:
    import psycopg

    from data_sync_service.config import get_settings

    s = get_settings()
    with psycopg.connect(s.database_url) as conn:
        df = pd.read_sql(
            "select trade_date, close from index_daily "
            "where ts_code='000300.SH' and trade_date between %s and %s order by trade_date",
            conn,
            params=(start, end),
        )
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df.set_index("trade_date")["close"].astype(float)


def states_for(idx: pd.Series) -> tuple[pd.Series, pd.Series]:
    ma200 = idx.rolling(200).mean()
    ret60 = idx / idx.shift(60) - 1
    dd250 = idx / idx.rolling(250).max() - 1
    st2 = pd.Series(np.where(idx > ma200, "bull", "bear"), index=idx.index)
    st3 = pd.Series("chop", index=idx.index)
    st3[(idx > ma200) & (ret60 > 0.05) & (ma200 > ma200.shift(20))] = "bull"
    st3[(idx < ma200) & ((ret60 < -0.05) | (dd250 < -0.15))] = "bear"
    return st2, st3


def month_targets(st: pd.Series, dates: pd.DatetimeIndex) -> pd.Series:
    ser = pd.Series(dates, index=dates)
    me = ser.groupby([ser.dt.year, ser.dt.month]).last()
    out = []
    last = None
    for d in dates:
        if d in set(me.values):
            last = st.loc[d]
        out.append(last)
    return pd.Series(out, index=dates).ffill()


def run(navs: pd.DataFrame, targets: pd.Series, mapping: dict[str, str], cost: float = 0.0010):
    dates = navs.index
    me = pd.Series(dates, index=dates).groupby([dates.year, dates.month]).last()
    me_set = set(me.values)
    nav = 1.0
    series = []
    prev = None
    switches = 0
    for i, d in enumerate(dates):
        if i > 0 and prev:
            a = navs[prev].iloc[i]
            b = navs[prev].iloc[i - 1]
            if b and np.isfinite(a) and np.isfinite(b):
                nav *= a / b
        series.append(nav)
        if d in me_set:
            nxt = mapping.get(targets.loc[d], prev)
            if nxt and nxt != prev:
                nav *= 1.0 - cost
                prev = nxt
                switches += 1
            elif prev is None:
                prev = nxt
    return pd.Series(series, index=dates), switches


def stats(s: pd.Series) -> dict:
    r = s.pct_change().dropna()
    return {
        "total": round((s.iloc[-1] - 1) * 100, 1),
        "mdd": round((s / s.cummax() - 1).min() * 100, 1),
        "sr": round(float(r.mean() / r.std() * np.sqrt(252)), 2),
        "cagr": round(float((s.iloc[-1] ** (252 / len(r)) - 1) * 100), 1),
    }


def block_mc(r: np.ndarray, n: int, paths: int = 2000, block: int = 20, seed: int = 42):
    rng = np.random.default_rng(seed)
    out, dd = [], []
    nb = int(np.ceil(n / block))
    for _ in range(paths):
        idx = rng.integers(0, len(r) - block, nb)
        x = np.concatenate([r[i : i + block] for i in idx])[:n]
        nav = np.cumprod(1 + x)
        out.append(nav[-1] - 1)
        dd.append((nav / np.maximum.accumulate(nav) - 1).min())
    out, dd = np.array(out), np.array(dd)
    return {
        "p5": round(float(np.quantile(out, 0.05)) * 100, 1),
        "p50": round(float(np.quantile(out, 0.50)) * 100, 1),
        "p95": round(float(np.quantile(out, 0.95)) * 100, 1),
        "p_dd10": round(float((dd < -0.10).mean()) * 100, 1),
        "p_dd20": round(float((dd < -0.20).mean()) * 100, 1),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--navs", default=str(OUT_DEFAULT / "daily_navs.csv"))
    ap.add_argument("--out", default=str(OUT_DEFAULT / "regime_switch_exact.json"))
    args = ap.parse_args()

    navs = pd.read_csv(args.navs, index_col="date", parse_dates=True).sort_index()
    needed = ["h2a25", "starport", "m30"]
    navs = navs.dropna(subset=[c for c in needed if c in navs.columns])
    lookback = (navs.index[0] - pd.Timedelta(days=500)).strftime("%Y-%m-%d")
    idx_full = load_index(lookback, str(navs.index[-1].date()))
    st2, st3 = states_for(idx_full)
    tg2 = month_targets(st2, navs.index)
    tg3 = month_targets(st3, navs.index)

    variants = {
        "2state": (tg2, {"bull": "h2a25", "bear": "starport"}),
        "E": (tg3, {"bull": "h2a25", "bear": "b3", "chop": "starport"}),
        "D": (tg3, {"bull": "h2a25", "bear": "m30", "chop": "starport"}),
    }
    out = {"start": str(navs.index[0].date()), "end": str(navs.index[-1].date()), "variants": {}}
    for name, (tg, mp) in variants.items():
        s, sw = run(navs, tg, mp)
        rec = {**stats(s), "switches": sw}
        rec["yearly"] = {
            str(d.year): round(float(v), 1)
            for d, v in ((s.resample("YE").last() / s.resample("YE").last().shift(1) - 1) * 100).dropna().items()
        }
        rec["mc_1y"] = block_mc(s.pct_change().dropna().values, 252)
        rec["mc_3y"] = block_mc(s.pct_change().dropna().values, 756)
        out["variants"][name] = rec
        print(name, rec, flush=True)

    Path(args.out).write_text(json.dumps(out, ensure_ascii=False, indent=2))
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
