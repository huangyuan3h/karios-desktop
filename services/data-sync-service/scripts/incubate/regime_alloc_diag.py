"""R1 Phase 0 — style-separation diagnostic (regime allocation leg).

Prereg: docs/designs/regime-allocation-incubator-prereg-2026-09-11.md
Frozen spec (no grid):
  T (trend) : cross-sectional mom60 top decile, monthly, t pick -> t+1 hold.
  V (value) : V1 value composite (EP/BP/SP/FCF) top decile, monthly,
              PiT as-of (latest statement with ann_date <= t, <=270d old).
  R (regime): 000300.SH close >= its MA200 (month-end, as-of t).
  spread = V - T per month.

Read-only; never touches Live. Window limited to 2021+ (total_mv and
index_daily 000300.SH both start 2021).

Usage:
    PYTHONPATH=src python3 scripts/incubate/regime_alloc_diag.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import numpy as np
import pandas as pd

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "full": ("2021-01-01", "2026-12-31"),
}
LIQ_MIN = 70_000.0


def _spearman(a: pd.Series, b: pd.Series) -> float:
    return float(a.rank().corr(b.rank()))


def main() -> int:
    from data_sync_service.db import get_connection
    from data_sync_service.service.fin_panel import load_mv_map, mv_asof, value_panel

    with get_connection() as conn:
        px = pd.read_sql(
            "SELECT ts_code, trade_date, close, amount FROM daily "
            "WHERE trade_date >= '2020-01-01' AND close > 0 "
            "AND (ts_code LIKE '%.SH' OR ts_code LIKE '%.SZ' OR ts_code LIKE '%.BJ')",
            conn, parse_dates=["trade_date"],
        )
    px = px[px["ts_code"].str[0].isin(list("0368489"))].sort_values(["ts_code", "trade_date"])
    px["mom60"] = px.groupby("ts_code")["close"].pct_change(60, fill_method=None)
    px["liq60"] = px.groupby("ts_code")["amount"].transform(
        lambda s: s.rolling(60, min_periods=30).mean())
    sess = px.groupby("trade_date").size()
    cal_ts = sess[sess >= 3000].index
    px = px[px["trade_date"].isin(cal_ts)]
    cal = [d.strftime("%Y-%m-%d") for d in cal_ts]
    idx = {d: i for i, d in enumerate(cal)}

    px["ym"] = px["trade_date"].dt.to_period("M")
    first = px.groupby("ym")["trade_date"].min()
    ms = px[px["trade_date"].isin(first.values)].copy()
    ms["liq_ok"] = ms["liq60"] >= LIQ_MIN

    mo = px.groupby(["ts_code", "ym"]).agg(first=("close", "first"), last=("close", "last")).reset_index()
    mo["ret"] = mo["last"] / mo["first"] - 1
    ret_by = {str(y): g.set_index("ts_code")["ret"] for y, g in mo.groupby("ym")}
    ym_list = sorted(mo["ym"].unique())

    # ---- T picks per month ----
    T = {}
    tn = {}
    for ym, g in ms.groupby("ym"):
        g = g[g["liq_ok"] & g["mom60"].notna()]
        if len(g) < 100:
            continue
        thr = g["mom60"].quantile(0.9)
        pick = g[g["mom60"] >= thr]["ts_code"].tolist()
        i = ym_list.index(ym)
        if i + 1 >= len(ym_list):
            continue
        r = ret_by.get(str(ym_list[i + 1]))
        s = r.reindex(pick).dropna() if r is not None else pd.Series(dtype=float)
        if len(s) >= 15:
            T[str(ym)] = float(s.mean())
            tn[str(ym)] = len(s)
    T = pd.Series(T).sort_index()

    # ---- V picks per month (PiT as-of) ----
    panel = value_panel()
    panel = panel[~panel["is_fin"]].copy()
    legs = ["n_income_attr_p_sq_ttm", "total_revenue_sq_ttm",
            "total_hldr_eqy_inc_min_int", "free_cashflow_sq_ttm"]
    panel = panel[panel[legs].notna().all(axis=1)].copy()
    cover = len(panel)
    panel["ann"] = panel["ann_date"]
    tmv = load_mv_map("total_mv")
    month_dates = {str(y): d.strftime("%Y-%m-%d") for y, d in first.items()}

    V = {}
    vn = {}
    for ym in ym_list[1:]:
        t_ts = first.get(ym)
        if t_ts is None:
            continue
        t_str = t_ts.strftime("%Y-%m-%d")
        # as-of: latest ann_date <= t, within 270 days
        lo = (t_ts - pd.Timedelta(days=270))
        g = panel[(panel["ann"] <= t_ts) & (panel["ann"] >= lo)]
        if g.empty:
            continue
        g = g.sort_values("ann").drop_duplicates("ts_code", keep="last").copy()
        g["tmv"] = [mv_asof(tmv, cal, idx, c, t_str) for c in g["ts_code"]]
        g = g[g["tmv"].notna() & (g["tmv"] > 0)]
        if len(g) < 100:
            continue
        g["EP"] = g["n_income_attr_p_sq_ttm"] / g["tmv"]
        g["BP"] = g["total_hldr_eqy_inc_min_int"] / g["tmv"]
        g["SP"] = g["total_revenue_sq_ttm"] / g["tmv"]
        g["FCFP"] = g["free_cashflow_sq_ttm"] / g["tmv"]
        for c in ("EP", "BP", "SP", "FCFP"):
            lo2, hi2 = g[c].quantile([0.01, 0.99])
            g[c + "w"] = g[c].clip(lo2, hi2)
        g["comp"] = g[[c + "w" for c in ("EP", "BP", "SP", "FCFP")]].rank().mean(axis=1)
        thr = g["comp"].quantile(0.9)
        pick = g[g["comp"] >= thr]["ts_code"].tolist()
        i = ym_list.index(ym)
        if i + 1 >= len(ym_list):
            continue
        r = ret_by.get(str(ym_list[i + 1]))
        s = r.reindex(pick).dropna() if r is not None else pd.Series(dtype=float)
        if len(s) >= 15:
            V[str(ym)] = float(s.mean())
            vn[str(ym)] = len(s)
    V = pd.Series(V).sort_index()

    # ---- regime ----
    with get_connection() as conn:
        idxdf = pd.read_sql(
            "SELECT trade_date, close FROM index_daily WHERE ts_code='000300.SH' "
            "AND trade_date >= '2021-01-01' ORDER BY trade_date",
            conn, parse_dates=["trade_date"])
    idxdf["ma200"] = idxdf["close"].rolling(200, min_periods=200).mean()
    idxdf["ym"] = idxdf["trade_date"].dt.to_period("M")
    idxdf["on"] = idxdf["close"] >= idxdf["ma200"]
    reg = idxdf.groupby("ym").last()["on"]

    common = sorted(set(T.index) & set(V.index))
    df = pd.DataFrame({"T": T.reindex(common), "V": V.reindex(common)}).dropna()
    df["R"] = [reg.get(pd.Period(y, freq="M"), None) for y in df.index]
    df = df[df["R"].notna()].copy()
    df["R"] = df["R"].astype(bool)
    df["spread"] = df["V"] - df["T"]

    print(f"V panel 4-leg rows={cover}; months={len(df)}; "
          f"corr(T,V)={_spearman(df['T'], df['V']):.3f}")
    print(f"T avg hold n={np.mean(list(tn.values())):.0f}; V avg hold n={np.mean(list(vn.values())):.0f}")
    print("\nby regime (monthly mean %):")
    for r, g in df.groupby("R"):
        print(f"  R={'ON ' if r else 'OFF'} n={len(g):3d}  T {g['T'].mean()*100:+.2f}  "
              f"V {g['V'].mean()*100:+.2f}  spread(V-T) {g['spread'].mean()*100:+.2f}")

    print("\nby window:")
    res = {}
    for name, (lo, hi) in WINDOWS.items():
        d = df[(df.index >= lo) & (df.index <= hi)]
        if len(d) < 3:
            print(f"  {name:6s} n={len(d)} (too few)")
            continue
        off = d[~d["R"]]["spread"].mean() * 100 if (~d["R"]).any() else float("nan")
        on = d[d["R"]]["spread"].mean() * 100 if d["R"].any() else float("nan")
        sp = _spearman(d["T"], d["V"])
        res[name] = (off, on, sp)
        print(f"  {name:6s} n={len(d):2d}  R=off spread {off:+.2f}  R=on spread {on:+.2f}  corr {sp:.3f}")

    corr_all = _spearman(df["T"], df["V"])
    print("\nPASS checks:")
    print(f"  1 corr(T,V)<0.5: {corr_all:.3f} -> {corr_all < 0.5}")
    same = 0
    for name, (off, on, sp) in res.items():
        if np.isnan(off) or np.isnan(on):
            print(f"  2 [{name:5s}] missing regime cell")
            continue
        ok = off > 0 and on <= 0
        same += int(ok)
        print(f"  2 [{name:5s}] off>0 & on<=0: off {off:+.2f} on {on:+.2f} -> {ok}")
    print(f"  3 direction in >=2 of 3 windows: {same}/3 -> {same >= 2}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
