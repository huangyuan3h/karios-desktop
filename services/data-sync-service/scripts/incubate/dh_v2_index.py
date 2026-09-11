"""DH-2 = real cap-weighted A-share index beta + vol target (honest, tradable).

Uses index_daily (000300 沪深300 / 000905 中证500 / 000001 上证综指) now
backfilled to 2005. No selection, no look-ahead (indices are passive), so this
is the true market beta. Vol target scales daily exposure by inverse trailing
60d realized index vol. Annualized 2007-2026 + this-year window.

Usage:
    PYTHONPATH=src python3 scripts/incubate/dh_v2_index.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_sync_service.db import get_connection  # noqa: E402

CODES = ["000300.SH", "000905.SH", "000001.SH"]


def load_indices() -> dict[str, pd.Series]:
    with get_connection() as conn:
        df = pd.read_sql(
            "SELECT ts_code, trade_date, close FROM index_daily "
            "WHERE ts_code = ANY(%s) AND close > 0 ORDER BY trade_date",
            conn, params=(CODES,), parse_dates=["trade_date"],
        )
    return {c: g.set_index("trade_date")["close"] for c, g in df.groupby("ts_code")}


def vt_returns(px: pd.Series, target: float, win: int = 60, cap: float = 1.0,
               cost_side: float = 0.001) -> pd.Series:
    r = px.pct_change().dropna()
    vol = (r.rolling(win, min_periods=30).std() * np.sqrt(252)).shift(1)
    exp = (target / vol).clip(upper=cap).where(vol > 0, 1.0).fillna(1.0)
    d_exp = exp.diff().abs().fillna(0.0)
    return exp * r - d_exp * 2 * cost_side


def stats(r: pd.Series, label: str) -> dict:
    r = r.dropna()
    nav = (1 + r).cumprod()
    yrs = len(r) / 252
    cagr = (nav.iloc[-1] ** (1 / yrs) - 1) * 100
    v = r.std() * np.sqrt(252) * 100
    dd = float((nav / nav.cummax() - 1).min()) * 100
    sh = (r.mean() * 252) / (r.std() * np.sqrt(252)) if r.std() else 0
    print(f"  {label:34s} {cagr:+5.1f}%/yr vol {v:5.1f}% DD {dd:6.1f}% sr {sh:5.2f} nav x{nav.iloc[-1]:.2f} n{len(r)}")
    return {"cagr": cagr, "dd": dd, "sharpe": sh}


def main() -> int:
    idx = load_indices()
    # build a 50/50 blend of 300 and 500 (daily rebalanced) as a broad market
    common = idx["000300.SH"].index.intersection(idx["000905.SH"].index)
    r300 = idx["000300.SH"].reindex(common).pct_change()
    r500 = idx["000905.SH"].reindex(common).pct_change()
    blend = (1 + 0.5 * r300 + 0.5 * r500).cumprod()
    blend.index = common
    idx["BLEND300/500"] = blend

    print("=== DH-2 index beta, full 2007-2026 (annualized) ===")
    for name in ["000300.SH", "000905.SH", "BLEND300/500", "000001.SH"]:
        px = idx[name]
        px = px[px.index >= "2007-01-01"]
        print(f"{name}:")
        stats(px.pct_change().dropna(), "no-vt buy&hold")
        for t in (0.25, 0.20, 0.15, 0.10):
            stats(vt_returns(px, t), f"vt{int(t*100)}%")

    print("\n=== sub-periods (BLEND300/500, no-vt vs vt15) ===")
    px = idx["BLEND300/500"]
    for lo, hi, sp in [("2007", "2010", "07-09 GFC"), ("2010", "2015", "10-14"),
                       ("2015", "2016", "2015 crash"), ("2016", "2019", "16-18"),
                       ("2019", "2022", "19-21 bull"), ("2022", "2027", "22-26")]:
        b = px[(px.index >= lo) & (px.index < hi)]
        rb = b.pct_change().dropna()
        rv = vt_returns(b, 0.15)
        navb = (1 + rb).prod(); navv = (1 + rv).prod()
        print(f"  {sp:12s} no-vt x{navb:.2f}  vt15 x{navv:.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
