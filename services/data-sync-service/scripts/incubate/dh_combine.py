"""DH-2 x S-3 combination lab (Phase 0, read-only).

DH-2 = 000905.SH (中证500) + 20% vol target (real index beta, no look-ahead).
Tests several ways to combine it with the S-3 stock alpha:

  S1  fixed blend         w*DH2 + (1-w)*S3,  w in {0.3, 0.5, 0.7}
  S2  risk parity         trailing-vol inverse weights
  S3  idle cash -> DH2    S-3 keeps its book; uninvested cash earns DH-2
  S4  DH2 when invest<50%  idle cash -> DH-2 only when S-3 gross < 50%

S-3 daily NAV + gross exposure come from the engine (positions_by_day).
Evaluated on the S-3 audit windows (+ long). Read-only.

Usage:
    PYTHONPATH=src python3 scripts/incubate/dh_combine.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_sync_service.service.backtest_engine import BacktestConfig, simulate  # noqa: E402
from run_walk_forward import S3_CONFIG, WINDOWS  # noqa: E402

DH_INDEX = "000905.SH"
DH_TARGET = 0.20


def build_dh2(target: float = DH_TARGET, win: int = 60, cap: float = 1.0,
              cost_side: float = 0.001) -> tuple[pd.Series, pd.Series]:
    from data_sync_service.db import get_connection

    with get_connection() as conn:
        df = pd.read_sql(
            "SELECT trade_date, close FROM index_daily WHERE ts_code=%s AND close>0 "
            "ORDER BY trade_date", conn, params=(DH_INDEX,), parse_dates=["trade_date"],
        )
    px = df.set_index("trade_date")["close"]
    r = px.pct_change().dropna()
    vol = (r.rolling(win, min_periods=30).std() * np.sqrt(252)).shift(1)
    exp = (target / vol).clip(upper=cap).where(vol > 0, 1.0).fillna(1.0)
    d_exp = exp.diff().abs().fillna(0.0)
    dh = exp * r - d_exp * 2 * cost_side
    # as-of trend gate: index above its 200d MA as of the PRIOR close
    trend = (px > px.rolling(200, min_periods=120).mean()).shift(1).reindex(dh.index).fillna(False)
    return dh, trend


def metrics(r: pd.Series) -> str:
    r = r.fillna(0.0)
    nav = (1 + r).cumprod()
    total = (nav.iloc[-1] - 1) * 100
    v = r.std() * np.sqrt(252) * 100
    dd = float((nav / nav.cummax() - 1).min()) * 100
    sh = (r.mean() * 252) / (r.std() * np.sqrt(252)) if r.std() else 0
    return f"total {total:+7.1f}% vol {v:5.1f}% DD {dd:6.1f}% sr {sh:5.2f}"


def s3_series(run):
    n = len(run.positions_by_day)
    dates = pd.to_datetime([p["date"] for p in run.positions_by_day])
    navs = np.array([1.0] + list(run.nav_curve[:n]))
    r = pd.Series(navs[1:] / navs[:-1] - 1, index=dates)
    invest = pd.Series([sum(x["position_pct"] for x in p["positions"]) for p in run.positions_by_day],
                       index=dates)
    return r, invest


def main() -> int:
    dh, trend = build_dh2()
    print(f"DH-2 = {DH_INDEX} + vt{int(DH_TARGET*100)}% | {len(dh)} days "
          f"{dh.index.min().date()}..{dh.index.max().date()} | full {metrics(dh)}"
          f" | trend-on {trend.mean():.0%} of days")
    print()

    for w in ("OOS2", "train", "valid", "long"):
        start, end = WINDOWS[w]
        run = simulate(BacktestConfig(start_date=start, end_date=end, **S3_CONFIG))
        r3, inv = s3_series(run)
        dhr = dh.reindex(r3.index).fillna(0.0)
        tr = trend.reindex(r3.index).fillna(False).astype(float)
        r3 = r3.fillna(0.0)
        idle = (1.0 - inv).shift(1).fillna(1.0 - inv.iloc[0])
        corr = np.corrcoef(dhr.values, r3.values)[0, 1]

        print(f"== {w} ==")
        print(f"  S-3 alone                  {metrics(r3)}  | avgInvest {inv.mean():.2f} trend {tr.mean():.0%}")
        print(f"  DH-2 (window)              {metrics(dhr)}")
        print(f"  corr(DH2,S3)={corr:+.2f}")
        for ww in (0.3, 0.5, 0.7):
            print(f"  S1 blend w={ww:.1f}             {metrics(ww * dhr + (1 - ww) * r3)}")
        v3 = r3.rolling(60, min_periods=20).std()
        vd = dhr.rolling(60, min_periods=20).std()
        wt = ((1 / vd) / (1 / vd + 1 / v3)).shift(1).fillna(0.5).clip(0.0, 0.9)
        print(f"  S2 risk-parity             {metrics(wt * dhr + (1 - wt) * r3)}  | avgW_DH {wt.mean():.2f}")
        print(f"  S3 idle->DH2               {metrics(r3 + idle * dhr)}  | avgIdle {idle.mean():.2f}")
        park = idle.where(inv.shift(1).fillna(inv.iloc[0]) < 0.5, 0.0)
        print(f"  S4 park DH2 when invest<50% {metrics(r3 + park * dhr)}  | avgPark {park.mean():.2f}")
        print(f"  S5 idle->DH2 (trend gate)  {metrics(r3 + idle * dhr * tr)}  | avgGate {(idle*tr).mean():.2f}")
        gbl = (0.3 * tr)
        print(f"  S6 blend w=0.3+MA200 gate  {metrics(gbl * dhr + (1 - gbl) * r3)}")
        print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
