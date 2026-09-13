#!/usr/bin/env python3
"""H-ADAPT pre-registered: asymmetric twin-star x B3 (keep strong markets, cut
weak-market drawdown). Daily causal regime switch.

Prereg & thresholds: docs/designs/twin-stable-adaptive-prereg-2026-09-12.md

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/diag_twin_stable_adaptive.py --save-report
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
REPORT_DIR = ROOT / "data" / "backtest_reports"

from diag_twin_stable_combo import _build_legs, _metrics  # noqa: E402

WINDOWS = {"OOS2": ("2024-08-01", "2025-08-01"), "train": ("2025-08-01", "2026-02-01"),
           "valid": ("2026-03-01", "2026-08-07"), "long": ("2021-01-05", "2026-08-07")}
COST = 0.0005
LABELS = {"T0": "100% 双子星", "T3": "静态50/50", "A1": "T-NAV×MA60 开关",
          "A2": "沪深300×MA200 开关", "A3": "沪深300×MA200 软切(0.5)"}


def _combine(rT: pd.Series, rS: pd.Series, target: np.ndarray) -> pd.Series:
    nav = [1.0]
    w = float(target[0])
    for i in range(1, len(rT)):
        if target[i] != w:
            nav[-1] *= (1.0 - COST * 2 * abs(target[i] - w))
            w = float(target[i])
        nav.append(nav[-1] * (1.0 + w * float(rT.iloc[i]) + (1 - w) * float(rS.iloc[i])))
    return pd.Series(nav, index=rT.index)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    T, S = _build_legs()
    rT, rS = T.pct_change().dropna(), S.pct_change().dropna()
    common = rT.index.intersection(rS.index)
    rT, rS = rT.reindex(common), rS.reindex(common)
    navT = T.reindex(common)

    from data_sync_service.config import get_settings
    with psycopg.connect(get_settings().database_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT trade_date, close FROM index_daily WHERE ts_code='000300.SH' "
            "AND trade_date BETWEEN %s AND %s ORDER BY trade_date",
            (common[0], common[-1]),
        )
        idx = pd.Series({str(d): float(c) for d, c in cur.fetchall()})
    idx = idx.reindex(common).ffill()
    print(f"aligned {common[0]}~{common[-1]} days {len(common)}; idx300 {idx.notna().sum()}")

    n = len(common)
    t_nav = navT.values
    i300 = idx.values

    def ma_series(vals: np.ndarray, win: int) -> np.ndarray:
        out = np.full(n, np.nan)
        for i in range(win, n):
            out[i] = np.mean(vals[i - win:i])  # through i-1 (shifted)
        return out

    t_ma60 = ma_series(t_nav, 60)
    i_ma200 = ma_series(i300, 200)
    t_prev = np.concatenate([[np.nan], t_nav[:-1]])  # t-1 (causal)
    i_prev = np.concatenate([[np.nan], i300[:-1]])

    tgt: dict[str, np.ndarray] = {
        "T0": np.ones(n),
        "T3": np.full(n, 0.5),
        "A1": np.where((~np.isnan(t_ma60)) & (t_prev > t_ma60), 1.0, 0.0),
        "A2": np.where((~np.isnan(i_ma200)) & (i_prev > i_ma200), 1.0, 0.0),
        "A3": np.where((~np.isnan(i_ma200)) & (i_prev > i_ma200), 1.0, 0.5),
    }
    tgt["A1"][:60] = 1.0  # warmup -> full twin (no signal)
    tgt["A2"][:200] = 1.0
    tgt["A3"][:200] = 1.0

    navs = {k: _combine(rT, rS, v) for k, v in tgt.items()}
    res: dict[str, dict] = {}
    print("\n## by window (CAGR / vol / Sharpe / MDD / Calmar / total)")
    for w, (s, e) in WINDOWS.items():
        res[w] = {}
        for k in LABELS:
            sl = navs[k][(navs[k].index >= s) & (navs[k].index <= e)]
            res[w][k] = _metrics(sl)
            res[w][k]["total"] = round(100 * (sl.iloc[-1] / sl.iloc[0] - 1), 1)
        print(f"  {w:<7} " + "  ".join(
            f"{k}:{res[w][k].get('cagr')}/{res[w][k].get('sharpe')}/{res[w][k].get('mdd')}" for k in LABELS))

    t0 = res["long"]["T0"]
    ok = []
    for k in LABELS:
        v = res["valid"][k]
        l = res["long"][k]
        o = res["OOS2"][k]
        k1 = (v.get("cagr") or -9) >= 0.8 * (res["valid"]["T0"].get("cagr") or 0)
        k2 = (l.get("mdd") or -999) >= (t0.get("mdd") or 0) + 10.0 and (o.get("mdd") or -9) > (res["OOS2"]["T0"].get("mdd") or -999)
        k3 = (l.get("calmar") or -9) >= (t0.get("calmar") or 9) and (l.get("sharpe") or -9) >= (t0.get("sharpe") or 9)
        if k1 and k2 and k3:
            ok.append(k)
        print(f"  {k} {LABELS[k]:<18} K1强市保留 {'ok' if k1 else 'FAIL'} ({v.get('cagr')}) · "
              f"K2弱市降回撤 {'ok' if k2 else 'FAIL'} (long {l.get('mdd')}, OOS2 {o.get('mdd')}) · "
              f"K3全周期 {'ok' if k3 else 'FAIL'} (calmar {l.get('calmar')}, sharpe {l.get('sharpe')})")
    print(f"\n## H-ADAPT verdict: usable = {ok if ok else 'NONE'}")

    payload = {"tag": "twin-stable-adaptive-2026-09-12",
               "prereg": "docs/designs/twin-stable-adaptive-prereg-2026-09-12.md",
               "results": res, "usable": ok, "as_of": datetime.now(UTC).isoformat(timespec="seconds")}
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        (REPORT_DIR / "twin_stable_adaptive_2026-09-12.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print("saved report")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
