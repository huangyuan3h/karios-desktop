#!/usr/bin/env python3
"""Detailed 3-window head-to-head: 习惯双子星 (T0) vs 稳健双子星 (T3 = 50/50).

Rich metrics per window: total/CAGR/vol/Sharpe/Sortino/MDD/Calmar/DD-duration,
daily & monthly win-rate, best/worst month. Reuses the frozen H-COMBO legs.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/compare_twin_stable_detail.py --save-report
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
REPORT_DIR = ROOT / "data" / "backtest_reports"

from diag_twin_stable_combo import _build_legs, _combine  # noqa: E402

WINDOWS = {"OOS2": ("2024-08-01", "2025-08-01"), "train": ("2025-08-01", "2026-02-01"),
           "valid": ("2026-03-01", "2026-08-07"), "long": ("2021-01-05", "2026-08-07")}


def _detail(nav: pd.Series) -> dict:
    if len(nav) < 5:
        return {"n": len(nav)}
    nav = nav.copy()
    nav.index = pd.to_datetime(nav.index)
    r = nav.pct_change().dropna()
    years = len(nav) / 242.0
    total = nav.iloc[-1] / nav.iloc[0] - 1
    cagr = (1 + total) ** (1 / years) - 1
    vol = float(r.std() * np.sqrt(242))
    sharpe = float(r.mean() / r.std() * np.sqrt(242)) if r.std() > 0 else None
    downside = r[r < 0]
    ds = float(downside.std() * np.sqrt(242)) if len(downside) > 1 else 0.0
    sortino = float(r.mean() * 242 / ds) if ds > 0 else None
    cum = nav / nav.cummax()
    dd = cum - 1.0
    mdd = float(dd.min())
    calmar = (cagr / abs(mdd)) if mdd < 0 else None
    # longest drawdown duration (sessions)
    longest = cur = 0
    for v in dd.values:
        cur = cur + 1 if v < -1e-9 else 0
        longest = max(longest, cur)
    monthly = nav.resample("ME").last().pct_change().dropna()
    return {
        "n": len(nav), "total": round(100 * total, 1), "cagr": round(100 * cagr, 2),
        "vol": round(100 * vol, 2), "sharpe": round(sharpe, 2) if sharpe is not None else None,
        "sortino": round(sortino, 2) if sortino is not None else None,
        "mdd": round(100 * mdd, 2), "calmar": round(calmar, 2) if calmar is not None else None,
        "dd_days_max": longest,
        "daily_win": round(100 * float((r > 0).mean()), 1),
        "month_win": round(100 * float((monthly > 0).mean()), 1) if len(monthly) else None,
        "best_month": round(100 * float(monthly.max()), 1) if len(monthly) else None,
        "worst_month": round(100 * float(monthly.min()), 1) if len(monthly) else None,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    T, S = _build_legs()
    rT, rS = T.pct_change().dropna(), S.pct_change().dropna()
    common = rT.index.intersection(rS.index)
    rT, rS = rT.reindex(common), rS.reindex(common)

    legs = {
        "习惯双子星(T0)": _combine(rT, rS, 1.0),
        "稳健双子星(T3)": _combine(rT, rS, 0.5),
        "B3稳健核心": _combine(rT, rS, 0.0),
    }
    print(f"aligned {common[0]}~{common[-1]}  days {len(common)}  corr {np.corrcoef(rT, rS)[0,1]:.3f}")

    res: dict[str, dict] = {}
    for w, (s, e) in WINDOWS.items():
        res[w] = {}
        for name, nav in legs.items():
            sl = nav[(nav.index >= s) & (nav.index <= e)]
            res[w][name] = _detail(sl)

    hdr = f"{'窗口':<7}{'策略':<16}{'总收益':>8}{'CAGR':>7}{'波动':>7}{'Sharpe':>7}{'Sortino':>8}{'MDD':>7}{'Calmar':>7}{'回撤天':>7}{'日胜':>6}{'月胜':>6}{'最差月':>7}"
    print("\n" + hdr)
    for w in WINDOWS:
        for name in legs:
            m = res[w][name]
            print(f"{w:<7}{name:<16}{m['total']:>8}{m['cagr']:>7}{m['vol']:>7}{m['sharpe']:>7}"
                  f"{str(m['sortino']):>8}{m['mdd']:>7}{str(m['calmar']):>7}{m['dd_days_max']:>7}"
                  f"{m['daily_win']:>6}{str(m['month_win']):>6}{m['worst_month']:>7}")
        print()

    payload = {"tag": "twin-vs-stable-detail-2026-09-12", "results": res,
               "as_of": datetime.now(UTC).isoformat(timespec="seconds")}
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        (REPORT_DIR / "twin_vs_stable_detail_2026-09-12.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print("saved report")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
