#!/usr/bin/env python3
"""H-COMBO pre-registered diagnostic: twin-star x stable core (B3), portfolio-level.

T = twin-star clip4 opp_50 (frozen engine). S = B3 inverse-vol stable core
(diag_stable_core). Combines at monthly-rebalanced fixed weights + a risk-band.
Prereg & thresholds: docs/designs/twin-stable-combo-prereg-2026-09-12.md

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/diag_twin_stable_combo.py --save-report
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

from data_sync_service.service.ps_g50_blend import blend_nav_opportunity  # noqa: E402
from data_sync_service.service.state_bucket_track import (  # noqa: E402
    load_sgap_context,
    replay_sgap_from_context,
)
from pick_strong_grid import fetch_etf_closes  # noqa: E402

from compare_sat_clip import _pick_strong_nav, _sat_series  # noqa: E402
from diag_stable_core import _load as load_etf, _nav as b3_nav  # noqa: E402

START, END = "2021-01-04", "2026-08-07"
CTX_START, CTX_END = "2020-11-01", "2026-08-07"
WINDOWS = {"OOS2": ("2024-08-01", "2025-08-01"), "train": ("2025-08-01", "2026-02-01"),
           "valid": ("2026-03-01", "2026-08-07"), "long": (START, END)}
COST = 0.0005
LABELS = {"T0": "100% 双子星", "T1": "80T/20S", "T2": "60T/40S", "T3": "50T/50S", "T4": "风险预算(1/σ)"}


def _build_legs() -> tuple[pd.Series, pd.Series]:
    print("loading S-gap context ...", flush=True)
    ctx = load_sgap_context(CTX_START, CTX_END)
    etf_close = fetch_etf_closes()
    sat = replay_sgap_from_context(ctx, start=START, end=END, skip_t1_limit=True,
                                   pool_mode="strict", max_pos=4, position_pct=0.125)
    dates, sat_nav, _slots, active = _sat_series(sat)
    core = _pick_strong_nav(dates, START, END, etf_close)
    n = min(len(core), len(sat_nav), len(active))
    twin = blend_nav_opportunity(core[:n], sat_nav[:n], active[:n], sat_weight=0.5)
    T = pd.Series(twin, index=dates[:n])
    dfb = load_etf()
    S = b3_nav(dfb, "B3")
    common = T.index.intersection(S.index)
    return T.reindex(common).dropna(), S.reindex(common).dropna()


def _metrics(nav: pd.Series) -> dict:
    if len(nav) < 5:
        return {"n": len(nav)}
    r = nav.pct_change().dropna()
    years = len(nav) / 242.0
    cagr = (nav.iloc[-1] / nav.iloc[0]) ** (1 / years) - 1
    vol = float(r.std() * np.sqrt(242))
    sharpe = float(r.mean() / r.std() * np.sqrt(242)) if r.std() > 0 else None
    cum = nav / nav.cummax()
    mdd = float(cum.min() - 1.0)
    calmar = (cagr / abs(mdd)) if mdd < 0 else None
    return {"n": len(nav), "cagr": round(100 * cagr, 2), "vol": round(100 * vol, 2),
            "sharpe": round(sharpe, 2) if sharpe is not None else None,
            "mdd": round(100 * mdd, 2),
            "calmar": round(calmar, 2) if calmar is not None else None}


def _combine(rT: pd.Series, rS: pd.Series, wT: float | None) -> pd.Series:
    """Monthly-rebalanced two-leg nav. wT=None -> risk-budget (1/σ60, shifted)."""
    idx = rT.index
    rebal = {}
    seen = set()
    for d in idx:
        m = d[:7]
        if m not in seen:
            seen.add(m)
            rebal[d] = True
    w = wT if wT is not None else 0.5
    nav = [1.0]
    for i in range(1, len(idx)):
        a, b = float(rT.iloc[i]), float(rS.iloc[i])
        port = w * a + (1 - w) * b
        nav.append(nav[-1] * (1 + port))
        denom = w * (1 + a) + (1 - w) * (1 + b)
        if denom > 0:
            w = w * (1 + a) / denom
        if rebal.get(idx[i]) and i >= 60:
            if wT is not None:
                target = wT
            else:
                sT = float(rT.iloc[i - 60:i].std())
                sS = float(rS.iloc[i - 60:i].std())
                invT = 1.0 / sT if sT > 0 else 0.0
                invS = 1.0 / sS if sS > 0 else 0.0
                target = invT / (invT + invS) if (invT + invS) > 0 else 0.5
            cost = COST * 2 * abs(target - w)
            nav[-1] *= (1 - cost)
            w = target
    return pd.Series(nav, index=idx)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    T, S = _build_legs()
    rT, rS = T.pct_change().dropna(), S.pct_change().dropna()
    common = rT.index.intersection(rS.index)
    rT, rS = rT.reindex(common), rS.reindex(common)
    print(f"legs aligned {common[0]} -> {common[-1]}  days {len(common)}", flush=True)
    corr = float(np.corrcoef(rT.values, rS.values)[0, 1])
    print(f"leg correlation (daily) {corr:.3f}")

    combos = {"T0": _combine(rT, rS, 1.0), "T1": _combine(rT, rS, 0.8),
              "T2": _combine(rT, rS, 0.6), "T3": _combine(rT, rS, 0.5),
              "T4": _combine(rT, rS, None)}

    print("\n## long window (CAGR% / vol% / Sharpe / MDD% / Calmar)")
    res: dict[str, dict] = {}
    for b, lbl in LABELS.items():
        res[b] = {w: _metrics(combos[b][(combos[b].index >= s) & (combos[b].index <= e)]) for w, (s, e) in WINDOWS.items()}
        m = res[b]["long"]
        print(f"  {b} {lbl:<16} {m['cagr']}/{m['vol']}/{m['sharpe']}/{m['mdd']}/{m.get('calmar')}")

    print("\n## by window (CAGR / Sharpe / MDD)")
    for w in WINDOWS:
        print(f"  {w:<7} " + "  ".join(
            f"{b}:{res[b][w].get('cagr')}/{res[b][w].get('sharpe')}/{res[b][w].get('mdd')}" for b in LABELS))

    t0 = res["T0"]["long"]
    ok = []
    for b in LABELS:
        m = res[b]["long"]
        if (m.get("mdd") or -999) >= (t0.get("mdd") or 0) + 10.0 \
                and (m.get("sharpe") or -9) >= (t0.get("sharpe") or 9) - 0.10 \
                and (m.get("cagr") or -9) >= 0.5 * (t0.get("cagr") or 0):
            ok.append(b)
    print(f"\n## H-COMBO verdict: stable combos = {ok if ok else 'NONE'}")
    print(f"   (long: MDD <= T0-10pt & Sharpe >= T0-0.10 & CAGR >= 0.5*T0; T0 "
          f"{t0.get('cagr')}/{t0.get('sharpe')}/{t0.get('mdd')})")

    payload = {"tag": "twin-stable-combo-2026-09-12",
               "prereg": "docs/designs/twin-stable-combo-prereg-2026-09-12.md",
               "corr_daily": round(corr, 3), "results": res, "stable": ok,
               "as_of": datetime.now(UTC).isoformat(timespec="seconds")}
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        (REPORT_DIR / "twin_stable_combo_2026-09-12.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print("\nsaved report")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
