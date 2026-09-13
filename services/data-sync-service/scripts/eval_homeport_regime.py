#!/usr/bin/env python3
"""Homeport market-state switch (H-MIX-DYN · 2026-09-13).

CSI300 x MA200 (t-1 close) decides the Harbor weight: strong market ->
100% Harbor, otherwise -> static 50/50 (Harbor x B3). See
docs/designs/homeport-regime-prereg-2026-09-13.md

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/eval_homeport_regime.py --save-report
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
REPORT_DIR = ROOT / "data" / "backtest_reports"

from eval_harbor_riskbudget import (  # noqa: E402
    COST,
    WINS,
    _blend_monthly,
    _harbor_nav,
    _load_panel,
    _row,
    _rp_nav,
    _slice,
)

CSI300 = "510300.SH"
STRESS = ("2022-01-01", "2023-12-31")
W_STRONG = 1.0
W_WEAK = 0.5


def _close_on_cal(raw: dict[str, float], cal: list[str]) -> list[float | None]:
    out: list[float | None] = []
    last = None
    for d in cal:
        v = raw.get(d)
        if v is not None:
            last = v
        out.append(last)
    return out


def _ma_on_cal(raw: dict[str, float], cal: list[str], window: int = 200) -> list[float | None]:
    ds = sorted(raw)
    vals = [raw[d] for d in ds]
    out: list[float | None] = []
    j = 0
    for d in cal:
        while j < len(ds) and ds[j] <= d:
            j += 1
        out.append(float(np.mean(vals[j - window : j])) if j >= window else None)
    return out


def _targets(px: dict[str, dict[str, float]], cal: list[str]) -> list[float]:
    closes = _close_on_cal(px.get(CSI300) or {}, cal)
    mas = _ma_on_cal(px.get(CSI300) or {}, cal)
    target = [W_WEAK]
    for i in range(1, len(cal)):
        c, m = closes[i - 1], mas[i - 1]
        target.append(W_STRONG if c is not None and m is not None and c > m else W_WEAK)
    return target


def _blend_switch(
    a: list[float], b: list[float], target: list[float], cost: float
) -> tuple[list[float], int, int]:
    out = [1.0]
    w = target[0]
    switches = 0
    days_strong = 0
    for i in range(1, len(target)):
        if target[i] != target[i - 1]:
            out[-1] *= 1.0 - cost * abs(target[i] - target[i - 1])
            w = target[i]
            switches += 1
        if target[i] == W_STRONG:
            days_strong += 1
        ra = a[i] / a[i - 1] - 1.0 if a[i - 1] else 0.0
        rb = b[i] / b[i - 1] - 1.0 if b[i - 1] else 0.0
        port = 1.0 + w * ra + (1.0 - w) * rb
        out.append(out[-1] * port)
        if port > 0:
            w = w * (1.0 + ra) / port
    return out, switches, days_strong


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    px = _load_panel()
    res: dict[str, dict] = {}
    legs: dict[str, tuple] = {}
    for w, (s, e) in WINS.items():
        cal, harbor = _harbor_nav(px, s, e)
        rp = _rp_nav(px, cal, COST)
        m50 = _blend_monthly(harbor, rp, 0.5, cal, COST)
        target = _targets(px, cal)
        dyn, switches, days_strong = _blend_switch(harbor, rp, target, COST)
        legs[w] = (cal, harbor, m50, dyn)
        res[w] = {
            "harbor": _row(harbor),
            "m50": _row(m50),
            "dyn": _row(dyn),
            "switches": switches,
            "days_strong": days_strong,
            "days": len(cal),
        }
        print(
            f"  {w:<6} harbor {res[w]['harbor']['total']:+7.1f}  "
            f"m50 {res[w]['m50']['total']:+7.1f}  dyn {res[w]['dyn']['total']:+7.1f}"
            f"  (cagr {res[w]['dyn']['cagr']:.1f} mdd {res[w]['dyn']['mdd']:.1f} "
            f"sr {res[w]['dyn']['sharpe']:.2f})  strong {days_strong}/{len(cal)}d  "
            f"switches {switches}"
        )

    k1 = res["long"]["dyn"]["cagr"] >= res["long"]["m50"]["cagr"]
    k2 = res["long"]["dyn"]["sharpe"] >= res["long"]["m50"]["sharpe"]
    k3 = res["long"]["dyn"]["mdd"] >= res["long"]["m50"]["mdd"] - 2.0
    k4 = all(
        res[w]["dyn"]["cagr"] >= res[w]["m50"]["cagr"] - 5.0 for w in ("OOS2", "train", "valid")
    )
    k5 = res["valid"]["dyn"]["cagr"] >= 0.8 * res["valid"]["harbor"]["cagr"]
    cal_l, _, m50_l, dyn_l = legs["long"]
    stress_m50 = _row(_slice(m50_l, cal_l, *STRESS))
    stress_dyn = _row(_slice(dyn_l, cal_l, *STRESS))
    k6 = stress_dyn["mdd"] >= stress_m50["mdd"] - 2.0
    verdict = {"k1": k1, "k2": k2, "k3": k3, "k4": k4, "k5": k5, "k6": k6}
    verdict["pass"] = all(verdict.values())

    print("\n## verdict (H-MIX-DYN · frozen K1-K6)")
    for name, ok in verdict.items():
        print(f"  {name}: {'ok' if ok else 'FAIL'}")
    print(
        f"  long: m50 cagr/sr/mdd {res['long']['m50']['cagr']:.2f}/{res['long']['m50']['sharpe']:.2f}/"
        f"{res['long']['m50']['mdd']:.1f}  vs dyn {res['long']['dyn']['cagr']:.2f}/"
        f"{res['long']['dyn']['sharpe']:.2f}/{res['long']['dyn']['mdd']:.1f}"
    )
    print(
        f"  stress 2022-23: m50 {stress_m50['sharpe']:.2f}/{stress_m50['mdd']:.1f} "
        f"vs dyn {stress_dyn['sharpe']:.2f}/{stress_dyn['mdd']:.1f}"
    )
    print(f"  DYN {'PASS -> product candidate' if verdict['pass'] else 'REJECT'}")

    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        (REPORT_DIR / "homeport_regime_2026-09-13.json").write_text(
            json.dumps(
                {
                    "tag": "homeport-regime-2026-09-13",
                    "prereg": "docs/designs/homeport-regime-prereg-2026-09-13.md",
                    "signal": f"{CSI300} close(t-1) > MA200(t-1)",
                    "stress": {"m50": stress_m50, "dyn": stress_dyn},
                    "results": res,
                    "verdict": verdict,
                    "as_of": datetime.now(UTC).isoformat(timespec="seconds"),
                },
                ensure_ascii=False,
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )
        print("saved report")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
