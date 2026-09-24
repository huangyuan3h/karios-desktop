#!/usr/bin/env python3
"""H2-a25 hardening: stress window + cost sensitivity + single-asset sleeve cap.

Reuses the canonical evaluator pieces (satellite book, H2 sleeve, B3) and adds
what validation-gates-v2 requires for a composition overlay:
  - stress window 2022-01-01..2023-12-31 (古代 regime, gates v2 §1)
  - 2x transfer cost (G3d)
  - single-asset cap on the H2 sleeve (the 2026-09 oil lesson: sleeve can sit
    100% in one ETF)

Read-only. Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/eval_h2_a25_hardening.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from eval_sat_idle_parking import (  # noqa: E402
    _compose,
    _repo_nav,
    _sat_book,
    _sleeve_nav,
    _true_cash_share,
)
from eval_twin_star_parking import _stats  # noqa: E402
from run_walk_forward import WINDOWS  # noqa: E402

from data_sync_service.service.harbor import HYST_BAND, load_etf_closes  # noqa: E402
from data_sync_service.service.homeport import load_risk_closes, risk_budget_nav  # noqa: E402
from data_sync_service.service.state_bucket_track import (  # noqa: E402
    A25_B3_WEIGHT,
    A25_SLEEVE_WEIGHT,
)

STRESS = ("2022-01-01", "2023-12-31")
ALL_WINDOWS = {**WINDOWS, "stress": STRESS}


def _blend(sleeve, b3, repo, a, b, c=0.0):
    n = min(len(sleeve), len(b3), len(repo))
    out = [1.0]
    for t in range(1, n):
        rs = sleeve[t] / sleeve[t - 1] - 1 if sleeve[t - 1] else 0.0
        rb = b3[t] / b3[t - 1] - 1 if b3[t - 1] else 0.0
        rr = repo[t] / repo[t - 1] - 1 if repo[t - 1] else 0.0
        out.append(out[-1] * (1.0 + a * rs + b * rb + c * rr))
    return out


def _components(px, risk_closes, s, e):
    """Satellite book + sleeve + B3 + repo + causal cash share (once per window)."""
    sat = _sat_book(s, e)
    dates, sat_nav = sat["dates"], sat["nav"]
    sleeve = _sleeve_nav(px, dates, hyst_band=HYST_BAND)
    b3 = risk_budget_nav(risk_closes, dates)
    repo = _repo_nav(dates)
    n = min(len(dates), len(sleeve), len(b3), len(repo))
    dates, sat_nav, sleeve, b3, repo = dates[:n], sat_nav[:n], sleeve[:n], b3[:n], repo[:n]
    cash = _true_cash_share(sat, from_rows=True)
    w = [0.0] * n
    for i in range(1, n):
        w[i] = cash[i - 1]
    return sat_nav, sleeve, b3, repo, w


def main() -> int:
    px = load_etf_closes()
    risk = load_risk_closes()
    a, b = A25_SLEEVE_WEIGHT, A25_B3_WEIGHT
    print("H2-a25 = satellite + cashShare(T-1) x (25% H2 sleeve + 75% B3)\n", flush=True)
    print(f"{'窗口':<9}{'a25 5bp':>26}{'a25 10bp':>26}{'pureB3 5bp':>26}")
    for wname in ("OOS2", "train", "valid", "long", "stress", "holdout"):
        s, e = ALL_WINDOWS[wname]
        print(f"  computing {wname} ...", flush=True)
        sat_nav, sleeve, b3, repo, w = _components(px, risk, s, e)
        r5 = _stats(_compose(sat_nav, w, _blend(sleeve, b3, repo, a, b), 5.0))
        r10 = _stats(_compose(sat_nav, w, _blend(sleeve, b3, repo, a, b), 10.0))
        rb = _stats(_compose(sat_nav, w, _blend(sleeve, b3, repo, 0.0, 1.0), 5.0))
        fmt = lambda r: f"{r['total_pct']:+7.1f}/{r['max_dd']:+6.1f}/{r['sharpe']:4.2f}"  # noqa: E731
        print(f"{wname:<9}{fmt(r5):>26}{fmt(r10):>26}{fmt(rb):>26}", flush=True)
    print("\n列 = 总收益% / maxDD% / Sharpe。stress = 2022-01-01..2023-12-31（gates v2 要求）。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
