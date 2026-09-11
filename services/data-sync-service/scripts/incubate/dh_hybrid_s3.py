"""DH-1 x S-3 hybrid — Phase 0: portfolio-level vol brake on the S-3 NAV.

W1/DH-1's only edge is a realized-vol exposure scaler. Here we test whether
that same brake helps the S-3 STOCK leg. Post-hoc on the engine daily NAV:
scale each day's return by scale_t = clip(target / realized_vol_{<t}, 0, cap),
computed only from strictly past returns (no look-ahead), with a trading cost
on exposure changes. Sharpe is scale-invariant, so a Sharpe lift = real timing
value (not just de-leveraging). Read-only diagnostic.

Usage:
    PYTHONPATH=src python3 scripts/incubate/dh_hybrid_s3.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # scripts/

from data_sync_service.service.backtest_engine import BacktestConfig, simulate  # noqa: E402
from run_walk_forward import S3_CONFIG, WINDOWS  # noqa: E402


def metrics(r: np.ndarray) -> tuple[float, float, float, float]:
    nav = np.cumprod(1 + r)
    total = nav[-1] - 1
    vol = r.std() * np.sqrt(252)
    dd = float((nav / np.maximum.accumulate(nav) - 1).min())
    sharpe = (r.mean() * 252) / vol if vol > 0 else 0.0
    return total * 100, vol * 100, dd * 100, sharpe


def overlay(nav_curve: list[float], target: float, win: int, upd: int,
            cap: float = 1.0, cost_side: float = 0.001) -> tuple[np.ndarray, np.ndarray]:
    navs = np.array([1.0] + list(nav_curve))
    arr = navs[1:] / navs[:-1] - 1
    n = len(arr)
    vol = np.full(n, np.nan)
    for i in range(n):
        lo = max(0, i - win)
        if i - lo >= 20:
            vol[i] = arr[lo:i].std() * np.sqrt(252)
    scale = np.where((~np.isnan(vol)) & (vol > 0), np.minimum(cap, target / np.nan_to_num(vol, nan=1e9)), 1.0)
    scale = np.clip(scale, 0.0, cap)
    if upd > 1:  # hold exposure for `upd` sessions
        for i in range(n):
            if i % upd:
                scale[i] = scale[i - 1]
    prev = np.concatenate([[scale[0]], scale[:-1]])
    cost = np.abs(scale - prev) * 2 * cost_side
    return scale * arr - cost, scale


def main() -> int:
    base_runs = {}
    for w in ("OOS2", "train", "valid", "long"):
        start, end = WINDOWS[w]
        run = simulate(BacktestConfig(start_date=start, end_date=end, **S3_CONFIG))
        base_runs[w] = run.nav_curve
        navs = np.array([1.0] + list(run.nav_curve))
        r = navs[1:] / navs[:-1] - 1
        t, v, d, s = metrics(r)
        print(f"[{w:5s}] S-3 base          total {t:+7.1f}% vol {v:5.1f}% DD {d:6.1f}% sharpe {s:.2f}")

    print("\n=== vol brake overlay (cap 1.0) — ΔSharpe vs base ===")
    print(f"{'window':6s} {'win':>4s} {'upd':>4s} {'target':>7s} | {'total':>8s} {'vol':>6s} {'DD':>7s} {'sharpe':>6s} {'dSh':>6s} {'avgExp':>6s}")
    for w in ("OOS2", "train", "valid", "long"):
        navs = np.array([1.0] + list(base_runs[w]))
        rb = navs[1:] / navs[:-1] - 1
        _, _, _, s_base = metrics(rb)
        for win in (20, 60):
            for upd in (1, 5, 21):
                for target in (0.10, 0.15, 0.20, 0.25):
                    r, sc = overlay(base_runs[w], target, win, upd)
                    t, v, d, s = metrics(r)
                    print(f"{w:6s} {win:4d} {upd:4d} {target:7.2f} | {t:+8.1f} {v:5.1f}% {d:6.1f}% {s:6.2f} {s - s_base:+6.2f} {sc.mean():6.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
