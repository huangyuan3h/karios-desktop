#!/usr/bin/env python3
"""Evaluate the ETF layer ON TOP of the S-3 stock leg (clean, post-OPT-177).

Baseline = S-3 engine NAV (CN+HK, next_open). Then the pick-strong core adds
the ETF menu (GOLD/OIL/NASDAQ/BOND10). All ETF-trail comparisons use the causal
implementation (trail_causal=True).

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/eval_s3_etf_layer.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from data_sync_service.service.backtest_engine import BacktestConfig, simulate  # noqa: E402
from pick_strong_grid import MULTI_TS, build_nav_from_cache, fetch_etf_closes, warm_window  # noqa: E402
from run_walk_forward import HK_S3_CONFIG, S3_CONFIG  # noqa: E402

START, END = "2021-01-04", "2026-08-07"
WINDOWS = {"OOS2": ("2024-08-01", "2025-08-01"), "train": ("2025-08-01", "2026-02-01"),
           "valid": ("2026-03-01", "2026-08-07"), "long": (START, END)}
ETF_KEYS = set(MULTI_TS)


def _metrics(dates: list[str], navs: list[float], s: str, e: str) -> dict:
    pts = [(d, v) for d, v in zip(dates, navs) if s <= d <= e and v > 0]
    if len(pts) < 5:
        return {}
    nv = np.array([v for _, v in pts])
    r = nv[1:] / nv[:-1] - 1.0
    years = len(nv) / 242.0
    cagr = (nv[-1] / nv[0]) ** (1 / years) - 1
    vol = float(r.std() * np.sqrt(242))
    sharpe = float(r.mean() / r.std() * np.sqrt(242)) if r.std() > 0 else None
    cum = nv / np.maximum.accumulate(nv)
    mdd = float(cum.min() - 1.0)
    return {"total": round(100 * (nv[-1] / nv[0] - 1), 1), "cagr": round(100 * cagr, 2),
            "vol": round(100 * vol, 1), "sharpe": round(sharpe, 2) if sharpe else None,
            "mdd": round(100 * mdd, 1)}


def main() -> int:
    etf_close = fetch_etf_closes()
    print(f"warm_window {START}~{END} ...", flush=True)
    cache = warm_window(START, END, etf_close)

    cfg_cn = BacktestConfig(start_date=START, end_date=END, **S3_CONFIG)
    cfg_hk = BacktestConfig(start_date=START, end_date=END, **HK_S3_CONFIG)
    run_cn = simulate(cfg_cn, cache["data_cn"])
    run_hk = simulate(cfg_hk, cache["data_hk"])

    runs: dict[str, tuple[list[str], list[float]]] = {}
    runs["S-3 CN 单独"] = (list(cache["data_cn"].calendar), list(run_cn.nav_curve))
    runs["S-3 CN+HK"] = (list(cache["calendar"]),
                         _blend_cn_hk(cache, run_cn, run_hk))

    core0 = build_nav_from_cache(cache, trail_pct=0.0, lookback=60, ma_window=200, min_hold=1, cost=0.0)
    core8 = build_nav_from_cache(cache, trail_pct=8.0, trail_causal=True, lookback=60, ma_window=200, min_hold=1, cost=0.0)
    etf_only = build_nav_from_cache(cache, trail_pct=0.0, lookback=60, ma_window=200, min_hold=1, cost=0.0, stock_min_n=999)
    runs["S-3+ETF (无trail)"] = (sorted(core0["nav"]), [core0["nav"][d] for d in sorted(core0["nav"])])
    runs["S-3+ETF (因果trail8)"] = (sorted(core8["nav"]), [core8["nav"][d] for d in sorted(core8["nav"])])
    runs["ETF-only"] = (sorted(etf_only["nav"]), [etf_only["nav"][d] for d in sorted(etf_only["nav"])])

    print(f"\n{'配置':<22}" + "".join(f"{w:>9}" for w in WINDOWS) + "    (total%)")
    for name, (ds, nv) in runs.items():
        cells = "".join(f"{str(_metrics(ds, nv, *WINDOWS[w]).get('total')):>9}" for w in ("OOS2", "train", "valid", "long"))
        print(f"{name:<22}{cells}")

    print(f"\n{'配置':<22}" + "".join(f"{w:>9}" for w in WINDOWS) + "    (Sharpe)")
    for name, (ds, nv) in runs.items():
        cells = "".join(f"{str(_metrics(ds, nv, *WINDOWS[w]).get('sharpe')):>9}" for w in ("OOS2", "train", "valid", "long"))
        print(f"{name:<22}{cells}")

    print(f"\n{'配置':<22}" + "".join(f"{w:>9}" for w in WINDOWS) + "    (MDD%)")
    for name, (ds, nv) in runs.items():
        cells = "".join(f"{str(_metrics(ds, nv, *WINDOWS[w]).get('mdd')):>9}" for w in ("OOS2", "train", "valid", "long"))
        print(f"{name:<22}{cells}")

    # pick decomposition (long window)
    pm = core0["pick_map"]
    days = [d for d in sorted(pm) if WINDOWS["long"][0] <= d <= WINDOWS["long"][1]]
    from collections import Counter
    cnt = Counter()
    for d in days:
        p = pm[d]
        cnt["STOCK" if p == "STOCK" else ("REPO" if p == "REPO" else "ETF")] += 1
    tot = sum(cnt.values()) or 1
    print(f"\n核心持仓分布（long {len(days)} 天）：STOCK {cnt['STOCK']/tot:.0%} · "
          f"ETF {cnt['ETF']/tot:.0%} · REPO {cnt['REPO']/tot:.0%}")
    return 0


def _blend_cn_hk(cache: dict, run_cn, run_hk) -> list[float]:
    """CN+HK 50/50 daily-return blend on the master calendar (matches engine convention)."""
    cn = {str(d): v for d, v in zip(cache["data_cn"].calendar, run_cn.nav_curve)}
    hk = {str(d): v for d, v in zip(cache["data_hk"].calendar, run_hk.nav_curve)}
    cal = list(cache["calendar"])
    out = [1.0]
    lc = lh = 1.0
    for i in range(1, len(cal)):
        d, p = cal[i], cal[i - 1]
        rc = (cn.get(d, lc) / cn.get(p, lc) - 1.0) if cn.get(p) else 0.0
        rh = (hk.get(d, lh) / hk.get(p, lh) - 1.0) if hk.get(p) else 0.0
        lc, lh = cn.get(d, lc), hk.get(d, lh)
        out.append(out[-1] * (1.0 + 0.5 * rc + 0.5 * rh))
    return out


if __name__ == "__main__":
    raise SystemExit(main())
