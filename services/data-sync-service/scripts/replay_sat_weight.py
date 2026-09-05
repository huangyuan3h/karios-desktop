#!/usr/bin/env python3
"""40/60 prereg run: regime-weighted satellite vs frozen 50/50 — READ-ONLY.

Prereg: docs/designs/sat-weight-6040-prereg-2026-09-05.md (weights, windows,
reject gates frozen before this run). Gross basis, same recipe everywhere
(C1 3% + same_1430 + body=3 + day-3 1430 sell, strict 4x12.5%, opp_50 base).
Regime = 000001.SH 20-day change (>+3% up / <-3% down / else choppy), G1 caliber.
V1: up 0.6 / choppy 0.5 / down 0.5.  V2: up 0.6 / choppy 0.4 / down 0.4.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/replay_sat_weight.py --save-report
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from data_sync_service.db import get_connection  # noqa: E402
from data_sync_service.service.ps_g50_blend import blend_nav_opportunity  # noqa: E402
from data_sync_service.service.state_bucket_track import (  # noqa: E402
    FILL_SAME_1430,
    load_sgap_context,
    replay_sgap_from_context,
)
from pick_strong_grid import build_nav_from_cache, fetch_etf_closes, warm_window  # noqa: E402

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "2021": ("2021-01-01", "2021-12-31"),
    "2022bear": ("2022-01-01", "2022-12-31"),
    "2023": ("2023-01-01", "2023-12-31"),
}
GATE_WINDOWS = ("OOS2", "train", "valid")
REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"
W_UP, W_MID, W_DOWN_V1, W_DOWN_V2 = 0.6, 0.5, 0.5, 0.4
W_CHOP_V2 = 0.4
CLIP_NAV = 0.125
BASE_RT_BPS = 20.0
STRESS_RT_BPS = 60.0


def _stats(nav: list[float]) -> dict[str, float]:
    n = len(nav)
    if n < 2 or not nav[0]:
        return {"n_days": n, "total_pct": 0.0, "max_dd": 0.0, "sharpe": 0.0}
    total = (nav[-1] / nav[0] - 1) * 100
    peak = nav[0]
    mdd = 0.0
    for v in nav:
        if v > peak:
            peak = v
        if peak:
            mdd = max(mdd, (peak - v) / peak * 100)
    rets = [nav[i] / nav[i - 1] - 1 for i in range(1, n) if nav[i - 1] > 0]
    sharpe = 0.0
    if len(rets) > 10:
        std = float(np.std(rets))
        if std > 0:
            sharpe = float(np.mean(rets) / std * (252**0.5))
    return {"n_days": n, "total_pct": round(total, 1), "max_dd": round(mdd, 1), "sharpe": round(sharpe, 2)}


def _fmt(m: dict[str, float]) -> str:
    return f"{m['total_pct']:+.1f}/{m['sharpe']:.2f}/{m['max_dd']:.1f}"


def _blend_w(core: list[float], sat: list[float], active: list[bool], w: list[float]) -> list[float]:
    """Same formula as blend_nav_opportunity but with a per-day sat weight."""
    n = min(len(core), len(sat), len(active), len(w))
    if n == 0:
        return [1.0]
    out = [1.0]
    for i in range(1, n):
        c0, s0 = core[i - 1], sat[i - 1]
        cr = core[i] / c0 - 1.0 if c0 > 0 else 0.0
        sr = sat[i] / s0 - 1.0 if s0 > 0 else 0.0
        ret = cr + w[i] * (sr - cr) if active[i] else cr
        out.append(out[-1] * (1.0 + ret))
    return out


def _regime_map() -> dict[str, str]:
    """G1 caliber: 上证指数 (index_daily 000001.SH, NOT a stock code)."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT trade_date, close FROM index_daily WHERE ts_code = '000001.SH' "
                "AND trade_date >= '2020-11-01' AND close IS NOT NULL ORDER BY 1"
            )
            rows = [(str(d)[:10], float(c)) for d, c in cur.fetchall() if c and c > 0]
    out: dict[str, str] = {}
    closes = [c for _, c in rows]
    for i, (d, _) in enumerate(rows):
        if i < 20:
            continue
        chg = closes[i] / closes[i - 20] - 1.0
        out[d] = "up" if chg > 0.03 else ("down" if chg < -0.03 else "choppy")
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    ap.add_argument("--report-name", default="sat_weight_6040_2026-09-05.json")
    args = ap.parse_args()

    regime = _regime_map()
    print(f"regime days: {len(regime)}", flush=True)
    etf_close = fetch_etf_closes()
    results: dict[str, dict] = {}
    for ctx_range, names in ((("2021-01-01", "2023-12-31"), ("2021", "2022bear", "2023")),
                             (("2024-04-01", "2026-08-28"), ("OOS2", "train", "valid"))):
        print(f"loading context {ctx_range[0]}~{ctx_range[1]} ...", flush=True)
        ctx = load_sgap_context(*ctx_range)
        print("  loaded.", flush=True)
        for wname in names:
            s, e = WINDOWS[wname]
            print(f"=== {wname} ({s}~{e}) ===", flush=True)
            sat = replay_sgap_from_context(
                ctx, start=s, end=e, skip_t1_limit=True, pool_mode="strict",
                max_pos=4, position_pct=0.25, fill_mode=FILL_SAME_1430,
                fill_hhmm="1430", exit_hhmm="1430", max_open_to_1430_pct=0.03,
            )
            rows = sat["rows"]
            dates = [r["date"] for r in rows]
            sat_nav = [float(r["satNav"]) for r in rows]
            if sat_nav and sat_nav[0] > 0:
                sat_nav = [v / sat_nav[0] for v in sat_nav]
            active = [bool(r.get("satActive")) for r in rows]
            cache = warm_window(s, e, etf_close)
            r = build_nav_from_cache(cache, lookback=60, ma_window=200, min_hold=1,
                                     cost=0.0, score="mom", top2=False, trail_pct=8.0)
            pk_map = r["nav"]
            last = 1.0
            core = []
            for d in dates:
                v = pk_map.get(d)
                if v is not None:
                    last = v
                core.append(last)
            n = min(len(core), len(sat_nav))
            w1 = [W_UP if regime.get(d) == "up" else W_MID for d in dates[:n]]
            w2 = [W_UP if regime.get(d) == "up" else (W_CHOP_V2 if regime.get(d) != "down" else W_DOWN_V2)
                  for d in dates[:n]]
            base = blend_nav_opportunity(core[:n], sat_nav[:n], active[:n], sat_weight=0.5)
            t1 = _blend_w(core[:n], sat_nav[:n], active[:n], w1)
            t2 = _blend_w(core[:n], sat_nav[:n], active[:n], w2)
            m0, m1, m2 = _stats(base), _stats(t1), _stats(t2)
            fills = int((sat.get("summary") or {}).get("fillCount") or 0)
            up_pct = round(100.0 * sum(1 for d in dates[:n] if regime.get(d) == "up") / max(n, 1), 1)
            print(f"  base {_fmt(m0)}  V1 {_fmt(m1)} (Δ{m1['total_pct']-m0['total_pct']:+.1f})  "
                  f"V2 {_fmt(m2)} (Δ{m2['total_pct']-m0['total_pct']:+.1f})  fills {fills} up-days {up_pct}%", flush=True)
            results[wname] = {
                "base": m0, "V1": m1, "V2": m2,
                "dV1_pt": round(m1["total_pct"] - m0["total_pct"], 1),
                "dV2_pt": round(m2["total_pct"] - m0["total_pct"], 1),
                "dV1_sr": round(m1["sharpe"] - m0["sharpe"], 2),
                "dV2_sr": round(m2["sharpe"] - m0["sharpe"], 2),
                "fills": fills, "up_pct": up_pct,
            }

    print("\n| 窗口 | base 50/50 | V1 Δ | V2 Δ |")
    print("|---|---|---|---|")
    for wname in WINDOWS:
        r = results[wname]
        print(f"| {wname} | {_fmt(r['base'])} | {r['dV1_pt']:+.1f}/{r['dV1_sr']:+.2f} | {r['dV2_pt']:+.1f}/{r['dV2_sr']:+.2f} |")
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        path = REPORT_DIR / args.report_name
        path.write_text(json.dumps({"tag": "sat-weight-6040-2026-09-05", "windows": results,
                                    "as_of": datetime.now(UTC).isoformat(timespec="seconds")},
                                   ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(f"saved {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
