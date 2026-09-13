#!/usr/bin/env python3
"""Estimate the 双子星 + 打新 overlay add WITHOUT touching the main strategy.

The frozen twin-star already holds CN stocks when core picks STOCK or the
satellite is active; those are the 打新 底仓 (no extra capital, no timing change).
Quota is per-market (沪/深) on the T-2 trailing-20-session average market value,
so we measure 沪/深 NAV shares separately and price each market's IPOs.

Read-only. No strategy changes.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/diag_twin_ipo_overlay.py --save-report
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from data_sync_service.service.state_bucket_track import (  # noqa: E402
    FILL_SAME_1430,
    load_sgap_context,
    replay_sgap_from_context,
)
from pick_strong_grid import build_nav_from_cache, fetch_etf_closes, warm_window  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
PERF = ROOT / "data" / "ipo" / "ipo_perf.csv"
REPORT_DIR = ROOT / "data" / "backtest_reports"
WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "past_year": ("2025-08-01", "2026-08-07"),
}
SAT_W = 0.5
AUM = (200_000, 500_000, 1_000_000, 3_000_000)


def _mkt(ts_code: str) -> str | None:
    p = str(ts_code).split(".")[0]
    if p.startswith(("688", "689", "600", "601", "603", "605")):
        return "SH"
    if p.startswith(("300", "301", "000", "001", "002", "003")):
        return "SZ"
    return None


def _is_cn(ts_code: str) -> bool:
    return not str(ts_code).upper().endswith(".HK")


def _load_ipo_long() -> list[dict]:
    rows = [r for r in csv.DictReader(PERF.open()) if r["board"] != "bj"]
    out = []
    for r in rows:
        d = r["ipo_date"]
        iso = f"{d[:4]}-{d[4:6]}-{d[6:8]}" if len(d) == 8 else d
        if "2021-01-01" <= iso <= "2026-08-07":
            r["ipo_date"] = iso
            out.append(r)
    return out


def _profit(ipos: list[dict], sh_wan: float, sz_wan: float) -> float:
    """Annual 打新 profit (yuan) for 沪/深 底仓 market values (万元)."""
    profit = 0.0
    for r in ipos:
        mk = _mkt(r["ts_code"])
        if mk is None:
            continue
        try:
            price = float(r["price"]); ballot = float(r["ballot"])
            limit = float(r["limit_amount"]); sell = float(r["sell_ret"]) / 100.0
        except (ValueError, KeyError):
            continue
        if price <= 0:
            continue
        mv = sh_wan if mk == "SH" else sz_wan
        quota = min(mv * 1000.0, limit * 10000.0)
        profit += quota * ballot / 100.0 * price * sell
    return profit


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    ipos = _load_ipo_long()
    yrs = 5.6
    print(f"IPO long universe: {len(ipos)} (2021-01~2026-08, ex-BJ), {yrs}y\n", flush=True)

    ctx = load_sgap_context("2024-08-01", "2026-08-07")
    etf_close = fetch_etf_closes()

    results = {}
    for w, (s, e) in WINDOWS.items():
        print(f"=== {w} ({s}~{e}) ===", flush=True)
        sat = replay_sgap_from_context(
            ctx, start=s, end=e, skip_t1_limit=True, pool_mode="strict",
            max_pos=4, position_pct=0.25, fill_mode=FILL_SAME_1430,
            fill_hhmm="1430", exit_hhmm="1430", max_open_to_1430_pct=0.03,
            rank_key="amp_1430",
        )
        dates = [r["date"] for r in sat["rows"]]
        active = [bool(r.get("satActive")) for r in sat["rows"]]
        cache = warm_window(s, e, etf_close)
        r = build_nav_from_cache(cache, lookback=60, ma_window=200, min_hold=1, cost=0.0,
                                 score="mom", top2=False, trail_pct=8.0)
        pick_map = r["pick_map"]
        snap_cn, snap_hk = cache["snap_cn"], cache["snap_hk"]

        sh_nav: list[float] = []
        sz_nav: list[float] = []
        stock_days = 0
        for i, day in enumerate(dates):
            cn_share = 0.0
            sh_frac = 0.5
            if pick_map.get(day) == "STOCK":
                stock_days += 1
                pos = []
                for snap in (snap_cn.get(day), snap_hk.get(day)):
                    if snap:
                        pos += list(snap.get("positions") or [])
                cn = [p for p in pos if _is_cn(p.get("ts_code"))]
                if pos:
                    cn_share = len(cn) / len(pos)
                if cn:
                    n_sh = sum(1 for p in cn if _mkt(p.get("ts_code")) == "SH")
                    sh_frac = n_sh / len(cn)
            act = active[i] if i < len(active) else False
            core_w = 1 - SAT_W * act
            core_cn = core_w * cn_share
            sat_cn = SAT_W * act  # satellite S-gap stocks are CN; assume 50/50 market
            sh_nav.append(core_cn * sh_frac + sat_cn * 0.5)
            sz_nav.append(core_cn * (1 - sh_frac) + sat_cn * 0.5)

        arr_sh, arr_sz = np.array(sh_nav), np.array(sz_nav)
        sm_sh = [float(arr_sh[max(0, i - 19):i + 1].mean()) for i in range(len(arr_sh))]
        sm_sz = [float(arr_sz[max(0, i - 19):i + 1].mean()) for i in range(len(arr_sz))]
        br_sh, br_sz = float(np.mean(sm_sh)), float(np.mean(sm_sz))
        results[w] = {
            "stock_occupancy_pct": round(100 * stock_days / len(dates), 1),
            "avg_cn_nav_pct": round(100 * (arr_sh + arr_sz).mean(), 1),
            "budget_sh_pct": round(100 * br_sh, 1),
            "budget_sz_pct": round(100 * br_sz, 1),
        }
        print(f"  STOCK occ {results[w]['stock_occupancy_pct']}%  avg CN NAV {results[w]['avg_cn_nav_pct']}%  "
              f"20d budget 沪 {results[w]['budget_sh_pct']}% / 深 {results[w]['budget_sz_pct']}%", flush=True)

    print("\n## 组合级打新年化增量（占 AUM %）")
    print("  window        " + "".join(f"{a//10000:>7}万" for a in AUM))
    overlay = {}
    for w in WINDOWS:
        br_sh = results[w]["budget_sh_pct"] / 100.0
        br_sz = results[w]["budget_sz_pct"] / 100.0
        cells = []
        for a in AUM:
            sh_wan = br_sh * a / 10000.0
            sz_wan = br_sz * a / 10000.0
            ann = _profit(ipos, sh_wan, sz_wan) / yrs      # 元/年
            cells.append(ann / a * 100)                    # 占 AUM %
        overlay[w] = {str(a): round(c, 2) for a, c in zip(AUM, cells)}
        print(f"  {w:<12}" + "".join(f"{c:>+7.2f}" for c in cells))
    print()

    payload = {"tag": "twin-ipo-overlay-2026-09-12", "windows": results,
               "portfolio_add_pct": overlay, "aum": list(AUM),
               "as_of": datetime.now(UTC).isoformat(timespec="seconds")}
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        (REPORT_DIR / "twin_ipo_overlay_2026-09-12.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print("saved report")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
