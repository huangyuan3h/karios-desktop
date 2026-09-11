#!/usr/bin/env python3
"""Which decision clock should Live and the backtest share?

All variants use the SAME engine / core blend / clip4 (4×12.5%), only the
signal+fill clock changes:
  frozen_nextopen  signal = T-1 close (non-lookahead), fill = T open, exit close
  habit_1430       signal/fill = T 14:30 print, C1 3%, exit day-3 14:30 (Live)
  s1430_close      signal/fill = T 14:30 print, no C1, exit day-3 close

Ranking inside the bucket uses each clock's own-day amplitude (the frozen
engine ranks next_open on the signal day's full-day amp = known at T-1 close;
same_1430 ranks on the decision day's daily amp, a known lookahead residual).

Score twin tot + Sharpe + maxDD vs core across OOS2 / train / valid +
past_year / aligned. Frozen engine untouched for Live.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/compare_sat_fill_clock.py --save-report
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

from pick_strong_grid import build_nav_from_cache, fetch_etf_closes, warm_window  # noqa: E402

from data_sync_service.service.ps_g50_blend import blend_nav_opportunity  # noqa: E402
from data_sync_service.service.state_bucket_track import (  # noqa: E402
    FILL_SAME_1430,
    load_sgap_context,
    replay_sgap_from_context,
)

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "past_year": ("2025-08-01", "2026-08-07"),
    "aligned": ("2025-08-28", "2026-08-28"),
}
WF_WINDOWS = ("OOS2", "train", "valid")
FULL_START = "2024-08-01"
FULL_END = "2026-08-28"
REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"

VARIANTS: tuple[dict[str, object], ...] = (
    {
        "id": "frozen_nextopen",
        "label": "信号 T-1 收盘 → T 开盘买 → 第3日收盘卖",
        "kwargs": dict(skip_t1_limit=True, pool_mode="strict"),
    },
    {
        "id": "habit_1430",
        "label": "14:20 信号+14:30 买 → 第3日14:30卖 (Live)",
        "kwargs": dict(
            skip_t1_limit=True, pool_mode="strict", fill_mode=FILL_SAME_1430,
            fill_hhmm="1430", exit_hhmm="1430", max_open_to_1430_pct=0.03,
        ),
    },
    {
        "id": "s1430_close",
        "label": "14:20 信号+14:30 买 → 第3日收盘卖",
        "kwargs": dict(
            skip_t1_limit=True, pool_mode="strict", fill_mode=FILL_SAME_1430,
            fill_hhmm="1430",
        ),
    },
)


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


def _sat_series(sat: dict) -> tuple[list[str], list[float], list[int], list[bool]]:
    rows = sat["rows"]
    dates = [r["date"] for r in rows]
    nav = [float(r["satNav"]) for r in rows]
    slots = [int(r.get("satPositions") or 0) for r in rows]
    active = [bool(r.get("satActive")) for r in rows]
    if nav and nav[0] > 0:
        base = nav[0]
        nav = [v / base for v in nav]
    return dates, nav, slots, active


def _pick_strong_nav(dates: list[str], start: str, end: str, etf_close) -> list[float]:
    cache = warm_window(start, end, etf_close)
    r = build_nav_from_cache(
        cache, lookback=60, ma_window=200, min_hold=1, cost=0.0,
        score="mom", top2=False, trail_pct=8.0,
    )
    pk_map = r["nav"]
    last = 1.0
    out: list[float] = []
    for d in dates:
        v = pk_map.get(d)
        if v is not None:
            last = v
        out.append(last)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    print("Satellite decision-clock scorecard: frozen next_open vs 14:30 habit\n")
    print(f"loading context {FULL_START}~{FULL_END} ...", flush=True)
    ctx = load_sgap_context(FULL_START, FULL_END)
    print("  loaded.", flush=True)
    etf_close = fetch_etf_closes()

    results: dict[str, dict] = {}
    for wname, (s, e) in WINDOWS.items():
        print(f"=== {wname} ({s}~{e}) ===", flush=True)
        core: list[float] | None = None
        core_m: dict[str, float] | None = None
        row: dict[str, dict] = {}
        for var in VARIANTS:
            kwargs = dict(var["kwargs"])
            sat = replay_sgap_from_context(
                ctx, start=s, end=e, max_pos=4, position_pct=0.25, **kwargs,
            )
            dates, sat_nav, slots, active = _sat_series(sat)
            if core is None:
                core = _pick_strong_nav(dates, s, e, etf_close)
                core_m = _stats(core)
                print(f"  core {_fmt(core_m)}", flush=True)
            n = min(len(core), len(sat_nav))
            twin = blend_nav_opportunity(core[:n], sat_nav[:n], active[:n], sat_weight=0.5)
            twin_m = _stats(twin)
            sat_m = _stats(sat_nav[:n])
            summary = sat.get("summary") or {}
            row[str(var["id"])] = {
                "label": var["label"],
                "sat": sat_m,
                "twin": twin_m,
                "delta_core_pt": round(twin_m["total_pct"] - core_m["total_pct"], 1),
                "delta_core_sharpe": round(twin_m["sharpe"] - core_m["sharpe"], 2),
                "delta_core_dd": round(twin_m["max_dd"] - core_m["max_dd"], 1),
                "fillCount": summary.get("fillCount"),
            }
            print(
                f"  {var['id']:<16} twin {_fmt(twin_m)}  Δcore "
                f"tot {row[str(var['id'])]['delta_core_pt']:+.1f} "
                f"sr {row[str(var['id'])]['delta_core_sharpe']:+.2f} "
                f"dd {row[str(var['id'])]['delta_core_dd']:+.1f}  "
                f"sat {_fmt(sat_m)}  fills {summary.get('fillCount')}",
                flush=True,
            )
        assert core_m is not None
        results[wname] = {"core": core_m, "variants": row}

    print("\n## 三窗 (twin tot / Sharpe / maxDD)\n")
    hdr = f"{'variant':<16}" + "".join(f"{w:>22}" for w in WF_WINDOWS)
    print(hdr)
    for var in VARIANTS:
        vid = str(var["id"])
        cells = []
        for w in WF_WINDOWS:
            m = results[w]["variants"][vid]["twin"]
            cells.append(f"{m['total_pct']:+.1f}/{m['sharpe']:.2f}/{m['max_dd']:.1f}")
        print(f"{vid:<16}" + "".join(f"{c:>22}" for c in cells))
    print(f"\n{'core':<16}" + "".join(
        f"{results[w]['core']['total_pct']:+.1f}/{results[w]['core']['sharpe']:.2f}/{results[w]['core']['max_dd']:.1f}".rjust(22)
        for w in WF_WINDOWS
    ))

    payload = {
        "tag": "sat-fill-clock-2026-09-11",
        "protocol": (
            "window-local empty book; clip4 4x12.5%; frozen pick-strong trail8; opp_50. "
            "Variants differ ONLY in signal/fill clock. Score tot+Sharpe+maxDD."
        ),
        "variants": VARIANTS,
        "windows": results,
        "as_of": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        path = REPORT_DIR / "sat_fill_clock_2026-09-11.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print(f"\nsaved {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
