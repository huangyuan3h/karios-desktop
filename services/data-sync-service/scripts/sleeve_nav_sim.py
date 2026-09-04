#!/usr/bin/env python3
"""Three-window NAV validation for the third-asset sleeve (T6 落地).

Replays the S-3 CN line (S3_CONFIG) over the three fixed walk-forward
windows, runs the sleeve NAV simulator on top (idle cash earns 513100 while
above MA200, GC001 otherwise), and prints the delta vs the idle-0% baseline.

Usage:
  PYTHONPATH=src python3 scripts/sleeve_nav_sim.py
  PYTHONPATH=src python3 scripts/sleeve_nav_sim.py --min-idle 20
  PYTHONPATH=src python3 scripts/sleeve_nav_sim.py --json /tmp/sleeve.json

Acceptance (todo §19 铁律): all three windows must show delta >= 0.
Baseline must track S-3 engine NAV (2026-08-29 P0-1); design-era
+3.1/+15.3/+39.0pt targets are directional only.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from run_walk_forward import S3_CONFIG, WINDOWS  # noqa: E402

from data_sync_service.service.backtest_engine import (  # noqa: E402
    BacktestConfig,
    BacktestData,
    simulate,
)
from data_sync_service.service.portfolio_nav_sim import (  # noqa: E402
    engine_nav_by_day_from_run,
    load_third_asset_cache,
    simulate_sleeve_nav,
)

REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"
CACHE_FILE = Path(__file__).resolve().parents[1] / "data" / "third_asset_cache.json"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--windows", default="OOS2,train,valid", help="Comma-separated windows")
    ap.add_argument("--min-idle", type=float, default=0.0, help="Sleeve engagement idle threshold %")
    ap.add_argument("--json", help="Write the report to this file")
    args = ap.parse_args()

    cache = json.loads(CACHE_FILE.read_text())
    etf_close, repo_rate = load_third_asset_cache(cache)

    windows = [w.strip() for w in args.windows.split(",") if w.strip()]
    missing = [w for w in windows if w not in WINDOWS]
    if missing:
        print(f"ERROR: unknown windows {missing} (valid: {list(WINDOWS)})", file=sys.stderr)
        return 2

    results: dict[str, dict] = {}
    print(
        "| 窗口 | 基线收益% | 套筒收益% | 增量pt | 基线DD% | 套筒DD% | 持有天 | 闲置天 | 平均闲置% |"
    )
    print(
        "|------|-----------|-----------|--------|---------|---------|--------|--------|-----------|"
    )
    for w in windows:
        start, end = WINDOWS[w]
        cfg = BacktestConfig(start_date=start, end_date=end, **S3_CONFIG)
        data = BacktestData(cfg)
        run = simulate(cfg, data)
        eng_nav = engine_nav_by_day_from_run(data.calendar, run.nav_curve)
        sim = simulate_sleeve_nav(
            positions_by_day=run.positions_by_day,
            close_by_ts_day=data.close_by_ts_day,
            calendar=data.calendar,
            etf_close_by_day=etf_close,
            repo_rate_by_day=repo_rate,
            min_idle_pct=args.min_idle,
            engine_nav_by_day=eng_nav,
        )
        s = sim["summary"]
        results[w] = {"window": f"{start}..{end}", **s}
        print(
            f"| {w:6s} | {s['totalBasePct']:9.1f} | {s['totalSleevePct']:9.1f} | "
            f"{s['deltaPct']:+7.1f} | {s['maxDdBasePct']:7.1f} | {s['maxDdSleevePct']:7.1f} | "
            f"{s['holdDays']:6d} | {s['idleDays']:6d} | {s['avgIdlePct']:9.1f} |"
        )

    print()
    failures = [w for w, r in results.items() if r["deltaPct"] < 0]
    if failures:
        print(f"⚠ 未通过：{', '.join(failures)} 窗增量为负（三窗无劣化铁律）")
    else:
        print("✅ 三窗套筒增量全部非负（基线=引擎 NAV；idle 吃 513100/GC001）")

    report = {
        "generatedAt": datetime.now(UTC).isoformat(),
        "config": {"minIdlePct": args.min_idle, "s3": S3_CONFIG},
        "results": results,
    }
    out_file = Path(args.json) if args.json else REPORT_DIR / "sleeve_nav_latest.json"
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    print(f"report -> {out_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())