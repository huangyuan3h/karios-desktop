#!/usr/bin/env python3
"""Walk-forward tool: run the S-3 config over the three fixed windows.

The three windows are the audit standard for every parameter change
(see docs/modules/strategy-params.md "复核流程" / docs/todo.md §19.2):

  OOS2    2024-08-01 .. 2025-08-01   (weak-market year)
  train   2025-08-01 .. 2026-02-01   (tuning window)
  valid   2026-03-01 .. 2026-08-07   (recent validation)

A change is only acceptable when all three windows hold up vs the baseline
(no walk-forward violation). This is the C1 skeleton from §19.2.

Usage:
  PYTHONPATH=src python3 scripts/run_walk_forward.py                 # baseline S-3
  PYTHONPATH=src python3 scripts/run_walk_forward.py --save-baseline
  PYTHONPATH=src python3 scripts/run_walk_forward.py --param score_threshold=70
  PYTHONPATH=src python3 scripts/run_walk_forward.py --param trailing_stop_pct=-6 \
      --param max_positions=15 --tag "trial-1"
  PYTHONPATH=src python3 scripts/run_walk_forward.py --windows train,valid

Report is printed as a markdown table and saved to
data/backtest_reports/walk_forward_latest.json.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from dataclasses import fields  # noqa: E402

from data_sync_service.service.backtest_engine import BacktestConfig, simulate  # noqa: E402

# S-3 定案（docs/modules/strategy-params.md §1 真值表 · 2026-08-09 双窗验证）
S3_CONFIG: dict[str, float | int | str] = {
    "score_threshold": 65.0,
    "max_hold_days": 60,
    "stop_loss_pct": -5.0,
    "target_pnl_pct": 100.0,
    "score_floor": 0.0,
    "market": "CN",
    "gates": "full",
    "trailing_stop_pct": -8.0,
    "position_pct": 0.10,
    "max_positions": 20,
    "rs_rank_min": 0.5,
    "diverging_scale": 1.0,
    "panic_cooldown_days": 3,
    "slippage_pct": 0.05,
    "pyramid_trigger_pct": 2.5,
    "pyramid_add_scale": 0.5,
    "pyramid_max_adds": 1,
    "exclude_boards": "300",
}

WINDOWS: dict[str, tuple[str, str]] = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
}

REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"
BASELINE_FILE = REPORT_DIR / "walk_forward_baseline.json"


def _overrides(args: argparse.Namespace) -> dict[str, float | int | str]:
    field_types = {f.name: f.type for f in fields(BacktestConfig)}
    valid = set(field_types)
    out: dict[str, float | int | str] = {}
    for kv in args.param:
        key, _, value = kv.partition("=")
        key = key.strip()
        if key not in valid:
            print(f"WARN: unknown BacktestConfig field {key!r} (ignored)", file=sys.stderr)
            continue
        if str(field_types[key]) == "str":
            out[key] = value.strip()
            continue
        raw: float | int | str
        try:
            raw = float(value)
        except ValueError:
            raw = value
        if isinstance(raw, float) and raw.is_integer():
            raw = int(raw)
        out[key] = raw
    return out


def _summarize(run) -> dict[str, float | int | str | None]:
    s = run.summary
    return {
        "closed": s.closed,
        "winRate": s.win_rate,
        "avgNetPnlPct": s.avg_net_pnl_pct,
        "totalNetPnlPct": s.total_net_pnl_pct,
        "maxDrawdownPct": s.max_drawdown_pct,
        "sharpe": s.sharpe,
    }


def _md_table(
    windows: list[str],
    results: dict[str, dict[str, float | int | str | None]],
    baseline: dict[str, dict[str, float | int | str | None]] | None,
) -> str:
    lines = ["| 窗口 | 收益% | 回撤% | 夏普 | 胜率% | 笔数 | vs 基线 |", "|------|-------|-------|------|-------|------|---------|"]
    for w in windows:
        r = results[w]
        diff = ""
        if baseline and w in baseline:
            b = baseline[w]
            d = float(r["totalNetPnlPct"] or 0) - float(b.get("totalNetPnlPct") or 0)
            diff = f"{d:+.1f}pt" if abs(d) >= 0.05 else "持平"
        lines.append(
            f"| {w:6s} | {r['totalNetPnlPct'] or 0:7.1f} | {r['maxDrawdownPct']:5.1f} | "
            f"{r['sharpe'] if r['sharpe'] is not None else 0:5.2f} | "
            f"{(r['winRate'] or 0) * 100:5.1f} | {r['closed']:4d} | {diff} |"
        )
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--param", action="append", default=[], help="BacktestConfig override key=value (repeatable)")
    ap.add_argument("--windows", default="OOS2,train,valid", help="Comma-separated windows to run")
    ap.add_argument("--tag", default="", help="Optional label for the report")
    ap.add_argument("--save-baseline", action="store_true", help="Persist this run as the S-3 baseline")
    ap.add_argument("--json", help="Write the full report to this file (default walk_forward_latest.json)")
    args = ap.parse_args()

    overrides = _overrides(args)
    config = {**S3_CONFIG, **overrides}
    windows = [w.strip() for w in args.windows.split(",") if w.strip()]
    missing = [w for w in windows if w not in WINDOWS]
    if missing:
        print(f"ERROR: unknown windows {missing} (valid: {list(WINDOWS)})", file=sys.stderr)
        return 2

    baseline: dict[str, dict[str, float | int | str | None]] | None = None
    if BASELINE_FILE.exists():
        try:
            baseline = json.loads(BASELINE_FILE.read_text())["results"]
        except (json.JSONDecodeError, KeyError):
            baseline = None

    results: dict[str, dict[str, float | int | str | None]] = {}
    for w in windows:
        start, end = WINDOWS[w]
        cfg = BacktestConfig(start_date=start, end_date=end, **config)
        run = simulate(cfg)
        results[w] = _summarize(run)
        print(f"[{w}] {start}..{end} closed={run.summary.closed} "
              f"win={run.summary.win_rate} total={run.summary.total_net_pnl_pct:+.1f}% "
              f"dd={run.summary.max_drawdown_pct:.1f}% sharpe={run.summary.sharpe}")

    print()
    print(_md_table(windows, results, baseline))
    print()
    verdicts: list[str] = []
    if baseline and windows[0] in baseline:
        for w in windows:
            d = float(results[w]["totalNetPnlPct"] or 0) - float(baseline[w].get("totalNetPnlPct") or 0)
            if d < -5:
                verdicts.append(f"{w} 劣化 {d:+.1f}pt")
    if verdicts:
        print(f"⚠ 未通过：{'；'.join(verdicts)}（相对 S-3 基线三窗比较，>5pt 劣化拒收）")
    else:
        print("✅ 三窗口径：相对基线无显著劣化" if baseline else "（首次运行：建议 --save-baseline 固化基线）")

    report = {
        "generatedAt": datetime.now(UTC).isoformat(),
        "tag": args.tag,
        "config": config,
        "results": results,
        "baselineUsed": baseline is not None,
    }
    out_file = Path(args.json) if args.json else REPORT_DIR / "walk_forward_latest.json"
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    print(f"report -> {out_file}")
    if args.save_baseline:
        BASELINE_FILE.parent.mkdir(parents=True, exist_ok=True)
        BASELINE_FILE.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str))
        print(f"baseline saved -> {BASELINE_FILE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
