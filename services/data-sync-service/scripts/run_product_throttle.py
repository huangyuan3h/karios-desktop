#!/usr/bin/env python3
"""TIP-016 A: product-level NAV-watermark throttle — pre-registered experiment.

Outer loop (diagnosis doc product-post-peak-drift-2026-09-09.md §9.3):

  pass 1  run the CN + HK frozen lines over the window -> joint NAV (50/50
          daily-rebalanced on the union calendar) -> drawdown watermark ->
          throttle windows (causal: day t's state uses NAV <= t-1).
  pass 2  re-run both lines with BacktestConfig.throttle_windows +
          throttle_scale; compare joint totals vs pass 1 per window.

Grid (frozen, no peeking): theta in {7, 10, 12} x mode in {half (0.5x),
pause (0)} x hysteresis release at theta/2. Voting windows: OOS2/train/
valid/long (>5pt degradation on ANY = reject); past_year = reference only.

Usage:
  PYTHONPATH=src python3 scripts/run_product_throttle.py                 # all 6 variants
  PYTHONPATH=src python3 scripts/run_product_throttle.py --variant "-10:half"
  PYTHONPATH=src python3 scripts/run_product_throttle.py --windows OOS2,train,valid
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from run_walk_forward import HK_S3_CONFIG, S3_CONFIG, WINDOWS  # noqa: E402

from data_sync_service.service.backtest_engine import (  # noqa: E402
    BacktestConfig,
    BacktestData,
    compute_throttle_windows,
    simulate,
)

# Diagnosis window (past_year reference; overlaps train/valid — reference only).
PAST_YEAR = ("2025-08-01", "2026-08-07")
WINDOWS_A: dict[str, tuple[str, str]] = {**WINDOWS, "past_year": PAST_YEAR}
VOTING = ("OOS2", "train", "valid", "long")

VARIANTS: list[tuple[float, float, str]] = [
    (7.0, 0.5, "half"),
    (10.0, 0.5, "half"),
    (12.0, 0.5, "half"),
    (7.0, 0.0, "pause"),
    (10.0, 0.0, "pause"),
    (12.0, 0.0, "pause"),
]

REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"


def _line_curve(config: dict, data: BacktestData, start: str, end: str):
    """Run one line and return (dates, nav levels) on its own calendar."""
    run = simulate(BacktestConfig(start_date=start, end_date=end, **config), data=data)
    cal = [str(d) for d in data.calendar]
    navs = [float(v) for v in run.nav_curve[: len(cal)]]
    return cal, navs, run


def _joint_level(
    cn: tuple[list[str], list[float]],
    hk: tuple[list[str], list[float]],
) -> tuple[list[str], list[float]]:
    """50/50 daily-rebalanced joint levels on the union calendar (frozen twin recipe).

    A line that does not trade on a date contributes 0 return (cash) —
    forward-fill via last known level.
    """
    cn_map = dict(zip(cn[0], cn[1], strict=True))
    hk_map = dict(zip(hk[0], hk[1], strict=True))
    dates = sorted(set(cn_map) | set(hk_map))
    levels = [1.0]
    last_cn = last_hk = 1.0
    for d in dates:
        cn_v = cn_map.get(d)
        hk_v = hk_map.get(d)
        cn_r = (cn_v / last_cn - 1.0) if cn_v is not None else 0.0
        hk_r = (hk_v / last_hk - 1.0) if hk_v is not None else 0.0
        if cn_v is not None:
            last_cn = cn_v
        if hk_v is not None:
            last_hk = hk_v
        levels.append(levels[-1] * (1.0 + (cn_r + hk_r) / 2.0))
    return dates, levels[1:]


def _window_throttle_days(
    windows: tuple[tuple[str, str], ...], calendar: list[str]
) -> int:
    return sum(1 for d in calendar if any(str(a) <= d <= str(b) for a, b in windows))


def run_variant_window(
    theta: float,
    scale: float,
    start: str,
    end: str,
    cn_data: BacktestData,
    hk_data: BacktestData,
    base_ctx: dict,
) -> dict:
    windows = compute_throttle_windows(base_ctx["levels"], base_ctx["dates"], theta)
    thr_cn_cfg = {**S3_CONFIG, "throttle_windows": windows, "throttle_scale": scale}
    thr_hk_cfg = {**HK_S3_CONFIG, "throttle_windows": windows, "throttle_scale": scale}
    t_cn_cal, t_cn_nav, t_cn_run = _line_curve(thr_cn_cfg, cn_data, start, end)
    t_hk_cal, t_hk_nav, t_hk_run = _line_curve(thr_hk_cfg, hk_data, start, end)
    _, j2_levels = _joint_level((t_cn_cal, t_cn_nav), (t_hk_cal, t_hk_nav))
    thr_total = (j2_levels[-1] - 1.0) * 100.0

    blocked = (
        t_cn_run.summary.gated_blocks.get("product_throttle", 0)
        + t_hk_run.summary.gated_blocks.get("product_throttle", 0)
    )
    j_levels = base_ctx["levels"]
    return {
        "baseJointPct": round(base_ctx["total"], 2),
        "thrJointPct": round(thr_total, 2),
        "deltaPt": round(thr_total - base_ctx["total"], 2),
        "baseJointDdPct": round((1.0 - min(j_levels) / max(j_levels)) * 100.0, 1),
        "thrJointDdPct": round((1.0 - min(j2_levels) / max(j2_levels)) * 100.0, 1),
        "thrCnPct": round((t_cn_run.summary.total_net_pnl_pct or 0.0), 1),
        "thrHkPct": round((t_hk_run.summary.total_net_pnl_pct or 0.0), 1),
        "baseCnPct": round(base_ctx["cnPct"], 1),
        "baseHkPct": round(base_ctx["hkPct"], 1),
        "windows": [[a, b] for a, b in windows],
        "windowCount": len(windows),
        "throttledDays": _window_throttle_days(windows, base_ctx["dates"]),
        "blockedEntries": blocked,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--variant", default="", help='"theta:mode" e.g. "-10:half" (default: all 6)')
    ap.add_argument("--windows", default="OOS2,train,valid,past_year,long")
    ap.add_argument("--json", default="", help="Output json path")
    args = ap.parse_args()

    if args.variant:
        theta_raw, _, mode = args.variant.partition(":")
        theta = abs(float(theta_raw))
        scale = 0.5 if mode == "half" else 0.0
        variants = [(theta, scale, mode)]
    else:
        variants = VARIANTS

    win_names = [w.strip() for w in args.windows.split(",") if w.strip()]
    unknown = [w for w in win_names if w not in WINDOWS_A]
    if unknown:
        print(f"ERROR: unknown windows {unknown}", file=sys.stderr)
        return 2

    report: dict = {"generatedAt": datetime.now(UTC).isoformat(), "variants": {}}
    fail_lines: list[str] = []

    # Window-outer / variant-inner: only one window's BacktestData is alive
    # at a time (the long window is heavy); baselines are shared per window.
    for w in win_names:
        start, end = WINDOWS_A[w]
        cn_data = BacktestData(BacktestConfig(start_date=start, end_date=end, **S3_CONFIG))
        hk_data = BacktestData(BacktestConfig(start_date=start, end_date=end, **HK_S3_CONFIG))
        cn_cal, cn_nav, cn_run = _line_curve(dict(S3_CONFIG), cn_data, start, end)
        hk_cal, hk_nav, hk_run = _line_curve(dict(HK_S3_CONFIG), hk_data, start, end)
        j_dates, j_levels = _joint_level((cn_cal, cn_nav), (hk_cal, hk_nav))
        base_total = (j_levels[-1] - 1.0) * 100.0
        base_cn_pct = cn_run.summary.total_net_pnl_pct or 0.0
        base_hk_pct = hk_run.summary.total_net_pnl_pct or 0.0
        base_ctx = {
            "dates": j_dates,
            "levels": j_levels,
            "total": base_total,
            "cnPct": base_cn_pct,
            "hkPct": base_hk_pct,
        }
        print(f"[base {w}] joint {base_total:+.1f}% (cn {base_cn_pct:+.1f} / hk {base_hk_pct:+.1f})")

        for theta, scale, mode in variants:
            tag = f"-{theta:g}:{mode}"
            r = run_variant_window(theta, scale, start, end, cn_data, hk_data, base_ctx)
            report["variants"].setdefault(tag, {})[w] = r
            print(
                f"[{tag} {w}] base {r['baseJointPct']:+.1f}% -> thr {r['thrJointPct']:+.1f}% "
                f"({r['deltaPt']:+.1f}pt) dd {r['baseJointDdPct']:.0f}->{r['thrJointDdPct']:.0f}% "
                f"win={r['windowCount']} days={r['throttledDays']} blocked={r['blockedEntries']}"
            )
        del cn_data, hk_data, base_ctx

    for theta, _scale, mode in variants:
        tag = f"-{theta:g}:{mode}"
        results = report["variants"][tag]
        voting_bad = [
            f"{w} {results[w]['deltaPt']:+.1f}pt"
            for w in win_names
            if w in VOTING and w in results and results[w]["deltaPt"] < -5.0
        ]
        if voting_bad:
            fail_lines.append(f"{tag}: {'; '.join(voting_bad)}")

    print()
    header = "| 变体 (theta:mode) | " + " | ".join(win_names) + " |"
    print(header)
    print("|---" * (len(win_names) + 1) + "|")
    for theta, _scale, mode in variants:
        tag = f"-{theta:g}:{mode}"
        cells = " | ".join(
            f"{report['variants'][tag][w]['deltaPt']:+.1f}" for w in win_names
        )
        print(f"| {tag} | {cells} |")
    print()
    if fail_lines:
        print(f"❌ 未通过（投票窗 OOS2/train/valid/long 任一 >5pt 劣化即拒）：{'；'.join(fail_lines)}")
    else:
        print("✅ 投票窗口无 >5pt 劣化（long 计入投票；past_year 仅参考）")

    out = Path(args.json) if args.json else REPORT_DIR / "product_throttle_latest.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    print(f"report -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
