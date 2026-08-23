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
    "drawdown_circuit_pct": -25.0,
    "slippage_pct": 0.05,
    "pyramid_trigger_pct": 2.5,
    "pyramid_add_scale": 0.5,
    "pyramid_max_adds": 1,
    "exclude_boards": "300",
    # OPT-105 (2026-08-13 固化): regime-adaptive stops — Strong sessions use
    # the entry-locked ATR% x 2.0 line (let winners run), Diverging/Weak fall
    # back to the fixed -5/-8. Three-window verified (OOS2 +123.3 / train
    # +73.8 / valid +89.1, all within tolerance of the fixed baseline).
    "atr_stop_mult": 2.0,
    "atr_stop_strong_only": True,
    # TIP-014 (2026-08-14 固化): block new entries on TRUE neutral days and
    # implicit-weak days (breadth ratio < 0.5 with only normal/caution
    # risk_mode). Valid window: +10.7pt (89.1→99.8), dd 12.1→2.7, win rate
    # 60.8→78.2; OOS2/train unchanged (no sentiment data there → UNKNOWN).
    "neutral_block": True,
    # TIP-014 (2026-08-14 固化): environment-aware entry style — uptrend days
    # buy momentum (RS>=0.7), fan days buy pullbacks (5d ret <= -3%),
    # weak/neutral blocked, unknown days unfiltered. Valid: +4.7pt
    # (99.8→104.4), dd 1.4%, win rate 81.8%; fan-day avg +12.8→+17.4%.
    # OOS2/train unchanged (no env labels there → UNKNOWN → no filter).
    "entry_style": "auto",
    "entry_style_rs_min": 0.7,
    "entry_style_dip_min": 3.0,
    # D2 (2026-08-14 固化): environment-aware max-hold — positions entered
    # on an UPTREND day force-close after 45 days (主升日买入吃主升段就跑).
    # Global hold45 was rejected (OOS2 -13.5) but env-aware passes everything:
    # valid +11.4pt (104.4→115.8), long +11.4pt (279.8→291.2), OOS2/train
    # unchanged (no env labels → no shorten). hold30 -32.3 / hold50 +0.3
    # / hold55 持平 → 45 is the peak.
    "max_hold_env_shorten": 45,
    # D3 (2026-08-15 固化): environment-aware position sizing — new entries
    # are scaled by their ENTRY day's env label. v4 passed the three-window
    # bar: uptrend 1.25x (主升日入场质量最高, 放大下注) / fan 0.75x (电风扇
    # 减仓控尾) — OOS2 +24.6 / train +19.5 / valid +26.4 (vs base), long
    # 270.1→333.9 (+64pt), 三窗夏普两升一平. v1 (1.2/0.8) also passed but
    # weaker; v3 (fan-only) failed valid.
    "env_position_scale": "uptrend:1.25,fan:0.75",
    # E2 流动性 2026-08-22: min_avg_amount 0→0.7亿 (60日均额，P17)，三窗重跑验证
    "min_avg_amount": 0.7,
    # E2 (2026-08-14 数据回填后修正): panic_cooldown 3 → 2. 回填情绪历史
    # (2024-08 起) 后 panic 冷却在弱市年频繁触发, 3 天把 OOS2 锁死
    # (288964 次拦截 → 199 笔)。三窗+长窗同口径对比 (新基线=有情绪数据):
    #   panic=2: OOS2 92.6(+8.2) · train 103.1(+22) · valid 115.8(持平)
    #            · long 270.1(+34.7)
    #   panic=3: OOS2 84.4 · train 81.1 · valid 115.8 · long 235.4
    "panic_cooldown_days": 2,
}

WINDOWS: dict[str, tuple[str, str]] = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    # 2026-08-12: full-cycle window (2021 top → 2022 bear → 2023 weak →
    # 2024-25 structural bull) — cross-cycle robustness check, NOT part of
    # the fixed three-window audit. Baseline file has no "long" entry, so
    # the table shows no delta column for it.
    "long": ("2021-08-01", "2026-08-07"),
    # 2026-08-22 V1: hold-out 2026-08-08+ 只读不调参，n≥100 前不改参，>5pt 判定排除 holdout
    "holdout": ("2026-08-08", "2027-02-08"),
}

REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"
# 2026-08-10: per-market baselines — CN and HK are independent strategy lines.
BASELINE_FILE = REPORT_DIR / "walk_forward_baseline.json"
HK_BASELINE_FILE = REPORT_DIR / "walk_forward_hk_baseline.json"

# HK parallel line (2026-08-10 定案 · strategy-params.md §HK) — gates=regime
# (no sector fund-flow for HK), wider trailing (-12) for HK volatility,
# stricter RS (top 40%), no exclude_boards (HK has no 创业板 equivalent).
HK_S3_CONFIG: dict[str, float | int | str] = {
    "score_threshold": 65.0,
    "max_hold_days": 60,
    "stop_loss_pct": -5.0,
    "target_pnl_pct": 100.0,
    "score_floor": 0.0,
    "market": "HK",
    "gates": "regime",
    "trailing_stop_pct": -12.0,
    "position_pct": 0.10,
    "max_positions": 20,
    "rs_rank_min": 0.6,
    "diverging_scale": 1.0,
    "slippage_pct": 0.05,
    "pyramid_trigger_pct": 2.5,
    "pyramid_add_scale": 0.5,
    "pyramid_max_adds": 1,
    "exclude_boards": "",
}


def _overrides(args: argparse.Namespace) -> dict[str, float | int | str | dict]:
    from data_sync_service.service.trendok_params import DEFAULT_TRENDOK_PARAMS
    field_types = {f.name: f.type for f in fields(BacktestConfig)}
    valid = set(field_types)
    trendok_fields = set(DEFAULT_TRENDOK_PARAMS.__dataclass_fields__.keys())
    out: dict[str, float | int | str | dict] = {}
    trendok_override: dict[str, float] = {}
    for kv in args.param:
        key, _, value = kv.partition("=")
        key = key.strip()
        if key.startswith("trendok_"):
            tkey = key[len("trendok_"):]
            if tkey not in trendok_fields:
                print(f"WARN: unknown TrendOKParams field {tkey!r} (ignored)", file=sys.stderr)
                continue
            try:
                v = float(value)
            except ValueError:
                print(f"WARN: TrendOKParams {tkey} expects numeric, got {value!r} (ignored)", file=sys.stderr)
                continue
            trendok_override[tkey] = v
            continue
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
    if trendok_override:
        out["trendok_params"] = trendok_override
    return out


def _wilson_ci(k: int, n: int, z: float = 1.96) -> tuple[float, float] | None:
    if n == 0 or k < 0 or k > n:
        return None
    p = k / n
    denom = 1 + z * z / n
    centre = p + z * z / (2 * n)
    delta = z * ((p * (1 - p) + z * z / (4 * n)) / n) ** 0.5
    lo = (centre - delta) / denom
    hi = (centre + delta) / denom
    return (max(0.0, lo), min(1.0, hi))


def _summarize(run) -> dict[str, float | int | str | None]:
    s = run.summary
    ci = None
    warn = None
    if s.closed and s.wins is not None:
        ci = _wilson_ci(s.wins, s.closed)
        if s.closed < 100:
            warn = "⚠️ underpowered n<100"
    return {
        "closed": s.closed,
        "winRate": s.win_rate,
        "winRateCI": ci,
        "underpowered": warn,
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
    lines = ["| 窗口 | 收益% | 回撤% | 夏普 | 胜率% | 笔数 | vs 基线 | 备注 |", "|------|-------|-------|------|-------|------|---------|------|"]
    for w in windows:
        r = results[w]
        diff = ""
        if baseline and w in baseline:
            b = baseline[w]
            d = float(r["totalNetPnlPct"] or 0) - float(b.get("totalNetPnlPct") or 0)
            diff = f"{d:+.1f}pt" if abs(d) >= 0.05 else "持平"
        ci = r.get("winRateCI")
        ci_s = f" CI {ci[0]*100:.0f}-{ci[1]*100:.0f}%" if isinstance(ci, (list, tuple)) and len(ci) == 2 else ""
        warn = r.get("underpowered") or ""
        if w == "holdout" and r.get("closed", 0) < 100:
            warn = (warn + " holdout n<100" if warn else "holdout n<100")
        lines.append(
            f"| {w:6s} | {r['totalNetPnlPct'] or 0:7.1f} | {r['maxDrawdownPct']:5.1f} | "
            f"{r['sharpe'] if r['sharpe'] is not None else 0:5.2f} | "
            f"{(r['winRate'] or 0) * 100:5.1f}{ci_s} | {r['closed']:4d} | {diff} | {warn} |"
        )
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--param", action="append", default=[], help="BacktestConfig override key=value (repeatable)")
    ap.add_argument("--windows", default="OOS2,train,valid", help="Comma-separated windows to run")
    ap.add_argument("--market", choices=["CN", "HK"], default="CN", help="Strategy line (CN S-3 or HK parallel line)")
    ap.add_argument("--tag", default="", help="Optional label for the report")
    ap.add_argument("--save-baseline", action="store_true", help="Persist this run as the S-3 baseline")
    ap.add_argument("--json", help="Write the full report to this file (default walk_forward_latest.json)")
    args = ap.parse_args()

    overrides = _overrides(args)
    base_config = HK_S3_CONFIG if args.market == "HK" else S3_CONFIG
    config = {**base_config, **overrides}
    windows = [w.strip() for w in args.windows.split(",") if w.strip()]
    missing = [w for w in windows if w not in WINDOWS]
    if missing:
        print(f"ERROR: unknown windows {missing} (valid: {list(WINDOWS)})", file=sys.stderr)
        return 2

    baseline: dict[str, dict[str, float | int | str | None]] | None = None
    baseline_file = HK_BASELINE_FILE if args.market == "HK" else BASELINE_FILE
    if baseline_file.exists():
        try:
            baseline = json.loads(baseline_file.read_text())["results"]
        except (json.JSONDecodeError, KeyError):
            baseline = None

    results: dict[str, dict[str, float | int | str | None]] = {}
    for w in windows:
        start, end = WINDOWS[w]
        cfg = BacktestConfig(start_date=start, end_date=end, **config)
        if "trendok_params" in config:
            from data_sync_service.service.backtest_engine import BacktestData

            data = BacktestData(cfg)
            recomputed = data.recompute_scores_with_params(config["trendok_params"])  # type: ignore[arg-type]
            data.scores_by_day = recomputed
            run = simulate(cfg, data=data)
        else:
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
            if w in ("holdout", "long"):
                continue  # holdout/long 只读不参与 >5pt 票决
            b = baseline.get(w) or {}
            d = float(results[w]["totalNetPnlPct"] or 0) - float(b.get("totalNetPnlPct") or 0)
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
        import hashlib

        baseline_file.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(report, ensure_ascii=False, indent=2, default=str)
        baseline_file.write_text(payload)
        print(f"baseline saved -> {baseline_file}")
        # V2: immutability — also save versioned copy + SHA256
        versioned = baseline_file.with_name(f"walk_forward_baseline_{datetime.now(UTC).strftime('%Y%m%d')}.json")
        versioned.write_text(payload)
        sha = hashlib.sha256(payload.encode()).hexdigest()[:12]
        print(f"versioned -> {versioned}  sha256:{sha}  tag: git tag s3-baseline-{datetime.now(UTC).strftime('%Y%m%d')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
