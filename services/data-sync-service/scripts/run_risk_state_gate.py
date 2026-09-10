#!/usr/bin/env python3
"""TIP-017: risk-state gates — breadth (A) + national-team (B) experiments.

Pre-registration: docs/backtests/risk-state-sensors-2026-09-09.md. Windows are
deterministic from price/flow data (causal: day d's state uses closes ≤ d-1);
they are fed to the engine via the generic throttle entry-block mechanism
(scale=0 = pause). Judging protocol identical to TIP-016: voting windows
OOS2/train/valid/long, >5pt degradation on ANY = reject; past_year reference.

A: breadth gate — 6 sleeves (CN/HK/GOLD/OIL/NASDAQ/BOND10) above MA200 count
   < K → both lines paused. Release ∈ {price (CN or HSI > MA20), breadth,
   cooldown 20}. Grid: K ∈ {2,3} × release → 6 variants.
B: national-team gate — CN line only: 沪深300 < MA200 AND broad-ETF 20d share
   delta ≤ 0. Single pre-registered variant (N=20, 4-ETF pool).

Usage:
  PYTHONPATH=src python3 scripts/run_risk_state_gate.py                # all
  PYTHONPATH=src python3 scripts/run_risk_state_gate.py --candidate A --windows OOS2,train,valid
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from run_product_throttle import (  # noqa: E402  # pyright: ignore[reportMissingImports]
    PAST_YEAR,
    _joint_level,
    _line_curve,
)
from run_walk_forward import HK_S3_CONFIG, WINDOWS  # noqa: E402
from run_walk_forward import S3_CONFIG as _S3_FROZEN  # noqa: E402

# This script re-derives each risk-state gate from scratch and feeds it via
# throttle_windows. Disable the now-frozen native TIP-017 B gate so "base"
# means no-gate (otherwise the experiment would double-apply and show ~0).
S3_CONFIG = {**_S3_FROZEN, "national_team_gate": False}

from data_sync_service.service.backtest_engine import (  # noqa: E402
    BacktestConfig,
    BacktestData,
    simulate,
)
from data_sync_service.service.risk_state_gate import (  # noqa: E402
    BREADTH_SERIES,
    _load_close_series,
    breadth_state_by_day,
    load_etf_share_series,
    national_team_state_by_day,
    state_runs,
)

WINDOWS_RS: dict[str, tuple[str, str]] = {**WINDOWS, "past_year": PAST_YEAR}
VOTING = ("OOS2", "train", "valid", "long")
REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"

LOOKBACK_DAYS = 400  # MA200 warmup + MA20 + share-delta history before window start


def _shift_back(day: str, days: int) -> str:
    return (datetime.fromisoformat(day) - timedelta(days=days)).date().isoformat()


def _line_calendar(data: BacktestData) -> list[str]:
    return [str(d) for d in data.calendar]


def build_breadth_windows(
    line_cal: list[str], lookback_start: str, end: str, k: int, release: str
) -> tuple[tuple[str, str], ...]:
    series = {
        name: _load_close_series(table, ts, lookback_start, end)
        for name, (table, ts) in BREADTH_SERIES.items()
    }
    # master calendar = the union of all series dates clipped to the line's own
    # trading days is approximated by the line calendar itself; series dates
    # outside it simply never match (forward state via last close).
    state = breadth_state_by_day(series, line_cal, k=k, release=release)
    return state_runs(state, line_cal)


def build_national_team_windows(
    line_cal: list[str], lookback_start: str, end: str
) -> tuple[tuple[str, str], ...]:
    index_series = _load_close_series("index_daily", "000300.SH", lookback_start, end)
    share = load_etf_share_series(
        ["510300.SH", "510500.SH", "510510.SH", "159915.SZ"], lookback_start, end
    )
    state = national_team_state_by_day(index_series, share, line_cal)
    return state_runs(state, line_cal)


def run_gated(
    cfgd: dict,
    windows: tuple[tuple[str, str], ...],
    data: BacktestData,
    start: str,
    end: str,
):
    cfg = BacktestConfig(
        start_date=start, end_date=end, **{**cfgd, "throttle_windows": windows, "throttle_scale": 0.0}
    )
    run = simulate(cfg, data=data)
    cal = [str(d) for d in data.calendar]
    navs = [float(v) for v in run.nav_curve[: len(cal)]]
    return cal, navs, run


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--candidate", default="A,B", help="Which candidates to run: A, B, or A,B")
    ap.add_argument("--windows", default="OOS2,train,valid,past_year,long")
    ap.add_argument("--json", default="")
    args = ap.parse_args()

    cands = [c.strip().upper() for c in args.candidate.split(",") if c.strip()]
    win_names = [w.strip() for w in args.windows.split(",") if w.strip()]
    unknown = [w for w in win_names if w not in WINDOWS_RS]
    if unknown:
        print(f"ERROR: unknown windows {unknown}", file=sys.stderr)
        return 2

    variants: list[dict] = []
    if "A" in cands:
        for k in (2, 3):
            for release in ("price", "breadth", "cooldown"):
                variants.append({"tag": f"A:K{k}:{release}", "kind": "A", "k": k, "release": release, "lines": ("CN", "HK")})
    if "B" in cands:
        variants.append({"tag": "B:nt20", "kind": "B", "lines": ("CN",)})

    report: dict = {"generatedAt": datetime.now(UTC).isoformat(), "variants": {}}
    fail_lines: list[str] = []

    for w in win_names:
        start, end = WINDOWS_RS[w]
        lookback = _shift_back(start, LOOKBACK_DAYS)
        cn_data = BacktestData(BacktestConfig(start_date=start, end_date=end, **S3_CONFIG))
        hk_data = BacktestData(BacktestConfig(start_date=start, end_date=end, **HK_S3_CONFIG))
        cn_cal_raw = _line_calendar(cn_data)
        hk_cal_raw = _line_calendar(hk_data)
        # frozen baselines (no gate) — shared across variants
        cn_cal, cn_nav, cn_run = _line_curve(dict(S3_CONFIG), cn_data, start, end)
        hk_cal, hk_nav, hk_run = _line_curve(dict(HK_S3_CONFIG), hk_data, start, end)
        j_dates, j_levels = _joint_level((cn_cal, cn_nav), (hk_cal, hk_nav))
        base_joint = (j_levels[-1] - 1.0) * 100.0
        base_cn = cn_run.summary.total_net_pnl_pct or 0.0
        base_hk = hk_run.summary.total_net_pnl_pct or 0.0
        print(f"[base {w}] joint {base_joint:+.1f}% (cn {base_cn:+.1f} / hk {base_hk:+.1f})")

        for v in variants:
            gate_w_cn: tuple[tuple[str, str], ...] = ()
            gate_w_hk: tuple[tuple[str, str], ...] = ()
            if v["kind"] == "A":
                gate_w_cn = build_breadth_windows(cn_cal_raw, lookback, end, v["k"], v["release"])
                gate_w_hk = build_breadth_windows(hk_cal_raw, lookback, end, v["k"], v["release"])
            else:
                gate_w_cn = build_national_team_windows(cn_cal_raw, lookback, end)
                gate_w_hk = ()
            t_cn_cal, t_cn_nav, t_cn_run = run_gated(S3_CONFIG, gate_w_cn, cn_data, start, end)
            t_hk_cal, t_hk_nav, t_hk_run = run_gated(HK_S3_CONFIG, gate_w_hk, hk_data, start, end)
            _, j2 = _joint_level((t_cn_cal, t_cn_nav), (t_hk_cal, t_hk_nav))
            thr_joint = (j2[-1] - 1.0) * 100.0
            thr_cn = t_cn_run.summary.total_net_pnl_pct or 0.0
            thr_hk = t_hk_run.summary.total_net_pnl_pct or 0.0
            blocked = (
                t_cn_run.summary.gated_blocks.get("product_throttle", 0)
                + t_hk_run.summary.gated_blocks.get("product_throttle", 0)
            )
            if v["kind"] == "A":
                delta = thr_joint - base_joint
                base_pct, thr_pct = base_joint, thr_joint
            else:
                delta = thr_cn - base_cn
                base_pct, thr_pct = base_cn, thr_cn
            rec = {
                "basePct": round(base_pct, 2),
                "thrPct": round(thr_pct, 2),
                "deltaPt": round(delta, 2),
                "gateWindowsCn": len(gate_w_cn),
                "gateWindowsHk": len(gate_w_hk),
                "blockedEntries": blocked,
                "cnPct": [round(base_cn, 1), round(thr_cn, 1)],
                "hkPct": [round(base_hk, 1), round(thr_hk, 1)],
                "windowsCn": [[a, b] for a, b in gate_w_cn][:12],
            }
            report["variants"].setdefault(v["tag"], {})[w] = rec
            print(
                f"[{v['tag']} {w}] base {base_pct:+.1f} -> thr {thr_pct:+.1f} ({delta:+.1f}pt) "
                f"wCn={len(gate_w_cn)} wHk={len(gate_w_hk)} blocked={blocked}"
            )
        del cn_data, hk_data

    print()
    header = "| 变体 | " + " | ".join(win_names) + " |"
    print(header)
    print("|---" * (len(win_names) + 1) + "|")
    for v in variants:
        cells = " | ".join(
            f"{report['variants'][v['tag']][w]['deltaPt']:+.1f}" if w in report["variants"][v["tag"]] else "—"
            for w in win_names
        )
        print(f"| {v['tag']} | {cells} |")
    print()
    for v in variants:
        results = report["variants"][v["tag"]]
        bad = [
            f"{w} {results[w]['deltaPt']:+.1f}pt"
            for w in win_names
            if w in VOTING and w in results and results[w]["deltaPt"] < -5.0
        ]
        if bad:
            fail_lines.append(f"{v['tag']}: {'; '.join(bad)}")
    if fail_lines:
        print(f"❌ 未通过（投票窗任一 >5pt 劣化即拒）：{'；'.join(fail_lines)}")
    else:
        print("✅ 投票窗口无 >5pt 劣化（long 计入投票；past_year 仅参考）")

    out = Path(args.json) if args.json else REPORT_DIR / "risk_state_gate_latest.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    print(f"report -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
