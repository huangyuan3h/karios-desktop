#!/usr/bin/env python3
"""H-ETF-C pre-registered diagnostic: sector/broad ETF breakdown as an S-3 exit.

Per §一.10 discipline: print coverage + whip/save split + hold distribution only.
No engine replay. Read-only.

Prereg & decision lines: docs/designs/s3-etf-sync-stop-prereg-2026-09-12.md
Rules (frozen, no grid):
  sector_trail8 (primary): matched sector ETF drawdown from peak-since-entry >=8% -> sell at stock close
  sector_ma20 / board_trail8 / board_ma20: shape checks only

Delta = R_stop - R_nat (positive = stop saved; negative = whipped/cut a winner).

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/diag_s3_etf_sync_stop.py --save-report
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

from data_sync_service.service.backtest_engine import (  # noqa: E402
    BacktestConfig,
    BacktestData,
    simulate,
)
from run_walk_forward import S3_CONFIG  # noqa: E402
from diag_s3_alpha_vs_etf import (  # noqa: E402
    _board_bench,
    _industry_map,
    _load_etf,
    _sector_bench,
    _symbol_to_ts,
)

ROOT = Path(__file__).resolve().parents[1]
REPORT_DIR = ROOT / "data" / "backtest_reports"
WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "long": ("2021-08-01", "2026-08-07"),
}
TRAIL = 0.08
PRIMARY = "sector_trail8"
VARIANTS = ("sector_trail8", "sector_ma20", "board_trail8", "board_ma20")


def _ma20(etf: dict[str, dict[str, float]]) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    for ts, mp in etf.items():
        days = sorted(mp)
        s = 0.0
        row: dict[str, float] = {}
        for i, d in enumerate(days):
            s += mp[d]
            if i >= 20:
                s -= mp[days[i - 20]]
            if i >= 19:
                row[d] = s / 20.0
        out[ts] = row
    return out


def _first_stop(etf_mp: dict[str, float], ma20_mp: dict[str, float], entry: str, close_date: str, rule: str) -> str | None:
    days = [d for d in etf_mp if entry <= d < close_date]
    days.sort()
    peak: float | None = None
    trail = rule.endswith("trail8")
    for d in days:
        ec = etf_mp[d]
        if trail:
            peak = ec if peak is None else max(peak, ec)
            if peak > 0 and ec < peak * (1.0 - TRAIL):
                return d
        else:
            ma = ma20_mp.get(d)
            if ma and ec < ma:
                return d
    return None


def _agg(deltas: list[float], n: int) -> dict:
    if not deltas:
        return {"stopped": 0, "coverage_pct": round(100.0 * 0 / n, 1) if n else 0.0, "whip_pct": None}
    a = np.array(deltas)
    return {
        "stopped": len(deltas),
        "coverage_pct": round(100.0 * len(deltas) / n, 1) if n else 0.0,
        "whip_pct": round(float((a < 0).mean()) * 100, 1),
        "save_pct": round(float((a > 0).mean()) * 100, 1),
        "mean_delta_stopped_pct": round(float(a.mean()) * 100, 2),
        "median_delta_stopped_pct": round(float(np.median(a)) * 100, 2),
        "mean_delta_all_pct": round(float(a.sum()) * 100 / n, 2) if n else 0.0,
    }


def _run_window(start: str, end: str, etf: dict, etf_ma: dict, ind: dict) -> tuple[dict, list[dict]]:
    cfg = BacktestConfig(start_date=start, end_date=end, **S3_CONFIG)
    data = BacktestData(cfg)
    run = simulate(cfg, data)
    close = data.close_by_ts_day
    deltas: dict[str, list[float]] = {v: [] for v in VARIANTS}
    trades_out: list[dict] = []
    n = 0
    hold_days: list[int] = []
    for t in run.trades:
        ts = _symbol_to_ts(t.symbol)
        if ts is None:
            continue
        smp = close.get(ts)
        if not smp:
            continue
        sc0, sc1 = smp.get(t.entry_date), smp.get(t.close_date)
        if not sc0 or not sc1 or sc0 <= 0:
            continue
        n += 1
        hold_days.append(int(t.holding_days))
        r_nat = sc1 / sc0 - 1.0
        industry = ind.get(str(t.symbol))
        sector_ts = _sector_bench(industry)
        board_ts = _board_bench(ts)
        picks = {}
        picks["sector_trail8"] = sector_ts
        picks["sector_ma20"] = sector_ts
        picks["board_trail8"] = board_ts
        picks["board_ma20"] = board_ts
        rec = {"symbol": str(t.symbol), "entry": t.entry_date, "close": t.close_date,
               "hold_days": int(t.holding_days), "r_nat_pct": round(r_nat * 100, 2)}
        for v in VARIANTS:
            ets = picks[v]
            if not ets:
                continue
            stop_d = _first_stop(etf.get(ets, {}), etf_ma.get(ets, {}), t.entry_date, t.close_date, v)
            if not stop_d:
                continue
            sstop = smp.get(stop_d)
            if not sstop or sstop <= 0:
                continue
            r_stop = sstop / sc0 - 1.0
            deltas[v].append(r_stop - r_nat)
            if v == PRIMARY:
                rec[f"{v}_stop"] = stop_d
                rec[f"{v}_delta_pct"] = round((r_stop - r_nat) * 100, 2)
        trades_out.append(rec)
    result = {"n": n, "mean_hold_days": round(float(np.mean(hold_days)), 1) if hold_days else 0.0}
    for v in VARIANTS:
        result[v] = _agg(deltas[v], n)
    return result, trades_out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    etf = _load_etf()
    etf_ma = _ma20(etf)
    ind = _industry_map()
    print(f"ETF panel {len(etf)}; MA20 ready; industry {len(ind)}.\n", flush=True)

    results: dict[str, dict] = {}
    for w, (s, e) in WINDOWS.items():
        print(f"=== {w} ({s}~{e}) running S-3 …", flush=True)
        r, _tr = _run_window(s, e, etf, etf_ma, ind)
        results[w] = r
        print(f"  n={r['n']} meanHold={r['mean_hold_days']}d", flush=True)
        for v in VARIANTS:
            a = r[v]
            if a.get("stopped", 0) == 0:
                print(f"    {v:<14} stopped 0", flush=True)
                continue
            print(f"    {v:<14} cov {a['coverage_pct']:>4}% whip {a['whip_pct']:>4}% "
                  f"save {a['save_pct']:>4}% meanΔ {a['mean_delta_stopped_pct']:+.2f} "
                  f"medΔ {a['median_delta_stopped_pct']:+.2f} allΔ {a['mean_delta_all_pct']:+.2f}", flush=True)
        print(flush=True)

    long = results["long"]
    wf = ["OOS2", "train", "valid"]
    p = long[PRIMARY]
    main_cols = [results[w][PRIMARY].get("mean_delta_stopped_pct", 0) for w in wf]
    k1 = p.get("mean_delta_stopped_pct", -1) > 0 and sum(1 for d in main_cols if d > 0) >= 2
    k2 = 10.0 <= p.get("coverage_pct", 0) <= 70.0
    k3 = (p.get("whip_pct") or 100.0) < 50.0
    verdict = "OPEN ENGINE REPLAY (pre-reg NAV)" if (k1 and k2 and k3) else "REJECT / close line"

    print("## H-ETF-C verdict (primary = sector_trail8)\n")
    print(f"  K1 long meanΔ {p.get('mean_delta_stopped_pct')}% / WF {[round(d,2) for d in main_cols]} -> {'pass' if k1 else 'FAIL'}")
    print(f"  K2 coverage {p.get('coverage_pct')}% in [10,70] -> {'pass' if k2 else 'FAIL'}")
    print(f"  K3 whip {p.get('whip_pct')}% < 50 -> {'pass' if k3 else 'FAIL'}")
    print(f"  => {verdict}")

    payload = {
        "tag": "s3-etf-sync-stop-2026-09-12",
        "prereg": "docs/designs/s3-etf-sync-stop-prereg-2026-09-12.md",
        "windows": results,
        "verdict": {"k1": k1, "k2": k2, "k3": k3, "primary": PRIMARY, "call": verdict},
        "as_of": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        (REPORT_DIR / "s3_etf_sync_stop_2026-09-12.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print("\nsaved report")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
