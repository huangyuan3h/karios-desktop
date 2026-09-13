#!/usr/bin/env python3
"""Read-only diagnostic: marginal d3->d4 (14:30) return of frozen habit fills.

H-SAT-RANK (2026-09-11 PASS) showed the stage_1430 label (S2-advance x
runup5-climax) predicts a per-trade 3-day edge. This asks whether the SAME
ex-ante label also predicts a positive 4th-day extension on the frozen
body=3 fills -- i.e. is "hold tier0/1 four days, tier2 three days" worth an
engine change?

Method: run the frozen Live habit replay (C1 3% same_1430, amp_1430 rank,
clip4, day-3 14:30 exit). For each completed body_exit fill, take the SAME
name and measure 14:30 price day-4 vs 14:30 price day-3. The round-trip cost
is identical on both paths, so it cancels in the extension decision. Bucket
by stage at entry (prior sessions + entry-day 14:30 print = zero lookahead).

Observation only. Live / engine untouched. If no ex-ante bucket is robustly
positive across three windows, the conditional-hold idea is closed before
any replay.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/diag_sat_hold_d4.py
  PYTHONPATH=src:scripts python3 scripts/diag_sat_hold_d4.py --save-report
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import UTC, date, datetime
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from data_sync_service.service.state_bucket_track import (  # noqa: E402
    FILL_SAME_1430,
    _intraday_px,
    load_sgap_context,
    replay_sgap_from_context,
    stage_labels,
    stage_tier,
)

WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
}
FULL_START = "2024-08-01"
FULL_END = "2026-08-07"
REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"
TIER_LABEL = {0: "S2&climax", 1: "either", 2: "neither", 3: "unlabeled"}


def _stage_at_entry(ctx: dict, ts: str, entry_date: str) -> dict[str, str] | None:
    """Zero-lookahead stage_1430 label at entry: prior sessions + entry 14:30."""
    series = ctx["per_ts"].get(ts)
    di = ctx["date_idx"].get(ts, {}).get(entry_date, -1)
    if not series or di < 0:
        return None
    closes = [float(r["close"]) for r in series[:di] if r.get("close")]
    px = _intraday_px(ctx, ts, entry_date, "1430")
    if px and px > 0:
        closes.append(float(px))
    if len(closes) < 61:
        return None
    return stage_labels(closes)


def _exit_px(ctx: dict, ts: str, day: str) -> tuple[float | None, str]:
    px = _intraday_px(ctx, ts, day, "1430")
    if px and px > 0:
        return float(px), "1430"
    cl = (ctx.get("close_by_ts") or {}).get(ts, {}).get(day)
    if cl and float(cl) > 0:
        return float(cl), "close"
    return None, "none"


def _week(d: str) -> str:
    return date.fromisoformat(d).strftime("%G-W%V")


def _agg(rows: list[dict]) -> dict:
    """rows: {marginal, base3, week}. Pooled mean + cluster-by-week t."""
    if not rows:
        return {"n": 0, "mean": None, "median": None, "hit": None, "se": None, "t": None, "base3": None}
    m = np.array([r["marginal"] for r in rows], dtype=float)
    b = np.array([r["base3"] for r in rows], dtype=float)
    clusters: dict[str, list[float]] = defaultdict(list)
    for r in rows:
        clusters[r["week"]].append(r["marginal"])
    cmeans = np.array([float(np.mean(v)) for v in clusters.values()], dtype=float)
    se = float(np.std(cmeans, ddof=1) / np.sqrt(len(cmeans))) if len(cmeans) > 1 else None
    mean = float(np.mean(m))
    return {
        "n": len(rows),
        "mean": mean,
        "median": float(np.median(m)),
        "hit": float(np.mean(m > 0) * 100),
        "se": se,
        "t": (mean / se) if se and se > 0 else None,
        "base3": float(np.mean(b)),
        "n_clusters": len(cmeans),
    }


def _pct(v: float | None) -> str:
    return "—" if v is None else f"{v * 100:+.2f}%"


def _num(v: float | None, nd: int = 2) -> str:
    return "—" if v is None else f"{v:+.{nd}f}"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    print("Diag: frozen habit body=3 fills, marginal d3->d4 14:30 return by stage\n", flush=True)
    print(f"loading context {FULL_START}~{FULL_END} ...", flush=True)
    ctx = load_sgap_context(FULL_START, FULL_END)
    cal = ctx["cal"]
    idx_by_day = ctx["idx_by_day"]
    print("  loaded.", flush=True)

    results: dict[str, dict] = {}
    cross: dict[int, dict[str, float | None]] = {t: {} for t in (0, 1, 2, 3)}
    for wname, (s, e) in WINDOWS.items():
        sat = replay_sgap_from_context(
            ctx,
            start=s,
            end=e,
            skip_t1_limit=True,
            pool_mode="strict",
            max_pos=4,
            position_pct=0.25,
            fill_mode=FILL_SAME_1430,
            fill_hhmm="1430",
            exit_hhmm="1430",
            max_open_to_1430_pct=0.03,
            rank_key="amp_1430",
        )
        blotter = sat.get("blotter") or []
        fills = [r for r in blotter if r.get("kind") == "fill" and r.get("closeReason") == "body_exit"]
        by_tier: dict[int, list[dict]] = defaultdict(list)
        by_stage2: dict[tuple[str, str], list[dict]] = defaultdict(list)
        missing_d4 = 0
        missing_stage = 0
        for r in fills:
            ts = str(r["ts"])
            entry_date = str(r["entryDate"])
            exit_date = str(r["exitDate"])
            p3, _ = _exit_px(ctx, ts, exit_date)
            ei = idx_by_day.get(exit_date, -1)
            if p3 is None or ei < 0 or ei + 1 >= len(cal):
                missing_d4 += 1
                continue
            d4 = cal[ei + 1]
            p4, _ = _exit_px(ctx, ts, d4)
            if p4 is None:
                missing_d4 += 1
                continue
            labels = _stage_at_entry(ctx, ts, entry_date)
            if labels is None:
                missing_stage += 1
            tier = stage_tier(labels)
            base3 = (float(r["pnlPct"]) / 100.0) if r.get("pnlPct") is not None else float("nan")
            rec = {
                "ts": ts,
                "entryDate": entry_date,
                "exitDate": exit_date,
                "d4Date": d4,
                "marginal": p4 / p3 - 1.0,
                "base3": base3,
                "week": _week(entry_date),
            }
            by_tier[tier].append(rec)
            if labels is not None:
                by_stage2[(labels.get("wein", "?"), labels.get("runup5", "?"))].append(rec)

        print(f"\n=== {wname} ({s}~{e}) habit fills={len(fills)} "
              f"d4-usable={len(fills) - missing_d4} (skip d4-missing {missing_d4}, "
              f"stage-missing {missing_stage}) ===", flush=True)
        print(f"{'tier':<10}{'n':>5}{'mean d4':>10}{'median':>9}{'hit':>7}{'clSE':>8}{'t':>7}{'base3':>9}")
        for t in (0, 1, 2, 3):
            a = _agg(by_tier.get(t, []))
            results.setdefault(wname, {})[t] = a
            if a["n"]:
                cross[t][wname] = a["mean"]
            hit = "—" if a["hit"] is None else f"{a['hit']:.0f}%"
            print(
                f"{TIER_LABEL[t]:<10}{a['n']:>5}{_pct(a['mean']):>10}{_pct(a['median']):>9}"
                f"{hit:>7}{_pct(a['se']):>8}{_num(a['t']):>7}{_pct(a['base3']):>9}"
            )
        allrec = [r for t in by_tier for r in by_tier[t]]
        a_all = _agg(allrec)
        results.setdefault(wname, {})["all"] = a_all
        hit_all = "—" if a_all["hit"] is None else f"{a_all['hit']:.0f}%"
        print(
            f"{'ALL':<10}{a_all['n']:>5}{_pct(a_all['mean']):>10}{_pct(a_all['median']):>9}"
            f"{hit_all:>7}{_pct(a_all['se']):>8}{_num(a_all['t']):>7}{_pct(a_all['base3']):>9}"
        )

        print("\n  by (wein, runup5):")
        for key in sorted(by_stage2):
            a = _agg(by_stage2[key])
            print(f"    {key[0]:<12} x {key[1]:<7} n={a['n']:>3}  mean d4 {_pct(a['mean'])}  hit {a['hit']:.0f}%  base3 {_pct(a['base3'])}")

    print("\n## Cross-window summary: mean marginal d3->d4 by tier\n")
    print(f"{'tier':<10}" + "".join(f"{w:>12}" for w in WINDOWS) + f"{'verdict':>28}")
    for t in (0, 1, 2, 3):
        cells = []
        ok = True
        seen = 0
        for w in WINDOWS:
            v = cross[t].get(w)
            cells.append("—" if v is None else f"{v * 100:+.2f}%")
            if v is None:
                ok = False
            else:
                seen += 1
                if v <= 0:
                    ok = False
        verdict = "no data" if seen == 0 else ("all-windows positive" if ok and seen == 3 else "not consistent")
        print(f"{TIER_LABEL[t]:<10}" + "".join(f"{c:>12}" for c in cells) + f"{verdict:>28}")

    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        out = REPORT_DIR / "diag_sat_hold_d4_2026-09-12.json"
        payload = {
            "tag": "diag-sat-hold-d4-2026-09-12",
            "protocol": (
                "read-only; frozen Live habit replay (C1 3% same_1430, amp_1430 rank, "
                "clip4 4x25%, body=3, day-3 14:30 exit); for each body_exit fill measure "
                "same-name 14:30 day-4 vs 14:30 day-3; stage_1430 label at entry "
                "(zero lookahead); round-trip cost cancels in the extension decision"
            ),
            "windows": {
                w: {str(k): v for k, v in row.items()} for w, row in results.items()
            },
            "cross_window_mean": {str(t): cross[t] for t in cross},
            "savedAt": datetime.now(tz=UTC).isoformat(),
        }
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n")
        print(f"\nsaved {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
