#!/usr/bin/env python3
"""A: vendor 5min vs independent source, point-by-point — READ-ONLY, report only.

Method (frozen 2026-09-05, docs/backtests/vendor/vendor-minute-compare-2026-09-05.md):
local data/2024_5min vendor CSVs (parse_vendor_csv, LAST_HOUR_TIMES seven
bars) vs baostock 5min full-year refetch (fetch_baostock_5min, adjustflag=3),
matched on (ts_code, trade_date, time) close. Sample: blue chips + random
rest, seed=7. Price only — vol units differ x100 (vendor 手 vs baostock 股).

Gates: original = abs diff <= 0.01 on all seven bars >= 99.5% (FAILED
90.59% on snapshot-timing jitter, not wrong numbers); revised (user-approved)
= habit uses 1430/1500 only + C-stage slippage sensitivity (base 10bps /
stress 30bps). This script REPORTS both; it never judges入库.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/compare_vendor_minute.py --save-report
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

REPO_DATA = Path(__file__).resolve().parents[3] / "data"
VENDOR_2024_DIR = REPO_DATA / "2024_5min"
REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"

ABS_TOL = 0.01
ORIGINAL_GATE = 0.995
DEFAULT_BLUE_CHIPS = [
    "600000.SH",
    "600519.SH",
    "601318.SH",
    "600036.SH",
    "601166.SH",
    "601899.SH",
]


def pick_sample(codes: list[str], *, seed: int = 7, blue_chips: list[str] | None = None, n: int = 40) -> list[str]:
    """Blue chips (those present) + random rest, deterministic by seed."""
    blue = [c for c in (blue_chips or DEFAULT_BLUE_CHIPS) if c in codes]
    rest = sorted(set(codes) - set(blue))
    rng = random.Random(seed)
    rng.shuffle(rest)
    return (blue + rest)[:n]


def match_points(
    vendor_rows: list[dict], indep_rows: list[dict]
) -> list[dict]:
    """Join on (trade_date, time); per-point abs/rel close diff + OHL agreement."""
    indep = {(str(r.get("trade_date")), str(r.get("time"))): r for r in indep_rows}
    out: list[dict] = []
    for v in vendor_rows:
        key = (str(v.get("trade_date")), str(v.get("time")))
        b = indep.get(key)
        if b is None:
            out.append({"trade_date": key[0], "time": key[1], "missing": True})
            continue
        try:
            vc, bc = float(v["close"]), float(b["close"])
        except (KeyError, TypeError, ValueError):
            continue
        if bc <= 0:
            continue
        abs_diff = abs(vc - bc)
        ohl_match = all(
            _close_enough(v.get(k), b.get(k)) for k in ("open", "high", "low")
        )
        out.append(
            {
                "trade_date": key[0],
                "time": key[1],
                "v_close": vc,
                "b_close": bc,
                "abs_diff": abs_diff,
                "rel_diff": abs_diff / bc,
                "abs_match": abs_diff <= ABS_TOL,
                "ohl_match": ohl_match,
            }
        )
    return out


def _close_enough(a, b) -> bool:
    try:
        return abs(float(a) - float(b)) <= ABS_TOL
    except (TypeError, ValueError):
        return False


def _percentile(xs: list[float], pct: float) -> float | None:
    if not xs:
        return None
    ordered = sorted(xs)
    k = min(len(ordered) - 1, max(0, int(pct / 100 * len(ordered))))
    return ordered[k]


def summarize(matches: list[dict]) -> dict:
    """Aggregate: overall rate, rel-diff distribution, per-time breakdown."""
    scored = [m for m in matches if not m.get("missing")]
    missing = len(matches) - len(scored)
    matched = sum(1 for m in scored if m["abs_match"])
    rels = sorted(m["rel_diff"] for m in scored)
    per_time: dict[str, dict] = {}
    for m in scored:
        cell = per_time.setdefault(m["time"], {"n": 0, "matched": 0})
        cell["n"] += 1
        cell["matched"] += 1 if m["abs_match"] else 0
    for cell in per_time.values():
        cell["rate"] = cell["matched"] / cell["n"] if cell["n"] else 0.0
    return {
        "n": len(scored),
        "missing": missing,
        "matched": matched,
        "rate": matched / len(scored) if scored else 0.0,
        "rel_p50": _percentile(rels, 50),
        "rel_p90": _percentile(rels, 90),
        "rel_p99": _percentile(rels, 99),
        "rel_max": max(rels) if rels else None,
        "ohl_mismatch": sum(1 for m in scored if not m["ohl_match"]),
        "per_time": per_time,
    }


def verdict(summary: dict) -> dict:
    """Report both gates (original literal + revised conditional)."""
    original_pass = summary["rate"] >= ORIGINAL_GATE
    t1430 = summary["per_time"].get("1430", {})
    t1500 = summary["per_time"].get("1500", {})
    return {
        "original_gate": f"seven-bar abs<={ABS_TOL} >= {ORIGINAL_GATE:.1%}",
        "original_rate": summary["rate"],
        "original_pass": original_pass,
        "note_1430": t1430.get("rate"),
        "note_1500": t1500.get("rate"),
        "revised": "habit uses 1430/1500 only + C-stage slippage base10bps/stress30bps",
    }


def _load_vendor(ts_code: str, keep_times) -> list[dict]:
    from data_sync_service.service.ext_minute_csv import filename_to_ts_code, parse_vendor_csv

    for path in sorted(VENDOR_2024_DIR.glob("*.csv")):
        if filename_to_ts_code(path.name) == ts_code:
            _, rows = parse_vendor_csv(path, keep_times=keep_times)
            return rows
    return []


def main() -> int:
    from data_sync_service.service.bar_5min import LAST_HOUR_TIMES, fetch_baostock_5min
    from data_sync_service.service.ext_minute_csv import filename_to_ts_code

    ap = argparse.ArgumentParser(description="A-stage vendor 5min point compare (read-only)")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--sample", type=int, default=40)
    ap.add_argument("--year", default="2024")
    ap.add_argument("--limit-codes", default="")
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    codes = sorted(
        {
            c
            for p in VENDOR_2024_DIR.glob("*.csv")
            if (c := filename_to_ts_code(p.name)) is not None
        }
    )
    if args.limit_codes:
        want = {c.strip() for c in args.limit_codes.split(",") if c.strip()}
        codes = [c for c in codes if c in want]
    sample = pick_sample(codes, seed=args.seed, n=args.sample)
    print(f"vendor files: {len(codes)} codes; sample: {len(sample)} (seed={args.seed})")

    all_matches: list[dict] = []
    for i, ts_code in enumerate(sample):
        vendor_rows = _load_vendor(ts_code, LAST_HOUR_TIMES)
        indep_rows = fetch_baostock_5min(ts_code, f"{args.year}-01-01", f"{args.year}-12-31")
        pts = match_points(vendor_rows, indep_rows)
        for p in pts:
            p["ts_code"] = ts_code
        all_matches.extend(pts)
        print(f"[{i + 1}/{len(sample)}] {ts_code}: vendor={len(vendor_rows)} indep={len(indep_rows)} matched_pts={len(pts)}")
        time.sleep(0.2)

    summary = summarize(all_matches)
    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "method": "vendor LAST_HOUR_TIMES vs baostock adjustflag=3, (ts,day,time) close",
        "sample": sample,
        "seed": args.seed,
        "summary": summary,
        "verdict": verdict(summary),
    }
    print(json.dumps({"summary": summary, "verdict": report["verdict"]}, ensure_ascii=False, indent=1, default=str))
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        path = REPORT_DIR / f"vendor_minute_compare_{args.year}_{datetime.now(UTC):%Y%m%d}.json"
        path.write_text(json.dumps(report, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
        print(f"report: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
