#!/usr/bin/env python3
"""Read-only research: should hold length be conditional on stock style?

Frozen Live habit fills are all body=3 (sell the 3rd session at 14:30). This
script asks, on the SAME frozen fills (no slot-tax confound), two questions:

  marg23 = d3 14:30 / d2 14:30 - 1   value of the 3rd day  (negative → cut to 2)
  marg34 = d4 14:30 / d3 14:30 - 1   value of the 4th day  (positive → extend to 4)

Bucketed by ex-ante style features knowable at the 14:30 entry:
  - stage_1430 label (previous sessions + entry 14:30 print; zero lookahead):
    the five dims dd60 / rally20 / blast / wein / runup5 + stage_tier
  - 14:30-knowable amplitude (bar_5min <= 14:30)
  - overnight gap (open/prev close - 1)
  - prior-day total market cap (size)

Round-trip cost is identical across all three hold lengths, so it cancels in
the marginal comparison; we report gross marginals.

Exploratory only — many buckets are scanned, so cross-window consistency is
the screen, and any candidate still needs a pre-registered three-window replay.
Live / engine untouched.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/diag_sat_hold_days.py --save-report
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
    _cached_day_features,
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
DIMS = ("wein", "runup5", "dd60", "rally20", "blast")


def _px(ctx: dict, ts: str, day: str) -> float | None:
    px = _intraday_px(ctx, ts, day, "1430")
    if px and px > 0:
        return float(px)
    cl = (ctx.get("close_by_ts") or {}).get(ts, {}).get(day)
    return float(cl) if cl and float(cl) > 0 else None


def _stage_at_entry(ctx: dict, ts: str, entry_date: str) -> dict[str, str] | None:
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


def _week(d: str) -> str:
    return date.fromisoformat(d).strftime("%G-W%V")


def _agg(rows: list[dict]) -> dict:
    if not rows:
        return {"n": 0, "m23": None, "m34": None, "se23": None, "se34": None,
                "hit23": None, "hit34": None, "base3": None}
    m23 = np.array([r["m23"] for r in rows], dtype=float)
    m34 = np.array([r["m34"] for r in rows], dtype=float)
    base3 = np.array([r["base3"] for r in rows], dtype=float)
    c23: dict[str, list[float]] = defaultdict(list)
    c34: dict[str, list[float]] = defaultdict(list)
    for r in rows:
        c23[r["week"]].append(r["m23"])
        c34[r["week"]].append(r["m34"])
    se = {}
    for name, c in (("se23", c23), ("se34", c34)):
        means = np.array([float(np.mean(v)) for v in c.values()], dtype=float)
        se[name] = float(np.std(means, ddof=1) / np.sqrt(len(means))) if len(means) > 1 else None
    return {
        "n": len(rows),
        "m23": float(np.mean(m23)),
        "m34": float(np.mean(m34)),
        "se23": se["se23"],
        "se34": se["se34"],
        "hit23": float(np.mean(m23 > 0) * 100),
        "hit34": float(np.mean(m34 > 0) * 100),
        "base3": float(np.mean(base3)),
        "n_clusters": len(c23),
    }


def _p(v: float | None) -> str:
    return "—" if v is None else f"{v * 100:+.2f}%"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    print("Conditional hold research: marginal day2->3 (cut to 2) vs day3->4 (extend to 4)\n", flush=True)
    print(f"loading context {FULL_START}~{FULL_END} ...", flush=True)
    ctx = load_sgap_context(FULL_START, FULL_END)
    cal = ctx["cal"]
    idx_by_day = ctx["idx_by_day"]
    mv_map = ctx["mv_map"]
    px_hl = ctx.get("px_hl_1430") or {}
    print("  loaded.", flush=True)

    # bucket values are collected pool-wide, then terciles define the "amp" / "size" keys.
    allrows: list[dict] = []
    for wname, (s, e) in WINDOWS.items():
        sat = replay_sgap_from_context(
            ctx, start=s, end=e, skip_t1_limit=True, pool_mode="strict",
            max_pos=4, position_pct=0.25, fill_mode=FILL_SAME_1430,
            fill_hhmm="1430", exit_hhmm="1430", max_open_to_1430_pct=0.03,
            rank_key="amp_1430",
        )
        fills = [r for r in (sat.get("blotter") or [])
                 if r.get("kind") == "fill" and r.get("closeReason") == "body_exit"]
        kept = miss = 0
        for r in fills:
            ts = str(r["ts"])
            entry_date = str(r["entryDate"])
            ei = idx_by_day.get(entry_date, -1)
            if ei < 0 or ei + 3 >= len(cal):
                miss += 1
                continue
            d2, d3, d4 = cal[ei + 1], cal[ei + 2], cal[ei + 3]
            p2, p3, p4 = _px(ctx, ts, d2), _px(ctx, ts, d3), _px(ctx, ts, d4)
            if not (p2 and p3 and p4):
                miss += 1
                continue
            labels = _stage_at_entry(ctx, ts, entry_date)
            feats, _ = _cached_day_features(ctx, entry_date)
            gap = (feats.get(ts) or {}).get("gap")
            hl = (px_hl.get(ts) or {}).get(entry_date)
            p1430 = _intraday_px(ctx, ts, entry_date, "1430")
            amp1430 = (float(hl[0] - hl[1]) / float(p1430)) if (hl and p1430) else None
            prev_day = cal[ei - 1] if ei > 0 else None
            size = (mv_map.get(prev_day, {}) or {}).get(ts) if prev_day else None
            allrows.append({
                "_win": wname,
                "ts": ts,
                "entryDate": entry_date,
                "d4Date": d4,
                "m23": p3 / p2 - 1.0,
                "m34": p4 / p3 - 1.0,
                "base3": (float(r["pnlPct"]) / 100.0) if r.get("pnlPct") is not None else float("nan"),
                "week": _week(entry_date),
                "tier": stage_tier(labels),
                "labels": labels,
                "gap": gap if (gap is not None and gap == gap) else None,
                "amp1430": amp1430,
                "size": size,
            })
            kept += 1
        print(f"  {wname}: fills={len(fills)} usable={kept} skipped={miss}", flush=True)

    # pooled tercile thresholds for amp1430 / size
    def _terciles(vals: list[float]) -> tuple[float, float]:
        a = np.array(vals, dtype=float)
        return float(np.quantile(a, 1 / 3)), float(np.quantile(a, 2 / 3))

    amps = [r["amp1430"] for r in allrows if r["amp1430"] is not None]
    sizes = [r["size"] for r in allrows if r["size"] is not None]
    amp_lo, amp_hi = _terciles(amps) if amps else (0.0, 0.0)
    size_lo, size_hi = _terciles(sizes) if sizes else (0.0, 0.0)
    print(f"\npooled thresholds  amp1430 p33/p66 = {amp_lo * 100:.2f}% / {amp_hi * 100:.2f}%"
          f"   size(亿元) p33/p66 = {size_lo:.1f} / {size_hi:.1f}", flush=True)

    def key_tercile(v: float | None, lo: float, hi: float, name: str) -> str:
        if v is None:
            return f"{name}=na"
        return f"{name}={'low' if v <= lo else ('mid' if v <= hi else 'high')}"

    by_bucket: dict[str, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))
    for r in allrows:
        w = r["_win"]
        by_bucket[f"tier={r['tier']} {TIER_LABEL[r['tier']]}"][w].append(r)
        if r["labels"]:
            for dim in DIMS:
                by_bucket[f"{dim}={r['labels'][dim]}"][w].append(r)
        by_bucket[key_tercile(r["amp1430"], amp_lo, amp_hi, "amp1430")][w].append(r)
        by_bucket[key_tercile(r["size"], size_lo, size_hi, "size")][w].append(r)
        if r["gap"] is not None:
            gk = "gap<0" if r["gap"] < 0 else ("gap0-3%" if r["gap"] <= 0.03 else "gap>3%")
            by_bucket[gk][w].append(r)

    def _table(keys: list[str]) -> None:
        print(f"\n{'bucket':<20}" + "".join(f"{w + ' n/m23/m34':>26}" for w in WINDOWS) + "  verdict")
        for k in keys:
            cells = []
            verdict = "cons"
            for w in WINDOWS:
                a = _agg(by_bucket[k].get(w, []))
                cells.append(f"{a['n']:>3} {_p(a['m23'])} {_p(a['m34'])}")
            a2 = [by_bucket[k].get(w, []) for w in WINDOWS]
            # consistency: same sign across windows for both marginals
            for name in ("m23", "m34"):
                vals = [_agg(x)[name] for x in a2 if x]
                if len(vals) < 3 or not (all(v > 0 for v in vals) or all(v < 0 for v in vals)):
                    verdict = "mixed"
            print(f"{k:<20}" + "".join(f"{c:>26}" for c in cells) + f"  {verdict}")

    print("\n## stage / tier / amp / size / gap  (m23 = value of 3rd day; m34 = value of 4th day)\n")
    _table(sorted(by_bucket.keys(), key=lambda x: (not x.startswith("tier"), x)))

    print("\n## cross-window verdicts: 2-day candidates (m23 all-windows negative) "
          "and 4-day candidates (m34 all-windows positive)\n")
    for k in sorted(by_bucket):
        a2 = [_agg(by_bucket[k].get(w, [])) for w in WINDOWS]
        if any(a["n"] == 0 for a in a2):
            continue
        m23 = [a["m23"] for a in a2]
        m34 = [a["m34"] for a in a2]
        if all(v is not None and v < 0 for v in m23):
            print(f"  2-day  {k:<20} m23 " + ", ".join(f"{_p(v)}" for v in m23)
                  + "  n " + "/".join(str(a["n"]) for a in a2))
        if all(v is not None and v > 0 for v in m34):
            print(f"  4-day  {k:<20} m34 " + ", ".join(f"{_p(v)}" for v in m34)
                  + "  n " + "/".join(str(a["n"]) for a in a2))

    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        out = REPORT_DIR / "diag_sat_hold_days_2026-09-12.json"
        payload = {
            "tag": "diag-sat-hold-days-2026-09-12",
            "protocol": (
                "read-only; frozen habit replay (C1 3% same_1430, amp_1430 rank, clip4, "
                "body=3 day-3 14:30 exit). marg23=d3/d2-1, marg34=d4/d3-1, 14:30 prints. "
                "Ex-ante buckets: stage_1430 dims, amp1430 terciles, size terciles, gap. "
                "Cost cancels across hold lengths. Exploration only."
            ),
            "thresholds": {"amp1430_p33": amp_lo, "amp1430_p66": amp_hi,
                           "size_p33": size_lo, "size_p66": size_hi},
            "windows": {w: {"fills": sum(len(v[w]) for v in by_bucket.values())} for w in WINDOWS},
            "savedAt": datetime.now(tz=UTC).isoformat(),
        }
        # compact per-bucket dump
        dump = {k: {w: _agg(by_bucket[k].get(w, [])) for w in WINDOWS} for k in by_bucket}
        payload["buckets"] = dump
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n")
        print(f"\nsaved {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
