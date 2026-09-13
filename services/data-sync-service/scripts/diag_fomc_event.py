#!/usr/bin/env python3
"""H-RATE-A pre-registered diagnostic: FOMC rate-decision event study.

Event day d0 = first CN session on/after the decision date (US afternoon release
-> next CN session reacts). For each ETF compute d0 return, CAR[0,+2], pre CAR[-5,-1],
and abnormal = d0 return - asset full-sample mean daily return, split by
hike/cut/hold and by sub-period.

Prereg & kill lines: docs/designs/fomc-rate-prereg-2026-09-12.md
Read-only, no parameter search.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/diag_fomc_event.py --save-report
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
ETF = ROOT / "data" / "etf" / "etf_daily.csv"
META = ROOT / "data" / "etf" / "etf_meta.csv"
FOMC = ROOT / "data" / "macro" / "fomc.csv"
REPORT_DIR = ROOT / "data" / "backtest_reports"

FOCUS = {
    "518880.SH": "黄金", "513100.SH": "纳指100", "513110.SH": "纳指100b",
    "513180.SH": "恒生科技", "510300.SH": "沪深300", "510500.SH": "中证500",
    "159915.SZ": "创业板", "588000.SH": "科创50", "511260.SH": "十年国债",
}
PERIODS = {"2021-2022": ("2021-01-01", "2022-12-31"), "2023-2024": ("2023-01-01", "2024-12-31"),
           "2025": ("2025-01-01", "2025-12-31")}


def _load() -> tuple[dict[str, dict[str, float]], dict[str, str]]:
    px: dict[str, dict[str, float]] = defaultdict(dict)
    labels = {}
    with META.open() as fh:
        for r in csv.DictReader(fh):
            labels[r["ts_code"]] = r["label"]
    with ETF.open() as fh:
        for r in csv.DictReader(fh):
            d = str(r["trade_date"])
            iso = f"{d[:4]}-{d[4:6]}-{d[6:8]}" if len(d) == 8 else d
            px[r["ts_code"]][iso] = float(r["close_adj"])
    return px, labels


def _events() -> list[dict]:
    out = []
    with FOMC.open() as fh:
        for r in csv.DictReader(fh):
            if r["date"] < "2021-01-01":
                continue
            out.append(r)
    return out


def _ret(mp: dict[str, float], days: list[str], i: int) -> float | None:
    if i <= 0 or i >= len(days):
        return None
    a, b = mp.get(days[i - 1]), mp.get(days[i])
    if not a or not b or a <= 0:
        return None
    return b / a - 1.0


def _car(mp: dict[str, float], days: list[str], i: int, j: int) -> float | None:
    if i < 1 or j >= len(days):
        return None
    nav = 1.0
    for k in range(i, j + 1):
        r = _ret(mp, days, k)
        if r is None:
            return None
        nav *= 1 + r
    return nav - 1.0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    px, labels = _load()
    events = [e for e in _events() if e["action"] in ("hike", "cut", "hold")]
    print(f"FOMC events 2021+: {len(events)}  "
          f"hike={sum(1 for e in events if e['action']=='hike')} "
          f"cut={sum(1 for e in events if e['action']=='cut')} "
          f"hold={sum(1 for e in events if e['action']=='hold')}\n", flush=True)

    results: dict[str, dict] = {}
    for ts in FOCUS:
        mp = px.get(ts)
        if not mp:
            continue
        days = sorted(mp)
        base = float(np.mean([_ret(mp, days, i) for i in range(1, len(days)) if _ret(mp, days, i) is not None]))
        rec = {"label": labels.get(ts, ts), "baseline_pct": round(base * 100, 3)}
        for action in ("hike", "cut", "hold"):
            abn, car2, pre = [], [], []
            by_period: dict[str, list[float]] = defaultdict(list)
            for e in events:
                if e["action"] != action:
                    continue
                # first session >= decision date
                idx = None
                for i, d in enumerate(days):
                    if d >= e["date"]:
                        idx = i
                        break
                if idx is None or idx == 0:
                    continue
                r0 = _ret(mp, days, idx)
                if r0 is None:
                    continue
                abn.append(r0 - base)
                c2 = _car(mp, days, idx, min(idx + 2, len(days) - 1))
                if c2 is not None:
                    car2.append(c2)
                pc = _car(mp, days, max(1, idx - 5), idx - 1)
                if pc is not None:
                    pre.append(pc)
                for pname, (s, en) in PERIODS.items():
                    if s <= e["date"] <= en:
                        by_period[pname].append(r0 - base)
            rec[action] = {
                "n": len(abn),
                "abnormal_d0_pct": round(100 * float(np.mean(abn)), 3) if abn else None,
                "hit_pos_pct": round(100 * float(np.mean([x > 0 for x in abn])), 0) if abn else None,
                "car02_pct": round(100 * float(np.mean(car2)), 2) if car2 else None,
                "pre_car_pct": round(100 * float(np.mean(pre)), 2) if pre else None,
                "by_period_pct": {p: round(100 * float(np.mean(v)), 2) for p, v in by_period.items() if v},
            }
        results[ts] = rec

    print("## d0 abnormal return vs baseline (%), by action")
    print(f"  {'asset':<10}{'label':<9}{'hike':>9}{'cut':>9}{'hold':>9}")
    for ts, rec in results.items():
        def c(a):
            v = rec[a]["abnormal_d0_pct"]
            return f"{v:+.2f}" if v is not None else "—"
        print(f"  {ts:<10}{rec['label']:<9}{c('hike'):>9}{c('cut'):>9}{c('hold'):>9}")

    print("\n## 分段一致性（hike/cut abnormal d0 by period）")
    for ts in ("518880.SH", "513100.SH", "513180.SH", "159915.SZ", "510300.SH"):
        if ts not in results:
            continue
        r = results[ts]
        print(f"  {r['label']}({ts}): hike {r['hike']['by_period_pct']}  cut {r['cut']['by_period_pct']}")

    n_dir = sum(1 for e in events if e["action"] in ("hike", "cut"))
    # K1: mechanism = cut -> +, hike -> - for duration assets (gold/nasdaq/growth/hstech)
    dur = ("518880.SH", "513100.SH", "513110.SH", "513180.SH", "159915.SZ", "588000.SH")
    k1_hits = []
    for ts in dur:
        r = results.get(ts)
        if not r:
            continue
        h = r["hike"]["abnormal_d0_pct"]; c = r["cut"]["abnormal_d0_pct"]
        if h is None or c is None:
            continue
        hp = r["hike"]["by_period_pct"]; cp = r["cut"]["by_period_pct"]
        ok_sign = h < 0 and c > 0
        # consistency: hike negative in >=2 periods or cut positive in >=2 periods
        hc = sum(1 for v in hp.values() if v < 0) >= 2 if hp else False
        cc = sum(1 for v in cp.values() if v > 0) >= 2 if cp else False
        if abs(h) > 0.5 or abs(c) > 0.5:
            k1_hits.append((ts, round(h, 2), round(c, 2), ok_sign, hc, cc))
    k1 = any(x[3] and (x[4] or x[5]) for x in k1_hits)
    k2 = n_dir >= 15
    verdict = "OPEN rate-signal design" if (k1 and k2) else "REJECT / close line"
    print("\n## H-RATE-A verdict\n")
    print(f"  K1 mechanism+consistency in duration assets -> {'pass' if k1 else 'FAIL'}  {k1_hits}")
    print(f"  K2 directional events >=15 -> {n_dir} ({'pass' if k2 else 'FAIL'})")
    print(f"  => {verdict}")

    payload = {"tag": "fomc-rate-2026-09-12", "prereg": "docs/designs/fomc-rate-prereg-2026-09-12.md",
               "results": results, "n_events": len(events), "n_directional": n_dir,
               "verdict": {"k1": k1, "k2": k2, "call": verdict, "k1_hits": k1_hits},
               "as_of": datetime.now(UTC).isoformat(timespec="seconds")}
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        (REPORT_DIR / "fomc_rate_2026-09-12.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print("\nsaved report")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
