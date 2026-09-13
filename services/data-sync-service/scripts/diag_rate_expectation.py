#!/usr/bin/env python3
"""H-RATE-B pre-registered diagnostic: US rate-expectation surprise -> CN assets.

Signal: overnight change in US 2Y treasury yield (bps) as the market-implied policy
repricing ("预期差"). For each CN session t, use the last US treasury session
strictly before t (no look-ahead) and correlate with the asset's next-day return.

Prereg & kill lines: docs/designs/rate-expectation-prereg-2026-09-12.md
Read-only, no parameter search.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/diag_rate_expectation.py --save-report
"""

from __future__ import annotations

import argparse
import bisect
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
YIELDS = ROOT / "data" / "macro" / "us_yields.csv"
FOMC = ROOT / "data" / "macro" / "fomc.csv"
REPORT_DIR = ROOT / "data" / "backtest_reports"
WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "long": ("2021-01-01", "2026-08-07"),
}
FOCUS = {
    "518880.SH": "黄金", "513100.SH": "纳指100", "513180.SH": "恒生科技",
    "159915.SZ": "创业板", "588000.SH": "科创50", "510300.SH": "沪深300",
    "510500.SH": "中证500", "511260.SH": "十年国债",
}
QF = (0.2, 0.4, 0.6, 0.8)


def _load_etf() -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = defaultdict(dict)
    with ETF.open() as fh:
        for r in csv.DictReader(fh):
            d = str(r["trade_date"])
            iso = f"{d[:4]}-{d[4:6]}-{d[6:8]}" if len(d) == 8 else d
            out[r["ts_code"]][iso] = float(r["close_adj"])
    return out


def _load_dy2() -> tuple[list[str], dict[str, float]]:
    rows = []
    with YIELDS.open() as fh:
        for r in csv.DictReader(fh):
            rows.append((r["date"], float(r["us2y"])))
    rows.sort()
    dy = {}
    for i in range(1, len(rows)):
        dy[rows[i][0]] = (rows[i][1] - rows[i - 1][1]) * 100.0  # bps
    return [d for d, _ in rows], dy


def _prior_us(us_dates: list[str], t: str) -> str | None:
    i = bisect.bisect_left(us_dates, t)
    if i == 0:
        return None
    return us_dates[i - 1]


def _fomc_dates() -> set[str]:
    out = set()
    with FOMC.open() as fh:
        for r in csv.DictReader(fh):
            out.add(r["date"])
    return out


def _stats(x: np.ndarray, y: np.ndarray) -> dict:
    if len(x) < 30 or float(np.std(x)) == 0 or float(np.std(y)) == 0:
        return {"n": len(x), "corr": None}
    return {"n": len(x), "corr": round(float(np.corrcoef(x, y)[0, 1]), 3)}


def _quintiles(x: np.ndarray, y: np.ndarray) -> list[float] | None:
    if len(x) < 50:
        return None
    qs = np.quantile(x, QF)
    buckets = [[] for _ in range(5)]
    for xi, yi in zip(x, y):
        b = 0
        for q in qs:
            if xi > q:
                b += 1
        buckets[b].append(yi)
    return [round(100 * float(np.mean(b)), 3) for b in buckets]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    etf = _load_etf()
    us_dates, dy = _load_dy2()
    fomc = _fomc_dates()
    print(f"ETF {len(etf)}; US2Y Δ days {len(dy)}; FOMC dates {len(fomc)}\n", flush=True)

    results: dict[str, dict] = {}
    for ts, label in FOCUS.items():
        mp = etf.get(ts)
        if not mp:
            continue
        days = sorted(mp)
        xs, ys, dates = [], [], []
        for i in range(1, len(days)):
            t = days[i]
            pu = _prior_us(us_dates, t)
            if pu is None or pu not in dy:
                continue
            a, b = mp.get(days[i - 1]), mp.get(t)
            if not a or not b or a <= 0:
                continue
            xs.append(dy[pu]); ys.append(b / a - 1.0); dates.append(t)
        xs, ys = np.array(xs), np.array(ys)
        rec: dict = {"label": label, "n": len(xs), "all_corr": _stats(xs, ys)["corr"],
                     "quintiles_pct": _quintiles(xs, ys)}
        for w, (s, e) in WINDOWS.items():
            m = np.array([s <= d <= e for d in dates])
            rec[w] = _stats(xs[m], ys[m])["corr"] if m.sum() >= 30 else None
        # FOMC-day subset
        fm = np.array([_prior_us(us_dates, d) in fomc for d in dates])
        rec["fomc"] = _stats(xs[fm], ys[fm])["corr"] if fm.sum() >= 30 else {"n": int(fm.sum())}
        results[ts] = rec

    print("## corr(ΔUS2Y_prior, next-day return)  [quintile means bp->ret%]")
    print(f"  {'asset':<10}{'n':>6}{'all':>8}{'OOS2':>7}{'train':>7}{'valid':>7}{'fomc':>7}")
    for ts, r in results.items():
        def c(v):
            return f"{v:+.3f}" if isinstance(v, float) else "—"
        print(f"  {r['label']:<10}{r['n']:>6}{c(r['all_corr']):>8}{c(r['OOS2']):>7}"
              f"{c(r['train']):>7}{c(r['valid']):>7}{c(r.get('fomc')):>7}")
    print("\n## quintile mean returns (bps bucket: low->high ΔUS2Y)")
    for ts, r in results.items():
        if r["quintiles_pct"]:
            print(f"  {r['label']:<10}{r['quintiles_pct']}")

    dur = ("518880.SH", "513100.SH", "513180.SH", "159915.SZ", "588000.SH")
    k1_hits = []
    for ts in dur:
        r = results.get(ts)
        if not r:
            continue
        cs = [r[w] for w in ("OOS2", "train", "valid")]
        neg = sum(1 for v in cs if isinstance(v, float) and v < 0)
        if isinstance(r["all_corr"], float) and r["all_corr"] < 0 and abs(r["all_corr"]) >= 0.05 and neg >= 2:
            k1_hits.append((r["label"], r["all_corr"], cs))
    k1 = bool(k1_hits)
    k2_hits = []
    for ts in dur:
        q = results.get(ts, {}).get("quintiles_pct")
        if q and (q[0] - q[4]) > 0.3:
            k2_hits.append((results[ts]["label"], q[0] - q[4]))
    k2 = bool(k2_hits)
    verdict = "OPEN rate-expectation design" if (k1 and k2) else "REJECT / close line"
    print("\n## H-RATE-B verdict\n")
    print(f"  K1 duration corr<0 |corr|>=.05 & >=2 windows -> {'pass' if k1 else 'FAIL'} {k1_hits}")
    print(f"  K2 top-vs-bottom quintile >0.3%/day -> {'pass' if k2 else 'FAIL'} {k2_hits}")
    print(f"  => {verdict}")

    payload = {"tag": "rate-expectation-2026-09-12", "prereg": "docs/designs/rate-expectation-prereg-2026-09-12.md",
               "results": results, "verdict": {"k1": k1, "k2": k2, "call": verdict},
               "as_of": datetime.now(UTC).isoformat(timespec="seconds")}
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        (REPORT_DIR / "rate_expectation_2026-09-12.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print("\nsaved report")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
