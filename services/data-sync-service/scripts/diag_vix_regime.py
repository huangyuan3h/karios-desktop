#!/usr/bin/env python3
"""H-VIX-REGIME read-only diagnostic (B20 · 2026-09-24).

Tests whether VIX (prior US close, trailing-252d percentile) is a composable
slow-variable risk-regime brick on the allocation layer:
  A) conditional forward-20d returns by regime for the B3 universe + B3 nav;
  B) a VIX-gated B3 (HIGH -> 100% 511260 at monthly rebalance) vs baseline B3.

Prereg & frozen kill lines: docs/designs/vix-regime-prereg-2026-09-24.md
Read-only, zero grid, no Live/product change.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/diag_vix_regime.py --save-report
"""

from __future__ import annotations

import argparse
import bisect
import csv
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
REPORT_DIR = ROOT / "data" / "backtest_reports"
ETF_CSV = ROOT / "data" / "etf" / "etf_daily.csv"

from eval_etf_parking_baseline import _metrics  # noqa: E402

B3 = ["510300.SH", "510500.SH", "518880.SH", "513100.SH", "511260.SH"]
BOND = "511260.SH"
COST = 0.0005
FWD = 20
VIX_HIGH_PCT = 0.80
WINS: dict[str, tuple[str, str]] = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "long": ("2021-08-01", "2026-08-07"),
}


def _load_panel(wanted: set[str]) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    with ETF_CSV.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            ts = row["ts_code"]
            if ts not in wanted:
                continue
            d = str(row["trade_date"])
            d = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
            try:
                c = float(row["close_adj"])
            except (TypeError, ValueError):
                continue
            if c > 0:
                out.setdefault(ts, {})[d] = c
    return out


def _load_vix() -> dict[str, float]:
    from data_sync_service.db.macro_daily import fetch_macro_daily

    rows = fetch_macro_daily("VIX", limit=10000)
    out: dict[str, float] = {}
    for r in rows:
        d = str(r.get("trade_date") or "")
        c = r.get("close")
        if d and c is not None:
            out[d] = float(c)
    return out


def _series_on_cal(px: dict[str, float], cal: list[str]) -> list[float]:
    out: list[float] = []
    last = None
    for d in cal:
        v = px.get(d)
        if v:
            last = v
        out.append(last)
    first = next((v for v in out if v), None)
    return [1.0] * len(cal) if first is None else [(v / first if v else 1.0) for v in out]


def _rp_nav(
    series: dict[str, list[float]],
    cal: list[str],
    cost: float,
    high_by_i: dict[int, bool] | None = None,
) -> list[float]:
    n = len(cal)
    w_by_i: dict[int, dict[str, float]] = {}
    for i in range(n):
        if i < 60:
            w_by_i[i] = {ts: 1.0 / len(B3) for ts in B3}
            continue
        if i > 0 and cal[i][:7] == cal[i - 1][:7]:
            w_by_i[i] = w_by_i[i - 1]
            continue
        if high_by_i and high_by_i.get(i):
            w_by_i[i] = {BOND: 1.0}
            continue
        vol: dict[str, float] = {}
        for ts in B3:
            r = [
                series[ts][j] / series[ts][j - 1] - 1
                for j in range(max(1, i - 60), i)
                if series[ts][j - 1]
            ]
            vol[ts] = float(np.std(r)) or 1e-9
        inv = {ts: 1.0 / vol[ts] for ts in B3}
        tot = sum(inv.values())
        w_by_i[i] = {ts: inv[ts] / tot for ts in B3}
    nav, cur_w = [1.0], w_by_i[0]
    for i in range(1, n):
        r = sum(
            cur_w.get(ts, 0.0)
            * (series[ts][i] / series[ts][i - 1] - 1 if series[ts][i - 1] else 0.0)
            for ts in cur_w
        )
        nav.append(nav[-1] * (1.0 + r))
        if w_by_i[i] != cur_w:
            keys = set(w_by_i[i]) | set(cur_w)
            turn = sum(abs(w_by_i[i].get(ts, 0.0) - cur_w.get(ts, 0.0)) for ts in keys) / 2.0
            nav[-1] *= 1.0 - cost * turn
            cur_w = w_by_i[i]
    return nav


def _regime_fn(vix: dict[str, float]):
    dates = sorted(vix)
    vals = [vix[d] for d in dates]

    def at(t: str) -> str | None:
        i = bisect.bisect_left(dates, t) - 1
        if i < 60:
            return None
        lo = max(0, i - 251)
        window = vals[lo : i + 1]
        v = vals[i]
        pct = sum(1 for x in window if x <= v) / len(window)
        return "HIGH" if pct >= VIX_HIGH_PCT else "NORMAL"

    return at


def _fwd(series: list[float], i: int, n: int = FWD) -> float | None:
    if i + n < len(series) and series[i]:
        return series[i + n] / series[i] - 1.0
    return None


def _cond(series: list[float], cal: list[str], reg: dict[str, str | None]) -> dict:
    acc: dict[str, list[float]] = {"HIGH": [], "NORMAL": []}
    for i in range(len(cal)):
        r = reg.get(cal[i])
        f = _fwd(series, i)
        if r in acc and f is not None:
            acc[r].append(f)
    out = {}
    for k in ("HIGH", "NORMAL"):
        a = acc[k]
        out[k] = {"n": len(a), "mean": round(100 * float(np.mean(a)), 2) if a else None}
    if out["HIGH"]["mean"] is not None and out["NORMAL"]["mean"] is not None:
        out["diff"] = round(out["HIGH"]["mean"] - out["NORMAL"]["mean"], 2)
    else:
        out["diff"] = None
    return out


def _buckets(series: list[float], cal: list[str], vix: dict[str, float], fn) -> list[dict]:
    dates = sorted(vix)
    vals = [vix[d] for d in dates]
    acc: dict[str, list[float]] = {"<20": [], "20-50": [], "50-80": [], ">=80": []}
    for i in range(len(cal)):
        j = bisect.bisect_left(dates, cal[i]) - 1
        if j < 60:
            continue
        lo = max(0, j - 251)
        window = vals[lo : j + 1]
        pct = sum(1 for x in window if x <= vals[j]) / len(window)
        key = "<20" if pct < 0.20 else "20-50" if pct < 0.50 else "50-80" if pct < 0.80 else ">=80"
        f = _fwd(series, i)
        if f is not None:
            acc[key].append(f)
    return [
        {"bucket": k, "n": len(v), "mean": round(100 * float(np.mean(v)), 2) if v else None}
        for k, v in acc.items()
    ]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    px = _load_panel(set(B3))
    vix = _load_vix()
    print(
        f"panel {len(px)} series · VIX rows {len(vix)} ({min(vix) if vix else '-'}~{max(vix) if vix else '-'})"
    )
    at = _regime_fn(vix)

    all_dates = sorted(
        {d for ts in B3 for d in px.get(ts, {}) if WINS["long"][0] <= d <= WINS["long"][1]}
    )
    results: dict[str, dict] = {}
    for w, (s, e) in WINS.items():
        cal = [d for d in all_dates if s <= d <= e]
        series = {ts: _series_on_cal(px.get(ts, {}), cal) for ts in B3}
        reg = {d: at(d) for d in cal}
        base = _rp_nav(series, cal, COST)
        high = {i: reg.get(cal[i]) == "HIGH" for i in range(len(cal))}
        gated = _rp_nav(series, cal, COST, high_by_i=high)
        n_high = sum(1 for v in high.values() if v)
        cond = {"B3": _cond(base, cal, reg)}
        for ts in B3:
            cond[ts] = _cond(series[ts], cal, reg)
        results[w] = {
            "days": len(cal),
            "high_days": n_high,
            "base": {"total": round(100 * (base[-1] - 1), 1), **_metrics(base)},
            "gated": {"total": round(100 * (gated[-1] - 1), 1), **_metrics(gated)},
            "cond": cond,
        }
        print(f"\n== {w} {s}~{e}  n={len(cal)}  HIGH days={n_high} ==")
        for name, c in cond.items():
            print(
                f"   fwd20 {name:<9} HIGH {c['HIGH']['mean']} (n{c['HIGH']['n']})  "
                f"NORMAL {c['NORMAL']['mean']} (n{c['NORMAL']['n']})  diff {c['diff']}"
            )
        b, g = results[w]["base"], results[w]["gated"]
        print(
            f"   B3 base  tot {b['total']:+.1f} mdd {b['mdd']} sr {b['sharpe']}  |  "
            f"gated tot {g['total']:+.1f} mdd {g['mdd']} sr {g['sharpe']}"
        )

    # 4-bucket table on the long window B3 nav
    s, e = WINS["long"]
    cal = [d for d in all_dates if s <= d <= e]
    series = {ts: _series_on_cal(px.get(ts, {}), cal) for ts in B3}
    base = _rp_nav(series, cal, COST)
    buckets = _buckets(base, cal, vix, at)
    print("\n== long B3 fwd20 by VIX percentile bucket ==")
    for row in buckets:
        print(f"   {row['bucket']:<6} n={row['n']:<4} mean {row['mean']}")

    # frozen verdict
    walk = [w for w in ("OOS2", "train", "valid")]
    long_ok = (
        results["long"]["cond"]["B3"]["diff"] is not None
        and results["long"]["cond"]["B3"]["diff"] < 0
    )
    k1 = long_ok and sum(1 for w in walk if (results[w]["cond"]["B3"]["diff"] or 0) < 0) >= 2
    diffs = [results[w]["gated"]["total"] - results[w]["base"]["total"] for w in walk]
    g1 = sum(results[w]["gated"]["total"] for w in walk) >= sum(
        results[w]["base"]["total"] for w in walk
    )
    g3 = sum(1 for d in diffs if d > 0) >= 2
    long_nw = results["long"]["gated"]["total"] >= results["long"]["base"]["total"]
    mdd_ok = sum(1 for w in walk if results[w]["gated"]["mdd"] >= results[w]["base"]["mdd"]) >= 2
    k2 = g1 and g3 and long_nw and mdd_ok
    verdict = {"K1": k1, "K2": k2, "pass": k1 and k2}
    print("\n## verdict (H-VIX-REGIME · frozen)")
    print(
        f"   K1 conditional-edge: {'ok' if k1 else 'FAIL'} (long B3 diff {results['long']['cond']['B3']['diff']})"
    )
    print(
        f"   K2 gated increment : {'ok' if k2 else 'FAIL'} (G1 {g1} G3 {g3} long {long_nw} mdd {mdd_ok})"
    )
    print(f"   -> {'PASS candidate' if verdict['pass'] else 'REJECT'}")

    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        (REPORT_DIR / "vix_regime_2026-09-24.json").write_text(
            json.dumps(
                {
                    "tag": "vix-regime-2026-09-24",
                    "prereg": "docs/designs/vix-regime-prereg-2026-09-24.md",
                    "buckets_long": buckets,
                    "results": results,
                    "verdict": verdict,
                    "as_of": datetime.now(UTC).isoformat(timespec="seconds"),
                },
                ensure_ascii=False,
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )
        print("saved report")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
