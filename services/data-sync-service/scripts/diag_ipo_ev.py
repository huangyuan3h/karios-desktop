#!/usr/bin/env python3
"""H-IPO-A pre-registered diagnostic: retail 打新 expected-value layer.

EV per account of market value M (万元, half 沪 / half 深):
  quota = min( (M/2)*1000 , limit_amount*10000 )   # shares, capped by 申购上限
  hit_shares = quota * ballot% / 100
  profit = hit_shares * price * sell_ret
Annual 打新 return on M = sum(profit) / (M*10000).

Prereg & kill lines: docs/designs/ipo-ev-prereg-2026-09-12.md
Read-only. No grid (sell rule / apps fixed).

Usage:
  cd services/data-sync-service
  PYTHONPATH=src python3 scripts/diag_ipo_ev.py --save-report
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PERF = ROOT / "data" / "ipo" / "ipo_perf.csv"
REPORT_DIR = ROOT / "data" / "backtest_reports"
WINDOWS = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
    "long": ("2021-01-01", "2026-08-07"),
}
MS = (10, 20, 50, 100)  # 万元


def _market(ts: str) -> str | None:
    p = ts.split(".")[0]
    if p.startswith(("688", "689", "600", "601", "603", "605")):
        return "SH"
    if p.startswith(("300", "301", "000", "001", "002", "003")):
        return "SZ"
    return None


def _years(s: str, e: str) -> float:
    from datetime import date
    d0 = date.fromisoformat(s)
    d1 = date.fromisoformat(e)
    return max((d1 - d0).days / 365.25, 1e-9)


def _ev(rows: list[dict], M: float) -> dict:
    mv = M / 2.0 * 1000.0  # shares quota per market
    profit = 0.0
    hits = 0.0
    for r in rows:
        mk = _market(r["ts_code"])
        if mk is None:
            continue
        try:
            price = float(r["price"])
            ballot = float(r["ballot"])
            limit = float(r["limit_amount"])
            sell = float(r["sell_ret"])
        except (ValueError, KeyError):
            continue
        if price <= 0:
            continue
        quota = min(mv, limit * 10000.0)
        hit = quota * ballot / 100.0
        hits += hit
        profit += hit * price * (sell / 100.0)
    return {"profit_yuan": round(profit, 0), "exp_hit_shares": round(hits, 1),
            "ev_pct_of_M": round(profit / (M * 10000.0) * 100, 2), "n": len(rows)}


def _iso(d: str) -> str:
    return f"{d[:4]}-{d[4:6]}-{d[6:8]}" if len(d) == 8 else d


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    rows = [r for r in csv.DictReader(PERF.open()) if r["board"] != "bj"]
    for r in rows:
        r["ipo_date"] = _iso(r["ipo_date"])
    print(f"IPOs (ex-BJ): {len(rows)}")
    neg = sum(1 for r in rows if float(r["sell_ret"]) < 0)
    print(f"破发(sell_ret<0): {neg}/{len(rows)} = {100*neg/len(rows):.1f}%\n")

    out = {}
    print("## EV% of 底仓市值, by window × M")
    hdr = "  window      " + "  ".join(f"M{M}万" for M in MS) + "     n"
    print(hdr)
    for w, (s, e) in WINDOWS.items():
        sub = [r for r in rows if s <= r["ipo_date"] <= e]
        yrs = _years(s, e)
        cells = []
        for M in MS:
            m = _ev(sub, M)
            out.setdefault(w, {})[f"M{M}"] = {**m, "annual_ev_pct": round(m["ev_pct_of_M"] / yrs, 2)}
            cells.append(f"{m['ev_pct_of_M']:+.2f}")
        print(f"  {w:<10} {cells[0]:>7} {cells[1]:>7} {cells[2]:>7} {cells[3]:>7}   {len(sub)}")
    print()

    print("## per-year EV% (M=20万, annualized)")
    years = sorted({r["ipo_date"][:4] for r in rows})
    per_year = {}
    for y in years:
        sub = [r for r in rows if r["ipo_date"][:4] == y]
        m = _ev(sub, 20)
        per_year[y] = {"n": len(sub), "ev_pct": m["ev_pct_of_M"], "exp_hits": m["exp_hit_shares"]}
        print(f"  {y}: n={len(sub):<4} EV {m['ev_pct_of_M']:+7.2f}%  期望中签股数 {m['exp_hit_shares']:.1f}")
    print()

    wf = ["OOS2", "train", "valid"]
    e20 = {w: out[w]["M20"]["ev_pct_of_M"] for w in WINDOWS}
    k1 = e20["long"] > 0
    k2 = sum(1 for w in wf if e20[w] > 0) >= 2
    k3 = per_year.get("2025", {}).get("ev_pct", -1) > 0 and per_year.get("2026", {}).get("ev_pct", -1) > 0
    verdict = "BUILD 打新层" if (k1 and k2 and k3) else "存量操作 only / 不进产品线"
    print("## H-IPO-A verdict (M=20万)\n")
    print(f"  K1 long EV {e20['long']:+.2f}% ({'pass' if k1 else 'FAIL'})")
    print(f"  K2 WF {[round(e20[w],2) for w in wf]} -> {sum(1 for w in wf if e20[w]>0)}/3 ({'pass' if k2 else 'FAIL'})")
    print(f"  K3 2025 {per_year.get('2025',{}).get('ev_pct')}% / 2026 {per_year.get('2026',{}).get('ev_pct')}% ({'pass' if k3 else 'FAIL'})")
    print(f"  => {verdict}")

    payload = {"tag": "ipo-ev-2026-09-12", "prereg": "docs/designs/ipo-ev-prereg-2026-09-12.md",
               "windows": out, "per_year": per_year,
               "verdict": {"k1": k1, "k2": k2, "k3": k3, "call": verdict,
                           "wf_ev_pct": {w: e20[w] for w in wf}},
               "as_of": datetime.now(UTC).isoformat(timespec="seconds")}
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        (REPORT_DIR / "ipo_ev_2026-09-12.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print("\nsaved report")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
