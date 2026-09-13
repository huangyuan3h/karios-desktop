#!/usr/bin/env python3
"""H-BOUNCE pre-registered diagnostic (market-level, read-only).

超跌区（000300 < MA200）下，国家队净买（4 宽基 ETF 20 日份额净增 > 0）日，
未来 1/3/5/10 日指数收益 vs 超跌但未净买日。

Entry: T+1 open -> T+1+N close (signal day T uses data <= T).
Prereg + kill lines: docs/designs/bounce-rescue-prereg-2026-09-12.md
No parameter search.

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/diag_bounce_rescue.py --save-report
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import psycopg

ROOT = Path(__file__).resolve().parents[1]
REPORT_DIR = ROOT / "data" / "backtest_reports"
WINDOWS = {"OOS2": ("2024-08-01", "2025-08-01"), "train": ("2025-08-01", "2026-02-01"),
           "valid": ("2026-03-01", "2026-08-07"), "long": ("2021-01-01", "2026-08-07")}
NS = (1, 3, 5, 10)
PRIMARY = 5
INDEX_CODE = "000300.SH"
BROAD_ETF_CODES = ("510300.SH", "510500.SH", "510510.SH", "159915.SZ")
LOAD_START = "2018-01-01"
LOAD_END = "2026-08-31"


def _load_index() -> list[tuple[str, float, float]]:
    from data_sync_service.config import get_settings
    with psycopg.connect(get_settings().database_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT trade_date, open, close FROM index_daily WHERE ts_code = %s "
            "AND trade_date BETWEEN %s AND %s ORDER BY trade_date",
            (INDEX_CODE, LOAD_START, LOAD_END),
        )
        out = []
        for d, o, c in cur.fetchall():
            if o is not None and c is not None:
                out.append((str(d), float(o), float(c)))
        return out


def _share_total_by_day() -> dict[str, float]:
    from data_sync_service.config import get_settings
    per: dict[str, dict[str, float]] = {c: {} for c in BROAD_ETF_CODES}
    with psycopg.connect(get_settings().database_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT trade_date, ts_code, fd_share FROM cn_etf_share "
            "WHERE ts_code = ANY(%s) AND trade_date BETWEEN %s AND %s ORDER BY trade_date",
            (list(BROAD_ETF_CODES), LOAD_START, LOAD_END),
        )
        for d, ts, s in cur.fetchall():
            if s is not None:
                per[str(ts)][str(d)] = float(s)
    all_days = sorted({d for m in per.values() for d in m})
    last: dict[str, float] = {}
    totals: dict[str, float] = {}
    for d in all_days:
        for c, m in per.items():
            if d in m:
                last[c] = m[d]
        if last:
            totals[d] = sum(last.values())
    return totals


def _ffill_onto(calendar: list[str], totals: dict[str, float]) -> list[float | None]:
    days = sorted(totals)
    out: list[float | None] = []
    pos = 0
    last: float | None = None
    for d in calendar:
        while pos < len(days) and days[pos] <= d:
            last = totals[days[pos]]
            pos += 1
        out.append(last)
    return out


def _mean(xs: list[float]) -> float | None:
    return round(100 * float(np.mean(xs)), 2) if xs else None


def _med(xs: list[float]) -> float | None:
    return round(100 * float(np.median(xs)), 2) if xs else None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--save-report", action="store_true")
    args = ap.parse_args()

    idx = _load_index()
    calendar = [r[0] for r in idx]
    opens = {r[0]: r[1] for r in idx}
    closes = {r[0]: r[2] for r in idx}
    closes_sorted = [r[2] for r in idx]
    n = len(calendar)

    total = _ffill_onto(calendar, _share_total_by_day())

    # per-day state + forward returns
    rows: dict[str, dict] = {}
    for i, d in enumerate(calendar):
        if i < 200:
            continue
        ma200 = sum(closes_sorted[i - 199: i + 1]) / 200.0
        below = closes_sorted[i] < ma200
        ret20 = closes_sorted[i] / closes_sorted[i - 20] - 1.0 if i >= 20 else None
        crash = bool(below and ret20 is not None and ret20 <= -0.05)
        t_now, t_20 = total[i], total[i - 20] if i >= 20 else None
        natd20 = (t_now - t_20) if (t_now is not None and t_20 is not None) else None
        nt_buy = natd20 is not None and natd20 > 0
        fwd: dict[int, float] = {}
        for k in NS:
            j = i + 1 + k
            if i + 1 < n and j < n:
                o1 = opens[calendar[i + 1]]
                if o1 > 0:
                    fwd[k] = closes[calendar[j]] / o1 - 1.0
        rows[d] = {"below": below, "crash": crash, "nt_buy": nt_buy,
                   "natd20": natd20, "ret20": ret20, "fwd": fwd}

    def group(pred) -> dict[int, list[float]]:
        g: dict[int, list[float]] = {k: [] for k in NS}
        for d, r in rows.items():
            if pred(r):
                for k, v in r["fwd"].items():
                    g[k].append(v)
        return g

    A = group(lambda r: r["below"] and r["nt_buy"])
    B = group(lambda r: r["below"] and not r["nt_buy"])
    A_all = group(lambda r: r["nt_buy"])
    B_all = group(lambda r: not r["nt_buy"])
    Ac = group(lambda r: r["crash"] and r["nt_buy"])
    Bc = group(lambda r: r["crash"] and not r["nt_buy"])

    print(f"\nindex {INDEX_CODE} · days(>=200 warm) {n - 200} · shares from {LOAD_START}")
    print(f"below days {sum(1 for r in rows.values() if r['below'])} · "
          f"A(below&ntbuy) {len(A[PRIMARY])} · B(below&!ntbuy) {len(B[PRIMARY])} · "
          f"crash A/B {len(Ac[PRIMARY])}/{len(Bc[PRIMARY])}")

    print("\n## forward index ret (%) T+1 open -> close(T+1+N)  [mean / median]")
    print(f"  {'group':<18}" + "".join(f"{'N'+str(k):>16}" for k in NS) + "       n")
    for name, g in (("below&A(救)", A), ("below&!B(超跌)", B),
                    ("crash&A", Ac), ("crash&B", Bc),
                    ("all&A", A_all), ("all&B", B_all)):
        cells = "".join(f"{str(_mean(g[k]))+'/'+str(_med(g[k])):>16}" for k in NS)
        print(f"  {name:<18}{cells}{len(g[PRIMARY]):>8}")

    print("\n## increment A-B")
    for k in NS:
        a, b = _mean(A[k]), _mean(B[k])
        ac, bc = _mean(Ac[k]), _mean(Bc[k])
        print(f"  N{k:<3} below Δ {(a-b) if a is not None and b is not None else None:+.2f}   "
              f"crash Δ {(ac-bc) if ac is not None and bc is not None else None:+.2f}")

    print(f"\n## by window (ret_{PRIMARY}: below&A / below&B / Δ ; crash&A / crash&B / Δ)")
    win: dict[str, dict] = {}
    for w, (s, e) in WINDOWS.items():
        def gm(pred):
            vals = [r["fwd"][PRIMARY] for d, r in rows.items() if pred(r) and s <= d <= e and PRIMARY in r["fwd"]]
            return _mean(vals)
        wa = gm(lambda r: r["below"] and r["nt_buy"])
        wb = gm(lambda r: r["below"] and not r["nt_buy"])
        wc_a = gm(lambda r: r["crash"] and r["nt_buy"])
        wc_b = gm(lambda r: r["crash"] and not r["nt_buy"])
        win[w] = {"A": wa, "B": wb, "delta": round((wa - wb), 2) if wa is not None and wb is not None else None,
                  "crashA": wc_a, "crashB": wc_b,
                  "crashDelta": round((wc_a - wc_b), 2) if wc_a is not None and wc_b is not None else None}
        print(f"  {w:<8} A {wa}  B {wb}  Δ {win[w]['delta']}   | crash A {wc_a}  B {wc_b}  Δ {win[w]['crashDelta']}")

    wf = ["OOS2", "train", "valid"]
    coverage_ok = len(A[PRIMARY]) >= 30
    k1 = (win["long"]["A"] or -99) > 0 and sum(1 for w in wf if (win[w]["A"] or -99) > 0) >= 2
    k2 = (win["long"]["delta"] or -99) > 0 and sum(1 for w in wf if (win[w]["delta"] or -99) > 0) >= 2
    verdict = "CANDIDATE (needs engine replay)" if (k1 and k2 and coverage_ok) else "REJECT / close line"
    print("\n## H-BOUNCE verdict\n")
    print(f"  coverage A>=30 long -> {'pass' if coverage_ok else 'FAIL'} ({len(A[PRIMARY])})")
    print(f"  K1 below&A ret5 long>0 & >=2 windows -> {'pass' if k1 else 'FAIL'}")
    print(f"  K2 (A-B) ret5 long>0 & >=2 windows -> {'pass' if k2 else 'FAIL'}")
    print(f"  => {verdict}")

    # descriptive: rescue onset (natd20 <=0 -> >0 while below)
    onset = []
    prev_d = None
    for d in calendar:
        r = rows.get(d)
        if r is None:
            continue
        if d < WINDOWS["long"][0] or d > WINDOWS["long"][1]:
            prev_d = d
            continue
        pr = rows.get(prev_d) if prev_d else None
        if (pr and pr["below"] and pr["natd20"] is not None and pr["natd20"] <= 0
                and r["below"] and r["nt_buy"]):
            onset.append({"date": d, "ret5": round(100 * r["fwd"].get(PRIMARY, float("nan")), 2)
                          if PRIMARY in r["fwd"] else None})
        prev_d = d
    print(f"\n## rescue-onset events (natD20 <=0 -> >0 while below), long window: {len(onset)}")
    for o in onset:
        print(f"  {o['date']}  ret5 {o['ret5']}")

    payload = {"tag": "bounce-rescue-2026-09-12",
               "prereg": "docs/designs/bounce-rescue-prereg-2026-09-12.md",
               "below_A": {k: _mean(A[k]) for k in NS}, "below_B": {k: _mean(B[k]) for k in NS},
               "crash_A": {k: _mean(Ac[k]) for k in NS}, "crash_B": {k: _mean(Bc[k]) for k in NS},
               "by_window": win, "n_A": len(A[PRIMARY]), "n_B": len(B[PRIMARY]),
               "onset_events": onset,
               "verdict": {"coverage_ok": coverage_ok, "k1": k1, "k2": k2, "call": verdict},
               "as_of": datetime.now(UTC).isoformat(timespec="seconds")}
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        (REPORT_DIR / "bounce_rescue_2026-09-12.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print("\nsaved report")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
