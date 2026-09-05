#!/usr/bin/env python3
"""D: vendor adj factors vs DB — READ-ONLY diagnostic, report only, no fixes.

Vendor files (repo data/): one CSV per stock, columns 股票代码,交易日期,复权因子.
  复权因子_后复权/CODE.csv = cumulative from listing (hou)
  复权因子_前复权/CODE.csv = hou / hou_latest, anchored 1.0 at latest (qian)

DB side: daily.adj_factor (tushare, clean step function) + daily OHLC
(Tencent qfq) + bar_5min 1500 closes (bfq, unadjusted).

Levels are NOT comparable (different bases/anchors by construction).
Comparison is anchor-free (qfq-space daily returns must match regardless
of anchor) plus self-consistency:
  1. return agreement: ret(vendor hou) vs ret(daily qfq close) on the same
     dates — both live in qfq space, so disagreement = real data divergence.
  2. qian self-consistency: vendor qian vs hou/hou_latest.
  3. qian sanity: negative/implausible vendor qian values (report only).
  4. DB ex-div reference: dates where |adj/adj_prev - 1| > 0.1% (tushare),
     with vendor-hou continuity check on those dates (a true hou-price
     must NOT gap on ex-div days).

Sample: most-active 200 stocks by 2024 daily amount (or --sample N).

Usage:
  cd services/data-sync-service
  PYTHONPATH=src:scripts python3 scripts/compare_vendor_adj.py --save-report
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_sync_service.db import get_connection  # noqa: E402

REPO_DATA = Path(__file__).resolve().parents[3] / "data"
HOU_DIR = REPO_DATA / "复权因子_后复权"
QIAN_DIR = REPO_DATA / "复权因子_前复权"
REPORT_DIR = Path(__file__).resolve().parents[1] / "data" / "backtest_reports"
JUMP_PCT = 0.001


def _norm_date(d: str) -> str:
    s = (d or "").strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s


def _read_factor(path: Path) -> dict[str, float]:
    out: dict[str, float] = {}
    try:
        with path.open(encoding="utf-8-sig", newline="") as fh:
            for rec in csv.DictReader(fh):
                d = _norm_date(rec.get("交易日期") or "")
                try:
                    v = float(rec.get("复权因子") or "")
                except (TypeError, ValueError):
                    continue
                if d and v == v:
                    out[d] = v
    except FileNotFoundError:
        pass
    return out


def _jump_dates(series: dict[str, float]) -> dict[str, float]:
    days = sorted(series)
    out: dict[str, float] = {}
    for i in range(1, len(days)):
        p, c = series[days[i - 1]], series[days[i]]
        if p and p > 0:
            r = c / p - 1.0
            if abs(r) > JUMP_PCT:
                out[days[i]] = r
    return out


def _most_active(n: int) -> list[str]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ts_code, SUM(amount) FROM daily "
                "WHERE trade_date >= '2024-01-01' AND trade_date < '2025-01-01' "
                "AND amount IS NOT NULL "
                "AND (ts_code LIKE '%%.SH' OR ts_code LIKE '%%.SZ') "
                "AND ts_code NOT LIKE '%%.BJ' "
                "GROUP BY 1 ORDER BY 2 DESC LIMIT %s",
                (n,),
            )
            return [str(r[0]) for r in cur.fetchall()]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sample", type=int, default=200)
    ap.add_argument("--hou-dir", type=str, default=None)
    ap.add_argument("--save-report", action="store_true")
    ap.add_argument("--report-name", default="vendor_adj_compare_2026-09-05.json")
    args = ap.parse_args()
    hou_dir = Path(args.hou_dir) if args.hou_dir else HOU_DIR

    codes = _most_active(args.sample)
    print(f"sample {len(codes)} most-active stocks")
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ts_code, trade_date, adj_factor FROM daily "
                "WHERE trade_date >= '2023-01-01' AND adj_factor IS NOT NULL"
            )
            db_adj: dict[str, dict[str, float]] = {}
            for ts, d, a in cur.fetchall():
                ds = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
                db_adj.setdefault(str(ts), {})[ds] = float(a)
            cur.execute(
                "SELECT ts_code, trade_date, close FROM daily "
                "WHERE trade_date >= '2023-01-01'"
            )
            db_close: dict[str, dict[str, float]] = {}
            for ts, d, c in cur.fetchall():
                if c is None:
                    continue
                ds = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
                db_close.setdefault(str(ts), {})[ds] = float(c)

    # (bar_5min not needed: returns are compared in qfq space)

    tot_pairs = 0
    ret_rel_errs: list[float] = []
    absdiff_bps: list[float] = []
    level_rel_errs: list[float] = []
    recon_rel_errs: list[float] = []
    exdiv_checked = exdiv_continuous = 0
    qian_neg = qian_bad = 0
    qian_self_errs: list[float] = []
    per_stock: list[dict] = []
    db_exdiv: dict[str, set[str]] = {c: set(_jump_dates(db_adj.get(c, {}))) for c in codes}
    for code in codes:
        hou = _read_factor(hou_dir / f"{code}.csv")
        qian = _read_factor(QIAN_DIR / f"{code}.csv") if QIAN_DIR.is_dir() else {}
        qn = sum(1 for v in qian.values() if v < 0)
        qb = sum(1 for v in qian.values() if v <= 0 or v > 2.0)
        qian_neg += qn
        qian_bad += qb
        hlatest = hou.get(max(hou)) if hou else None
        if hlatest:
            for d, qv in qian.items():
                hv = hou.get(d)
                if hv and hv > 0:
                    expect = hv / hlatest
                    if expect > 0:
                        qian_self_errs.append(abs(qv - expect) / expect)
        dclose = db_close.get(code, {})
        for d, hv in hou.items():
            dc = dclose.get(d)
            if dc and hv and hv > 0:
                # need previous common date for returns
                pass
        days = sorted(set(hou) & set(dclose))
        dbadj = db_adj.get(code, {})
        for d in days:
            da = dbadj.get(d)
            if da and hou[d] > 0:
                level_rel_errs.append(abs(hou[d] - da) / max(abs(da), 1e-9))
        prev = None
        agree = 0
        for d in days:
            if prev is not None and hou[prev] > 0 and dclose[prev] > 0:
                rv = hou[d] / hou[prev] - 1.0
                rd = dclose[d] / dclose[prev] - 1.0
                tot_pairs += 1
                denom = max(abs(rv), abs(rd), 0.002)
                ret_rel_errs.append(abs(rv - rd) / denom)
                absdiff_bps.append(abs(rv - rd) * 10000)
            prev = d
        # vendor-hou continuity on DB ex-div dates (hou-price must not gap)
        for d in db_exdiv.get(code, set()):
            if d in hou:
                exdiv_checked += 1
                i = sorted(hou).index(d) if d in hou else -1
                hh = sorted(hou)
                j = hh.index(d)
                if j > 0 and hou[hh[j - 1]] > 0:
                    if abs(hou[d] / hou[hh[j - 1]] - 1.0) < 0.05:
                        exdiv_continuous += 1
        per_stock.append({"ts": code, "common_days": len(days),
                          "qian_neg": qn, "qian_bad": qb})

    def _pct(vals: list[float], q: float) -> float:
        if not vals:
            return 0.0
        s = sorted(vals)
        return round(s[min(len(s) - 1, int(len(s) * q / 100))] * 100, 3)

    def _bps(vals: list[float], q: float) -> float:
        if not vals:
            return 0.0
        s = sorted(vals)
        return round(s[min(len(s) - 1, int(len(s) * q / 100))], 1)

    print(f"LEVEL vendor-hou vs db-adj: n={len(level_rel_errs)} median={_pct(level_rel_errs,50)}% p90={_pct(level_rel_errs,90)}% p99={_pct(level_rel_errs,99)}%")
    print(f"return-agreement pairs: {tot_pairs}")
    print(f"ret absdiff: median={_bps(absdiff_bps,50)}bps p90={_bps(absdiff_bps,90)}bps")
    print(f"ret relerr: median={_pct(ret_rel_errs,50)}% p90={_pct(ret_rel_errs,90)}% p99={_pct(ret_rel_errs,99)}%")
    print(f"qian self-consistency relerr: n={len(qian_self_errs)} median={_pct(qian_self_errs,50)}% p90={_pct(qian_self_errs,90)}%")
    print(f"ex-div continuity: {exdiv_continuous}/{exdiv_checked} DB ex-div dates without vendor-hou gap")
    print(f"vendor qian negatives: {qian_neg} rows, implausible(<=0|>2): {qian_bad} rows")
    if args.save_report:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        path = REPORT_DIR / args.report_name
        path.write_text(json.dumps({
            "tag": "vendor-adj-compare-2026-09-05",
            "rule": "report only, no fixes",
            "finding": "vendor hou-file is a hou-PRICE series (mislabeled factor); vendor qian is hou/hou_latest with broken ancient history",
            "sample": len(codes),
            "return_pairs": tot_pairs,
            "level_relerr_median_pct": _pct(level_rel_errs, 50),
            "level_relerr_p90_pct": _pct(level_rel_errs, 90),
            "level_relerr_p99_pct": _pct(level_rel_errs, 99),
            "level_n": len(level_rel_errs),
            "ret_absdiff_median_bps": _bps(absdiff_bps, 50),
            "ret_absdiff_p90_bps": _bps(absdiff_bps, 90),
            "ret_relerr_median_pct": _pct(ret_rel_errs, 50),
            "ret_relerr_p90_pct": _pct(ret_rel_errs, 90),
            "ret_relerr_p99_pct": _pct(ret_rel_errs, 99),
            "qian_self_median_pct": _pct(qian_self_errs, 50),
            "qian_self_p90_pct": _pct(qian_self_errs, 90),
            "exdiv_continuous": exdiv_continuous,
            "exdiv_checked": exdiv_checked,
            "qian_neg_rows": qian_neg, "qian_bad_rows": qian_bad,
            "per_stock": per_stock,
            "as_of": datetime.now(UTC).isoformat(timespec="seconds"),
        }, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"saved {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
