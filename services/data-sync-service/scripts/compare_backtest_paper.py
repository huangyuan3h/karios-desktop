#!/usr/bin/env python3
"""Reconcile the REAL paper book against the backtest on a given trading day.

The user's core concern (2026-08-11): the backtest cannot be blindly trusted
to replicate in the real world. This tool answers, for one day:

  - what the backtest (S-3 CN + HK lines, valid window) says we SHOULD hold
    at that day's close  (engine end-of-day holding snapshots)
  - what the paper book ACTUALLY holds that day (paper_trades open rows)
  - the diff, grouped by market, with entry-date alignment checks

Run weekly (e.g. every Monday for last Friday) and feed the report to the
decision agent / weekly review. Only regimes (traffic lights) drive the
allocation (see service/allocation.py R5c) — no lookahead inputs anywhere.

Usage:
  PYTHONPATH=src python3 scripts/compare_backtest_paper.py              # last Friday
  PYTHONPATH=src python3 scripts/compare_backtest_paper.py --date 2026-08-07
  PYTHONPATH=src python3 scripts/compare_backtest_paper.py --windows OOS2,train
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from run_walk_forward import HK_S3_CONFIG, S3_CONFIG, WINDOWS  # noqa: E402

from data_sync_service.db.paper_trading import list_paper_trades  # noqa: E402
from data_sync_service.service.backtest_engine import (  # noqa: E402
    BacktestConfig,
    BacktestData,  # noqa: E402
    simulate,
)


def _mk_config(market: str, start: str, end: str) -> BacktestConfig:
    base = HK_S3_CONFIG if market == "HK" else S3_CONFIG
    return BacktestConfig(start_date=start, end_date=end, **base)


def _entry(row: dict) -> str:
    """paper rows expose entryDate (camelCase via _row_to_dict) — fall back to snake."""
    return str(row.get("entryDate") or row.get("entry_date") or "")


def _close_date(row: dict) -> str:
    """paper rows expose closeDate (camelCase via _row_to_dict) — fall back to snake."""
    return str(row.get("closeDate") or row.get("close_date") or "")


def _paper_holdings_on(day: str) -> dict[str, dict]:
    """symbol -> row for paper trades open on `day` (status=open or closed after day)."""
    out: dict[str, dict] = {}
    for row in list_paper_trades():
        if row.get("status") == "open" and _entry(row) <= day:
            out[str(row.get("symbol"))] = row
        elif (
            row.get("status") == "closed"
            and _entry(row) <= day
            and (not _close_date(row) or _close_date(row) > day)
        ):
            out[str(row.get("symbol"))] = row
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", help="Trading day to reconcile (default: last Friday)")
    ap.add_argument("--end", help="Custom window end date (default: window end; use today for live reconciliation)")
    ap.add_argument("--windows", default="valid", help="Comma-separated windows to check (default valid)")
    args = ap.parse_args()

    day = args.date
    if not day:
        d = date.today()
        while d.weekday() != 4:
            d -= timedelta(days=1)
        day = d.isoformat()

    windows = [w.strip() for w in args.windows.split(",") if w.strip()]
    missing = [w for w in windows if w not in WINDOWS]
    if missing:
        print(f"ERROR: unknown windows {missing} (valid: {list(WINDOWS)})", file=sys.stderr)
        return 2

    paper = _paper_holdings_on(day)
    print(f"== 对账日 {day} ==")
    print(f"paper 当日持仓: {len(paper)} 只")

    for wname in windows:
        start, end = WINDOWS[wname]
        if args.end:
            end = max(end, args.end)
        if not (start <= day <= end):
            print(f"\n[{wname}] {day} 不在窗口 {start}..{end} 内 — 跳过")
            continue
        print(f"\n[{wname} {start}..{end}]")
        for market in ("CN", "HK"):
            cfg = _mk_config(market, start, end)
            data = BacktestData(cfg)
            run = simulate(cfg, data=data)
            snap = next((s for s in run.positions_by_day if s["date"] == day), None)
            if snap is None:
                print(f"  {market}: 无 {day} 快照（非交易日？）")
                continue
            expect = {p["symbol"]: p for p in snap["positions"]}
            actual = {k: v for k, v in paper.items() if str(v.get("market") or "CN") == market}
            missing_h = sorted(set(expect) - set(actual))
            extra = sorted(set(actual) - set(expect))
            aligned = sorted(set(expect) & set(actual))
            print(f"  {market}: 回测应持有 {len(expect)} / paper 实持 {len(actual)}")
            if aligned:
                print(f"    一致 {len(aligned)} 只")
                for s in aligned:
                    e, a = expect[s], actual[s]
                    flag = "" if str(e["entry_date"]) == str(a.get("entryDate")) else " (入场日不同)"
                    print(f"      {s}  entry={e['entry_date']}  score={e.get('score_at_entry')}{flag}")
            if missing_h:
                print(f"    ⚠ 回测应持有但 paper 没有（{len(missing_h)}）:")
                for s in missing_h:
                    p = expect[s]
                    print(f"      {s}  entry={p['entry_date']} score={p.get('score_at_entry')} pos={p.get('position_pct')}")
            if extra:
                print(f"    ! paper 持有但回测没有（{len(extra)}）:")
                for s in extra:
                    a = actual[s]
                    print(f"      {s}  entry={a.get('entryDate')} source={a.get('source')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
