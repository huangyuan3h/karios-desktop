#!/usr/bin/env python3
"""Replay the S-3 backtest engine's exact trade list into the paper book.

User decision (2026-08-14): the backtest is the source of truth — the paper
book must mirror the engine's trajectory exactly, so paper results are
directly comparable to the backtest numbers (and C4 twin-trade gaps are
eliminated by construction).

What this does:
  1. Runs simulate() over [start, end] for the chosen market using the
     S-3/HK-S-3 baseline configs (same code paths as run_walk_forward).
  2. Rebuilds paper_trades rows with source='S3' / 'S3HK' ONLY:
       - engine trades with a real close reason  -> insert open + close
       - engine trades closed 'end_of_window'    -> insert open only
         (the window end is an artifact; the engine would keep holding)
     Everything else in the paper book (TV / ALPHA / MANUAL) is untouched.
  3. Sleeve = engine position_pct (0.10), score/why copied from the engine.

Usage:
  PYTHONPATH=src python3 scripts/mirror_backtest_to_paper.py --market HK
  PYTHONPATH=src python3 scripts/mirror_backtest_to_paper.py --market CN --start 2026-08-03
  # --backup writes the pre-rebuild S3* rows to data/paper_backup_<ts>.json
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from run_walk_forward import HK_S3_CONFIG, S3_CONFIG  # noqa: E402

from data_sync_service.db.paper_trading import (  # noqa: E402
    SOURCE_S3,
    SOURCE_S3_HK,
    close_paper_trade,
    insert_paper_trade,
    list_paper_trades,
)
from data_sync_service.service.backtest_engine import (  # noqa: E402
    BacktestConfig,
    BacktestData,
    simulate,
)

END_OF_WINDOW = "end_of_window"


def _delete_source_rows(source: str) -> None:
    from data_sync_service.db import get_connection

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM paper_trades WHERE source = %s", (source,))
        conn.commit()


def _backup(source: str, out: Path) -> None:
    rows = [r for r in list_paper_trades(limit=10000) if r.get("source") == source]
    if rows:
        out.write_text(json.dumps(rows, ensure_ascii=False, indent=2, default=str))
        print(f"backup: {len(rows)} rows -> {out}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--market", default="HK", choices=("CN", "HK"))
    ap.add_argument("--start", default="2026-08-03", help="Engine window start (default 2026-08-03)")
    ap.add_argument("--end", help="Engine window end (default today)")
    ap.add_argument("--no-backup", action="store_true")
    args = ap.parse_args()

    end = args.end or date.today().isoformat()
    base = HK_S3_CONFIG if args.market == "HK" else S3_CONFIG
    source = SOURCE_S3_HK if args.market == "HK" else SOURCE_S3

    cfg = BacktestConfig(start_date=args.start, end_date=end, **base)
    run = simulate(cfg, data=BacktestData(cfg))
    if not run.positions_by_day:
        print("no engine days — abort")
        return 1

    last_snapshot = {p["symbol"]: p for p in run.positions_by_day[-1]["positions"]}
    print(f"== engine {args.market} {args.start}..{end} ==")
    print(f"closed trades: {len(run.trades)} · final holding: {len(last_snapshot)}")

    if not args.no_backup:
        out = Path(__file__).resolve().parents[1] / "data" / f"paper_backup_{source}_{date.today().isoformat()}.json"
        _backup(source, out)

    _delete_source_rows(source)
    inserted = closed = skipped = 0

    # 1) Engine trades (both closed and window-end leftovers).
    for t in run.trades:
        row = insert_paper_trade(
            symbol=t.symbol,
            entry_date=t.entry_date,
            side="BUY",
            entry_price=t.entry_price,
            score_at_entry=t.score_at_entry,
            why_at_entry=f"S-3 engine mirror ({t.close_reason})",
            sleeve_pct=t.position_pct or base.get("position_pct", 0.10),
            source=source,
            market=args.market,
        )
        if row is None:
            skipped += 1
            continue
        inserted += 1
        if t.close_reason != END_OF_WINDOW:
            closed += close_paper_trade(
                trade_id=row["id"],
                close_date=t.close_date,
                close_price=t.close_price,
                pnl_pct=t.pnl_pct,
                holding_days=t.holding_days,
                close_reason=t.close_reason,
                gross_pnl_pct=t.gross_pnl_pct,
                costs_pct=t.costs_pct,
            ) is not None

    # 2) Open positions the engine still holds at the window end (their
    #    'end_of_window' trades are in run.trades, but that close is an
    #    artifact — keep them open).
    print(f"inserted {inserted} · closed {closed} · skipped {skipped}")
    open_syms = sorted(last_snapshot)
    print(f"final open {len(open_syms)}:")
    for s in open_syms:
        print(f"  {s} entry={last_snapshot[s]['entry_date']} score={last_snapshot[s].get('score_at_entry')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
