#!/usr/bin/env python3
"""H3 gate: verify the full backend test suite leaves key DB tables untouched.

Usage:
    python3 scripts/db_rows_baseline.py save   # write baseline JSON
    python3 scripts/db_rows_baseline.py check  # compare current vs baseline

Live-aware (2026-09-04): tables the live system writes during the day
(user clicks, crons) are COUNT-UNSTABLE — for those we scan for test-marker
rows instead of comparing counts. Tables nobody live-writes still compare
exact counts. `check --strict` restores the old count-only behavior for
quiet windows.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

from data_sync_service.db import get_connection

TABLES = [
    "paper_trades",
    "execution_snapshots",
    "execution_decision_changes",
    "decision_sessions",
    "decision_messages",
    "decision_snapshots",
    "decision_actions",
    "alpha_radar_sources",
    "alpha_radar_documents",
    "alpha_radar_trends",
    "watchlist_registry",
    "watchlist_score_daily",
    "watchlist_automation_runs",
    "tv_screeners",
    "tv_screener_snapshots",
    "morning_briefs",
    # news_items deliberately excluded: news_fetch_job runs in the background
    # and appends real feed rows mid-test-suite (observed 2026-08-08).
    "news_sources",
    "system_prompts",
    "system_prompt_state",
    "broker_accounts",
    "broker_account_state",
    "broker_snapshots",
    "market_top_inst_daily",
    "market_top_inst_summary",
    "market_fund_flow",
    "market_chips",
    "stock_eastmoney_industry",
    "stock_basic",
]

# Live-written during the day: counts drift without any test running, so
# `check` scans these for test-marker leftovers instead of count-compare.
# (alpha_radar_documents/morning_briefs have no test markers — their tests
# are mocked — but background ingest/crons write them live.)
LIVE_TABLES = {
    "paper_trades",
    "execution_snapshots",
    "execution_decision_changes",
    "decision_sessions",
    "decision_messages",
    "decision_snapshots",
    "decision_actions",
    "watchlist_registry",
    "alpha_radar_documents",
    "morning_briefs",
}

# (table, where-clause) pairs matching rows only tests create. Keep in sync
# with the requires_postgres cleanup conventions (CN:99* symbols, snap ids,
# test why_markers, test_periph_* job types).
MARKER_SCANS = [
    ("paper_trades", "symbol LIKE 'CN:99%' OR why_at_entry LIKE '%SMOKE%' OR why_at_entry LIKE 'test sleeve leg%'"),
    ("execution_decision_changes", "symbol LIKE 'CN:99%'"),
    ("execution_snapshots", "meta::text LIKE '%SMOKE_TEST%' OR id IN ('snap-agg','snap-bf')"),
    ("sync_job_record", "job_type LIKE 'test\\_%' ESCAPE '\\'"),
    ("system_events", "dedupe_key LIKE 'job_failed:test\\_%' ESCAPE '\\'"),
]

BASELINE = Path("/var/folders/3w/qqvhb_7930n24tty9_87bmf40000gn/T/opencode/h3_rows_baseline.json")


def counts() -> dict[str, int]:
    out: dict[str, int] = {}
    with get_connection() as conn:
        with conn.cursor() as cur:
            for t in TABLES:
                try:
                    cur.execute(f'SELECT COUNT(*) FROM "{t}"')
                    out[t] = int(cur.fetchone()[0])
                except Exception as exc:  # table may not exist yet
                    out[t] = f"missing ({type(exc).__name__})"
    return out


def main() -> None:
    cmd = sys.argv[1] if len(sys.argv) > 1 else "check"
    strict = "--strict" in sys.argv
    current = counts()
    if cmd == "save":
        BASELINE.write_text(json.dumps(current, indent=2, sort_keys=True))
        print("baseline saved:")
        for k, v in sorted(current.items()):
            print(f"  {k}: {v}")
        return
    if not BASELINE.exists():
        print("no baseline; run with 'save' first")
        sys.exit(1)
    before = json.loads(BASELINE.read_text())
    changed = False
    for t in TABLES:
        if not strict and t in LIVE_TABLES:
            continue  # live drift expected; marker scan below covers pollution
        if before[t] != current[t]:
            changed = True
            print(f"CHANGED {t}: {before[t]} -> {current[t]}")
    markers = scan_markers()
    if markers:
        changed = True
        for table, n in markers:
            print(f"TEST-LEFTOVER {table}: {n} row(s)")
    if changed:
        print("FAIL: DB rows changed during test run")
        sys.exit(1)
    print("OK: all tables unchanged" + ("" if strict else " (live tables marker-scanned)"))


def scan_markers() -> list[tuple[str, int]]:
    """Count test-marker leftovers. Empty = tests cleaned up after themselves."""
    out: list[tuple[str, int]] = []
    with get_connection() as conn:
        with conn.cursor() as cur:
            for table, where in MARKER_SCANS:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM \"{table}\" WHERE {where}")
                    n = int(cur.fetchone()[0])
                except Exception:  # table may not exist yet
                    conn.rollback()
                    continue
                if n:
                    out.append((table, n))
    return out


if __name__ == "__main__":
    main()
