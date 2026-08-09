"""Data-source health check (剩余风险 ②: 数据源健康告警).

Checks, against the local dev database and filesystem:
  1. scheduler: every known sync job's latest run (success + freshness)
  2. daily quotes: max(trade_date) vs latest open trading day (trade_calendar)
  3. TV screener snapshots: freshness (workdays expected AM/PM)
  4. EM industry coverage: missing CN stocks ratio
  5. watchlist TrendOK scores: freshness vs latest open trading day
  6. DB backup age: newest ~/.karios/backups/postgres/*.dump mtime

Exit codes: 0 = all ok, 1 = warnings only, 2 = at least one failure.
Usage:
  python3 scripts/data_healthcheck.py            # stdout report
  python3 scripts/data_healthcheck.py --notify   # + macOS notification on FAIL
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from data_sync_service.api.sync_routes import SYNC_JOB_TYPES  # noqa: E402
from data_sync_service.db import get_connection  # noqa: E402

BACKUP_DIR = Path.home() / ".karios" / "backups" / "postgres"
SHANGHAI_OFFSET = timedelta(hours=8)

# Jobs whose cron was redirected to a live job (close_sync); their stale
# failure records are historical noise — real freshness is covered by the
# daily_freshness check.
DEPRECATED_JOB_TYPES = frozenset({"stock_daily_full"})

# These jobs are expected to run frequently and are the most signal-critical.
CRITICAL_JOB_FRESH_DAYS: dict[str, int] = {
    "stock_close_sync": 7,
    "stock_daily_full": 7,
    "index_daily_full": 7,
    "macro_daily_full": 7,
    "etf_daily_full": 7,
    "hk_daily_full": 7,
    "watchlist_automation": 7,
    "morning_brief_am": 2,
    "decision_snapshot": 2,
    "alpha_radar_pipeline": 7,
    "eastmoney_industry_sync": 7,
    "cn_industry_post_close_sync": 7,
}
# Every other known job must at least have *a* successful run on record.
OTHER_JOB_MAX_AGE_DAYS = 14


def _now_sh() -> datetime:
    return datetime.now(UTC) + SHANGHAI_OFFSET


def _ok(msg: str) -> dict[str, Any]:
    return {"status": "ok", "message": msg}


def _warn(msg: str) -> dict[str, Any]:
    return {"status": "warn", "message": msg}


def _fail(msg: str) -> dict[str, Any]:
    return {"status": "fail", "message": msg}


def check_scheduler_jobs() -> dict[str, Any]:
    fails: list[str] = []
    warns: list[str] = []
    with get_connection() as conn:
        with conn.cursor() as cur:
            for job in SYNC_JOB_TYPES:
                cur.execute(
                    """
                    SELECT sync_at, success, error_message FROM sync_job_record
                    WHERE job_type = %s ORDER BY sync_at DESC LIMIT 1
                    """,
                    (job,),
                )
                row = cur.fetchone()
                fresh_days = CRITICAL_JOB_FRESH_DAYS.get(job, OTHER_JOB_MAX_AGE_DAYS)
                if row is None:
                    warns.append(f"{job}: no run on record")
                    continue
                sync_at, success, err = row
                if job in DEPRECATED_JOB_TYPES:
                    continue
                age_days = (_now_sh() - (sync_at + SHANGHAI_OFFSET)).days
                if not success:
                    fails.append(f"{job}: last run failed: {(err or '')[:120]}")
                elif age_days > fresh_days:
                    warns.append(f"{job}: last success {age_days}d ago")
    if fails:
        return _fail("; ".join(fails))
    if warns:
        return _warn("; ".join(warns))
    return _ok(f"{len(SYNC_JOB_TYPES)} jobs all green")


def _latest_open_trading_day(cur: Any) -> str | None:
    cur.execute(
        """
        SELECT cal_date FROM trade_calendar
        WHERE exchange = 'SSE' AND is_open = 1 AND cal_date <= %s
        ORDER BY cal_date DESC LIMIT 1
        """,
        (_now_sh().date(),),
    )
    row = cur.fetchone()
    return str(row[0]) if row else None


def check_daily_freshness() -> dict[str, Any]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            latest = _latest_open_trading_day(cur)
            cur.execute("SELECT MAX(trade_date) FROM daily")
            max_date = cur.fetchone()[0]
    if latest is None:
        return _warn("no trading day in trade_calendar")
    if max_date is None:
        return _fail("daily table empty")
    if str(max_date) < latest:
        return _fail(f"daily max trade_date {max_date} < latest open day {latest}")
    return _ok(f"daily up to {max_date}")


def check_tv_snapshots() -> dict[str, Any]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(captured_at) FROM tv_screener_snapshots")
            latest = cur.fetchone()[0]
    if latest is None:
        return _warn("no tv_screener_snapshots yet")
    if isinstance(latest, str):
        latest = datetime.fromisoformat(latest.replace("Z", "+00:00"))
    age_hours = (_now_sh() - (latest + SHANGHAI_OFFSET)).total_seconds() / 3600
    if age_hours > 48:
        return _fail(f"tv snapshots stale ({age_hours:.0f}h)")
    if age_hours > 12:
        return _warn(f"tv snapshots stale ({age_hours:.0f}h)")
    return _ok(f"tv snapshots {age_hours:.0f}h fresh")


def check_em_coverage() -> dict[str, Any]:
    from data_sync_service.db.stock_eastmoney_industry import coverage_stats

    stats = coverage_stats()
    total = stats["totalCnStocks"]
    missing = stats["missingCount"]
    if total == 0:
        return _fail("stock_basic empty")
    ratio = 100.0 * missing / total
    if ratio > 5:
        return _fail(f"EM industry missing {missing}/{total} ({ratio:.1f}%)")
    if ratio > 1:
        return _warn(f"EM industry missing {missing}/{total} ({ratio:.1f}%)")
    return _ok(f"EM industry {total - missing}/{total} mapped")


def check_score_freshness() -> dict[str, Any]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            latest = _latest_open_trading_day(cur)
            cur.execute("SELECT MAX(trade_date) FROM watchlist_score_daily")
            max_date = cur.fetchone()[0]
    if latest is None:
        return _warn("no trading day in trade_calendar")
    if max_date is None:
        return _fail("watchlist_score_daily empty")
    if str(max_date) < latest:
        return _fail(f"scores max {max_date} < latest open day {latest}")
    return _ok(f"scores up to {max_date}")


def check_backup_age() -> dict[str, Any]:
    if not BACKUP_DIR.exists():
        return _fail(f"backup dir missing: {BACKUP_DIR}")
    dumps = sorted(BACKUP_DIR.glob("karios-*.dump"))
    if not dumps:
        return _fail("no backup dumps found")
    mtime = datetime.fromtimestamp(dumps[-1].stat().st_mtime, tz=UTC)
    age_hours = (_now_sh() - (mtime + SHANGHAI_OFFSET)).total_seconds() / 3600
    # db_backup.sh skips when the newest dump is < 25h old (sleep safety net),
    # so the max legit gap is ~49h; FAIL only beyond that (50h).
    if age_hours > 50:
        return _fail(f"backup stale ({age_hours:.0f}h, {dumps[-1].name})")
    return _ok(f"backup {age_hours:.0f}h ago ({dumps[-1].name})")


CHECKS = [
    ("scheduler_jobs", check_scheduler_jobs),
    ("daily_freshness", check_daily_freshness),
    ("tv_snapshots", check_tv_snapshots),
    ("em_coverage", check_em_coverage),
    ("score_freshness", check_score_freshness),
    ("backup_age", check_backup_age),
]

_STATUS_RANK = {"ok": 0, "warn": 1, "fail": 2}


def run() -> int:
    worst = 0
    lines: list[str] = []
    for name, fn in CHECKS:
        try:
            result = fn()
        except Exception as exc:  # noqa: BLE001 - one check failing must not kill the report
            result = _fail(f"check crashed: {exc}")
        lines.append(f"[{result['status'].upper():4s}] {name}: {result['message']}")
        worst = max(worst, _STATUS_RANK[result["status"]])
    for line in lines:
        print(line)
    return worst


def main() -> None:
    parser = argparse.ArgumentParser(description="Karios data-source health check")
    parser.add_argument("--notify", action="store_true", help="macOS notification on FAIL")
    args = parser.parse_args()

    code = run()
    if args.notify and code >= 2:
        try:
            subprocess.run(
                [
                    "osascript",
                    "-e",
                    'display notification "Data health FAIL — see console output" '
                    'with title "Karios Health Check"',
                ],
                check=False,
                capture_output=True,
            )
        except OSError:
            pass
    sys.exit(code)


if __name__ == "__main__":
    main()
