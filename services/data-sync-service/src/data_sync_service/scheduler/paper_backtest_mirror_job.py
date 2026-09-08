"""Mirror the S-3 backtest engine trajectory into the paper book (daily).

User decision (2026-08-14): the backtest is the source of truth — the paper
book re-plays the engine's exact trades every day (see
scripts/mirror_backtest_to_paper.py). Running after hk_daily_full_sync
(17:30 Asia/Shanghai) so today's HK bars are settled before the replay.

Window start is fixed (2026-08-03, HK line inception) — the engine is
steady-state, so the final holding is independent of the start date
(verified 2026-08-14: 5/13 and 3/01 starts both give the same 8/13 book).

CN line is NOT mirrored here: the CN S-3 paper book mirrors the engine only
if the engine holds CN positions (regime/flow-gated), and the CN paper rows
are TV/ALPHA-sourced — replay would overwrite those.
"""

from __future__ import annotations

import logging
import subprocess
import sys
from pathlib import Path

from apscheduler.triggers.cron import CronTrigger  # type: ignore[import-not-found]

from data_sync_service.db.sync_job_record import insert_record

logger = logging.getLogger(__name__)

JOB_ID = "paper_backtest_mirror"
# Daily 18:05 Asia/Shanghai — 35min after hk_daily_full_sync (17:30).
CRON_EXPRESSION = "5 18 * * *"
TIMEZONE = "Asia/Shanghai"

SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "mirror_backtest_to_paper.py"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def _resolve_python() -> str:
    """Pick an interpreter that can actually import the service deps.

    The repo venv is canonical but can go stale (2026-09-06: it lacked
    psycopg_pool added in OPT-125, mirror failed daily); sys.executable
    can be a bare or versioned (python3.12) system interpreter (2026-08-14:
    exit=2). Probe the import and take the first working candidate so env
    drift degrades to the other interpreter instead of paging Bark.
    """
    candidates = [
        Path(__file__).resolve().parents[3] / ".venv" / "bin" / "python",
        Path(sys.executable),
    ]
    seen: set[str] = set()
    for cand in candidates:
        s = str(cand)
        if s in seen or not cand.exists():
            continue
        seen.add(s)
        try:
            probe = subprocess.run(
                [s, "-c", "import psycopg_pool"],
                capture_output=True,
                timeout=30,
            )
        except Exception:  # noqa: BLE001
            continue
        if probe.returncode == 0:
            return s
    return str(candidates[-1])


def run() -> None:
    venv_python = _resolve_python()
    try:
        proc = subprocess.run(
            [str(venv_python), str(SCRIPT), "--market", "HK"],
            capture_output=True,
            text=True,
            timeout=600,
        )
    except Exception as exc:  # noqa: BLE001
        insert_record(JOB_ID, success=False, error_message=f"mirror spawn failed: {exc}")
        logger.warning("paper_backtest_mirror spawn failed: %s", exc)
        return
    if proc.returncode != 0:
        insert_record(
            JOB_ID, success=False,
            error_message=f"exit={proc.returncode}: {proc.stderr.strip()[-400:]}",
        )
        logger.warning("paper_backtest_mirror failed: %s", proc.stderr.strip()[-400:])
        return
    insert_record(JOB_ID, success=True)
    logger.info("paper_backtest_mirror ok: %s", proc.stdout.strip().splitlines()[0] if proc.stdout.strip() else "")
