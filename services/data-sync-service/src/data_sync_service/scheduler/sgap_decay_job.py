"""S-gap decay daily refresh (read-only computation, no DB writes).

Weekdays 19:15 Asia/Shanghai (right after factor_vault_refresh 19:10):
recompute data/backtest_reports/sgap_decay.json from the frozen audit input
(h2f_trades.json). Pure file computation — never touches live strategy
params, orders, or the broker. Fail-open: logs and keeps yesterday's file
on error.

2026-10-25 MVP: the run is recorded in sync_job_record (job_failed Bark on
failure via HIGH_JOB_TYPES). When K2 ``revival_pos`` steps up (0->10 or
10->20), a ``sgap_revival`` webhook event fires and the step is appended to
factor_vault_transitions.json (shared with the factor vault job) for the Hub.
"""

from __future__ import annotations

import json
import logging
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Any

from apscheduler.triggers.cron import CronTrigger

from data_sync_service.scheduler.factor_vault_job import append_transitions

logger = logging.getLogger(__name__)

JOB_ID = "sgap_decay_refresh"
CRON_EXPRESSION = "15 19 * * mon-fri"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def _reports_dir() -> Path:
    return Path(__file__).resolve().parents[3] / "data" / "backtest_reports"


def _read_revival_pos(reports_dir: Path) -> Any:
    """Current K2 revival_pos from sgap_decay.json (None when unreadable)."""
    try:
        decay = json.loads((reports_dir / "sgap_decay.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(decay, dict):
        return None
    pos = (decay.get("summary") or {}).get("revival_pos")
    try:
        return int(pos) if pos is not None else None
    except (TypeError, ValueError):
        return None


def run() -> None:
    from data_sync_service.scheduler._job_guard import record_failure, record_success

    reports_dir = _reports_dir()
    old_pos = _read_revival_pos(reports_dir)
    today_iso = date.today().isoformat()
    script = Path(__file__).resolve().parents[3] / "scripts" / "generate_sgap_decay.py"
    try:
        proc = subprocess.run(
            [sys.executable, str(script)],
            capture_output=True,
            text=True,
            timeout=120,
            cwd=str(script.parents[1]),
        )
    except Exception as exc:  # noqa: BLE001 - display-only daily file
        record_failure(JOB_ID, exc)
        logger.warning("sgap_decay_refresh error: %s", exc)
        return
    if proc.returncode != 0:
        # Missing h2f_trades input (or any regenerate failure) must page,
        # not silent-skip: the frozen feed going dark is exactly monitor-dark.
        record_failure(JOB_ID, proc.stderr[-500:] or "generate_sgap_decay.py failed")
        logger.warning("sgap_decay_refresh failed: %s", proc.stderr[-500:])
        return
    logger.info("sgap_decay_refresh ok: %s", proc.stdout.strip()[-300:])

    new_pos = _read_revival_pos(reports_dir)
    if (
        new_pos is not None
        and old_pos is not None
        and isinstance(new_pos, int)
        and isinstance(old_pos, int)
        and new_pos > old_pos
    ):
        entry: dict[str, Any] = {
            "date": today_iso,
            "kind": "sgap",
            "event_type": "sgap_revival",
            "from": old_pos,
            "to": new_pos,
            "dedupe_key": f"sgap_revival:{new_pos}:{today_iso}",
        }
        try:
            from data_sync_service.db.webhook import emit_event

            emit_event(
                "sgap_revival",
                {"old_pos": old_pos, "new_pos": new_pos, "date": today_iso},
                dedupe_key=str(entry["dedupe_key"]),
            )
        except Exception:  # noqa: BLE001 - refresh succeeded; emit is best-effort
            logger.warning("sgap_decay_refresh: sgap_revival emit failed")
        append_transitions(reports_dir, [entry], today_iso)
        logger.info("sgap_decay_refresh: revival_pos %s -> %s emitted", old_pos, new_pos)
    record_success(JOB_ID)
