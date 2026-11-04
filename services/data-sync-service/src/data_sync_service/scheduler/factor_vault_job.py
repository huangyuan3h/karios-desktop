"""Factor-vault daily refresh (read-only computation, no DB writes).

Weekdays 19:10 Asia/Shanghai (after the EOD close chain): recompute
data/backtest_reports/factor_vault.json + factor_vault_history.json from the
frozen audit inputs (h2f_trades + h2d blotters + embedded FACTOR_DEFS).
Pure file computation — never touches live strategy params, orders, or the
broker. Fail-open: logs and keeps yesterday's file on error.

2026-10-25 MVP: the run is recorded in sync_job_record (job_failed Bark on
failure via HIGH_JOB_TYPES) and status upgrades (cold->watch, *->revived)
emit webhook events + append to factor_vault_transitions.json for the Hub.
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

logger = logging.getLogger(__name__)

JOB_ID = "factor_vault_refresh"
CRON_EXPRESSION = "10 19 * * mon-fri"
TIMEZONE = "Asia/Shanghai"

TRANSITIONS_FILENAME = "factor_vault_transitions.json"
TRANSITIONS_MAX_ENTRIES = 500
TRANSITIONS_MAX_AGE_DAYS = 45


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def _reports_dir() -> Path:
    return Path(__file__).resolve().parents[3] / "data" / "backtest_reports"


def _load_prev_states(reports_dir: Path) -> dict[str, str]:
    """Yesterday's {factor_id: status} from the history file (fail-open)."""
    try:
        raw = json.loads((reports_dir / "factor_vault_history.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    hist = raw.get("history", []) if isinstance(raw, dict) else raw
    if not isinstance(hist, list) or not hist:
        return {}
    states = (hist[-1] or {}).get("states") or {}
    return {str(k): str(v) for k, v in states.items() if k and v}


def append_transitions(
    reports_dir: Path, new_entries: list[dict[str, Any]], today_iso: str
) -> None:
    """Append transition entries, prune to a short rolling list (fail-open)."""
    if not new_entries:
        return
    path = reports_dir / TRANSITIONS_FILENAME
    try:
        raw = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    except (OSError, ValueError):
        raw = {}
    existing = raw.get("transitions", []) if isinstance(raw, dict) else []
    if not isinstance(existing, list):
        existing = []
    try:
        cutoff = date.fromisoformat(today_iso).toordinal() - TRANSITIONS_MAX_AGE_DAYS
    except ValueError:
        cutoff = 0
    kept = []
    for e in existing + list(new_entries):
        if not isinstance(e, dict):
            continue
        try:
            if date.fromisoformat(str(e.get("date") or "1970-01-01")).toordinal() < cutoff:
                continue
        except ValueError:
            continue
        kept.append(e)
    kept = kept[-TRANSITIONS_MAX_ENTRIES:]
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"transitions": kept}, ensure_ascii=False), encoding="utf-8")
    except OSError:
        logger.warning("factor_vault_refresh: could not write %s", TRANSITIONS_FILENAME)


def run() -> None:
    from data_sync_service.scheduler._job_guard import record_failure, record_success
    from data_sync_service.service import factor_vault as fv

    reports_dir = _reports_dir()
    prev_states = _load_prev_states(reports_dir)
    today_iso = date.today().isoformat()
    script = Path(__file__).resolve().parents[3] / "scripts" / "generate_factor_vault.py"
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
        logger.warning("factor_vault_refresh error: %s", exc)
        return
    if proc.returncode != 0:
        record_failure(JOB_ID, proc.stderr[-500:] or "generate_factor_vault.py failed")
        logger.warning("factor_vault_refresh failed: %s", proc.stderr[-500:])
        return
    logger.info("factor_vault_refresh ok: %s", proc.stdout.strip()[-300:])

    # Transition detect + emit (all fail-open; the refresh itself succeeded).
    try:
        vault = json.loads((reports_dir / "factor_vault.json").read_text(encoding="utf-8"))
        factors = vault.get("factors", []) if isinstance(vault, dict) else []
        transitions = fv.diff_factor_transitions(prev_states, factors, today_iso)
    except (OSError, ValueError) as exc:
        logger.warning("factor_vault_refresh: transition diff skipped: %s", exc)
        record_success(JOB_ID)
        return
    if transitions:
        from data_sync_service.db.webhook import emit_event

        for t in transitions:
            try:
                emit_event(
                    str(t["event_type"]),
                    {
                        "factor_id": t["factor_id"],
                        "name": t.get("name"),
                        "from": t.get("from"),
                        "to": t.get("to"),
                        "percentile": t.get("percentile"),
                        "net": t.get("net"),
                        "date": t.get("date"),
                    },
                    dedupe_key=str(t["dedupe_key"]),
                )
            except Exception:  # noqa: BLE001 - one bad emit must not block the rest
                logger.warning("factor_vault_refresh: emit failed for %s", t.get("factor_id"))
        append_transitions(reports_dir, transitions, today_iso)
        logger.info("factor_vault_refresh: %d transition(s) emitted", len(transitions))
    record_success(JOB_ID)
