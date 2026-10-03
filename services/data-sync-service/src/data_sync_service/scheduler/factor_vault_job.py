"""Factor-vault daily refresh (read-only computation, no DB writes).

Weekdays 19:10 Asia/Shanghai (after the EOD close chain): recompute
data/backtest_reports/factor_vault.json + factor_vault_history.json from the
frozen audit inputs (h2f_trades + h2d blotters + embedded FACTOR_DEFS).
Pure file computation — never touches live strategy params, orders, or the
broker. Fail-open: logs and keeps yesterday's file on error.
"""

from __future__ import annotations

import logging
import subprocess
import sys
from pathlib import Path

from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger(__name__)

JOB_ID = "factor_vault_refresh"
CRON_EXPRESSION = "10 19 * * mon-fri"
TIMEZONE = "Asia/Shanghai"


def build_trigger() -> CronTrigger:
    return CronTrigger.from_crontab(CRON_EXPRESSION, timezone=TIMEZONE)


def run() -> None:
    script = Path(__file__).resolve().parents[2] / "scripts" / "generate_factor_vault.py"
    try:
        proc = subprocess.run(
            [sys.executable, str(script)],
            capture_output=True,
            text=True,
            timeout=120,
            cwd=str(script.parents[1]),
        )
        if proc.returncode != 0:
            logger.warning("factor_vault_refresh failed: %s", proc.stderr[-500:])
        else:
            logger.info("factor_vault_refresh ok: %s", proc.stdout.strip()[-300:])
    except Exception as exc:  # noqa: BLE001 - display-only daily file
        logger.warning("factor_vault_refresh error: %s", exc)
