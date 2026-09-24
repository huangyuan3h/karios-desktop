"""Guard: weekday crons must actually cover Monday-Friday.

Regression for the 2026-09-21 bug: crons written as the standard-cron ``1-5``
(Mon-Fri) were interpreted by APScheduler with ``0 = Monday``, i.e. **Tue-Sat**.
Every Monday job was silently skipped, and the time-sensitive 14:30 satellite
panel never produced a Monday result.

Fix: all weekday crons now use day NAMES (``mon-fri`` / ``mon`` / ``fri`` /
``tue-sat``), which are unambiguous. This test locks that in.
"""

from __future__ import annotations

import importlib
import pkgutil
from datetime import datetime
from zoneinfo import ZoneInfo

from apscheduler.triggers.cron import CronTrigger  # type: ignore[import-not-found]

from data_sync_service import scheduler as scheduler_pkg

TZ = ZoneInfo("Asia/Shanghai")
MONDAY = datetime(2026, 9, 21, 0, 0, tzinfo=TZ)  # 2026-09-21 is a Monday
SATURDAY = datetime(2026, 9, 19, 0, 0, tzinfo=TZ)


def _cron_modules() -> list[tuple[str, object]]:
    out: list[tuple[str, object]] = []
    for info in pkgutil.iter_modules(scheduler_pkg.__path__):
        if info.name.startswith("_"):
            continue
        mod = importlib.import_module(f"data_sync_service.scheduler.{info.name}")
        if isinstance(getattr(mod, "CRON_EXPRESSION", None), str):
            out.append((info.name, mod))
    return out


def test_satellite_panel_fires_on_monday_1430() -> None:
    """The 14:30 panel must fire on a Monday at 14:30 (the 2026-09-21 bug)."""
    from data_sync_service.scheduler import satellite_live_job

    nxt = satellite_live_job.build_trigger().get_next_fire_time(None, MONDAY)
    assert nxt is not None
    assert nxt.strftime("%Y-%m-%d %H:%M") == "2026-09-21 14:30"


def test_no_ambiguous_numeric_day_of_week() -> None:
    """No scheduler cron may use ``1-5``/``2-6`` (APScheduler reads them Tue-Sat)."""
    bad: list[str] = []
    for name, mod in _cron_modules():
        dow = str(mod.CRON_EXPRESSION).split()[4]  # type: ignore[attr-defined]
        if "-" in dow and dow.replace("-", "").isdigit():
            bad.append(f"{name}: {mod.CRON_EXPRESSION}")  # type: ignore[attr-defined]
    assert not bad, f"numeric day-of-week ranges are APScheduler-ambiguous: {bad}"


def test_weekday_crons_cover_monday() -> None:
    """Every ``mon-fri`` trigger must fire on a Monday and skip Saturday."""
    weekday = [(n, m) for n, m in _cron_modules() if "mon-fri" in getattr(m, "CRON_EXPRESSION", "")]
    assert len(weekday) >= 20, f"expected the weekday jobs, found {len(weekday)}"
    for name, mod in weekday:
        tr = mod.build_trigger()  # type: ignore[attr-defined]
        mon = tr.get_next_fire_time(None, MONDAY)
        assert mon is not None and mon.weekday() == 0, f"{name} skips Monday"
        sat = tr.get_next_fire_time(None, SATURDAY)
        assert sat is not None and sat.weekday() != 5, f"{name} should skip Saturday"


def test_named_triggers_are_cron() -> None:
    for _name, mod in _cron_modules():
        assert isinstance(mod.build_trigger(), CronTrigger)  # type: ignore[attr-defined]
