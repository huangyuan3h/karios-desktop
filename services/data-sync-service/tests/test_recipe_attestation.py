"""Live recipe attestation (H5): backend constants == shared Zod literals.

The frontend parses the live action payload with TwinStarClip4Schema /
TwinStarHabitSchema (z.literal), so a backend drift already throws loudly in
the UI. This test closes the loop from the other side: it reads the frozen
literals out of packages/shared/src/schemas/twinStar.ts and asserts the
Python engine/report constants equal them. Either side drifting breaks this.

No TS runtime needed — plain regex over the `as const` blocks.
"""

from __future__ import annotations

import re
from pathlib import Path

TS_PATH = (
    Path(__file__).resolve().parents[3] / "packages" / "shared" / "src" / "schemas" / "twinStar.ts"
)


def _block(name: str) -> str:
    text = TS_PATH.read_text(encoding="utf-8")
    m = re.search(r"export const " + re.escape(name) + r" = \{(.*?)\} as const;", text, re.S)
    assert m, f"{name} block not found in twinStar.ts"
    return m.group(1)


def _num(block: str, key: str) -> float:
    m = re.search(rf"{re.escape(key)}\s*:\s*([0-9.]+)", block)
    assert m, f"{key} not found"
    return float(m.group(1))


def _str(block: str, key: str) -> str:
    m = re.search(rf"{re.escape(key)}\s*:\s*'([^']+)'", block)
    assert m, f"{key} not found"
    return m.group(1)


def test_clip4_literals_match_engine() -> None:
    from data_sync_service.service import state_bucket_track as sbt
    from data_sync_service.service import twin_star_daily as tsd

    clip4 = _block("TWIN_STAR_CLIP4")
    assert _num(clip4, "maxPos") == sbt.MAX_POS == 4
    assert _num(clip4, "slotOfSleeve") == sbt.POSITION_PCT == 0.25
    assert _num(clip4, "satSlotNavPct") == tsd.SAT_SLOT_NAV_PCT == 12.5
    assert _num(clip4, "body") == sbt.BODY == 3
    assert _num(clip4, "protectStopPct") == tsd.SAT_PROTECT_STOP_PCT == 0
    assert _num(clip4, "rWideThreshold") == tsd.R_WIDE_THRESHOLD == 0.5
    assert _num(clip4, "bucketQ") == sbt.BUCKET_Q == 3
    # Sleeve split (50/50 when active, 100 core when idle).
    assert _num(clip4, "satSleevePct") == 50
    assert _num(clip4, "coreIdlePct") == 100
    assert _num(clip4, "coreSatActivePct") == 50


def test_habit_literals_match_engine() -> None:
    from data_sync_service.service import twin_star_daily as tsd

    habit = _block("TWIN_STAR_HABIT")
    assert _str(habit, "fillMode") == tsd.HABIT_FILL_MODE == "same_1430"
    assert _str(habit, "fillHhmm") == tsd.HABIT_FILL_HHMM == "1430"
    assert _num(habit, "c1Pct") == tsd.HABIT_C1_PCT == 0.03
    assert _str(habit, "exitHhmm") == tsd.HABIT_EXIT_HHMM == "1430"
    assert _num(habit, "body") == 3


def test_recipe_version_matches_paper_book() -> None:
    from data_sync_service.service import paper_twin_star as pts

    text = TS_PATH.read_text(encoding="utf-8")
    m = re.search(r"export const TWIN_STAR_RECIPE_VERSION = '([^']+)'", text)
    assert m, "TWIN_STAR_RECIPE_VERSION not found in twinStar.ts"
    version = m.group(1)
    assert version == "clip4 v3.1"
    # The paper book records the same recipe family (UI badge + paper rows agree).
    assert pts.HABIT_RECIPE.startswith(version.split(" ")[0])
