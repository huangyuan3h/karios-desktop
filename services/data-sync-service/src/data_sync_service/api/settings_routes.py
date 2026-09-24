"""Cross-layer settings API (OPT-223, 2026-09-17).

The UI is the source of truth for the selected strategy mode (localStorage);
it mirrors the value here so background pushes (Bark/notifications) can be
composed for the strategy the user is watching. State-changing PUTs are
covered by ``LocalOriginGuardMiddleware`` like every other local endpoint.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from data_sync_service.db import app_settings

router = APIRouter(prefix="/settings", tags=["settings"])

STRATEGY_MODE_KEY = "strategy_mode"
STRATEGY_MODES = (
    "harbor",
    "homeport",
    "starport",
    "starship",
    "starship_robust",
    "starship_b",
    "twin_star",
)
# User decision 2026-09-24 (updated): 星舰 B is the default research/display
# mode (shallower DD, stronger stress, 3-ETF parking that is easier to
# replicate). 稳健星舰 H2-a25 stays selectable. The selected mode still does
# not control Live order routing (Live = 港湾).
DEFAULT_STRATEGY_MODE = "starship_b"


class StrategyModeBody(BaseModel):
    mode: str


@router.get("/strategy-mode")
def get_strategy_mode() -> dict[str, Any]:
    return {"ok": True, "mode": app_settings.get_setting(STRATEGY_MODE_KEY, DEFAULT_STRATEGY_MODE)}


@router.put("/strategy-mode")
def put_strategy_mode(body: StrategyModeBody) -> dict[str, Any]:
    if body.mode not in STRATEGY_MODES:
        raise HTTPException(status_code=422, detail=f"mode must be one of {list(STRATEGY_MODES)}")
    app_settings.set_setting(STRATEGY_MODE_KEY, body.mode)
    return {"ok": True, "mode": body.mode}


def selected_strategy_mode() -> str:
    """Mode for background copy; falls back to the UI default (星舰稳健版)."""
    mode = app_settings.get_setting(STRATEGY_MODE_KEY, DEFAULT_STRATEGY_MODE)
    return str(mode) if mode in STRATEGY_MODES else DEFAULT_STRATEGY_MODE
