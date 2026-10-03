"""Read-only portfolio tier-plan API (Karios RESTRUCTURE PR1).

GET /portfolio/tier-plan — pure combo_tiers.plan_tier over query params.
No DB writes, no broker/order calls, no strategy-param edits.

Total-assets source chain (first hit wins):
  1. ?total_assets= query param (explicit)
  2. $KARIOS_TOTAL_ASSETS env (real account feed hook; configurable)
  3. frozen DEFAULT_TOTAL_ASSETS (1.2M, PROMPT Sec 3)
Response echoes `total_assets_source` so callers know which one applied.
"""

from __future__ import annotations

import os
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from data_sync_service.service import combo_tiers as ct
from data_sync_service.service import combo_tiers_config as cfg

router = APIRouter(prefix="/portfolio", tags=["portfolio"])


def resolve_total_assets(explicit: float | None) -> tuple[float, str]:
    if explicit is not None:
        if explicit <= 0:
            raise ValueError("total_assets must be positive")
        return (float(explicit), "query")
    env_raw = (os.getenv("KARIOS_TOTAL_ASSETS") or "").strip()
    if env_raw:
        try:
            v = float(env_raw)
        except ValueError as e:
            raise ValueError(f"bad KARIOS_TOTAL_ASSETS={env_raw!r}") from e
        if v <= 0:
            raise ValueError(f"bad KARIOS_TOTAL_ASSETS={env_raw!r}")
        return (v, "env")
    return (float(cfg.DEFAULT_TOTAL_ASSETS), "default")


def _parse_nav(nav_raw: str | None) -> list[float] | None:
    if nav_raw is None or not nav_raw.strip():
        return None
    parts = [p.strip() for p in nav_raw.replace(";", ",").split(",") if p.strip()]
    try:
        return [float(p) for p in parts]
    except ValueError as e:
        raise ValueError("nav_60d must be comma-separated numbers") from e


def _parse_opt_bool(v: str | None) -> bool | None:
    if v is None:
        return None
    s = v.strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    raise ValueError(f"bad bool {v!r}, use true/false")


@router.get("/tier-plan")
def tier_plan(
    total_assets: float | None = Query(default=None, gt=0),
    current_tier: str | None = Query(default=None),
    nav_60d: str | None = Query(default=None),
    starship_ready: str | None = Query(default=None),
    paper20_pass: str | None = Query(default=None),
    holdout_recovered: str | None = Query(default=None),
    filtered_valid_positive: str | None = Query(default=None),
) -> dict[str, Any]:
    """Plan the combo tier (read-only).

    - total_assets: account total assets in yuan (omit => env/default 1.2M)
    - current_tier: A | B | C (omit => A default attack tier)
    - nav_60d: comma-separated trailing NAVs, oldest -> newest (omit => no fuse)
    - starship_ready / paper20_pass / holdout_recovered /
      filtered_valid_positive: true/false (omit => 0% fail-closed)
    """
    try:
        ta, source = resolve_total_assets(total_assets)
        nav = _parse_nav(nav_60d)
        plan = _plan_with_starship(
            ta,
            current_tier,
            nav,
            starship_ready,
            paper20_pass,
            holdout_recovered,
            filtered_valid_positive,
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e) or e.__class__.__name__) from e
    plan["total_assets_source"] = source
    plan["disclaimer"] = (
        "Starship B stays live as the only baseline; "
        "this endpoint only sizes the observation weight (read-only)."
    )
    return plan


def _plan_with_starship(
    ta: float,
    current_tier: str | None,
    nav: list[float] | None,
    starship_ready: str | None,
    paper20_pass: str | None,
    holdout_recovered: str | None,
    filtered_valid_positive: str | None,
) -> dict[str, Any]:
    explicit_ready = _parse_opt_bool(starship_ready)
    if explicit_ready is not None:
        return ct.plan_tier(ta, current_tier, nav, explicit_ready)
    return ct.plan_tier(
        ta,
        current_tier,
        nav,
        None,
        paper20_pass=_parse_opt_bool(paper20_pass),
        holdout_recovered=_parse_opt_bool(holdout_recovered),
        filtered_valid_positive=_parse_opt_bool(filtered_valid_positive),
    )
