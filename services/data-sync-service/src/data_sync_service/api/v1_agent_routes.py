"""Agent contract / overlay endpoints (P0, 2026-11-10).

| Endpoint                                | Purpose |
|-----------------------------------------|---------|
| GET  /v1/agent/decision-contract        | One stable JSON for the decision agent |
| GET  /v1/agent/judgment-brief           | Full judgment pack (contract + market + backtest) |
| GET  /v1/agent/judgment-brief.md        | Same payload as compact Markdown (Sync & Copy) |
| GET  /v1/agent/overlays                 | Read proposed/accepted/expired overlays |
| POST /v1/agent/overlays                 | Propose / confirm / clear an overlay |
| POST /v1/agent/decision-log             | One minimal decision-log row (TIP-015) |

Overlays annotate; they never rewrite the engine's ``sleeve.action``, the
``TRAIL_PCT`` exit, the parking cooldown (k=0) or any strategy weight.
Auth mirrors the other ``/v1/*`` business routes (opt-in via KARIOS_API_KEYS).
"""

from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response
from pydantic import BaseModel, Field

from data_sync_service.api.auth import require_api_key
from data_sync_service.service.agent_brief import (
    build_judgment_brief,
    render_judgment_brief_markdown,
)
from data_sync_service.service.agent_contract import build_decision_contract
from data_sync_service.service.agent_overlays import (
    group_overlays,
    list_overlays,
    upsert_overlay,
)
from data_sync_service.service.trade_calendar_utils import shanghai_today_iso

router = APIRouter(
    prefix="/v1/agent",
    tags=["v1:agent"],
    dependencies=[Depends(require_api_key)],
)


class OverlayBody(BaseModel):
    type: str = Field(
        default="hold_same_key_rebuy",
        description="Overlay type. P0 supports 'hold_same_key_rebuy'.",
    )
    pickKey: str = Field(
        ...,
        min_length=1,
        description="Canonical parking key (GOLD / OIL / NASDAQ / BOND10).",
    )
    symbol: str | None = Field(default=None, description="Optional ETF symbol for display.")
    status: str = Field(
        default="proposed",
        description="proposed | accepted | rejected | cleared. Agent proposes; Yuan accepts.",
    )
    validUntil: str | None = Field(
        default=None,
        description="ISO date the overlay expires. Defaults to 5 open sessions.",
    )
    validDays: int | None = Field(
        default=None,
        ge=1,
        le=60,
        description="Alternative to validUntil: number of open sessions.",
    )
    note: str | None = Field(default=None, max_length=500)
    source: str = Field(default="agent", max_length=50)


class DecisionLogBody(BaseModel):
    tradeDate: str | None = Field(default=None, description="ISO date; default today.")
    judgment: str = Field(..., min_length=1, max_length=2000)
    contractSnapshotId: str | None = Field(
        default=None,
        description="The contractId returned by GET /v1/agent/decision-contract.",
    )
    adoptedOverlay: bool | None = Field(
        default=None,
        description="Whether the accepted overlay was followed.",
    )
    overlayId: str | None = None
    symbol: str | None = Field(default=None, description="Log symbol; defaults to the pick key.")
    source: str = Field(default="agent", max_length=50)
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    meta: dict[str, Any] | None = None


@router.get(
    "/decision-contract",
    summary="One-shot decision contract (mode + sleeve + health + next leg + whip).",
)
def get_decision_contract(
    tradeDate: str | None = Query(
        default=None,
        description="ISO date (YYYY-MM-DD); default today (Asia/Shanghai).",
    ),
) -> dict[str, Any]:
    try:
        return build_decision_contract(trade_date=tradeDate)
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"decision contract failed: {exc}") from exc


@router.get(
    "/judgment-brief",
    summary="Agent judgment pack: contract + gate + book + market facts + backtest.",
)
def get_judgment_brief(
    tradeDate: str | None = Query(
        default=None,
        description="ISO date (YYYY-MM-DD); default today (Asia/Shanghai).",
    ),
    mode: str = Query(
        default="full",
        description="full | compact. Compact caps list sizes; it is a subset of full.",
    ),
    format: str = Query(
        default="json",
        description="json | md. 'md' returns the same payload as compact Markdown.",
    ),
) -> Any:
    """One-shot judgment brief for the decision agent (乐乐).

    Deliberately excludes 研报 / Alpha Radar trends / catalyst stocks / Auto-QA
    penalties (``omitted`` lists them). Research reports survive only as
    freshness flags. Backtest comparison (harbor / homeport_m30 / starship_b +
    the current settings mode) is included without full equity curves.
    """
    mode_norm = str(mode or "full").strip().lower()
    if mode_norm not in ("full", "compact"):
        raise HTTPException(status_code=422, detail="mode must be 'full' or 'compact'")
    fmt = str(format or "json").strip().lower()
    if fmt not in ("json", "md", "markdown"):
        raise HTTPException(status_code=422, detail="format must be 'json' or 'md'")
    try:
        brief = build_judgment_brief(trade_date=tradeDate, mode=mode_norm)
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"judgment brief failed: {exc}") from exc
    if fmt in ("md", "markdown"):
        body = render_judgment_brief_markdown(brief, compact=mode_norm == "compact")
        return Response(content=body, media_type="text/markdown; charset=utf-8")
    return brief


@router.get(
    "/judgment-brief.md",
    summary="Agent judgment pack as compact Markdown (Sync & Copy clipboard body).",
    response_class=Response,
)
def get_judgment_brief_md(
    tradeDate: str | None = Query(
        default=None,
        description="ISO date (YYYY-MM-DD); default today (Asia/Shanghai).",
    ),
    mode: str = Query(default="full", description="full | compact."),
) -> Response:
    """Same payload as ``/v1/agent/judgment-brief``, rendered as Markdown."""
    mode_norm = str(mode or "full").strip().lower()
    if mode_norm not in ("full", "compact"):
        raise HTTPException(status_code=422, detail="mode must be 'full' or 'compact'")
    try:
        brief = build_judgment_brief(trade_date=tradeDate, mode=mode_norm)
        body = render_judgment_brief_markdown(brief, compact=mode_norm == "compact")
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"judgment brief failed: {exc}") from exc
    return Response(content=body, media_type="text/markdown; charset=utf-8")


@router.get("/overlays", summary="List agent overlays with effective status.")
def get_overlays(
    day: str | None = Query(default=None, description="ISO date; default today."),
    status: str | None = Query(
        default=None,
        description="Filter by effective status: proposed | accepted | expired | cleared | rejected.",
    ),
    pickKey: str | None = Query(default=None, description="Canonical parking key."),
    includeExpired: bool = Query(default=True),
) -> dict[str, Any]:
    as_of = str(day or shanghai_today_iso())[:10]
    items = list_overlays(
        day=as_of,
        status=status,
        pick_key=pickKey,
        include_expired=includeExpired,
    )
    return {
        "ok": True,
        "asOfDate": as_of,
        "overlays": group_overlays(items),
        "items": items,
    }


@router.post("/overlays", summary="Propose / confirm / clear an agent overlay.")
def post_overlay(body: OverlayBody) -> dict[str, Any]:
    try:
        overlay = upsert_overlay(
            overlay_type=body.type,
            pick_key=body.pickKey,
            symbol=body.symbol,
            status=body.status,
            valid_until=body.validUntil,
            valid_days=body.validDays,
            note=body.note,
            source=body.source,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    as_of = shanghai_today_iso()
    return {
        "ok": True,
        "overlay": overlay,
        "overlays": group_overlays(list_overlays(day=as_of)),
    }


@router.post("/decision-log", summary="Write one minimal decision-log row (TIP-015 actions).")
def post_decision_log(body: DecisionLogBody) -> dict[str, Any]:
    from data_sync_service.db.decision import insert_action

    day = body.tradeDate or shanghai_today_iso()
    try:
        snapshot_date = date.fromisoformat(str(day)[:10])
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"invalid tradeDate: {day}") from exc
    action = "hold_same_key_rebuy" if body.adoptedOverlay else "decision_log"
    try:
        row = insert_action(
            symbol=str(body.symbol or "CONTRACT").upper(),
            action=action,
            rationale=body.judgment,
            confidence=body.confidence,
            status="logged",
            source=body.source,
            snapshot_date=snapshot_date,
            outcome={
                "contractSnapshotId": body.contractSnapshotId,
                "adoptedOverlay": body.adoptedOverlay,
                "overlayId": body.overlayId,
                "meta": body.meta,
            },
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"decision log failed: {exc}") from exc
    return {"ok": True, "item": row}
