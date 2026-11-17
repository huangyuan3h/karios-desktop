"""Agent contract / overlay endpoints (P0, 2026-11-10).

| Endpoint                                | Purpose |
|-----------------------------------------|---------|
| GET  /v1/agent/decision-contract        | One stable JSON for the decision agent |
| GET  /v1/agent/judgment-brief           | Full judgment pack (contract + market + backtest) |
| GET  /v1/agent/judgment-brief.md        | Same payload as compact Markdown (Sync & Copy) |
| GET  /v1/agent/factor-system            | Advice-only factor-system policy (JSON / md) |
| GET  /v1/agent/strategy-alerts          | After-close decay / revival / excess pack |
| POST /v1/agent/strategy-alerts/run      | Build today's alert snapshot on demand |
| GET  /v1/agent/overlays                 | Read proposed/accepted/expired overlays |
| POST /v1/agent/overlays                 | Propose / confirm / clear an overlay |
| GET  /v1/agent/event-tags               | Read advice-only event / geo tags |
| POST /v1/agent/event-tags               | Record one sourced, advice-only tag |
| GET  /v1/agent/timeline-brief           | Compact, cache-first recent timeline note |
| GET  /v1/agent/decision-log             | Read back today's / recent decision rows |
| POST /v1/agent/decision-log             | One minimal decision-log row (TIP-015) |
| POST /v1/agent/daily-confirm            | Persist 14:30 advice + Yuan's decision |
| POST /v1/agent/daily-confirm/reconcile  | Fill next-session fills / deviations |
| GET  /v1/agent/watchlist                | Current watchlist registry (+ positions) |
| POST /v1/agent/watchlist                | Incremental symbol-scoped registry ops |

Overlays annotate; they never rewrite the engine's ``sleeve.action``, the
``TRAIL_PCT`` exit, the parking cooldown (k=0) or any strategy weight. Event
tags are further downstream: they can only advise whether to rebuy / which
mode to maintain, and can never veto a triggered sell.
Auth mirrors the other ``/v1/*`` business routes (opt-in via KARIOS_API_KEYS).
"""

from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response
from pydantic import BaseModel, ConfigDict, Field

from data_sync_service.api.auth import require_api_key
from data_sync_service.service.agent_brief import (
    build_judgment_brief,
    render_judgment_brief_markdown,
)
from data_sync_service.service.agent_contract import build_decision_contract
from data_sync_service.service.agent_event_tags import (
    EFFECT_WHITELIST,
    MIN_CONFIDENCE,
    list_event_tags,
    record_event_tag,
)
from data_sync_service.service.agent_overlays import (
    group_overlays,
    list_overlays,
    upsert_overlay,
)
from data_sync_service.service.factor_system import (
    build_factor_system,
    render_factor_system_markdown,
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


class DailyConfirmBody(BaseModel):
    tradeDate: str | None = Field(default=None, description="ISO date; default Shanghai today.")
    mode: str | None = Field(
        default=None,
        description="Strategy mode; defaults to GET /settings/strategy-mode (never forced).",
    )
    decision: str = Field(
        ...,
        description="confirmed | amended | deferred | rejected",
    )
    note: str | None = Field(default=None, max_length=2000)
    decisionLines: list[Any] | None = Field(
        default=None,
        description="His amended lines (required in spirit when decision=amended).",
    )
    contractSnapshotId: str | None = None
    source: str = Field(default="daily_confirm", max_length=50)


class DailyConfirmReconcileBody(BaseModel):
    tradeDate: str | None = Field(default=None, description="ISO date; default today.")
    mode: str | None = Field(default=None, description="Only this mode; default all modes.")
    source: str = Field(default="daily_confirm", max_length=50)


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
    refresh: bool = Query(
        default=False,
        description="Bypass the in-process brief/dashboard/backtest caches and rebuild.",
    ),
) -> Any:
    """One-shot judgment brief for the decision agent (乐乐).

    Deliberately excludes 研报 / Alpha Radar trends / catalyst stocks / Auto-QA
    penalties (``omitted`` lists them). Research reports survive only as
    freshness flags. Backtest comparison (harbor / homeport_m30 / starship_b +
    the current settings mode) is included without full equity curves.

    The payload is cached in-process for ~75s (``timingsMs`` / ``cache`` report
    where the time went); ``refresh=true`` forces a rebuild.
    """
    mode_norm = str(mode or "full").strip().lower()
    if mode_norm not in ("full", "compact"):
        raise HTTPException(status_code=422, detail="mode must be 'full' or 'compact'")
    fmt = str(format or "json").strip().lower()
    if fmt not in ("json", "md", "markdown"):
        raise HTTPException(status_code=422, detail="format must be 'json' or 'md'")
    try:
        brief = build_judgment_brief(trade_date=tradeDate, mode=mode_norm, refresh=bool(refresh))
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
    refresh: bool = Query(
        default=False,
        description="Bypass the in-process brief/dashboard/backtest caches and rebuild.",
    ),
) -> Response:
    """Same payload as ``/v1/agent/judgment-brief``, rendered as Markdown."""
    mode_norm = str(mode or "full").strip().lower()
    if mode_norm not in ("full", "compact"):
        raise HTTPException(status_code=422, detail="mode must be 'full' or 'compact'")
    try:
        brief = build_judgment_brief(trade_date=tradeDate, mode=mode_norm, refresh=bool(refresh))
        body = render_judgment_brief_markdown(brief, compact=mode_norm == "compact")
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"judgment brief failed: {exc}") from exc
    return Response(content=body, media_type="text/markdown; charset=utf-8")


@router.get(
    "/factor-system",
    summary="Advice-only factor-system policy from the frozen factor vault.",
)
def get_factor_system(
    format: str = Query(
        default="json",
        description="json | md. 'md' returns the same contract as Markdown.",
    ),
) -> Any:
    """Vault state -> follow / exit / wait-in-M30 advice (never an order).

    Fail-open: a missing/corrupt vault returns ``available: false`` with an
    error and a conservative wait-in-baseline policy instead of raising.
    Live preconditions (paper-20 / holdout recovery / filtered valid) block
    ``suggestedLiveWeight`` at 0 while still showing a revived light.
    """
    fmt = str(format or "json").strip().lower()
    if fmt not in ("json", "md", "markdown"):
        raise HTTPException(status_code=422, detail="format must be 'json' or 'md'")
    try:
        payload = build_factor_system()
    except Exception as exc:  # noqa: BLE001 — mirrors the other agent routes
        raise HTTPException(status_code=500, detail=f"factor system failed: {exc}") from exc
    if fmt in ("md", "markdown"):
        body = render_factor_system_markdown(payload)
        return Response(content=body, media_type="text/markdown; charset=utf-8")
    return payload


@router.get(
    "/factor-system.md",
    summary="Advice-only factor system as Markdown.",
    response_class=Response,
)
def get_factor_system_md() -> Response:
    """Same payload as ``/v1/agent/factor-system``, rendered as Markdown."""
    try:
        payload = build_factor_system()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"factor system failed: {exc}") from exc
    body = render_factor_system_markdown(payload)
    return Response(content=body, media_type="text/markdown; charset=utf-8")


@router.get(
    "/strategy-alerts",
    summary="After-close decay / revival / excess alerts (read-only snapshot).",
)
def get_strategy_alerts(
    asOf: str | None = Query(
        default=None,
        description=(
            "ISO date (YYYY-MM-DD); default the latest complete A-share session. "
            "Reads the persisted 19:20 snapshot; GET never writes one."
        ),
    ),
) -> dict[str, Any]:
    """Delta pack 乐乐 can broadcast in chat (advice only).

    Quiet when nothing new happened: standing cold / revived lights live in
    ``currentState`` and are never re-broadcast as today's events. Missing
    snapshot returns ``available: false`` with a reason instead of silently
    running the writer. Read-only: no alert row, decision row, or confirmation
    is written on this path.
    """
    from data_sync_service.service.agent_strategy_alerts import read_strategy_alerts

    day = str(asOf or "").strip()[:10]
    if day:
        try:
            date.fromisoformat(day)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=f"invalid asOf: {day}") from exc
    try:
        return read_strategy_alerts(as_of=day or None)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"strategy alerts failed: {exc}") from exc


@router.post(
    "/strategy-alerts/run",
    summary="Build (and persist) today's after-close strategy alert snapshot.",
)
def post_strategy_alerts_run(
    asOf: str | None = Query(
        default=None,
        description="ISO date (YYYY-MM-DD); default the latest complete A-share session.",
    ),
) -> dict[str, Any]:
    """One-shot writer (the weekday 19:20 cron calls the same service).

    Never places orders, never edits settings / the watchlist, never changes a
    target weight. Idempotent per ``asOf``: a re-run overwrites that day's
    snapshot and does not duplicate event keys.
    """
    from data_sync_service.service.agent_strategy_alerts import run_strategy_alerts

    day = str(asOf or "").strip()[:10]
    if day:
        try:
            date.fromisoformat(day)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=f"invalid asOf: {day}") from exc
    try:
        out = run_strategy_alerts(as_of=day or None)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"strategy alerts run failed: {exc}") from exc
    if not out.get("persisted"):
        raise HTTPException(
            status_code=500,
            detail=f"strategy alerts snapshot not persisted: {out.get('writeError')}",
        )
    try:
        from data_sync_service.service.agent_brief import invalidate_judgment_brief_cache

        invalidate_judgment_brief_cache("strategy_alerts")
    except Exception:  # noqa: BLE001 — never fail a successful write on cache eviction
        pass
    return out


@router.get(
    "/timeline-brief",
    summary="Compact recent timeline note (cache-first; never runs a cold simulate).",
)
def get_timeline_brief(
    strategy: str | None = Query(
        default=None,
        description=(
            "Timeline key (harbor | homeport | homeport_m30 | starport | ...). "
            "Default: the settings-mapped key — settings 'homeport' is timeline "
            "'homeport_m30'; timeline 'homeport' is the frozen M50 base."
        ),
    ),
    start: str | None = Query(
        default=None,
        description="Window start YYYY-MM-DD; default: trailing-year cache window.",
    ),
    end: str | None = Query(
        default=None,
        description="Window end YYYY-MM-DD; default: today.",
    ),
) -> dict[str, Any]:
    """Last-day return / drawdown / parking leg from the timeline cache.

    Cache-first: memory → per-strategy file → explicit miss. A miss returns
    ``available: false`` with a reason; it never blocks on a minutes-long
    cold build. The full curve stays at ``GET /api/backtest/timeline``.
    """
    from data_sync_service.api.backtest_routes import build_timeline_brief

    try:
        return build_timeline_brief(strategy=strategy, start=start, end=end)
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"timeline brief failed: {exc}") from exc


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


class EventTagSource(BaseModel):
    url: str | None = Field(default=None, description="Real article / document URL.")
    briefId: str | None = Field(default=None, description="News brief id (source list only).")
    title: str | None = Field(default=None, max_length=300)


class EventTagBody(BaseModel):
    tag: str = Field(..., min_length=1, max_length=80)
    effect: str = Field(
        ...,
        description=(
            "Whitelist only: advise_defer_same_key_rebuy | advise_strategy | "
            "trail_exit | below_ma200. Anything that blocks/delays a sell is 422."
        ),
    )
    confidence: float = Field(..., ge=0.0, le=1.0)
    sources: list[EventTagSource] = Field(
        default_factory=list,
        description="At least one of url / briefId is required (no fabricated sources).",
    )
    relatedAssets: list[str] = Field(default_factory=list)
    validFrom: str | None = Field(default=None, description="ISO date; defaults to today.")
    validUntil: str | None = Field(default=None, description="ISO date the tag expires.")
    validDays: int | None = Field(default=None, ge=1, le=90)
    note: str | None = Field(default=None, max_length=500)
    source: str = Field(default="agent", max_length=50)
    status: str = Field(default="active", description="active | cleared | rejected")
    newsBriefId: str | None = Field(
        default=None,
        description="Optional: when sources is empty, ingest this news brief's items as sources.",
    )


@router.get("/event-tags", summary="List advice-only event / geo tags.")
def get_event_tags(
    day: str | None = Query(default=None, description="ISO date; default today."),
    effect: str | None = Query(default=None, description="Filter by whitelist effect."),
    includeExpired: bool = Query(default=True),
) -> dict[str, Any]:
    as_of = str(day or shanghai_today_iso())[:10]
    items = list_event_tags(day=as_of, effect=effect, include_expired=includeExpired)
    active = [t for t in items if t.get("effectiveStatus") == "active"]
    return {
        "ok": True,
        "asOfDate": as_of,
        "threshold": MIN_CONFIDENCE,
        "effects": sorted(EFFECT_WHITELIST),
        "adviceOnly": True,
        "neverVetoesExits": True,
        "eventTags": {
            "active": active,
            "expired": [t for t in items if t.get("effectiveStatus") == "expired"],
            "all": items,
        },
    }


@router.post("/event-tags", summary="Record one sourced, advice-only event tag.")
def post_event_tag(body: EventTagBody) -> dict[str, Any]:
    from data_sync_service.service.agent_event_tags import news_brief_sources

    sources = [s.model_dump(exclude_none=True) for s in body.sources]
    if not sources and body.newsBriefId:
        # News brief is a source list only — never a raw trade signal.
        sources = [
            s for s in news_brief_sources() if str(s.get("briefId") or "") == str(body.newsBriefId)
        ]
    try:
        tag = record_event_tag(
            tag=body.tag,
            effect=body.effect,
            confidence=body.confidence,
            sources=sources,
            related_assets=body.relatedAssets,
            valid_from=body.validFrom,
            valid_until=body.validUntil,
            valid_days=body.validDays,
            note=body.note,
            source=body.source,
            status=body.status,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    as_of = shanghai_today_iso()
    return {
        "ok": True,
        "eventTag": tag,
        "eventTags": list_event_tags(day=as_of),
    }


@router.get(
    "/decision-log",
    summary="Read back today's / recent decision rows (TIP-015 actions).",
)
def get_decision_log(
    tradeDate: str | None = Query(
        default=None,
        description="ISO date (YYYY-MM-DD); default today (Asia/Shanghai).",
    ),
    lookback: int = Query(
        default=3,
        ge=1,
        le=20,
        description="Include the last N open sessions ending at tradeDate.",
    ),
    symbol: str | None = Query(default=None, description="Filter by symbol."),
    limit: int = Query(default=200, ge=1, le=500),
) -> dict[str, Any]:
    """Read the store ``POST /v1/agent/decision-log`` writes (fail-open).

    Returns tradeDate / judgment / adopted overlay / symbol / source /
    contractSnapshotId per row, plus ``kind`` / ``mode`` / ``advice`` /
    ``decision`` / ``reconcile`` for 14:30 confirmation rows. A missing DB
    degrades to ``ok: false`` with ``available: false`` and an error — it never
    500s.
    """
    from data_sync_service.service.agent_decision_log import read_decision_log

    day = str(tradeDate or shanghai_today_iso())[:10]
    try:
        date.fromisoformat(day)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"invalid tradeDate: {day}") from exc
    out = read_decision_log(
        trade_date=day,
        lookback=lookback,
        symbol=symbol.upper() if symbol else None,
        limit=limit,
    )
    return {"ok": bool(out.get("available")), **out}


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
    # The brief folds the decision log in; drop the 75s cached copy so the next
    # read sees the row that was just written. Fail-open (cache only).
    try:
        from data_sync_service.service.agent_brief import invalidate_judgment_brief_cache

        invalidate_judgment_brief_cache("decision_log")
    except Exception:  # noqa: BLE001 — never fail a successful write on cache eviction
        pass
    return {"ok": True, "item": row}


@router.post(
    "/daily-confirm",
    summary="Persist the 14:30 advice + Yuan's confirmation (one row per day+mode).",
)
def post_daily_confirm(body: DailyConfirmBody) -> dict[str, Any]:
    """Record 建议 + 他的决定; the server loads today's ``compose_daily_adjust``.

    Idempotent on ``(tradeDate, mode)``: a second confirmation updates the
    existing row's decision / advice instead of appending a duplicate. Advice
    composer failure is fail-open — his confirmation is still stored.
    """
    from data_sync_service.service.agent_daily_confirm import record_daily_confirm

    day = str(body.tradeDate or shanghai_today_iso())[:10]
    try:
        date.fromisoformat(day)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"invalid tradeDate: {day}") from exc
    try:
        out = record_daily_confirm(
            trade_date=day,
            mode=body.mode,
            decision=body.decision,
            note=body.note,
            decision_lines=body.decisionLines,
            contract_snapshot_id=body.contractSnapshotId,
            source=body.source or "daily_confirm",
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"daily confirm failed: {exc}") from exc
    try:
        from data_sync_service.service.agent_brief import invalidate_judgment_brief_cache

        invalidate_judgment_brief_cache("daily_confirm")
    except Exception:  # noqa: BLE001 — never fail a successful write on cache eviction
        pass
    return out


@router.post(
    "/daily-confirm/reconcile",
    summary="Fill next-session fills / deviations for pending 14:30 confirmations.",
)
def post_daily_confirm_reconcile(body: DailyConfirmReconcileBody) -> dict[str, Any]:
    """One-shot reconcile (the weekday 18:55 cron calls the same service).

    Read-only against the watchlist / user_trades. A missing input leaves the
    row ``pending`` / ``degraded``; the confirmation row is never deleted.
    """
    from data_sync_service.service.agent_daily_confirm import reconcile_pending

    day = str(body.tradeDate or shanghai_today_iso())[:10]
    try:
        date.fromisoformat(day)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"invalid tradeDate: {day}") from exc
    try:
        out = reconcile_pending(
            trade_date=day,
            mode=body.mode,
            source=body.source or "daily_confirm",
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=500, detail=f"daily confirm reconcile failed: {exc}"
        ) from exc
    try:
        from data_sync_service.service.agent_brief import invalidate_judgment_brief_cache

        invalidate_judgment_brief_cache("daily_confirm_reconcile")
    except Exception:  # noqa: BLE001
        pass
    return out


# ---------------------------------------------------------------------------
# /v1/agent/watchlist — incremental (never full replace)
# ---------------------------------------------------------------------------

WATCHLIST_OPS_DESCRIPTION = (
    "Incremental, symbol-scoped watchlist ops: upsert | set_position | remove. "
    "This never replaces the whole book, never places orders, and never writes "
    "the decision log. It updates the registry only (pool + position / cost)."
)


class AgentWatchlistOp(BaseModel):
    model_config = ConfigDict(extra="forbid")

    op: str = Field(..., description="upsert | set_position | remove")
    symbol: str = Field(..., min_length=1, description="MARKET:TICKER, e.g. ETF:511260.")
    force: bool | None = Field(
        default=None,
        description="remove only: required when the row still has positionPct > 0.",
    )
    name: str | None = None
    source: str | None = None
    color: str | None = None
    positionPct: float | None = Field(
        default=None,
        description="Clamped to [0, 100] and rounded to 2 decimals. set_position requires it.",
    )
    costPrice: float | None = None
    maxPrice: float | None = None
    entryDate: str | None = None


class AgentWatchlistBody(BaseModel):
    reason: str = Field(
        ...,
        min_length=1,
        max_length=500,
        description="Why the book is changing (required, 1-500 chars).",
    )
    source: str = Field(
        default="agent",
        max_length=50,
        description="Provenance tag; also the default source for new rows.",
    )
    dryRun: bool = Field(default=False, description="Preview the before/after, write nothing.")
    ops: list[AgentWatchlistOp] = Field(
        ...,
        min_length=1,
        max_length=50,
        description=WATCHLIST_OPS_DESCRIPTION,
    )


@router.get(
    "/watchlist",
    summary="Current watchlist registry (+ open positions). Incremental write API.",
)
def get_agent_watchlist(
    format: str = Query(
        default="json",
        description="json | md. 'md' returns a compact Markdown table (text/markdown).",
    ),
) -> Any:
    """Read the registry the agent writes with ``POST /v1/agent/watchlist``.

    ``placesOrders`` is always false: this API is the book of record (pool +
    position / cost), not an execution surface.
    """
    from data_sync_service.service.agent_watchlist import (
        build_watchlist_snapshot,
        render_watchlist_markdown,
    )

    fmt = str(format or "json").strip().lower()
    if fmt not in ("json", "md", "markdown"):
        raise HTTPException(status_code=422, detail="format must be 'json' or 'md'")
    try:
        snapshot = build_watchlist_snapshot()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"watchlist read failed: {exc}") from exc
    if fmt in ("md", "markdown"):
        return Response(
            content=render_watchlist_markdown(snapshot),
            media_type="text/markdown; charset=utf-8",
        )
    return snapshot


@router.get(
    "/watchlist.md",
    summary="Current watchlist registry as a compact Markdown table.",
    response_class=Response,
)
def get_agent_watchlist_md() -> Response:
    from data_sync_service.service.agent_watchlist import (
        build_watchlist_snapshot,
        render_watchlist_markdown,
    )

    try:
        snapshot = build_watchlist_snapshot()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"watchlist read failed: {exc}") from exc
    return Response(
        content=render_watchlist_markdown(snapshot),
        media_type="text/markdown; charset=utf-8",
    )


@router.post(
    "/watchlist",
    summary="Incremental, symbol-scoped watchlist ops (never a full replace).",
)
def post_agent_watchlist(body: AgentWatchlistBody) -> dict[str, Any]:
    """Apply ``upsert`` / ``set_position`` / ``remove`` ops to the registry.

    Every op carries an explicit symbol, so unrelated rows are never deleted
    (``POST /watchlist/registry`` keeps its desktop-UI full-replace contract).
    ``placesOrders`` is always false; ``dryRun`` returns the would-be state.
    """
    from data_sync_service.service.agent_watchlist import (
        WatchlistOpError,
        apply_watchlist_ops,
    )

    ops = [op.model_dump(exclude_unset=True) for op in body.ops]
    try:
        return apply_watchlist_ops(
            reason=body.reason,
            ops=ops,
            source=body.source,
            dry_run=bool(body.dryRun),
        )
    except WatchlistOpError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"watchlist write failed: {exc}") from exc
