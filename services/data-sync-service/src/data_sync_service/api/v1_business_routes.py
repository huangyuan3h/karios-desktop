"""OPT-045 Phase B / OPT-046: 3 read-only business endpoints under /v1/*.

These three paths are the actual surface an external AI assistant calls in
production. The 4 ``/v1/*`` discovery endpoints (see ``discovery_routes.py``)
tell the assistant *what* the API looks like; this router gives it *the data*.

| Endpoint                              | Wraps                              | Why |
|---------------------------------------|------------------------------------|-----|
| GET /v1/market/snapshot?symbols=...   | trendok + realtime quote (read)    | One call for N symbols' TrendOK / Score / price |
| GET /v1/watchlist/items               | /watchlist/registry                | The whole pool + positionPct / costPrice / entryDate |
| GET /v1/decision-journal/query        | /execution/changes                 | Recent Gate / Action / Why changes |

Design rules (see docs/designs/api-contract.md):

- READ-ONLY. No mutating endpoints live here. Writes go through the existing
  /watchlist/* and /execution/* routes used by the desktop UI.
- Every response carries ``asOfDate`` so the caller knows data freshness.
- Every field has a human-readable ``description`` (LLM reads it).
- Auth: opt-in via ``require_api_key`` (no-op when KARIOS_API_KEYS is empty).
"""

from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query  # type: ignore[import-not-found]
from pydantic import BaseModel, Field  # type: ignore[import-not-found]

from ..api.auth import require_api_key  # noqa: F401  (used as router-level dependency)
from ..db import execution_journal as ej_db
from ..db import paper_trading as pt_db
from ..db.watchlist_automation import list_registry
from ..service.execution_source import aggregate_source_stats
from ..service.paper_trading import compute_stats as pt_compute_stats
from ..service.trendok import compute_trendok_for_symbols

router = APIRouter(
    prefix="/v1",
    tags=["v1:business"],
    dependencies=[Depends(require_api_key)],
)


# ---------------------------------------------------------------------------
# /v1/market/snapshot
# ---------------------------------------------------------------------------


class MarketSnapshotItem(BaseModel):
    """One row in the /v1/market/snapshot response.

    Each row carries enough context for an AI assistant to write a sentence
    about the symbol without further lookups: name, current price, change%,
    the TrendOK verdict, the score, the recommended entry zone, and the
    hard-stop price. The buyAction field is the *machine-readable* go/no-go.
    """

    symbol: str = Field(
        ...,
        description=(
            "Internal symbol in the 'MARKET:TICKER' form Karios uses everywhere "
            "(e.g. 'CN:000001', 'HK:00700', 'ETF:510300'). The CALLER passes "
            "this form on the way in and gets it back unchanged on the way out."
        ),
        examples=["CN:000001", "HK:00700"],
    )
    name: str | None = Field(
        default=None,
        description="Human-readable company or fund name. null if not yet resolved.",
    )
    market: str = Field(
        ...,
        description="Market segment: 'CN' | 'HK' | 'ETF'.",
        examples=["CN", "HK", "ETF"],
    )
    trendOk: bool | None = Field(
        default=None,
        description=(
            "Whether the symbol passes the TrendOK health check (EMA / MACD / "
            "RSI / volume alignment). True = healthy, False = unhealthy, "
            "null = not enough data yet (e.g. very new listing)."
        ),
    )
    score: int | None = Field(
        default=None,
        description=(
            "Composite setup score on a 0–100 integer scale. >=85 is the "
            "typical fireable threshold; 30 is the GC floor; 60 is the "
            "Alpha-S recovering floor (V6.3). null when there is no data."
        ),
        ge=0,
        le=100,
    )
    currentPrice: float | None = Field(
        default=None,
        description=(
            "Latest price. Uses realtime quote if available, otherwise the "
            "most recent daily close. null if no price is on file."
        ),
    )
    changePct: float | None = Field(
        default=None,
        description=(
            "Percent change vs the previous close. Positive = up. null if the "
            "previous close is missing. Same value the UI shows in green/red."
        ),
    )
    buyAction: str | None = Field(
        default=None,
        description=(
            "Machine-readable action label from the buy-mode logic. One of "
            "'buy' | 'wait' | 'avoid' | null. null = no live data. The desktop "
            "UI uses this same field; the AI assistant should mirror it."
        ),
        examples=["buy", "wait", "avoid"],
    )
    buyZoneHigh: float | None = Field(
        default=None,
        description=(
            "Upper bound of the suggested entry zone. An AI assistant can "
            "treat `currentPrice <= buyZoneHigh` as a buyable signal when "
            "buyAction='buy'. null when buyAction is null or 'avoid'."
        ),
    )
    stopLossPrice: float | None = Field(
        default=None,
        description=(
            "Hard-stop price. Going long above stopLossPrice violates the "
            "ENTRY_BELOW_STOP rule; the Execution Gate will refuse a BUY if "
            "buyZoneHigh <= stopLossPrice. null = no stop on file."
        ),
    )


class MarketSnapshotResponse(BaseModel):
    """Response of GET /v1/market/snapshot."""

    asOfDate: str = Field(
        ...,
        description=(
            "ISO date (YYYY-MM-DD, Asia/Shanghai) the snapshot was computed. "
            "Compare to 'today' to detect stale data. If asOfDate < today, the "
            "market is either closed or the daily sync has not yet run."
        ),
    )
    items: list[MarketSnapshotItem] = Field(
        ...,
        description=(
            "One entry per requested symbol, in the same order as the input. "
            "Empty list when no symbols were requested or none resolved."
        ),
    )


@router.get(
    "/market/snapshot",
    response_model=MarketSnapshotResponse,
    summary="Get TrendOK / Score / current price for a list of symbols (read-only).",
)
def get_market_snapshot(
    symbols: list[str] = Query(
        ...,
        description=(
            "Required. One or more symbols in 'MARKET:TICKER' form. Repeat the "
            "param for multiple (e.g. '?symbols=CN:000001&symbols=HK:00700'). "
            "CN / HK / ETF all supported (see OPT-041, OPT-042)."
        ),
    ),
) -> MarketSnapshotResponse:
    if not symbols:
        raise HTTPException(status_code=422, detail="symbols is required")
    # De-dupe while preserving caller's order so the response is predictable.
    seen: set[str] = set()
    ordered: list[str] = []
    for s in symbols:
        if s and s not in seen:
            seen.add(s)
            ordered.append(s)
    try:
        rows = compute_trendok_for_symbols(ordered, realtime=False)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    items: list[MarketSnapshotItem] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        items.append(
            MarketSnapshotItem(
                symbol=str(r.get("symbol") or ""),
                name=r.get("name"),
                market=str(r.get("market") or ""),
                trendOk=r.get("trendOk"),
                score=r.get("score"),
                currentPrice=r.get("currentPrice"),
                changePct=r.get("changePct"),
                buyAction=r.get("buyAction"),
                buyZoneHigh=r.get("buyZoneHigh"),
                stopLossPrice=r.get("stopLossPrice"),
            )
        )
    return MarketSnapshotResponse(
        asOfDate=date.today().isoformat(),
        items=items,
    )


# ---------------------------------------------------------------------------
# /v1/watchlist/items
# ---------------------------------------------------------------------------


class WatchlistItem(BaseModel):
    """One row in the /v1/watchlist/items response.

    Mirrors the shape used by the desktop UI's WatchlistPage. Field names use
    camelCase to stay byte-identical to the existing /watchlist/registry
    contract (see OPT-009 / packages/shared).
    """

    symbol: str = Field(..., description="MARKET:TICKER form.")
    name: str | None = Field(default=None, description="Company or fund name; null if not resolved.")
    source: str | None = Field(
        default=None,
        description=(
            "How the symbol got into the pool. 'screener' | 'screener_fallback' | "
            "'alpha_radar' | 'manual'. Use this to attribute fires in /v1/"
            "decision-journal/query (TIP-011)."
        ),
    )
    color: str | None = Field(default=None, description="UI color tag, if any.")
    positionPct: float | None = Field(
        default=None,
        description=(
            "Current position as a percent of the satellite sleeve. null means "
            "either fully out of position or position number was never recorded. "
            "Used by SECTOR_CONC_BLOCK (30% per industry) and SLEEVE_CAP_BLOCK."
        ),
    )
    costPrice: float | None = Field(
        default=None,
        description=(
            "Average cost. null if not held. Combined with currentPrice this "
            "yields the floating P&L shown in the UI."
        ),
    )
    maxPrice: float | None = Field(
        default=None,
        description="Highest price since entry. Cleared on zero-position.",
    )
    entryDate: str | None = Field(
        default=None,
        description=(
            "ISO date of the most recent entry. A+ markets apply T+1: if "
            "entryDate == today, the Execution Gate cannot EXIT/TRIM (Locked_T1)."
        ),
    )


class WatchlistResponse(BaseModel):
    """Response of GET /v1/watchlist/items."""

    asOfDate: str = Field(
        ...,
        description=(
            "ISO date the registry was read. For read-only snapshots this is "
            "the date the call was made; the underlying row timestamps live "
            "inside each item."
        ),
    )
    count: int = Field(..., description="Number of items returned (== len(items)).")
    items: list[WatchlistItem] = Field(..., description="All current watchlist items, no filter applied.")


@router.get(
    "/watchlist/items",
    response_model=WatchlistResponse,
    summary="List the current watchlist registry (read-only).",
)
def get_watchlist_items() -> WatchlistResponse:
    try:
        rows = list_registry()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    items: list[WatchlistItem] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        items.append(
            WatchlistItem(
                symbol=str(r.get("symbol") or ""),
                name=r.get("name"),
                source=r.get("source"),
                color=r.get("color"),
                positionPct=r.get("positionPct"),
                costPrice=r.get("costPrice"),
                maxPrice=r.get("maxPrice"),
                entryDate=r.get("entryDate"),
            )
        )
    return WatchlistResponse(asOfDate=date.today().isoformat(), count=len(items), items=items)


# ---------------------------------------------------------------------------
# /v1/decision-journal/query
# ---------------------------------------------------------------------------


class DecisionChange(BaseModel):
    """One change row from the execution decision journal.

    Field names use camelCase to stay aligned with the underlying
    ``execution_decision_changes`` table. The ``why`` field is the most
    important: it carries a stable error-code-like string (e.g. 'SLEEVE_CAP_BLOCK',
    'TIME_LOCK_WEAK_REGIME', 'TREND_RECOVERING') that an LLM can use to
    group changes by reason.
    """

    changeId: str | None = Field(default=None, description="Internal change-row id; null if not yet persisted.")
    symbol: str | None = Field(default=None, description="Symbol this change is about; null for portfolio-level changes.")
    action: str | None = Field(
        default=None,
        description=(
            "The action card that changed. 'BUY' | 'ADD' | 'HOLD' | 'TRIM' | "
            "'EXIT' | 'WATCH' | 'WATCH_SILENT' | 'PURGE' | null (mode-only change)."
        ),
    )
    why: str | None = Field(
        default=None,
        description=(
            "Stable reason code for the change (SLEEVE_CAP_BLOCK, "
            "MOMENTUM_SURGE_ALLOW, TREND_RECOVERING, etc.). null when no reason "
            "was recorded. Use this to group / aggregate changes in reports."
        ),
    )
    capturedAt: str | None = Field(
        default=None,
        description="ISO timestamp when the change was captured.",
    )
    tradeDate: str | None = Field(
        default=None,
        description="ISO date (YYYY-MM-DD) the change applies to (Asia/Shanghai).",
    )


class DecisionJournalResponse(BaseModel):
    """Response of GET /v1/decision-journal/query."""

    asOfDate: str = Field(
        ...,
        description=(
            "ISO date the query was run. Combined with each change's "
            "capturedAt, this lets an AI assistant know how stale the feed is."
        ),
    )
    changes: list[DecisionChange] = Field(
        default_factory=list,
        description=(
            "Chronological changes (oldest first) since the requested date. "
            "Empty list when the journal has no rows in the window."
        ),
    )


@router.get(
    "/decision-journal/query",
    response_model=DecisionJournalResponse,
    summary="Query the execution decision journal for recent changes (read-only).",
)
def get_decision_journal_query(
    since: str | None = Query(
        default=None,
        description=(
            "ISO date (YYYY-MM-DD) lower bound. Changes with tradeDate >= since "
            "are returned. null = no lower bound (returns the most recent N rows)."
        ),
        examples=["2026-08-01"],
    ),
    limit: int = Query(
        default=100,
        ge=1,
        le=500,
        description="Maximum number of changes to return. Default 100, capped at 500.",
    ),
) -> DecisionJournalResponse:
    try:
        rows: list[dict[str, Any]] = list(ej_db.list_changes(since=since, limit=limit))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    changes: list[DecisionChange] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        changes.append(
            DecisionChange(
                changeId=r.get("changeId") or r.get("change_id"),
                symbol=r.get("symbol"),
                action=r.get("action"),
                why=r.get("why"),
                capturedAt=r.get("capturedAt") or r.get("captured_at"),
                tradeDate=r.get("tradeDate") or r.get("trade_date"),
            )
        )
    return DecisionJournalResponse(asOfDate=date.today().isoformat(), changes=changes)


# ---------------------------------------------------------------------------
# /v1/paper-trades
# ---------------------------------------------------------------------------


class PaperTrade(BaseModel):
    """One paper-trade row (OPT-049)."""

    id: str = Field(..., description="UUID primary key.")
    symbol: str = Field(..., description="MARKET:TICKER form (v0: CN only).")
    entryDate: str = Field(..., description="ISO date the simulated order was placed.")
    side: str = Field(
        ...,
        description="'BUY' or 'ADD'. v0 only emits these two — exits are implicit (cron closes).",
    )
    entryPrice: float = Field(..., description="Daily close on entryDate used as fill price.")
    scoreAtEntry: float | None = Field(
        default=None,
        description="Snapshot of TrendOK score at the time of the original BUY/ADD signal.",
    )
    whyAtEntry: str | None = Field(
        default=None,
        description="Stable reason code from the decision journal (MAINLINE_OK, etc.).",
    )
    sleevePct: float | None = Field(
        default=None,
        description="Suggested position size at entry (informational; the simulated fill is full-position).",
    )
    status: str = Field(
        ...,
        description="'open' | 'closed'. Closed rows are immutable.",
    )
    closeDate: str | None = Field(
        default=None,
        description="ISO date the cron closed the trade. null while still open.",
    )
    closePrice: float | None = Field(
        default=None,
        description="Latest close (or final close if status='closed').",
    )
    pnlPct: float | None = Field(
        default=None,
        description="(closePrice - entryPrice) / entryPrice * 100. Updated daily by the update cron.",
    )
    holdingDays: int | None = Field(
        default=None,
        description="Calendar days from entryDate to closeDate (or today for open rows).",
    )
    closeReason: str | None = Field(
        default=None,
        description=(
            "Why the cron closed the trade. v0.1 emits 'stop_hit' (pnl_pct <= -5%), "
            "'target_hit' (pnl_pct >= +10%), 'score_floor' (TrendOK score < 30), "
            "'pool_exit' (symbol purged from the watchlist) or "
            "'max_hold' (holding_days >= 5). null while still open."
        ),
    )
    source: str | None = Field(
        default=None,
        description=(
            "Provenance of the BUY/ADD signal (TIP-011). Closed enum: "
            "'TV' (TV screener funnel) | 'ALPHA' (Alpha Radar catalyst) | "
            "'MANUAL' (user / external AI agent). null = pre-TIP-011 row."
        ),
        examples=["TV", "ALPHA", "MANUAL"],
    )


class PaperTradeListResponse(BaseModel):
    """Response of GET /v1/paper-trades."""

    asOfDate: str = Field(..., description="ISO date the list was read.")
    count: int = Field(..., description="Number of rows in `items`.")
    items: list[PaperTrade] = Field(..., description="Latest first (entry_date DESC).")


class PaperTradeStatsResponse(BaseModel):
    """Response of GET /v1/paper-trades/stats."""

    since: str = Field(..., description="ISO date lower bound used for the stats window.")
    closedCount: int = Field(..., description="Number of closed trades in the window.")
    winningCount: int = Field(..., description="Subset of closedCount where pnl_pct > 0.")
    winRate: float | None = Field(
        default=None,
        description="winningCount / closedCount, in [0, 1]. null when closedCount == 0.",
    )
    avgPnlPct: float | None = Field(
        default=None,
        description="Arithmetic mean of pnl_pct across the window. null when closedCount == 0.",
    )


@router.get(
    "/paper-trades",
    response_model=PaperTradeListResponse,
    summary="List paper trades (OPT-049, v0.1: CN-only, read-only).",
)
def get_paper_trades(
    status: str | None = Query(
        default=None,
        description="Filter by status: 'open' | 'closed'. null returns both.",
    ),
    since: str | None = Query(
        default=None,
        description="ISO date (YYYY-MM-DD). Only rows with entry_date >= since are returned.",
    ),
    limit: int = Query(
        default=50,
        ge=1,
        le=500,
        description="Maximum rows to return. Capped at 500 to keep the response bounded.",
    ),
) -> PaperTradeListResponse:
    try:
        rows = pt_db.list_paper_trades(status=status, since=since, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return PaperTradeListResponse(
        asOfDate=date.today().isoformat(),
        count=len(rows),
        items=[PaperTrade(**r) for r in rows],
    )


@router.get(
    "/paper-trades/stats",
    response_model=PaperTradeStatsResponse,
    summary="Get aggregate stats for paper trades since a given date.",
)
def get_paper_trades_stats(
    since: str = Query(
        ...,
        description="ISO date (YYYY-MM-DD) lower bound. Required — stats are useless without a window.",
        examples=["2026-08-01"],
    ),
) -> PaperTradeStatsResponse:
    try:
        result = pt_compute_stats(since_iso=since)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if "error" in result:
        raise HTTPException(status_code=500, detail=str(result["error"]))
    return PaperTradeStatsResponse(
        since=result["since"],
        closedCount=int(result.get("closedCount") or 0),
        winningCount=int(result.get("winningCount") or 0),
        winRate=result.get("winRate"),
        avgPnlPct=result.get("avgPnlPct"),
    )


# ---------------------------------------------------------------------------
# /v1/execution/source-stats (TIP-011)
# ---------------------------------------------------------------------------


class SourceStatsBucket(BaseModel):
    """Per-source aggregation bucket."""

    buySignals: int = Field(
        ...,
        description="Count of BUY transitions in execution_decision_changes for this source.",
    )
    closed: int = Field(
        ...,
        description="Paper-trade closed count for this source in the window.",
    )
    wins: int = Field(..., description="Closed trades with pnl_pct > 0.")
    losses: int = Field(..., description="Closed trades with pnl_pct <= 0.")
    winRate: float = Field(
        ...,
        description="wins / (wins + losses). 0 when no closed trades.",
    )


class SourceStatsResponse(BaseModel):
    """Response of GET /v1/execution/source-stats (TIP-011)."""

    sinceDays: int = Field(
        ...,
        description="Lookback window in days used for the aggregation.",
    )
    lookbackDays: int = Field(
        ...,
        description="Mirror of sinceDays for callers that key on lookbackDays.",
    )
    generatedAt: str = Field(
        ...,
        description="ISO timestamp when this snapshot was computed (server clock).",
    )
    bySource: dict[str, SourceStatsBucket] = Field(
        ...,
        description=(
            "Per-source aggregates. Keys are 'TV' | 'ALPHA' | 'MANUAL' | 'UNKNOWN'. "
            "Only sources with at least one BUY signal OR one closed trade appear "
            "in the response — empty buckets are dropped."
        ),
    )
    openTradesBySource: dict[str, int] = Field(
        ...,
        description="Open paper-trade counts by source (for in-flight monitoring).",
    )


@router.get(
    "/execution/source-stats",
    response_model=SourceStatsResponse,
    summary="Per-source BUY/ADD win-rate (TIP-011).",
)
def get_execution_source_stats(
    sinceDays: int = Query(
        default=30,
        ge=1,
        le=365,
        description="Lookback window in days. Defaults to 30 (env EXECUTION_SOURCE_STATS_LOOKBACK_DAYS).",
    ),
) -> SourceStatsResponse:
    try:
        result = aggregate_source_stats(since_days=sinceDays)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return SourceStatsResponse(**result)
