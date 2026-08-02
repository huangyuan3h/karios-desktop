"""OPT-047 Phase C: GET /v1/explain/{symbol} — comprehensive context pack.

This is the endpoint an external AI assistant calls when it needs to *explain*
a single symbol to a human (write a paragraph, draft a Telegram message,
classify a candle, etc.). The endpoint returns **every piece of structured data
an LLM would need to write that explanation** — Karios does NOT call an LLM
itself. The "no LLM in Karios" rule comes from the responsibility split
documented in ``docs/designs/freelancer-architecture.md``: Karios is the
passive data + endpoint service; natural-language generation lives in the
external AI assistant project.

Returned fields (all human-described, no internal codes without explanation):

- symbol basics (name, market, inWatchlist, position/cost)
- full TrendOK payload (scoreParts / stopLossParts included — same fields the
  desktop UI gets so the assistant can quote numbers verbatim)
- last 5 decision-journal changes for this symbol with `why` codes
- aggregate counters (active journal changes, total score)
- asOfDate: the date the pack was assembled (LLM must call again if today
  has rolled over)

Auth: opt-in via ``require_api_key`` (no-op when KARIOS_API_KEYS is empty),
inherited from the router-level dependency.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Path  # type: ignore[import-not-found]
from pydantic import BaseModel, Field  # type: ignore[import-not-found]

from ..api.auth import require_api_key  # noqa: F401  (used as router-level dependency)
from ..db import execution_journal as ej_db
from ..db.watchlist_automation import list_registry
from ..service.trendok import compute_trendok_for_symbols

router = APIRouter(
    prefix="/v1",
    tags=["v1:explain"],
    dependencies=[Depends(require_api_key)],
)


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------


class WatchlistState(BaseModel):
    """The watchlist position state for this symbol, if any.

    Fields are nullable because a symbol may be in the watchlist without a
    position (or vice versa in theory). Use the `inWatchlist` boolean as the
    single source of truth for "is this symbol in the pool?".
    """

    inWatchlist: bool = Field(
        ...,
        description="True iff the symbol is in the current watchlist registry.",
    )
    source: str | None = Field(
        default=None,
        description=(
            "How the symbol got into the pool. 'screener' | 'screener_fallback' | "
            "'alpha_radar' | 'manual'. Null when inWatchlist is False."
        ),
    )
    positionPct: float | None = Field(
        default=None,
        description=(
            "Position as percent of satellite sleeve. Null means either flat "
            "or no position number recorded. Drives SECTOR_CONC_BLOCK / "
            "SLEEVE_CAP_BLOCK checks."
        ),
    )
    costPrice: float | None = Field(
        default=None,
        description="Average cost. Null when not held or no cost recorded.",
    )
    entryDate: str | None = Field(
        default=None,
        description=(
            "ISO date of most recent entry. A+ markets apply T+1: if entryDate "
            "== today the Execution Gate cannot EXIT/TRIM (Locked_T1)."
        ),
    )


class RecentChange(BaseModel):
    """One recent change row for the symbol, pulled from the decision journal."""

    action: str | None = Field(
        default=None,
        description=(
            "Action card that changed. 'BUY' | 'ADD' | 'HOLD' | 'TRIM' | "
            "'EXIT' | 'WATCH' | 'WATCH_SILENT' | 'PURGE' | null."
        ),
    )
    why: str | None = Field(
        default=None,
        description=(
            "Stable reason code (MAINLINE_OK, SLEEVE_CAP_BLOCK, "
            "TIME_LOCK_WEAK_REGIME, TREND_RECOVERING, etc.). This is the field "
            "an LLM aggregates on when summarizing 'why did we X'."
        ),
    )
    capturedAt: str | None = Field(
        default=None,
        description="ISO timestamp the change was captured.",
    )
    tradeDate: str | None = Field(
        default=None,
        description="ISO date (YYYY-MM-DD) the change applies to (Asia/Shanghai).",
    )


class ExplainResponse(BaseModel):
    """Response of GET /v1/explain/{symbol}.

    An AI assistant has all it needs from this one payload to write a
    paragraph, a Telegram message, a brief, or a Q&A reply. The intent of
    bundling this is so the assistant never needs more than ONE call to
    Karios to produce a complete answer for one symbol.
    """

    asOfDate: str = Field(
        ...,
        description=(
            "ISO date the pack was assembled. If the assistant is composing a "
            "message and `asOfDate < today`, the data is at least one trading "
            "session old and should be re-fetched before publishing."
        ),
    )
    symbol: str = Field(
        ...,
        description="The symbol in 'MARKET:TICKER' form, echoed back from the path.",
    )
    name: str | None = Field(
        default=None,
        description="Human-readable company or fund name. Null if not resolved.",
    )
    market: str | None = Field(
        default=None,
        description="Market segment: 'CN' | 'HK' | 'ETF'. Null if the symbol did not resolve.",
    )
    trendok: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Full TrendOK payload: trendOk, score, scoreParts, stopLossPrice, "
            "stopLossParts, buyMode, buyAction, buyZoneLow/High, buyRefPrice, "
            "currentPrice, changePct, etc. — verbatim from the same function "
            "that powers the desktop UI so any number quoted is byte-identical "
            "to what the user has on screen."
        ),
    )
    watchlist: WatchlistState = Field(
        ...,
        description=(
            "Watchlist position state for this symbol. inWatchlist=False means "
            "the symbol is not currently tracked; the other fields will be null."
        ),
    )
    recentChanges: list[RecentChange] = Field(
        default_factory=list,
        description=(
            "Up to 5 most recent decision-journal changes for this symbol, "
            "oldest first. Empty when there are no journal entries in the last "
            "30 trading days — which usually means the symbol is fresh or the "
            "journal is empty."
        ),
    )
    recentChangesWindowDays: int = Field(
        ...,
        description=(
            "How far back `recentChanges` was scanned, in calendar days. Use "
            "this to know the coverage window (currently 30)."
        ),
    )


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------


@router.get(
    "/explain/{symbol}",
    response_model=ExplainResponse,
    summary="Comprehensive context pack for a single symbol (read-only).",
)
def get_explain(
    symbol: str = Path(
        ...,
        description=(
            "Symbol in 'MARKET:TICKER' form: 'CN:000001' | 'HK:00700' | "
            "'ETF:510300'. The same form the rest of the /v1/* surface uses."
        ),
        examples=["CN:000001", "HK:00700", "ETF:510300"],
    ),
) -> ExplainResponse:
    sym = (symbol or "").strip()
    if not sym:
        raise HTTPException(status_code=422, detail="symbol is required")

    as_of = date.today().isoformat()

    # 1) TrendOK — full payload, verbatim.
    trendok_payload: dict[str, Any] = {}
    market: str | None = None
    name: str | None = None
    try:
        rows = compute_trendok_for_symbols([sym], realtime=False)
        if rows:
            trendok_payload = dict(rows[0])
            market = trendok_payload.get("market") or None
            name = trendok_payload.get("name") or None
            # Drop the symbol key — caller already has it on the envelope.
            trendok_payload.pop("symbol", None)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    # 2) Watchlist state — find the matching row (if any).
    in_watchlist = False
    wl_source: str | None = None
    wl_position: float | None = None
    wl_cost: float | None = None
    wl_entry: str | None = None
    try:
        registry = list_registry()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    for row in registry:
        if not isinstance(row, dict):
            continue
        if str(row.get("symbol") or "").strip() == sym:
            in_watchlist = True
            wl_source = row.get("source")
            wl_position = row.get("positionPct")
            wl_cost = row.get("costPrice")
            wl_entry = row.get("entryDate")
            break

    # 3) Recent journal changes — 30 days window, filtered to this symbol.
    window_days = 30
    since = (date.today() - timedelta(days=window_days)).isoformat()
    recent: list[RecentChange] = []
    try:
        all_changes = list(ej_db.list_changes(since=since, limit=200))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    for c in all_changes:
        if not isinstance(c, dict):
            continue
        if str(c.get("symbol") or "").strip() != sym:
            continue
        recent.append(
            RecentChange(
                action=c.get("action"),
                why=c.get("why"),
                capturedAt=c.get("capturedAt") or c.get("captured_at"),
                tradeDate=c.get("tradeDate") or c.get("trade_date"),
            )
        )
        if len(recent) >= 5:
            break

    return ExplainResponse(
        asOfDate=as_of,
        symbol=sym,
        name=name,
        market=market,
        trendok=trendok_payload,
        watchlist=WatchlistState(
            inWatchlist=in_watchlist,
            source=wl_source,
            positionPct=wl_position,
            costPrice=wl_cost,
            entryDate=wl_entry,
        ),
        recentChanges=recent,
        recentChangesWindowDays=window_days,
    )
