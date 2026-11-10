"""Agent judgment brief — one stable read for Karios' decision agent (乐乐).

``GET /v1/agent/judgment-brief`` is the single call the decision agent makes
before a judgment. It packages the facts that used to be copied by hand from
the Dashboard: the decision contract, data freshness flags, the CN/HK
execution gate, the book (holdings + live quotes + idle parking), the market
facts (sentiment / industry fund flow / macro lights) and the **frozen
backtest comparison** (the strategies 乐乐 actually discusses).

Deliberately OUT of this payload (see ``OMITTED_PACKS``): research reports
(研报), Alpha Radar structured trends, Alpha Radar top catalyst stocks and the
Alpha Auto-QA penalties. Those pages stay in the app for human browsing; they
are not judgment inputs and they made the copy pack bloated.

Read-only: nothing here rewrites ``sleeve.action``, ``TRAIL_PCT``, the parking
cooldown (k=0) or any strategy weight. Every block is fail-open — a broken
sub-source degrades to ``{"available": false, "error": ...}`` instead of
failing the whole brief. Only a broken decision contract raises.

Speed (2026-11-10): the assembled brief, the dashboard summary it reads and
the frozen backtest catalog are cached in-process (``timingsMs`` + ``cache``
in the payload report the cost). Startup, dashboard sync and close sync
invalidate + re-warm it so the next 乐乐 read is warm; ``?refresh=true``
forces a rebuild. A cached entry built before the latest weekday 15:00 close
is never served after it.
"""

from __future__ import annotations

import copy
import json
import logging
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

BRIEF_VERSION = "2026-11-10"
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")

# Packs intentionally left out of the agent judgment brief. Documented in the
# payload itself so the agent never wonders whether it is missing something.
OMITTED_PACKS: tuple[str, ...] = (
    "research_reports",
    "alpha_radar_trends",
    "alpha_catalyst_stocks",
    "alpha_auto_qa",
)

# The strategies 乐乐 actually discusses. ``harbor`` = Live sunset base,
# ``homeport_m30`` = product baseline, ``starship_b`` = observation arm. The
# current settings mode is appended when it is none of these.
BACKTEST_COMPARE_KEYS: tuple[str, ...] = ("harbor", "homeport_m30", "starship_b")

NEWS_LIMIT_FULL = 20
NEWS_LIMIT_COMPACT = 8
CHANGES_LIMIT_FULL = 30
CHANGES_LIMIT_COMPACT = 10
SNAPSHOT_CARD_LIMIT_FULL = 20
SNAPSHOT_CARD_LIMIT_COMPACT = 10
INDUSTRY_TOP_N = 8
QUOTE_TTL_SECONDS = 30

# In-process cache TTLs (2026-11-10 speed pass). The first live read after an
# evening restart took ~53s; 乐乐 calls this routinely, so warm reads must stay
# in seconds. Everything here is process-local + thread-safe (single uvicorn
# worker is the norm); `?refresh=true` bypasses all of it.
BRIEF_CACHE_TTL_SECONDS = 75
DASHBOARD_CACHE_TTL_SECONDS = 75
# Off-session the dashboard is close-caliber (2026-08-12 rule) and its write
# paths invalidate this cache, so a 36s vendor round every 75s buys nothing.
DASHBOARD_IDLE_TTL_SECONDS = 900
# Frozen backtest catalog / overview files only change when a job rewrites them.
BACKTEST_CACHE_TTL_SECONDS = 300

_QUOTE_CACHE: dict[str, Any] = {"at": 0.0, "symbols": (), "quotes": {}}


def _last_session_close_ts() -> float:
    """Timestamp of the most recent weekday 15:00 Asia/Shanghai close.

    Same rule as the timeline file cache (``backtest_routes._load_timeline_file``):
    an entry built before the latest close must not be served after it, so a
    pre-close snapshot can never become the evening brief without a rebuild.
    """
    now = datetime.now(SHANGHAI_TZ)
    close = now.replace(hour=15, minute=0, second=0, microsecond=0)
    if now < close:
        close -= timedelta(days=1)
    while close.weekday() >= 5:
        close -= timedelta(days=1)
    return close.timestamp()


def _dashboard_cache_ttl() -> float:
    """75s while the CN session can move the book; 15 min off-session.

    The dashboard path is deliberately close-caliber (2026-08-12 rule) and the
    dashboard / close sync hooks invalidate this cache, so repeating a slow
    vendor round outside market hours buys nothing.
    """
    try:
        from data_sync_service.service.trade_calendar_utils import compute_market_status

        if compute_market_status().get("isMarketOpen"):
            return float(DASHBOARD_CACHE_TTL_SECONDS)
    except Exception:  # noqa: BLE001 — fall back to the short TTL
        return float(DASHBOARD_CACHE_TTL_SECONDS)
    return float(DASHBOARD_IDLE_TTL_SECONDS)


class _TTLCache:
    """Tiny thread-safe TTL cache with an optional session-close expiry.

    Values are cached by reference; callers that hand the value to a request
    copy it first (the brief does) so a reader cannot mutate the cache. The
    TTL may be a fixed number of seconds or a callable evaluated at write time.
    """

    def __init__(
        self,
        ttl_seconds: float | Callable[[], float],
        *,
        close_boundary: bool = True,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._ttl = ttl_seconds
        self._close_boundary = close_boundary
        self._clock = clock or time.time
        self._lock = threading.Lock()
        self._entries: dict[Any, tuple[float, float, Any]] = {}

    def _ttl_now(self) -> float:
        return float(self._ttl() if callable(self._ttl) else self._ttl)

    def get(self, key: Any) -> tuple[Any, float] | None:
        """Return ``(value, age_seconds)`` when fresh, else ``None``."""
        now = self._clock()
        with self._lock:
            row = self._entries.get(key)
            if row is None:
                return None
            built_at, ttl, value = row
            if self._stale(built_at, now, ttl):
                self._entries.pop(key, None)
                return None
            return value, max(0.0, now - built_at)

    def put(self, key: Any, value: Any) -> None:
        with self._lock:
            self._entries[key] = (self._clock(), self._ttl_now(), value)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()

    def _stale(self, built_at: float, now: float, ttl: float) -> bool:
        if now - built_at > ttl:
            return True
        if not self._close_boundary:
            return False
        close_ts = _last_session_close_ts()
        return now >= close_ts and built_at < close_ts


_DASHBOARD_CACHE = _TTLCache(_dashboard_cache_ttl)
_BACKTEST_OVERVIEW_CACHE = _TTLCache(BACKTEST_CACHE_TTL_SECONDS, close_boundary=False)
_BACKTEST_CATALOG_CACHE = _TTLCache(BACKTEST_CACHE_TTL_SECONDS, close_boundary=False)
_BRIEF_CACHE = _TTLCache(BRIEF_CACHE_TTL_SECONDS)
# One build at a time: a startup warmup and an incoming request must not both
# pay for the same cold build (that stampede is what made the first read slow).
_BRIEF_BUILD_LOCK = threading.Lock()


def _today_iso() -> str:
    from data_sync_service.service.trade_calendar_utils import shanghai_today_iso

    return shanghai_today_iso()


def _now_utc_iso() -> str:
    return datetime.now(UTC).isoformat()


def _now_shanghai_iso() -> str:
    return datetime.now(SHANGHAI_TZ).isoformat(timespec="seconds")


def _trim(value: Any) -> Any:
    """Drop None / empty containers so the payload stays tens of KB."""
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            tv = _trim(v)
            if tv is None or tv == {} or tv == []:
                continue
            out[k] = tv
        return out
    if isinstance(value, list):
        return [v for v in (_trim(x) for x in value) if v is not None]
    return value


def _pick(row: dict[str, Any], keys: tuple[str, ...]) -> dict[str, Any]:
    return {k: row.get(k) for k in keys if row.get(k) is not None}


def _fail(block: str, exc: Exception) -> dict[str, Any]:
    logger.warning("agent judgment brief %s failed: %s", block, exc)
    return {"available": False, "error": str(exc) or exc.__class__.__name__}


# ---------------------------------------------------------------------------
# book
# ---------------------------------------------------------------------------


def _holding_row(row: dict[str, Any]) -> dict[str, Any]:
    return _trim(
        _pick(
            row,
            (
                "symbol",
                "name",
                "key",
                "positionPct",
                "costPrice",
                "entryDate",
                "lastClose",
                "lastDate",
                "evaluatedPrice",
                "realtime",
                "pnlPct",
                "drawdownFromPeakPct",
                "holdingDays",
                "action",
                "reason",
                "stopRule",
                "stopLossLine",
                "trailingLine",
                "expireDate",
                "nearStop",
                "nearStopLabel",
                "realtimeWarning",
                "realtimeAlert",
                "pyramidTriggerLine",
                "pyramidAdded",
            ),
        )
    )


def _just_sold(*, day: str, limit: int = 10) -> list[dict[str, Any]]:
    """Paper-book exits from the last 5 sessions (re-entry / whip context)."""
    from data_sync_service.db.paper_trading import list_paper_trades

    since = (datetime.fromisoformat(day) - timedelta(days=10)).isoformat()
    rows = list_paper_trades(status="closed", since=since, limit=60)
    out: list[dict[str, Any]] = []
    for r in rows:
        close_date = str(r.get("closeDate") or "")[:10]
        if not close_date or close_date > day:
            continue
        out.append(
            _trim(
                _pick(
                    r,
                    (
                        "symbol",
                        "market",
                        "source",
                        "entryDate",
                        "entryPrice",
                        "closeDate",
                        "closePrice",
                        "pnlPct",
                        "holdingDays",
                        "closeReason",
                    ),
                )
            )
        )
        if len(out) >= limit:
            break
    return out


def _live_quotes(symbols: list[str]) -> dict[str, Any]:
    """Best-effort realtime quotes for held / just-sold symbols (30s cache)."""
    from data_sync_service.service.portfolio_health import _resolve_holding_ts
    from data_sync_service.service.realtime_quote import fetch_realtime_quotes

    uniq = sorted({s for s in symbols if s})
    if not uniq:
        return {"available": False, "error": "no_symbols"}
    now = time.time()
    if (
        _QUOTE_CACHE["quotes"]
        and _QUOTE_CACHE["symbols"] == tuple(uniq)
        and now - float(_QUOTE_CACHE["at"]) <= QUOTE_TTL_SECONDS
    ):
        return {"available": True, "cached": True, "quotes": _QUOTE_CACHE["quotes"]}

    ts_by_symbol: dict[str, str] = {}
    for sym in uniq[:30]:
        try:
            ts = _resolve_holding_ts(sym)
        except Exception:  # noqa: BLE001 — display-only enrichment
            ts = None
        if ts:
            ts_by_symbol[sym] = str(ts).upper()
    if not ts_by_symbol:
        return {"available": False, "error": "no_ts_code"}

    try:
        resp = fetch_realtime_quotes(sorted(set(ts_by_symbol.values())))
    except Exception as exc:  # noqa: BLE001
        return {"available": False, "error": str(exc)}
    if not isinstance(resp, dict) or not resp.get("ok"):
        return {"available": False, "error": str((resp or {}).get("error") or "quote_failed")}

    by_ts = {str(it.get("ts_code") or "").upper(): it for it in resp.get("items") or []}
    quotes: dict[str, Any] = {}
    for sym, ts in ts_by_symbol.items():
        it = by_ts.get(ts)
        if not it:
            continue
        quotes[sym] = _trim(
            {
                "price": it.get("price"),
                "pctChg": it.get("pct_chg"),
                "preClose": it.get("pre_close"),
                "tradeTime": it.get("trade_time"),
                "tsCode": ts,
                "source": it.get("source") or "realtime_quote",
            }
        )
    _QUOTE_CACHE.update({"at": now, "symbols": tuple(uniq), "quotes": quotes})
    return {"available": bool(quotes), "quotes": quotes}


def _book_block(
    *,
    health: dict[str, Any],
    contract: dict[str, Any],
    day: str,
    compact: bool,
    timings: dict[str, float] | None = None,
) -> dict[str, Any]:
    hk = health.get("hkHealth") or {}
    cn_rows = [r for r in (health.get("holdings") or []) if isinstance(r, dict)]
    hk_rows = [r for r in (hk.get("holdings") or []) if isinstance(r, dict)] if hk else []
    park_rows = [r for r in (health.get("multiAssetHoldings") or []) if isinstance(r, dict)]
    just_sold = _just_sold(day=day, limit=5 if compact else 10)

    symbols = [str(r.get("symbol") or "") for r in cn_rows + hk_rows + park_rows]
    symbols += [str(r.get("symbol") or "") for r in just_sold]
    quote_started = time.perf_counter()
    quotes = _live_quotes(symbols)
    if timings is not None:
        timings["quotesMs"] = round((time.perf_counter() - quote_started) * 1000.0, 1)

    sleeve = contract.get("sleeve") or {}
    return _trim(
        {
            "available": True,
            "asOfDate": day,
            "idle": {
                "idlePct": sleeve.get("idlePct"),
                "parkPct": sleeve.get("parkPct"),
                "action": sleeve.get("action"),
                "pickKey": sleeve.get("pickKey"),
                "parkingSymbol": sleeve.get("symbol"),
                "sellToRepo": sleeve.get("sellToRepo"),
            },
            "cnHoldings": [_holding_row(r) for r in cn_rows],
            "hkHoldings": [_holding_row(r) for r in hk_rows],
            "parkingHoldings": [_holding_row(r) for r in park_rows],
            "liveQuotes": quotes,
            "justSold": just_sold,
            "note": (
                "positions come from the watchlist registry; justSold is the paper "
                "book's exits in the last 10 calendar days"
            ),
        }
    )


# ---------------------------------------------------------------------------
# market facts (one dashboard summary read)
# ---------------------------------------------------------------------------


def _dashboard_summary(
    *,
    refresh: bool = False,
    cache_marks: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """The brief's one dashboard read, cached for ``DASHBOARD_CACHE_TTL_SECONDS``.

    The key carries the as-of day and the include flags, so a different
    ``dashboard_summary`` call shape never shares an entry. Compact and full
    briefs share this cache (compact must not redo the slow dashboard work).
    """
    from data_sync_service.service.dashboard import dashboard_summary

    flags = {
        "include_macro": True,
        "include_sentiment": True,
        "include_news": True,
        "include_industry": True,
    }
    key = ("dashboard", _today_iso(), tuple(sorted(flags.items())))
    if not refresh:
        hit = _DASHBOARD_CACHE.get(key)
        if hit is not None:
            if cache_marks is not None:
                cache_marks["dashboardCacheHit"] = True
            return hit[0]

    value = dashboard_summary(**flags)
    _DASHBOARD_CACHE.put(key, value)
    if cache_marks is not None:
        cache_marks["dashboardCacheHit"] = False
    return value


def _gate_block(summary: dict[str, Any]) -> dict[str, Any]:
    ms = summary.get("marketSentiment") or {}
    gate = ms.get("executionGate") or {}
    if not isinstance(gate, dict) or not gate:
        return {"available": False, "error": "execution gate unavailable"}
    hk = gate.get("hkGate") or {}
    cn = _trim(
        _pick(
            gate,
            (
                "mode",
                "allowNewEntries",
                "marketRegime",
                "indexLight",
                "riskMode",
                "reasons",
                "positionRangeHint",
                "downCount",
                "upgradeReason",
                "srvLevel",
            ),
        )
    )
    return {
        "available": True,
        "cn": cn,
        "hk": _trim(
            _pick(
                hk,
                (
                    "mode",
                    "allowNewEntries",
                    "marketRegime",
                    "indexLight",
                    "riskMode",
                    "reasons",
                    "satelliteNote",
                ),
            )
        )
        if isinstance(hk, dict)
        else None,
    }


def _sentiment_block(summary: dict[str, Any], *, compact: bool) -> dict[str, Any]:
    ms = summary.get("marketSentiment") or {}
    items = [r for r in (ms.get("items") or []) if isinstance(r, dict)]
    latest = items[-1] if items else {}
    days_n = 3 if compact else 5
    days = [
        _trim(
            _pick(
                r,
                (
                    "date",
                    "upCount",
                    "downCount",
                    "flatCount",
                    "upDownRatio",
                    "riskMode",
                    "marketTurnoverCny",
                    "failedLimitUpRate",
                    "yesterdayLimitUpPremium",
                ),
            )
        )
        for r in items[-days_n:]
    ]
    srv = ms.get("srvIndex") or {}
    return _trim(
        {
            "available": bool(items) or bool(srv),
            "asOfDate": ms.get("asOfDate"),
            "riskMode": latest.get("riskMode"),
            "breadth": _pick(latest, ("upCount", "downCount", "flatCount", "upDownRatio")),
            "turnoverCny": latest.get("marketTurnoverCny"),
            "rules": (latest.get("rules") or [])[:6],
            "srv": _pick(srv, ("level", "levelLabel", "overlapCount", "rank")),
            "marketEnvironmentZh": summary.get("marketEnvironmentZh"),
            "recentDays": days,
            "note": "riskMode + breadth only; the full 60-day history stays on the market page",
        }
    )


def _etf_flow_rows(block: dict[str, Any], limit: int) -> list[dict[str, Any]]:
    rows = [r for r in (block.get("items") or []) if isinstance(r, dict)]
    rows.sort(key=lambda r: abs(float(r.get("netFlow1d") or 0.0)), reverse=True)
    return [
        _trim(
            _pick(
                r,
                (
                    "name",
                    "symbol",
                    "netFlow1d",
                    "netFlow3d",
                    "tradeTime",
                    "flowAsOfDate",
                    "flowStatus",
                    "source",
                    "signalDisplay",
                ),
            )
        )
        for r in rows[:limit]
    ]


def _fund_block(summary: dict[str, Any], *, compact: bool) -> dict[str, Any]:
    ind = summary.get("industryFundFlow") or {}
    flow_in = ind.get("flow5d") or {}
    flow_out = ind.get("flow5dOut") or {}
    top_n = 5 if compact else INDUSTRY_TOP_N

    def _rows(block: dict[str, Any]) -> list[dict[str, Any]]:
        out = []
        for r in (block.get("top") or [])[:top_n]:
            if not isinstance(r, dict):
                continue
            series = [p for p in (r.get("series") or []) if isinstance(p, dict)]
            out.append(
                _trim(
                    {
                        "industryName": r.get("industryName"),
                        "sum5d": r.get("sum5d"),
                        "lastDate": series[-1].get("date") if series else None,
                        "lastNetInflow": series[-1].get("netInflow") if series else None,
                    }
                )
            )
        return out

    ms = summary.get("marketSentiment") or {}
    etf_signal = ms.get("etfFlowSignal") or {}
    etf_block = ms.get("etfFundFlow") or {}
    top_by_date = [r for r in (ind.get("topByDate") or []) if isinstance(r, dict)][
        -(3 if compact else 5) :
    ]
    return _trim(
        {
            "available": bool(flow_in or flow_out),
            "asOfDate": ind.get("asOfDate"),
            "days": ind.get("days"),
            "topByDate": top_by_date,
            "inflow5d": _rows(flow_in),
            "outflow5d": _rows(flow_out),
            "etfFlow": {
                "verdict": etf_signal.get("verdict"),
                "broadDirection": etf_signal.get("broadDirection"),
                "sectorDirection": etf_signal.get("sectorDirection"),
                "incomplete": etf_signal.get("incomplete"),
                "asOfDate": etf_block.get("asOfDate"),
                "top": _etf_flow_rows(etf_block, 4 if compact else 6),
            }
            if etf_signal or etf_block
            else None,
            "sources": ind.get("sources"),
        }
    )


def _macro_block(summary: dict[str, Any], *, compact: bool) -> dict[str, Any]:
    snap = summary.get("macroSnapshot") or {}
    lights = [r for r in (snap.get("cnIndexSignals") or []) if isinstance(r, dict)]
    series = [r for r in (snap.get("macro") or []) if isinstance(r, dict)]
    if not lights and not series:
        return {"available": False, "error": "macro snapshot unavailable"}

    def _light(r: dict[str, Any]) -> dict[str, Any]:
        return _trim(
            _pick(
                r,
                (
                    "name",
                    "tsCode",
                    "signal",
                    "positionRange",
                    "close",
                    "pctChg",
                    "ma5",
                    "ma20",
                    "asOfDate",
                    "realtime",
                    "tradeTime",
                    "rules",
                ),
            )
        )

    def _series(r: dict[str, Any]) -> dict[str, Any]:
        return _trim(
            _pick(
                r,
                (
                    "name",
                    "seriesId",
                    "category",
                    "close",
                    "pctChg",
                    "signal",
                    "signalLabel",
                    "ma5",
                    "ma20",
                    "asOfDate",
                    "source",
                    "realtime",
                    "tradeTime",
                    "warning",
                ),
            )
        )

    block: dict[str, Any] = {
        "available": True,
        "asOfDate": summary.get("asOfDate"),
        "indexLights": [_light(r) for r in lights],
        "macro": [_series(r) for r in series][: 6 if compact else 12],
        "warnings": snap.get("warnings"),
    }

    # Prefer live quotes when the summary lights are as-of a prior close: the
    # dashboard path is deliberately close-caliber (2026-08-12 regime rule), so
    # the realtime overlay rides ALONGSIDE it, never replacing it.
    today = _today_iso()
    stale_lights = [r for r in lights if str(r.get("asOfDate") or "")[:10] < today]
    if stale_lights and not compact:
        try:
            from data_sync_service.service.market_regime import get_index_signals

            live_rows = get_index_signals(as_of_date=None, include_breadth=False)
            live = [_light(r) for r in live_rows if isinstance(r, dict) and r.get("realtime")]
            if live:
                block["liveIndexQuotes"] = live
                block["liveNote"] = (
                    "indexLights are close-caliber (prior close); liveIndexQuotes is the "
                    "in-session realtime overlay. The gate still uses the close-caliber lights."
                )
        except Exception as exc:  # noqa: BLE001 — overlay only
            logger.warning("agent judgment brief live index quotes failed: %s", exc)
    return _trim(block)


def _news_block(summary: dict[str, Any], *, compact: bool) -> dict[str, Any]:
    news = summary.get("news") or {}
    items = [r for r in (news.get("items") or []) if isinstance(r, dict)]
    limit = NEWS_LIMIT_COMPACT if compact else NEWS_LIMIT_FULL
    return {
        "available": True,
        "hours": news.get("hours"),
        "total": news.get("total"),
        "items": [
            _trim(
                {
                    "title": r.get("title"),
                    "source": r.get("sourceId"),
                    # Asia/Shanghai, matching the brief's declared clock.
                    "time": _sh_time(r.get("publishedAt")),
                    "timeIso": r.get("publishedAt"),
                    "link": r.get("link"),
                }
            )
            for r in items[:limit]
        ],
        "note": "filtered headlines only (no AI essay, no 研报, no ratings/target prices)",
    }


# ---------------------------------------------------------------------------
# freshness / execution
# ---------------------------------------------------------------------------


def _freshness_block() -> dict[str, Any]:
    from data_sync_service.api.health_routes import datasource_freshness

    sources = datasource_freshness()
    # 研报 / Alpha Radar stay here as RECENCY FLAGS ONLY — never their bodies.
    compact = [
        _trim(
            _pick(
                s,
                ("source", "label", "lastSyncedAt", "ageMinutes", "thresholdMinutes", "stale"),
            )
        )
        for s in sources
    ]
    stale = [s["source"] for s in compact if s.get("stale")]
    return {
        "available": True,
        "generatedAt": _now_utc_iso(),
        "staleCount": len(stale),
        "staleSources": stale,
        "sources": compact,
    }


def _execution_block(*, day: str, compact: bool) -> dict[str, Any]:
    from data_sync_service.db import execution_journal as ej

    block: dict[str, Any] = {"available": True}
    try:
        changes = ej.list_changes(
            trade_date=day, limit=CHANGES_LIMIT_COMPACT if compact else CHANGES_LIMIT_FULL
        )
        block["changes"] = [
            _trim(
                _pick(
                    c,
                    ("changedAt", "scope", "symbol", "field", "oldValue", "newValue", "source"),
                )
            )
            for c in changes
        ]
    except Exception as exc:  # noqa: BLE001 — journal is optional context
        block["changes"] = []
        block["changesError"] = str(exc)
    try:
        snap = ej.fetch_latest_snapshot(trade_date=day)
        if snap:
            cards = [c for c in (snap.get("cards") or []) if isinstance(c, dict)]
            block["latestSnapshot"] = {
                "id": snap.get("id"),
                "tradeDate": snap.get("tradeDate"),
                "capturedAt": snap.get("capturedAt"),
                "source": snap.get("source"),
                "gateMode": (snap.get("gate") or {}).get("mode"),
                "cardCount": len(cards),
                # The snapshot cards ARE the cond-order draft the UI renders.
                "cards": cards[
                    : SNAPSHOT_CARD_LIMIT_COMPACT if compact else SNAPSHOT_CARD_LIMIT_FULL
                ],
            }
    except Exception as exc:  # noqa: BLE001
        block["latestSnapshot"] = None
        block["snapshotError"] = str(exc)
    block["note"] = (
        "changes = execution journal; latestSnapshot.cards = cond-order draft. "
        "Exec-attention ranking is computed in the UI (lib/exec-attention.ts) and is not served here."
    )
    return _trim(block)


# ---------------------------------------------------------------------------
# backtest (the new meat)
# ---------------------------------------------------------------------------


def _window_stats(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not row:
        return None
    return _trim(
        {
            "total": row.get("total"),
            "cagr": row.get("cagr"),
            "mdd": row.get("mdd"),
            "sharpe": row.get("sharpe"),
        }
    )


def _compare_row(
    *,
    timeline_key: str,
    catalog_row: dict[str, Any] | None,
    current_mode: str,
    current_timeline: str,
) -> dict[str, Any]:
    windows = (catalog_row or {}).get("windows") or {}
    return _trim(
        {
            "timelineStrategy": timeline_key,
            "name": (catalog_row or {}).get("name") or timeline_key,
            "catalogKey": (catalog_row or {}).get("key"),
            "status": (catalog_row or {}).get("status"),
            "statusLabel": (catalog_row or {}).get("statusLabel"),
            "role": (catalog_row or {}).get("role"),
            "tag": (catalog_row or {}).get("tag"),
            "updated": (catalog_row or {}).get("updated"),
            "doc": (catalog_row or {}).get("doc"),
            "isCurrentMode": timeline_key == current_timeline,
            "windows": {
                "OOS2": _window_stats(windows.get("OOS2")),
                "train": _window_stats(windows.get("train")),
                "valid": _window_stats(windows.get("valid")),
                "long": _window_stats(windows.get("long")),
            },
            "risk": (catalog_row or {}).get("risk"),
        }
    )


def _recent_timeline_note(*, strategy: str) -> dict[str, Any] | None:
    """Compact recent-window stat from an ALREADY WARM timeline cache.

    Never builds a timeline here (a cold build is minutes). The full curve is
    intentionally not inlined — call ``GET /api/backtest/timeline`` instead.
    """
    from data_sync_service.api import backtest_routes as br

    try:
        stats = br.warm_timeline_recent_stats(strategy=strategy)
    except Exception as exc:  # noqa: BLE001 — optional note
        logger.warning("agent judgment brief recent timeline stats failed: %s", exc)
        return None
    return stats


def _backtest_overview_cached(
    *,
    refresh: bool = False,
    cache_marks: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """``backtest_overview()`` behind a short cache (frozen files, cheap I/O)."""
    from data_sync_service.api import backtest_routes as br

    key = ("backtest_overview", _today_iso())
    if not refresh:
        hit = _BACKTEST_OVERVIEW_CACHE.get(key)
        if hit is not None:
            if cache_marks is not None:
                cache_marks["backtestCacheHit"] = True
            return hit[0]
    try:
        overview = br.backtest_overview()
    except Exception as exc:  # noqa: BLE001
        logger.warning("agent judgment brief backtest overview failed: %s", exc)
        if cache_marks is not None:
            cache_marks["backtestCacheHit"] = False
        return {}
    _BACKTEST_OVERVIEW_CACHE.put(key, overview)
    if cache_marks is not None:
        cache_marks["backtestCacheHit"] = False
    return overview


def _strategy_catalog_cached(
    *,
    refresh: bool = False,
    cache_marks: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """``strategy_catalog()`` behind the same short cache as the overview."""
    from data_sync_service.service.strategy_catalog import strategy_catalog

    key = ("strategy_catalog", _today_iso())
    if not refresh:
        hit = _BACKTEST_CATALOG_CACHE.get(key)
        if hit is not None:
            if cache_marks is not None:
                cache_marks["backtestCacheHit"] = True
            return hit[0]
    try:
        catalog_rows = strategy_catalog()
    except Exception as exc:  # noqa: BLE001
        logger.warning("agent judgment brief strategy catalog failed: %s", exc)
        if cache_marks is not None:
            cache_marks["backtestCacheHit"] = False
        return []
    _BACKTEST_CATALOG_CACHE.put(key, catalog_rows)
    if cache_marks is not None:
        cache_marks["backtestCacheHit"] = False
    return catalog_rows


def _backtest_block(
    *,
    contract: dict[str, Any],
    compact: bool,
    refresh: bool = False,
    cache_marks: dict[str, Any] | None = None,
) -> dict[str, Any]:
    strategy = contract.get("strategy") or {}
    mode = str(strategy.get("mode") or "")
    current_timeline = str(strategy.get("timelineStrategy") or "")

    overview = _backtest_overview_cached(refresh=refresh, cache_marks=cache_marks)
    catalog_rows = _strategy_catalog_cached(refresh=refresh, cache_marks=cache_marks)

    by_timeline: dict[str, dict[str, Any]] = {}
    for row in catalog_rows:
        key = str(row.get("timelineStrategy") or "")
        if not key:
            continue
        # Prefer the canonical / baseline row when a timeline key repeats.
        prev = by_timeline.get(key)
        if prev is None or (row.get("canonical") and not prev.get("canonical")):
            by_timeline[key] = row

    keys: list[str] = list(BACKTEST_COMPARE_KEYS)
    if current_timeline and current_timeline not in keys:
        keys.append(current_timeline)

    comparison = [
        _compare_row(
            timeline_key=key,
            catalog_row=by_timeline.get(key),
            current_mode=mode,
            current_timeline=current_timeline,
        )
        for key in keys
    ]

    recent: dict[str, Any] | None = None
    if not compact:
        recent_map = {}
        for key in keys:
            stats = _recent_timeline_note(strategy=key)
            if stats:
                recent_map[key] = stats
        if recent_map:
            recent = {
                "source": "in_process_timeline_cache",
                "windowSessions": 20,
                "perStrategy": recent_map,
                "note": "trailing-year timeline cache was warm; a cold cache omits this block",
            }

    rolling = overview.get("rollingOos") or {}
    cn_baseline = overview.get("cnBaseline") or {}
    return _trim(
        {
            "available": bool(comparison),
            "generatedAt": _now_utc_iso(),
            "currentMode": {
                "mode": mode,
                "timelineStrategy": current_timeline,
                "label": strategy.get("label"),
                "timelineLabel": strategy.get("timelineLabel"),
                "mappingNote": strategy.get("mappingNote"),
            },
            "windows": {
                "OOS2": "2024-08 ~ 2025-08 (1y out-of-sample)",
                "train": "2025-08 ~ 2026-02",
                "valid": "2026-03 ~ 2026-08 (weak-window check)",
                "long": "2021-08 ~ 2026-08 (~5y)",
            },
            "comparison": comparison,
            "s3Baseline": _trim(
                {
                    "tag": cn_baseline.get("tag"),
                    "generatedAt": cn_baseline.get("generatedAt"),
                    "windows": cn_baseline.get("windows"),
                    "longWindowCN": overview.get("longWindowCN"),
                }
            ),
            "paperRollingOos": _trim(
                {
                    "windowStart": rolling.get("windowStart"),
                    "windowEnd": rolling.get("windowEnd"),
                    "markets": rolling.get("markets"),
                    "warning": rolling.get("warning"),
                }
            )
            if rolling
            else None,
            "recent": recent,
            "caliber": "frozen engine, costs included, negative MDD convention, display data only",
            "note": (
                "full equity curves are NOT inlined (payload size); fetch "
                "GET /api/backtest/timeline?strategy=<key> and start/end for a curve"
            ),
        }
    )


# ---------------------------------------------------------------------------
# assembly
# ---------------------------------------------------------------------------


def _build_health(day: str) -> dict[str, Any] | None:
    from data_sync_service.service.portfolio_health import build_portfolio_health

    try:
        return build_portfolio_health(trade_date=day, markets=("CN", "HK"))
    except Exception as exc:  # noqa: BLE001 — the contract retries without it
        logger.warning("agent judgment brief portfolio health failed: %s", exc)
        return None


def _ms_since(started: float) -> float:
    return round((time.perf_counter() - started) * 1000.0, 1)


def _mode_cache_key() -> tuple[str, str]:
    """Current settings mode + timeline key, so a mode switch misses the cache."""
    try:
        from data_sync_service.api.settings_routes import selected_strategy_mode
        from data_sync_service.service.agent_contract import timeline_strategy_for_mode

        mode = str(selected_strategy_mode() or "")
        return mode, timeline_strategy_for_mode(mode)
    except Exception:  # noqa: BLE001 — cache key only, never blocks the read
        return "", ""


def _cached_brief_response(
    value: dict[str, Any], age_seconds: float, request_started: float
) -> dict[str, Any]:
    """Deep-copied cache hit with honest cache + timing metadata (additive)."""
    brief = copy.deepcopy(value)
    built_timings = brief.get("timingsMs") or {}
    brief["cached"] = True
    brief["cache"] = {
        **(brief.get("cache") or {}),
        "hit": True,
        "ageSeconds": round(age_seconds, 1),
        "ttlSeconds": BRIEF_CACHE_TTL_SECONDS,
    }
    brief["timingsMs"] = {
        "cacheLookupMs": _ms_since(request_started),
        "builtTotalMs": built_timings.get("totalMs"),
        "totalMs": _ms_since(request_started),
    }
    return brief


def build_judgment_brief(
    *,
    trade_date: str | None = None,
    mode: str = "full",
    refresh: bool = False,
) -> dict[str, Any]:
    """Assemble the agent judgment brief for ``trade_date`` (default: today).

    Cached in-process for ``BRIEF_CACHE_TTL_SECONDS``; a cached entry built
    before the latest weekday 15:00 close is never served after it. The
    dashboard / close sync hooks invalidate + re-warm this cache. ``refresh``
    bypasses reads and rebuilds.
    """
    from data_sync_service.service.agent_contract import build_decision_contract

    request_started = time.perf_counter()
    day = str(trade_date or _today_iso())[:10]
    compact = str(mode or "full").strip().lower() == "compact"
    brief_mode = "compact" if compact else "full"
    cache_key = (day, brief_mode, _mode_cache_key(), BRIEF_VERSION)

    if not refresh:
        hit = _BRIEF_CACHE.get(cache_key)
        if hit is not None:
            value, age = hit
            logger.info(
                "judgment brief cache hit mode=%s day=%s age=%.1fs lookup=%.1fms",
                brief_mode,
                day,
                age,
                _ms_since(request_started),
            )
            return _cached_brief_response(value, age, request_started)

    lock_started = time.perf_counter()
    with _BRIEF_BUILD_LOCK:
        lock_wait_ms = _ms_since(lock_started)
        if not refresh:
            # Another thread (startup/warmup or a concurrent read) may have
            # finished the same build while we waited for the lock.
            hit = _BRIEF_CACHE.get(cache_key)
            if hit is not None:
                value, age = hit
                logger.info(
                    "judgment brief cache hit after lock mode=%s day=%s age=%.1fs lookup=%.1fms",
                    brief_mode,
                    day,
                    age,
                    _ms_since(request_started),
                )
                return _cached_brief_response(value, age, request_started)

        timings: dict[str, float] = {"lockWaitMs": lock_wait_ms}
        cache_marks: dict[str, Any] = {}

        block_started = time.perf_counter()
        health = _build_health(day)
        timings["healthMs"] = _ms_since(block_started)

        block_started = time.perf_counter()
        contract = build_decision_contract(trade_date=day, health=health)
        timings["contractMs"] = _ms_since(block_started)

        block_started = time.perf_counter()
        summary: dict[str, Any] | None = None
        summary_error: Exception | None = None
        try:
            summary = _dashboard_summary(refresh=refresh, cache_marks=cache_marks)
        except Exception as exc:  # noqa: BLE001 — blocks degrade individually
            summary_error = exc
            logger.warning("agent judgment brief dashboard summary failed: %s", exc)
        timings["dashboardSummaryMs"] = _ms_since(block_started)

        def _block(name: str, fn: Any) -> Any:
            if summary is None:
                return _fail(name, summary_error or RuntimeError("dashboard summary unavailable"))
            try:
                return fn()
            except Exception as exc:  # noqa: BLE001
                return _fail(name, exc)

        def _safe(name: str, fn: Any) -> Any:
            try:
                return fn()
            except Exception as exc:  # noqa: BLE001
                return _fail(name, exc)

        def _timed(name: str, fn: Any) -> Any:
            started = time.perf_counter()
            try:
                return fn()
            finally:
                timings[name] = _ms_since(started)

        brief: dict[str, Any] = {
            "ok": True,
            "briefId": f"jb:{day}:{contract.get('strategy', {}).get('timelineStrategy')}:"
            f"{contract.get('sleeve', {}).get('action')}:"
            f"{contract.get('sleeve', {}).get('pickKey') or 'REPO'}:{BRIEF_VERSION}",
            "briefVersion": BRIEF_VERSION,
            "generatedAt": _now_utc_iso(),
            "generatedAtShanghai": _now_shanghai_iso(),
            "asOfDate": day,
            "timezone": "Asia/Shanghai",
            "mode": brief_mode,
            "contract": contract,
            "freshness": _timed("freshnessMs", lambda: _safe("freshness", _freshness_block)),
            "gate": _timed("gateMs", lambda: _block("gate", lambda: _gate_block(summary or {}))),
            "sentiment": _timed(
                "sentimentMs",
                lambda: _block(
                    "sentiment", lambda: _sentiment_block(summary or {}, compact=compact)
                ),
            ),
            "fund": _timed(
                "fundMs",
                lambda: _block("fund", lambda: _fund_block(summary or {}, compact=compact)),
            ),
            "macro": _timed(
                "macroMs",
                lambda: _block("macro", lambda: _macro_block(summary or {}, compact=compact)),
            ),
            "book": _timed(
                "bookMs",
                lambda: _safe(
                    "book",
                    lambda: _book_block(
                        health=health or {},
                        contract=contract,
                        day=day,
                        compact=compact,
                        timings=timings,
                    ),
                ),
            ),
            "execution": _timed(
                "executionMs",
                lambda: _safe("execution", lambda: _execution_block(day=day, compact=compact)),
            ),
            "news": _timed(
                "newsMs",
                lambda: _block("news", lambda: _news_block(summary or {}, compact=compact)),
            ),
            "backtest": _timed(
                "backtestMs",
                lambda: _safe(
                    "backtest",
                    lambda: _backtest_block(
                        contract=contract,
                        compact=compact,
                        refresh=refresh,
                        cache_marks=cache_marks,
                    ),
                ),
            ),
            "omitted": list(OMITTED_PACKS),
            "omittedNote": (
                "research_reports / alpha_radar_* / alpha_auto_qa are human-browsing pages; "
                "they are intentionally not inputs to this judgment brief"
            ),
            "cached": False,
            "cache": {
                "hit": False,
                "ageSeconds": 0.0,
                "ttlSeconds": BRIEF_CACHE_TTL_SECONDS,
                "dashboardTtlSeconds": _dashboard_cache_ttl(),
                "backtestTtlSeconds": BACKTEST_CACHE_TTL_SECONDS,
                "dashboardHit": bool(cache_marks.get("dashboardCacheHit")),
                "backtestHit": bool(cache_marks.get("backtestCacheHit")),
                "quotesHit": False,
            },
        }
        book_block = brief.get("book") if isinstance(brief.get("book"), dict) else {}
        quotes_block = (book_block or {}).get("liveQuotes") or {}
        brief["cache"]["quotesHit"] = bool(
            quotes_block.get("cached") if isinstance(quotes_block, dict) else False
        )
        timings.setdefault("quotesMs", 0.0)
        try:
            brief["payloadBytes"] = len(json.dumps(brief, ensure_ascii=False).encode("utf-8"))
        except Exception:  # noqa: BLE001 — size hint only
            pass
        timings["totalMs"] = _ms_since(request_started)
        brief["timingsMs"] = {key: round(float(value), 1) for key, value in timings.items()}

        _BRIEF_CACHE.put(cache_key, brief)
        logger.info(
            "judgment brief built mode=%s day=%s cached=%s lock_wait=%.0fms contract=%.0fms "
            "dashboard=%.0fms freshness=%.0fms execution=%.0fms backtest=%.0fms "
            "quotes=%.0fms total=%.0fms bytes=%s",
            brief_mode,
            day,
            brief["cache"],
            timings.get("lockWaitMs", 0.0),
            timings.get("contractMs", 0.0),
            timings.get("dashboardSummaryMs", 0.0),
            timings.get("freshnessMs", 0.0),
            timings.get("executionMs", 0.0),
            timings.get("backtestMs", 0.0),
            timings.get("quotesMs", 0.0),
            timings.get("totalMs", 0.0),
            brief.get("payloadBytes"),
        )
        return copy.deepcopy(brief)


def reset_judgment_brief_cache() -> None:
    """Drop every in-process brief cache entry (tests / manual reset)."""
    _BRIEF_CACHE.clear()
    _DASHBOARD_CACHE.clear()
    _BACKTEST_OVERVIEW_CACHE.clear()
    _BACKTEST_CATALOG_CACHE.clear()


def invalidate_judgment_brief_cache(reason: str = "") -> None:
    """Invalidate after dashboard / close sync so the evening read is rebuilt."""
    reset_judgment_brief_cache()
    logger.info("judgment brief cache invalidated (%s)", reason or "manual")


def warm_judgment_brief(
    *,
    trade_date: str | None = None,
    mode: str = "full",
    reason: str = "",
) -> bool:
    """Build (and therefore cache) the brief. Fail-open: never raises."""
    try:
        brief = build_judgment_brief(trade_date=trade_date, mode=mode)
    except Exception as exc:  # noqa: BLE001 — warmup must never block startup/sync
        logger.warning(
            "judgment brief warmup failed (%s): %s", reason or "warmup", exc, exc_info=True
        )
        return False
    logger.info(
        "judgment brief warmup ok (%s): mode=%s day=%s total=%sms cached=%s",
        reason or "warmup",
        mode,
        brief.get("asOfDate"),
        (brief.get("timingsMs") or {}).get("totalMs"),
        brief.get("cached"),
    )
    return True


def warm_judgment_brief_async(
    *,
    trade_date: str | None = None,
    mode: str = "full",
    reason: str = "",
) -> None:
    """Kick a background warmup thread (startup / post-sync hooks)."""
    threading.Thread(
        target=warm_judgment_brief,
        kwargs={"trade_date": trade_date, "mode": mode, "reason": reason},
        name="judgment-brief-warmup",
        daemon=True,
    ).start()


# ---------------------------------------------------------------------------
# markdown view (what the Sync & Copy button copies)
# ---------------------------------------------------------------------------


def _md_cell(value: Any) -> str:
    if value is None or value == "":
        return "—"
    text = str(value).replace("|", "/").replace("\n", " ").strip()
    return text or "—"


def _md_table(headers: list[str], rows: list[list[Any]]) -> str:
    if not rows:
        return ""
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]
    for row in rows:
        out.append("| " + " | ".join(_md_cell(c) for c in row) + " |")
    return "\n".join(out)


def _fmt_pct(value: Any, digits: int = 1) -> str:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return "—"
    return f"{num:+.{digits}f}%"


def _fmt_cny(value: Any) -> str:
    """Format a CNY amount as 亿 / 万 (matches the Dashboard copy style)."""
    try:
        num = float(value)
    except (TypeError, ValueError):
        return "—"
    if abs(num) >= 1e8:
        return f"{num / 1e8:.2f}亿"
    if abs(num) >= 1e4:
        return f"{num / 1e4:.2f}万"
    return f"{num:.0f}"


def _fmt_dd(value: Any) -> str:
    """Max-drawdown cell: the frozen docs store magnitude, Timeline stores sign."""
    try:
        num = float(value)
    except (TypeError, ValueError):
        return "—"
    return f"-{abs(num):.1f}%"


def _fmt_num(value: Any, digits: int = 2) -> str:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return "—"
    return f"{num:,.{digits}f}".rstrip("0").rstrip(".")


def _sh_time(value: Any) -> str:
    """Render an ISO timestamp in Asia/Shanghai (the brief's declared clock)."""
    if not value:
        return "—"
    try:
        ts = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return str(value)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    return ts.astimezone(SHANGHAI_TZ).strftime("%m-%d %H:%M")


def _render_contract(brief: dict[str, Any]) -> list[str]:
    contract = brief.get("contract") or {}
    strat = contract.get("strategy") or {}
    sleeve = contract.get("sleeve") or {}
    health = contract.get("health") or {}
    next_leg = contract.get("nextLeg") or {}
    whip = contract.get("whip") or {}
    overlays = contract.get("overlays") or {}

    lines = ["## 1. 契约 contract", ""]
    lines.append(
        f"- mode: `{strat.get('mode')}`（{strat.get('label')}）→ timeline `{strat.get('timelineStrategy')}`（{strat.get('timelineLabel')}）"
    )
    lines.append(
        "- 映射: settings mode `homeport` = 产品母港 M30 → timeline key `homeport_m30`；"
        "timeline key `homeport` 是冻结的 M50 基准（勿混用）"
    )
    lines.append(f"- 服务端口径: {strat.get('mappingNote')}")
    lines.append(f"- contractId: `{contract.get('contractId')}`")
    lines.append(
        f"- sleeve: `{sleeve.get('action')}` · {sleeve.get('label') or ''} · "
        f"pick={sleeve.get('pickKey') or 'REPO'} · holding={sleeve.get('holding')}"
    )
    if sleeve.get("message"):
        lines.append(f"- 说明: {sleeve.get('message')}")
    for key in ("executeHint", "intradayWarn"):
        if sleeve.get(key):
            lines.append(f"- {key}: {sleeve.get(key)}")
    if next_leg:
        lines.append(
            f"- 下一腿: {next_leg.get('key') or '—'} ({next_leg.get('symbol') or '—'}) "
            f"mom60={next_leg.get('mom60')} MA200={next_leg.get('ma200')} "
            f"aboveMA200={next_leg.get('aboveMa200')} · {next_leg.get('reason')}"
        )
    if whip:
        lines.append(
            f"- 鞭打(whip): sameKeyRebuy={whip.get('sameKeyRebuy')} · {whip.get('copy') or '—'}"
        )
    active = overlays.get("active") or []
    if active:
        lines.append(
            "- 生效 overlay: "
            + "; ".join(
                f"{o.get('pickKey')} {o.get('status')}(至{o.get('validUntil') or '—'})"
                for o in active
            )
        )
    else:
        lines.append("- 生效 overlay: 无")
    lines.append(
        f"- 健康: regime={health.get('regime')} gateOpen={health.get('gateOpen')} "
        f"panic={health.get('panicActive')} circuit={health.get('circuitBlocked')} "
        f"持仓={health.get('holdingsCount')} 候选={health.get('candidateCount')} "
        f"scoreFresh={health.get('scoreFresh')}"
    )
    if health.get("exits"):
        lines.append(f"- 触发离场: {', '.join(str(x) for x in health.get('exits') or [])}")
    authority = contract.get("authority") or []
    if authority:
        lines.append(f"- 权限序: {' > '.join(str(x) for x in authority)}")
    lines.append("")
    return lines


def _render_gate(brief: dict[str, Any]) -> list[str]:
    gate = brief.get("gate") or {}
    if not gate.get("available"):
        return ["## 2. 闸门 gate", "", f"- 不可用: {gate.get('error') or '—'}", ""]
    lines = ["## 2. 闸门 gate", ""]
    for market, label in (("cn", "CN"), ("hk", "HK")):
        row = gate.get(market) or {}
        if not row:
            continue
        lines.append(
            f"- {label}: `{row.get('mode')}` allowNewEntries={row.get('allowNewEntries')} "
            f"regime={row.get('marketRegime')} indexLight={row.get('indexLight')} "
            f"reasons={','.join(str(r) for r in (row.get('reasons') or [])) or '—'}"
        )
        if row.get("positionRangeHint"):
            lines.append(f"  - 仓位提示: {row.get('positionRangeHint')}")
        if row.get("satelliteNote"):
            lines.append(f"  - 卫星: {row.get('satelliteNote')}")
    lines.append("")
    return lines


def _render_book(brief: dict[str, Any]) -> list[str]:
    book = brief.get("book") or {}
    lines = ["## 3. 账簿 book", ""]
    idle = book.get("idle") or {}
    lines.append(
        f"- 闲置/停车: idlePct={idle.get('idlePct')} parkPct={idle.get('parkPct')} "
        f"action={idle.get('action')} pick={idle.get('pickKey')} "
        f"parkingSymbol={idle.get('parkingSymbol') or '—'}"
    )
    rows: list[list[Any]] = []
    for group, key in (("CN", "cnHoldings"), ("HK", "hkHoldings"), ("PARK", "parkingHoldings")):
        for row in book.get(key) or []:
            rows.append(
                [
                    group,
                    row.get("symbol"),
                    row.get("name"),
                    row.get("positionPct"),
                    row.get("costPrice"),
                    row.get("evaluatedPrice") or row.get("lastClose"),
                    _fmt_pct(row.get("pnlPct")),
                    row.get("action"),
                    row.get("trailingLine"),
                    row.get("stopLossLine"),
                    row.get("expireDate"),
                ]
            )
    if rows:
        lines.append("")
        lines.append(
            _md_table(
                [
                    "Mkt",
                    "Symbol",
                    "Name",
                    "Pos%",
                    "Cost",
                    "Now",
                    "P&L",
                    "Action",
                    "Trail",
                    "Stop",
                    "Expire",
                ],
                rows,
            )
        )
    else:
        lines.append("- 无持仓")
    quotes = (book.get("liveQuotes") or {}).get("quotes") or {}
    if quotes:
        lines.append("")
        lines.append("- 实时报价:")
        for sym, q in quotes.items():
            lines.append(
                f"  - {sym} {q.get('price')} ({_fmt_pct(q.get('pctChg'), 2)}) @ {q.get('tradeTime') or '—'}"
            )
    just_sold = book.get("justSold") or []
    if just_sold:
        lines.append("")
        lines.append("- 近期已卖（paper）:")
        for r in just_sold:
            lines.append(
                f"  - {r.get('closeDate')} {r.get('symbol')} @ {r.get('closePrice')} "
                f"pnl={_fmt_pct(r.get('pnlPct'), 2)} · {r.get('closeReason') or '—'}"
            )
    lines.append("")
    return lines


def _render_backtest(brief: dict[str, Any], compact: bool) -> list[str]:
    bt = brief.get("backtest") or {}
    lines = ["## 4. 回测 backtest", ""]
    if not bt.get("available"):
        return lines + [f"- 不可用: {bt.get('error') or '—'}", ""]
    cur = bt.get("currentMode") or {}
    lines.append(
        f"- 当前模式: `{cur.get('mode')}` → timeline `{cur.get('timelineStrategy')}`（{cur.get('timelineLabel')}）"
    )
    lines.append(f"- 映射: {cur.get('mappingNote')}")
    windows = bt.get("windows") or {}
    lines.append(
        "- 窗口: "
        + " · ".join(
            f"{k}={windows.get(k)}" for k in ("OOS2", "train", "valid", "long") if windows.get(k)
        )
    )
    lines.append("")
    rows: list[list[Any]] = []
    for row in bt.get("comparison") or []:
        w = row.get("windows") or {}
        long_w = w.get("long") or {}
        valid_w = w.get("valid") or {}

        def _cell(stats: dict[str, Any]) -> str:
            if not stats:
                return "—"
            return (
                f"{_fmt_pct(stats.get('total'))} / DD {_fmt_dd(stats.get('mdd'))} / "
                f"SR {stats.get('sharpe')}"
            )

        rows.append(
            [
                ("★ " if row.get("isCurrentMode") else "") + str(row.get("name")),
                row.get("timelineStrategy"),
                row.get("statusLabel"),
                _cell(w.get("OOS2") or {}),
                _cell(valid_w),
                _cell(long_w),
            ]
        )
    lines.append(_md_table(["策略", "timeline", "状态", "OOS2 1y", "valid 弱窗", "long ~5y"], rows))
    s3 = bt.get("s3Baseline") or {}
    if s3:
        lines.append("")
        s3_label = s3.get("tag") or s3.get("generatedAt") or "walk-forward baseline"
        lines.append(f"- S-3 基线（{s3_label}）：")
        for name, stats in (s3.get("windows") or {}).items():
            if not isinstance(stats, dict):
                continue
            win_rate = stats.get("winRate")
            suffix = f" / 胜率 {(win_rate * 100):.0f}%" if win_rate is not None else ""
            lines.append(
                f"  - {name}: {_fmt_pct(stats.get('totalNetPnlPct'))} / "
                f"DD {_fmt_dd(stats.get('maxDrawdownPct'))} / SR {stats.get('sharpe')} "
                f"/ {stats.get('trades')}笔{suffix}"
            )
        long_cn = s3.get("longWindowCN") or {}
        if long_cn:
            lines.append(
                f"  - long {long_cn.get('window')}: {_fmt_pct(long_cn.get('totalNetPnlPct'), 1)} / "
                f"DD {_fmt_dd(long_cn.get('maxDrawdownPct'))} / SR {long_cn.get('sharpe')} "
                f"/ {long_cn.get('trades')}笔"
            )
    rolling = bt.get("paperRollingOos") or {}
    if rolling:
        lines.append("")
        lines.append(
            f"- paper rolling OOS {rolling.get('windowStart')}~{rolling.get('windowEnd')}: "
            + " · ".join(
                f"{mkt} {_fmt_pct((stats or {}).get('totalNetPnlPct'))} / "
                f"DD {_fmt_dd((stats or {}).get('maxDrawdownPct'))}"
                for mkt, stats in (rolling.get("markets") or {}).items()
            )
        )
    recent = bt.get("recent") or {}
    if recent and not compact:
        lines.append("")
        for key, stats in (recent.get("perStrategy") or {}).items():
            lines.append(
                f"- 近{recent.get('windowSessions')}日 {key}: {_fmt_pct(stats.get('returnPct'))} "
                f"({stats.get('start')}~{stats.get('end')})"
            )
    lines.append(f"- 口径: {bt.get('caliber')}")
    lines.append(f"- 注: {bt.get('note')}")
    lines.append("")
    return lines


def _render_fund(brief: dict[str, Any], compact: bool) -> list[str]:
    fund = brief.get("fund") or {}
    lines = ["## 5. 资金 fund", ""]
    if not fund.get("available"):
        return lines + [f"- 不可用: {fund.get('error') or '—'}", ""]
    lines.append(f"- asOfDate: {fund.get('asOfDate')}")
    inflow = fund.get("inflow5d") or []
    if inflow:
        lines.append(
            "- 5日净流入 Top: "
            + " · ".join(f"{r.get('industryName')} {_fmt_cny(r.get('sum5d'))}" for r in inflow)
        )
    outflow = fund.get("outflow5d") or []
    if outflow and not compact:
        lines.append(
            "- 5日净流出 Top: "
            + " · ".join(f"{r.get('industryName')} {_fmt_cny(r.get('sum5d'))}" for r in outflow)
        )
    etf = fund.get("etfFlow") or {}
    if etf:
        lines.append(
            f"- ETF 资金确认: verdict={etf.get('verdict')} broad={etf.get('broadDirection')} "
            f"sector={etf.get('sectorDirection')} incomplete={etf.get('incomplete')}"
        )
        if not compact:
            for r in etf.get("top") or []:
                lines.append(
                    f"  - {r.get('name')}({r.get('symbol')}) 1D={_fmt_cny(r.get('netFlow1d'))} "
                    f"3D={_fmt_cny(r.get('netFlow3d'))} {r.get('signalDisplay') or ''}".rstrip()
                )
    lines.append("")
    return lines


def _render_macro(brief: dict[str, Any], compact: bool) -> list[str]:
    macro = brief.get("macro") or {}
    lines = ["## 6. 宏观 macro", ""]
    if not macro.get("available"):
        return lines + [f"- 不可用: {macro.get('error') or '—'}", ""]
    rows = [
        [
            r.get("name"),
            r.get("signal"),
            r.get("positionRange"),
            _fmt_pct(r.get("pctChg"), 2),
            _fmt_num(r.get("close")),
            r.get("asOfDate"),
            "RT" if r.get("realtime") else "close",
        ]
        for r in macro.get("indexLights") or []
    ]
    if rows:
        lines.append(_md_table(["指数", "灯", "仓位", "Chg", "Close", "AsOf", "口径"], rows))
        lines.append("")
    series_rows = [
        [
            r.get("name"),
            r.get("category"),
            r.get("signalLabel") or r.get("signal"),
            _fmt_pct(r.get("pctChg"), 2),
            _fmt_num(r.get("close")),
            r.get("asOfDate"),
        ]
        for r in macro.get("macro") or []
    ]
    if series_rows:
        lines.append(_md_table(["宏观", "类", "信号", "Chg", "Close", "AsOf"], series_rows))
        lines.append("")
    live = macro.get("liveIndexQuotes") or []
    if live and not compact:
        lines.append(
            "- 盘中实时覆盖: "
            + " · ".join(
                f"{r.get('name')} {_fmt_num(r.get('close'))} ({_fmt_pct(r.get('pctChg'), 2)}) "
                f"@ {r.get('tradeTime')}"
                for r in live
            )
        )
    if macro.get("warnings"):
        lines.append(f"- 警告: {'; '.join(str(w) for w in macro.get('warnings') or [])}")
    lines.append("")
    return lines


def _render_sentiment(brief: dict[str, Any]) -> list[str]:
    sent = brief.get("sentiment") or {}
    lines = ["## 7. 情绪 sentiment", ""]
    if not sent.get("available"):
        return lines + [f"- 不可用: {sent.get('error') or '—'}", ""]
    breadth = sent.get("breadth") or {}
    lines.append(
        f"- riskMode={sent.get('riskMode')} · 涨/跌={breadth.get('upCount')}/{breadth.get('downCount')} "
        f"· 成交额={_fmt_cny(sent.get('turnoverCny'))} · SRV={(sent.get('srv') or {}).get('level')}"
    )
    if sent.get("rules"):
        lines.append(f"- rules: {' • '.join(str(r) for r in sent.get('rules') or [])}")
    if sent.get("marketEnvironmentZh"):
        lines.append(f"- 环境: {sent.get('marketEnvironmentZh')}")
    rows = [
        [
            d.get("date"),
            d.get("upCount"),
            d.get("downCount"),
            _fmt_num(d.get("upDownRatio")),
            _fmt_cny(d.get("marketTurnoverCny")),
            d.get("riskMode"),
        ]
        for d in sent.get("recentDays") or []
    ]
    lines.append("")
    lines.append(_md_table(["date", "up", "down", "ratio", "turnover", "risk"], rows))
    lines.append("")
    return lines


def _render_news(brief: dict[str, Any]) -> list[str]:
    news = brief.get("news") or {}
    lines = ["## 8. 新闻 news", ""]
    items = news.get("items") or []
    if not items:
        lines.append(f"- 无（hours={news.get('hours')}, total={news.get('total')}）")
        lines.append("")
        return lines
    for r in items:
        lines.append(
            f"- [{r.get('time') or '—'}] {r.get('title')} · {r.get('source')} · {r.get('link') or ''}"
        )
    lines.append("")
    return lines


def _render_freshness(brief: dict[str, Any]) -> list[str]:
    fresh = brief.get("freshness") or {}
    lines = ["## 9. 数据新鲜度 freshness", ""]
    if not fresh.get("available"):
        return lines + [f"- 不可用: {fresh.get('error') or '—'}", ""]
    rows = [
        [
            s.get("label") or s.get("source"),
            s.get("lastSyncedAt"),
            s.get("ageMinutes"),
            "⚠️" if s.get("stale") else "ok",
        ]
        for s in fresh.get("sources") or []
    ]
    lines.append(_md_table(["源", "lastSyncedAt", "age(min)", "状态"], rows))
    lines.append("")
    lines.append("- 注: 研报 / Alpha Radar 只作时效标（recency flag），其正文与榜单不进本简报")
    lines.append("")
    return lines


def _render_execution(brief: dict[str, Any]) -> list[str]:
    ex = brief.get("execution") or {}
    lines = ["## 10. 执行 execution", ""]
    if not ex.get("available"):
        return lines + [f"- 不可用: {ex.get('error') or '—'}", ""]
    snap = ex.get("latestSnapshot") or {}
    if snap:
        lines.append(
            f"- 最新快照: {snap.get('tradeDate')} @ {snap.get('capturedAt')} "
            f"source={snap.get('source')} gate={snap.get('gateMode')} cards={snap.get('cardCount')}"
        )
        for card in (snap.get("cards") or [])[:10]:
            lines.append(
                f"  - {card.get('symbol')} {card.get('action') or card.get('side') or ''} "
                f"{card.get('reason') or ''}".rstrip()
            )
    else:
        lines.append("- 快照: 无")
    changes = ex.get("changes") or []
    if changes:
        lines.append("")
        lines.append("- 变更日志:")
        for c in changes[:15]:
            lines.append(
                f"  - {c.get('changedAt')} {c.get('symbol') or c.get('scope')} "
                f"{c.get('field')}: {c.get('oldValue')} → {c.get('newValue')}"
            )
    lines.append("")
    return lines


def render_judgment_brief_markdown(brief: dict[str, Any], *, compact: bool = False) -> str:
    """Render the same payload as compact Markdown (the Copy All clipboard body)."""
    contract = brief.get("contract") or {}
    strat = contract.get("strategy") or {}
    lines: list[str] = []
    lines.append("# 决策简报 judgment brief")
    lines.append("")
    lines.append(f"- briefId: `{brief.get('briefId')}`")
    lines.append(
        f"- generatedAt: {brief.get('generatedAtShanghai')}（Asia/Shanghai）· UTC {brief.get('generatedAt')}"
    )
    lines.append(f"- asOfDate: {brief.get('asOfDate')} · mode: {brief.get('mode')}")
    lines.append(
        f"- 模式: `{strat.get('mode')}` → timeline `{strat.get('timelineStrategy')}`"
        "（映射: settings 'homeport' = 产品母港 M30 = timeline 'homeport_m30'；"
        "timeline 'homeport' 是冻结 M50，不是产品档）"
    )
    lines.append("")
    lines.extend(_render_contract(brief))
    lines.extend(_render_gate(brief))
    lines.extend(_render_book(brief))
    lines.extend(_render_backtest(brief, compact))
    lines.extend(_render_fund(brief, compact))
    lines.extend(_render_macro(brief, compact))
    lines.extend(_render_sentiment(brief))
    lines.extend(_render_news(brief))
    lines.extend(_render_freshness(brief))
    lines.extend(_render_execution(brief))
    lines.append("## 11. 故意省略 omitted")
    lines.append("")
    lines.append("- " + " · ".join(str(x) for x in brief.get("omitted") or []))
    if brief.get("omittedNote"):
        lines.append(f"- 说明: {brief.get('omittedNote')}")
    lines.append("")
    return "\n".join(lines).strip() + "\n"
