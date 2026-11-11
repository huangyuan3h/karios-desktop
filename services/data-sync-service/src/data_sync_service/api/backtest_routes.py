"""Backtest API (OPT-063 / L3-P2). Read-only; simulations run on demand.

Endpoints:
- GET /api/backtest/run — one configuration, returns summary.
- GET /api/backtest/sensitivity — the default v0 grid, summaries only.
- GET /api/backtest/latest-report — the most recent CLI report
  (data/backtest_reports/latest.json), if one exists.

Neither run endpoint persists anything; it is a parameter-sensitivity tool,
not a release decision basis (see service/backtest_engine docstring).
"""

from __future__ import annotations

import json
import logging
import time
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from data_sync_service.service.backtest_engine import (
    BacktestConfig,
    default_sensitivity_grid,
    load_benchmarks,
    run_sensitivity,
    simulate,
    with_benchmark_excess,
)

router = APIRouter(prefix="/api/backtest", tags=["backtest"])

logger = logging.getLogger(__name__)


def default_timeline_window() -> tuple[str, str]:
    """The documented trailing-year cache window: ``(start, end)`` ISO dates.

    Startup warmup, ``GET /v1/agent/timeline-brief`` and the judgment brief's
    file-cache fallback all use this one window so a warm file is found without
    rebuilding. Feb 29 falls back to Feb 28 of the previous year.
    """
    d0 = date.today()
    end = d0.isoformat()
    try:
        start = d0.replace(year=d0.year - 1).isoformat()
    except ValueError:  # Feb 29 -> Feb 28 of previous year
        start = date(d0.year - 1, 2, 28).isoformat()
    return start, end


def warm_default_timelines(*, force: bool = False) -> None:
    """Prebuild the heavy default timelines in the background (lifespan warmup).

    After a restart the first UI hit used to rebuild the CN+HK S-3 sims inline
    (>2 min, blowing client timeouts on the timeline / return-attribution
    cards). Warm the trailing-year keys for the Live recipe + pick_strong so
    the first page hit lands on a warm in-process cache instead.

    Every warmed strategy is also written to its per-strategy file cache, so
    ``GET /v1/agent/timeline-brief`` and the judgment brief can read a compact
    recent window after a restart without paying for a cold simulate.

    ``force=True`` is the post-close refresh: it ignores stale memory/file
    entries, rebuilds the base engine once, then derives the other tabs from
    that fresh memory (``_base_refresh_force``).
    """
    if "PYTEST_CURRENT_TEST" in __import__("os").environ:
        return
    start, end = default_timeline_window()
    plans: list[dict[str, Any]] = [
        # Expensive base first: harbor writes the file cache that homeport /
        # homeport_m30 / starport derive from without a second engine run.
        {"strategy": "harbor", "need_engine": True},
        # Product default (settings mode 'homeport' -> timeline 'homeport_m30').
        {"strategy": "homeport_m30", "need_engine": True},
        # Yuan's observation tab; standalone satellite + 3-leg parking.
        {"strategy": "starship_b", "need_engine": True},
        {"strategy": "homeport", "need_engine": True},
        {"strategy": "starport", "need_engine": True},
        {"strategy": "pick_strong", "need_engine": True},
    ]
    for kwargs in plans:
        try:
            _get_or_build_timeline(start, end, force=force, **kwargs)
            logger.info("timeline warmup ok: %s %s %s", start, end, kwargs.get("strategy"))
        except Exception:  # noqa: BLE001
            logger.warning("timeline warmup failed: %s", kwargs, exc_info=True)


def warm_default_timelines_async(*, force: bool = False, reason: str = "warmup") -> None:
    """Run ``warm_default_timelines`` in the background, then re-warm the brief.

    Used by close sync (``force=True``): the post-close files are written
    first, then the judgment brief is invalidated/rebuilt so
    ``backtest.recent`` comes from the fresh cache instead of reporting a
    post-close miss. Fail-open: a warmup error only logs.
    """
    import os
    import threading

    if "PYTEST_CURRENT_TEST" in os.environ:
        return

    def _run() -> None:
        try:
            warm_default_timelines(force=force)
        except Exception:  # noqa: BLE001
            logger.warning("timeline async warmup failed (%s)", reason, exc_info=True)
        try:
            from data_sync_service.service.agent_brief import (
                invalidate_judgment_brief_cache,
                warm_judgment_brief,
            )

            invalidate_judgment_brief_cache(reason)
            warm_judgment_brief(reason=reason)
        except Exception:  # noqa: BLE001
            logger.warning(
                "judgment brief rewarm after timelines failed (%s)", reason, exc_info=True
            )

    threading.Thread(target=_run, name="timeline-warmup", daemon=True).start()


_timeline_cache: dict[tuple, dict[str, Any]] = {}
# Engine context for stock-leg attribution (not file-persisted).
_timeline_engine_cache: dict[tuple, dict[str, Any]] = {}
# Write timestamps for the in-memory cache (TTL below): the warmup builds a
# trailing-year window before the close sync, so without a TTL the evening UI
# (and the 18:35 H2 shadow job) would keep reading pre-close rows all day.
_timeline_cache_at: dict[tuple, float] = {}
TIMELINE_CACHE_DIR = (
    Path(__file__).resolve().parents[3] / "data" / "backtest_reports" / "timeline_cache"
)
TIMELINE_CACHE_TTL_HOURS = 24
TIMELINE_MEM_TTL_SECONDS = 15 * 60
# Bump when pick / trail logic changes so stale file caches are ignored.
_TIMELINE_MODE = "mom_compare_t8"
# File-cached strategies. The expensive engine builds plus every product /
# observation key the agent timeline brief needs after a restart. Derived
# blends (homeport / homeport_m30 / starport / twin_star) are persisted under
# their OWN key too: they are cheap to derive from a warm harbor, but on a
# cold process the brief must not rebuild the S-3 sim to answer.
_FILE_CACHE_STRATEGIES = frozenset(
    {
        "pick_strong",
        "harbor",
        "state_bucket",
        "starship",
        "starship_robust",
        "starship_b",
        "homeport",
        "homeport_m30",
        "starport",
        "twin_star",
    }
)

# Strategies accepted by GET /api/backtest/timeline and the agent timeline
# brief. Order is part of the public error message.
TIMELINE_STRATEGIES: tuple[str, ...] = (
    "harbor",
    "homeport",
    "homeport_m30",
    "starport",
    "starship",
    "starship_robust",
    "starship_b",
    "twin_star",
    "pick_strong",
    "state_bucket",
)


def _timeline_mem_get(cache_key: tuple) -> dict[str, Any] | None:
    """In-memory timeline hit honoring TTL + the close boundary.

    The same "built before the latest weekday 15:00 close is stale" rule as
    ``_load_timeline_file``: a startup warmup that ran pre-close must not keep
    serving pre-close rows after the close sync, even inside the 15-minute TTL.
    """
    cached = _timeline_cache.get(cache_key)
    if cached is None:
        return None
    written_at = _timeline_cache_at.get(cache_key, 0.0)
    if time.time() - written_at > TIMELINE_MEM_TTL_SECONDS or written_at < _last_session_close_ts():
        _timeline_cache.pop(cache_key, None)
        _timeline_engine_cache.pop(cache_key, None)
        _timeline_cache_at.pop(cache_key, None)
        return None
    return cached


def _timeline_mem_put(
    cache_key: tuple, result: dict[str, Any], engine: dict[str, Any] | None = None
) -> None:
    import time

    _timeline_cache[cache_key] = result
    _timeline_cache_at[cache_key] = time.time()
    if engine is not None:
        _timeline_engine_cache[cache_key] = engine


def _timeline_mem_drop(cache_key: tuple) -> None:
    _timeline_cache.pop(cache_key, None)
    _timeline_engine_cache.pop(cache_key, None)
    _timeline_cache_at.pop(cache_key, None)


def _row_nav(row: dict[str, Any]) -> float | None:
    for field in ("navMulti", "nav", "satNav", "navSingle"):
        value = row.get(field)
        if isinstance(value, (int, float)) and value:
            return float(value)
    return None


def timeline_recent_stats(result: dict[str, Any], *, sessions: int = 20) -> dict[str, Any] | None:
    """Compact trailing-``sessions`` stat from one timeline payload (pure)."""
    rows = [r for r in (result.get("rows") or []) if isinstance(r, dict)]
    if len(rows) < 2:
        return None
    tail = rows[-max(2, int(sessions) + 1) :]

    start_nav = _row_nav(tail[0])
    end_nav = _row_nav(tail[-1])
    if not start_nav or not end_nav:
        return None
    prev_nav = _row_nav(tail[-2])
    summary = result.get("summary") or {}
    stats: dict[str, Any] = {
        "start": str(tail[0].get("date") or ""),
        "end": str(tail[-1].get("date") or ""),
        "sessions": len(tail) - 1,
        "returnPct": round((end_nav / start_nav - 1.0) * 100.0, 2),
        "windowTotalPct": summary.get("fusedPct"),
        "windowMaxDdPct": summary.get("maxDdFusedPct"),
        "lastDayReturnPct": (round((end_nav / prev_nav - 1.0) * 100.0, 2) if prev_nav else None),
    }
    return {k: v for k, v in stats.items() if v is not None}


def warm_timeline_recent_stats(*, strategy: str, sessions: int = 20) -> dict[str, Any] | None:
    """Compact trailing-``sessions`` stat from an ALREADY WARM memory cache.

    Never builds anything (cold builds take minutes). The file-cache fallback
    lives in ``read_timeline_recent_stats`` and is used by the judgment brief
    when this returns ``None``.
    """
    now = time.time()
    best: tuple[str, dict[str, Any]] | None = None
    for key, result in list(_timeline_cache.items()):
        if not isinstance(key, tuple) or len(key) < 3 or key[2] != strategy:
            continue
        if now - _timeline_cache_at.get(key, 0.0) > TIMELINE_MEM_TTL_SECONDS:
            continue
        end = str(key[1])
        if best is None or end > best[0]:
            best = (end, result)
    if best is None:
        return None
    stats = timeline_recent_stats(best[1], sessions=sessions)
    if stats is None:
        return None
    return {**stats, "cacheEnd": best[0], "cacheSource": "memory"}


def _safe_filename_token(value: str) -> str:
    return "".join(ch if (ch.isalnum() or ch in "-_") else "_" for ch in str(value or "unknown"))


def _file_cache_meta(
    *,
    source: str,
    cache_key: tuple,
    path: Path | None = None,
    fallback: str | None = None,
) -> dict[str, Any]:
    meta: dict[str, Any] = {
        "source": source,
        "cacheKey": list(cache_key),
        "logicMode": _TIMELINE_MODE,
    }
    if source == "memory":
        written_at = _timeline_cache_at.get(cache_key)
        if written_at:
            meta["ageSeconds"] = round(max(0.0, time.time() - written_at), 1)
    elif source == "file" and path is not None:
        try:
            meta["file"] = path.name
            meta["ageSeconds"] = round(max(0.0, time.time() - path.stat().st_mtime), 1)
        except OSError:
            pass
    if fallback:
        meta["fallback"] = fallback
    return meta


def _latest_timeline_file(
    strategy: str,
) -> tuple[str, str, dict[str, Any], Path] | None:
    """Newest still-valid file for ``strategy`` (any start/end window).

    Used when the documented trailing-year window has moved on (a process that
    was not restarted after midnight): the previous file is still fresh under
    the close-expiry rule, so serve it instead of reporting a miss.
    """
    try:
        if not TIMELINE_CACHE_DIR.exists():
            return None
        suffix = f"_{_safe_filename_token(strategy)}_{_TIMELINE_MODE}.json"
        candidates = sorted(
            TIMELINE_CACHE_DIR.glob(f"*{suffix}"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
    except OSError:
        return None
    for path in candidates:
        parts = path.stem.split("_")
        if len(parts) < 2:
            continue
        data = _load_timeline_file(parts[0], parts[1], strategy)
        if data is not None:
            return parts[0], parts[1], data, path
    return None


def read_timeline_cache(
    *,
    strategy: str,
    start: str | None = None,
    end: str | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    """Read-only cache probe (memory → file → miss). Never builds a timeline.

    ``(payload, meta)`` where ``meta.source`` is ``memory`` / ``file`` / ``miss``.
    With no explicit window it first checks the documented trailing-year
    window, then falls back to the newest valid file for that strategy.
    """
    strategy = str(strategy or "homeport_m30")
    if start and end:
        cache_key = (start, end, strategy, _TIMELINE_MODE)
        cached = _timeline_mem_get(cache_key)
        if cached is not None:
            return cached, _file_cache_meta(source="memory", cache_key=cache_key)
        data = _load_timeline_file(start, end, strategy)
        if data is not None:
            _timeline_mem_put(cache_key, data)
            return data, _file_cache_meta(
                source="file", cache_key=cache_key, path=_timeline_file(start, end, strategy)
            )
        return None, _file_cache_meta(source="miss", cache_key=cache_key)

    default_start, default_end = default_timeline_window()
    cache_key = (default_start, default_end, strategy, _TIMELINE_MODE)
    cached = _timeline_mem_get(cache_key)
    if cached is not None:
        return cached, _file_cache_meta(source="memory", cache_key=cache_key)
    data = _load_timeline_file(default_start, default_end, strategy)
    if data is not None:
        _timeline_mem_put(cache_key, data)
        return data, _file_cache_meta(
            source="file",
            cache_key=cache_key,
            path=_timeline_file(default_start, default_end, strategy),
        )

    found = _latest_timeline_file(strategy)
    if found is not None:
        found_start, found_end, found_data, found_path = found
        found_key = (found_start, found_end, strategy, _TIMELINE_MODE)
        _timeline_mem_put(found_key, found_data)
        return found_data, _file_cache_meta(
            source="file",
            cache_key=found_key,
            path=found_path,
            fallback="latest_file",
        )
    return None, _file_cache_meta(source="miss", cache_key=cache_key)


def read_timeline_recent_stats(
    *, strategy: str, start: str | None = None, end: str | None = None, sessions: int = 20
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    """File-cache fallback for the judgment brief's ``backtest.recent``."""
    payload, meta = read_timeline_cache(strategy=strategy, start=start, end=end)
    if payload is None:
        return None, meta
    stats = timeline_recent_stats(payload, sessions=sessions)
    if stats is None:
        return None, {**meta, "reason": "cached timeline has fewer than 2 NAV rows"}
    return stats, meta


def _timeline_file(start: str, end: str, strategy: str = "pick_strong") -> Path:
    """Per-strategy cache file (strategy + logic mode are both in the name)."""
    safe_strategy = _safe_filename_token(strategy)
    safe = f"{start}_{end}_{safe_strategy}_{_TIMELINE_MODE}.json"
    return TIMELINE_CACHE_DIR / safe


def _last_session_close_ts() -> float:
    """Timestamp of the most recent weekday 15:00 Asia/Shanghai close.

    A file written before this boundary predates the latest close and must not
    be served afterwards — otherwise the startup warmup (which may run before
    the close sync) would leave pre-close rows on screen all evening.
    """
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    tz = ZoneInfo("Asia/Shanghai")
    now = datetime.now(tz)
    close = now.replace(hour=15, minute=0, second=0, microsecond=0)
    if now < close:
        close -= timedelta(days=1)
    while close.weekday() >= 5:
        close -= timedelta(days=1)
    return close.timestamp()


def _load_timeline_file(
    start: str, end: str, strategy: str = "pick_strong"
) -> dict[str, Any] | None:
    p = _timeline_file(start, end, strategy)
    if not p.exists():
        return None
    try:
        import time

        mtime = p.stat().st_mtime
        if time.time() - mtime > TIMELINE_CACHE_TTL_HOURS * 3600:
            return None
        if mtime < _last_session_close_ts():
            return None
        data = json.loads(p.read_text(encoding="utf-8"))
        # ``mode`` is the payload's display mode ("harbor" / "homeport" /
        # "starship_b"), not the pick/trail logic version. Files written by
        # the current code carry ``cacheMode`` for the version check; the
        # legacy fallback keeps old pick_strong files (mode == logic mode).
        if data.get("cacheMode") != _TIMELINE_MODE and data.get("mode") != _TIMELINE_MODE:
            return None
        return data
    except Exception:
        return None


def _save_timeline_file(
    start: str, end: str, data: dict[str, Any], strategy: str = "pick_strong"
) -> None:
    try:
        TIMELINE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        payload = dict(data)
        if payload.get("mode") != _TIMELINE_MODE:
            # Distinguish the logic version from the display mode so a harbor
            # / homeport / starship payload is still loadable.
            payload["cacheMode"] = _TIMELINE_MODE
        _timeline_file(start, end, strategy).write_text(
            json.dumps(payload, ensure_ascii=False), encoding="utf-8"
        )
    except Exception:
        pass


LATEST_REPORT = Path(__file__).resolve().parents[3] / "data" / "backtest_reports" / "latest.json"

REPORTS_DIR = Path(__file__).resolve().parents[3] / "data" / "backtest_reports"

# Frozen long-window (2021-08~2026-08, full-market universe, drawdown circuit
# on) result — decided 2026-08-12; source of truth: strategy-params.md §1.
LONG_WINDOW_CN = {
    "window": "2021-08-01 ~ 2026-08-11",
    "totalNetPnlPct": 250.8,
    "maxDrawdownPct": 40.9,
    "sharpe": 2.65,
    "trades": 1401,
    "byYear": {
        "2021": 341.0,
        "2022": 93.0,
        "2023": -263.0,
        "2024": 606.0,
        "2025": 956.0,
        "2026": 1325.0,
    },
}


@router.get("/overview")
def backtest_overview() -> dict[str, Any]:
    """S-3 conclusion board (2026-08-12): frozen baselines + rolling OOS +
    long-window — the source-of-truth view the BacktestPage displays.

    Reads the walk-forward baseline JSONs + rolling OOS snapshot; long-window
    numbers are the 2026-08-12 decided constants (CN line only).
    """

    def _load(name: str) -> dict[str, Any] | None:
        p = REPORTS_DIR / name
        if not p.exists():
            return None
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None

    def _baseline(raw: dict[str, Any] | None) -> dict[str, Any] | None:
        if not raw:
            return None
        windows = {}
        for name, w in (raw.get("results") or {}).items():
            if not isinstance(w, dict):
                continue
            windows[name] = {
                "totalNetPnlPct": w.get("totalNetPnlPct"),
                "winRate": w.get("winRate"),
                "sharpe": w.get("sharpe"),
                "trades": w.get("closed") if w.get("closed") is not None else w.get("trades"),
                "maxDrawdownPct": w.get("maxDrawdownPct"),
                "underpowered": w.get("underpowered"),
            }
        return {
            "generatedAt": raw.get("generatedAt"),
            "tag": raw.get("tag"),
            "windows": windows,
        }

    rolling = _load("rolling_oos_latest.json")
    return {
        "ok": True,
        "cnBaseline": _baseline(_load("walk_forward_baseline.json")),
        "hkBaseline": _baseline(_load("walk_forward_hk_baseline.json")),
        "rollingOos": rolling,
        "longWindowCN": LONG_WINDOW_CN,
    }


def _validate_window(start: str, end: str) -> None:
    if not start or not end:
        raise HTTPException(status_code=422, detail="start and end are required (YYYY-MM-DD)")
    if start > end:
        raise HTTPException(status_code=422, detail="start must be <= end")


@router.get("/recon/latest")
def backtest_recon_latest(limit: int = Query(4, ge=1, le=30)) -> dict[str, Any]:
    """Latest backtest-vs-paper reconciliation snapshots (2026-08-11).

    Weekly job (Monday 07:30) reconciles last Friday's backtest 'should
    hold' vs the paper book; the decision agent / weekly review reads this
    to turn drift into action.
    """
    from data_sync_service.db.reconciliation import latest_recon

    return {"ok": True, "items": latest_recon(limit=limit)}


@router.get("/sleeve-recon/latest")
def sleeve_recon_latest(day: str | None = None) -> dict[str, Any]:
    """OPT-151: core-leg (multi-asset sleeve) paper expected-vs-actual.

    Reproduces the 18:20 sleeve decision from the pre-job paper book, then
    checks the paper book recorded the flows and the user actually executed
    them. Computed on demand — no snapshot table.
    """
    from data_sync_service.service.sleeve_paper_recon import sleeve_paper_recon

    try:
        recon = sleeve_paper_recon(day=day)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"sleeve recon failed: {exc}") from exc
    return {"ok": True, "recon": recon}


@router.get("/satellite-signals/live")
def satellite_signals_live(date: str | None = None) -> dict[str, Any]:
    """OPT-186 slice 1: live 14:30 satellite candidate panel (read-only).

    Same panel the backtest replay computes for the day (gap>3% ->
    amp_1430 rank -> top-1/3 bucket -> skip_t1/C1/C2 + fillable guard ->
    strict pool -> R-wide gate), but from the stored live panel instead of
    a replay. Fills basis: 14:30 raw print; paper adds 30bps RT (slice 2).

    Only serves days with a complete panel; otherwise
    ``decisionAvailable: false`` with a reason (never a partial list).
    Computed on demand — nothing is persisted.
    """
    from datetime import date as date_type

    from data_sync_service.service.satellite_signals import load_live_satellite_signals

    day = date or date_type.today().isoformat()
    try:
        signals = load_live_satellite_signals(day)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"satellite signals failed: {exc}") from exc
    return {"ok": True, "signals": signals}


@router.get("/satellite-signals/live-panel")
def satellite_signals_live_panel() -> dict[str, Any]:
    """OPT-222: today's 14:30 live snapshot (file), written by the 14:30 job.

    ``panel=None`` when the snapshot has never been generated (or was not
    persisted — an incomplete capture leaves the previous file untouched);
    the card then shows "待 14:30 判定". The panel is produced by the same
    ``satellite_signals_for_day`` code as the paper/replay books.
    """
    from data_sync_service.service.satellite_live import load_live_panel
    from data_sync_service.service.trade_calendar_utils import shanghai_today_iso

    panel = load_live_panel()
    today = shanghai_today_iso()
    stale = bool(panel and str(panel.get("tradeDate") or "") != today)
    return {"ok": True, "panel": panel, "stale": stale}


@router.get("/satellite-signals/exit-due")
def satellite_signals_exit_due(
    entries: str = Query(..., description="comma-separated entry dates (YYYY-MM-DD)"),
) -> dict[str, Any]:
    """Rule-based satellite exit due date for arbitrary entry dates.

    The frozen habit recipe holds ``BODY`` sessions counting the entry session
    as session 1, so the due date is the body-th SSE open session from the entry
    (14:30 exit). The card needs it for a position the engine never booked — a
    leg the user entered on a live-panel signal while all slots were full
    (2026-09-22 中科曙光) has no ``openPositions`` row, so it would otherwise
    show "策略外 · 无策略到期".
    """
    from data_sync_service.service.state_bucket_track import BODY
    from data_sync_service.service.trade_calendar_utils import nth_open_date_from

    due: dict[str, str | None] = {}
    for raw in entries.split(","):
        entry = raw.strip()
        if entry:
            due[entry] = nth_open_date_from(entry, BODY)
    return {"ok": True, "body": BODY, "exitDue": due}


@router.get("/satellite-signals/last-1430")
def satellite_signals_last_1430(
    symbols: str = Query(..., description="comma-separated ts_codes (e.g. 002982.SZ)"),
) -> dict[str, Any]:
    """Latest 14:30 raw print per ts_code (``bar_5min``), for held-leg exit pricing.

    The satellite exit is the 14:30 print on the due day, so the card must
    show/record that price — not the last close (2026-09-28: a recorded exit
    used the 09-24 close, off by 2.5%/5.3%).
    """
    from data_sync_service.db import get_connection

    codes = [s.strip() for s in symbols.split(",") if s.strip()]
    prices: dict[str, float] = {}
    as_of: str | None = None
    if codes:
        with get_connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT ON (ts_code) ts_code, trade_date, close FROM bar_5min "
                "WHERE trade_time = '1430' AND ts_code = ANY(%s) AND close IS NOT NULL "
                "ORDER BY ts_code, trade_date DESC",
                (codes,),
            )
            for ts, d, c in cur.fetchall():
                ds = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)
                prices[str(ts)] = float(c)
                as_of = ds if as_of is None or ds > as_of else as_of
    return {"ok": True, "asOf": as_of, "prices": prices}


@router.get("/satellite-paper/user")
def satellite_paper_user(
    start: str | None = None,
    end: str | None = None,
    strategyMode: str = "starship_b",
) -> dict[str, Any]:
    """The user's REAL satellite forward book (read-only, from ``user_trades``).

    Same shape as ``/satellite-paper`` but sourced from the journal
    (``leg='satellite'``) instead of the frozen replay, so the ``paper 20 笔``
    prerequisite counts the trades the user actually made. The replay book stays
    available for the engine-vs-real comparison.
    """
    from data_sync_service.db.user_trades import STRATEGY_MODES
    from data_sync_service.service.satellite_paper import build_user_satellite_book

    if strategyMode not in STRATEGY_MODES:
        raise HTTPException(status_code=400, detail=f"invalid strategyMode: {strategyMode}")
    return build_user_satellite_book(
        start=start,
        end=end,
        strategy_mode=strategyMode,
    )


@router.get("/satellite-paper")
def satellite_paper(start: str | None = None, end: str | None = None) -> dict[str, Any]:
    """OPT-228: forward satellite paper book since the strategy freeze.

    Read-only projection of the frozen habit replay (same engine as the
    timeline), so the card can show the ``paper 20 笔`` go-live prerequisite,
    the current open legs and the realized closed-trade book. Nothing is
    persisted and no gate/order is affected.
    """
    from data_sync_service.service.satellite_paper import build_satellite_paper

    try:
        book = build_satellite_paper(end, start=start)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"satellite paper failed: {exc}") from exc
    return {"ok": True, "paper": book}


@router.get("/behavior-audit/latest")
def behavior_audit_latest(limit: int = Query(2, ge=1, le=10)) -> dict[str, Any]:
    """OPT-106: latest REAL-book vs backtest behavior audit (watchlist).

    Compares the user's actual registry holdings against the S-3 backtest
    "should hold" set: extra = 买了不该买 / 该卖没卖, missing = 该持没买.
    Refreshed by POST /backtest/behavior-audit/refresh (simulate
    ~minutes) or the daily close cron.
    """
    from data_sync_service.db.behavior_audit import latest_audit

    return {"ok": True, "items": latest_audit(limit=limit)}


@router.post("/behavior-audit/refresh")
def behavior_audit_refresh(
    tradeDate: str | None = Query(
        default=None, description="Audit day (YYYY-MM-DD); default today."
    ),
) -> dict[str, Any]:
    """OPT-106: run the behavior audit NOW and persist it.

    Runs the S-3 engine for the valid window (extended to today) — takes a
    few minutes. The watchlist page triggers this after the user's trades.
    """
    from data_sync_service.db.paper_trading import today_iso
    from data_sync_service.service.reconciliation import run_registry_and_persist

    day = tradeDate or today_iso()
    try:
        out = run_registry_and_persist(day)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"behavior audit failed: {exc}") from exc
    summary = {
        m: {
            "expected": v["expected"],
            "actual": v["actual"],
            "extra": v.get("extraList", []),
            "missing": v.get("missingList", []),
        }
        for m, v in out["markets"].items()
        if v.get("available")
    }
    return {"ok": True, "reconDate": out["reconDate"], "markets": summary}


@router.get("/run")
def backtest_run(
    start: str = Query(..., description="Window start (YYYY-MM-DD)."),
    end: str = Query(..., description="Window end (YYYY-MM-DD)."),
    score_threshold: float = Query(85.0, ge=0, le=100),
    max_hold_days: int = Query(5, ge=1, le=60),
    stop_loss_pct: float = Query(-5.0, le=0),
    target_pnl_pct: float = Query(10.0, ge=0),
    score_floor: float = Query(30.0, ge=0, le=100),
    market: str = Query("CN", pattern="^(CN|HK)$"),
    gates: str = Query("full", pattern="^(none|regime|full)$"),
    trailing_stop_pct: float = Query(
        0.0, le=0, description="Peak-pullback trailing stop (<=0; 0 disables)."
    ),
    position_pct: float = Query(
        0.05, gt=0, le=1, description="Per-trade position size (default 0.05 = 5% sleeve)."
    ),
    max_positions: int = Query(
        10, ge=1, le=100, description="Max simultaneous positions (default 10)."
    ),
    rs_rank_min: float = Query(
        0.0,
        ge=0,
        le=1,
        description="Min whole-market RS percentile (0 disables; 0.8 = top 20% 20d relative strength).",
    ),
    diverging_scale: float = Query(
        0.0,
        ge=0,
        le=1,
        description="Position size when regime=Diverging (0 = no entries, 0.5 = half size).",
    ),
    drawdown_circuit_pct: float = Query(
        0.0,
        le=0,
        description="Halt new entries when trailing 30d realized pnl <= this (<=0; 0 disables).",
    ),
    panic_cooldown_days: int = Query(
        0,
        ge=0,
        le=30,
        description="Days after a sentiment-panic day with no new entries (default 0; S-3 uses 3).",
    ),
    slippage_pct: float = Query(
        0.0,
        ge=0,
        le=2,
        description="One-way slippage % on top of the round-trip cost model. Default/expert 0: the cost model already carries slippage (CN 10bps, HK 15bps /side) — do not double-count (E5, 2026-09-10).",
    ),
    trend_score_min: float = Query(
        0.0,
        ge=0,
        le=100,
        description="A2 trend-quality score minimum (0 disables; 60 = MA-aligned, near-high, strong RS stocks only).",
    ),
    exclude_boards: str = Query(
        "",
        description="Comma-separated 3-digit board prefixes to exclude (e.g. '300' = ChiNext; empty = no filter).",
    ),
) -> dict[str, Any]:
    """Run one backtest configuration (signals = historical TrendOK scores
    filtered by entry gates — traffic-light regime / sector flow / mainline)."""
    _validate_window(start, end)
    try:
        config = BacktestConfig(
            start_date=start,
            end_date=end,
            score_threshold=score_threshold,
            max_hold_days=max_hold_days,
            stop_loss_pct=stop_loss_pct,
            target_pnl_pct=target_pnl_pct,
            score_floor=score_floor,
            market=market,
            gates=gates,
            trailing_stop_pct=trailing_stop_pct,
            position_pct=position_pct,
            max_positions=max_positions,
            rs_rank_min=rs_rank_min,
            diverging_scale=diverging_scale,
            drawdown_circuit_pct=drawdown_circuit_pct,
            panic_cooldown_days=panic_cooldown_days,
            slippage_pct=slippage_pct,
            trend_score_min=trend_score_min,
            exclude_boards=exclude_boards,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    try:
        run = simulate(config)
        benchmarks = load_benchmarks(start, end)
        with_benchmark_excess(run.summary, benchmarks)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"backtest failed: {exc}") from exc
    return {"ok": True, "summary": run.summary.to_dict(), "benchmarks": benchmarks}


@router.get("/sensitivity")
def backtest_sensitivity(
    start: str = Query(..., description="Window start (YYYY-MM-DD)."),
    end: str = Query(..., description="Window end (YYYY-MM-DD)."),
) -> dict[str, Any]:
    """Run the default grid (score x hold x stop x gates: none vs full)
    and return summaries."""
    _validate_window(start, end)
    try:
        grid = default_sensitivity_grid(start, end)
        results = run_sensitivity(grid)
        benchmarks = load_benchmarks(start, end)
        for r in results:
            with_benchmark_excess(r, benchmarks)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"sensitivity failed: {exc}") from exc
    return {
        "ok": True,
        "configs": len(grid),
        "benchmarks": benchmarks,
        "results": [r.to_dict() for r in results],
    }


@router.get("/paper-vs-backtest")
def backtest_paper_vs_backtest() -> dict[str, Any]:
    """C4 paper-vs-backtest report (data/backtest_reports/
    paper_vs_backtest_latest.json), generated by
    scripts/paper_vs_backtest_report.py.

    Reconciles every closed S-3 paper trade against the backtest engine's
    matching trade. 404 when the report script has never been run; the verdict
    field flags sample <20 as "not yet conclusive".
    """
    report_path = REPORTS_DIR / "paper_vs_backtest_latest.json"
    if not report_path.exists():
        raise HTTPException(
            status_code=404,
            detail="no C4 report yet — run scripts/paper_vs_backtest_report.py first",
        )
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=500, detail=f"report unreadable: {exc}") from exc
    return {"ok": True, "report": report}


@router.get("/sgap-decay")
def backtest_sgap_decay() -> dict[str, Any]:
    """S-gap decay series for the Backtest page "S-gap 失效趋势" tab.

    Read-only display data (data/backtest_reports/sgap_decay.json), generated
    by scripts/generate_sgap_decay.py from h2f_trades.json (no DB queries).
    404 when the script has never been run; the payload carries rolling
    40/60-trade means, win rates, random-baseline percentiles (K2 N=40/X=75),
    monthly counts, crowding medians, large-order edges and trade-space equity.
    """
    report_path = REPORTS_DIR / "sgap_decay.json"
    if not report_path.exists():
        raise HTTPException(
            status_code=404,
            detail="no sgap-decay report yet — run scripts/generate_sgap_decay.py first",
        )
    try:
        decay = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=500, detail=f"report unreadable: {exc}") from exc
    return {"ok": True, "decay": decay}


@router.get("/factor-vault")
def backtest_factor_vault() -> dict[str, Any]:
    """Factor cold-storage vault for the Backtest page "因子冷库" tab.

    Read-only display data (data/backtest_reports/factor_vault.json),
    generated by scripts/generate_factor_vault.py from frozen audit inputs
    (h2f_trades + h2d blotters + embedded FACTOR_DEFS; no DB queries).
    404 when the script has never been run; the payload carries one uniform
    H2k health row per once-effective factor (percentile vs random + net
    after costs, status 冷库/观察/复活, sparkline, position 0/10/20) plus
    the daily state history. History lives in factor_vault_history.json and
    is merged here when present (never required for the table itself).
    """
    report_path = REPORTS_DIR / "factor_vault.json"
    if not report_path.exists():
        raise HTTPException(
            status_code=404,
            detail="no factor-vault report yet — run scripts/generate_factor_vault.py first",
        )
    try:
        vault = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=500, detail=f"report unreadable: {exc}") from exc
    hist_path = REPORTS_DIR / "factor_vault_history.json"
    history: list[dict[str, Any]] = []
    try:
        if hist_path.exists():
            raw = json.loads(hist_path.read_text(encoding="utf-8"))
            if isinstance(raw, dict) and isinstance(raw.get("history"), list):
                history = raw["history"]
            elif isinstance(raw, list):
                history = raw
    except (OSError, json.JSONDecodeError):
        history = []
    return {"ok": True, "vault": vault, "history": history}


@router.get("/fleet")
def backtest_fleet() -> dict[str, Any]:
    """Fleet (Jian Dui) for the Backtest page "舰队" tab.

    Read-only display data (data/backtest_reports/fleet.json), generated
    by scripts/generate_fleet.py from the frozen audit (scratch/fleet;
    no DB queries). 404 when the script has never been run; the payload
    carries the 4-window metrics table (fleet + base), defense-line
    states, and equity vs starship-B/base/HS300. Starship B stays the
    live baseline; fleet is research/display only (no orders).
    """
    report_path = REPORTS_DIR / "fleet.json"
    if not report_path.exists():
        raise HTTPException(
            status_code=404,
            detail="no fleet report yet — run scripts/generate_fleet.py first",
        )
    try:
        fleet = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=500, detail=f"report unreadable: {exc}") from exc
    return {"ok": True, "fleet": fleet}


@router.get("/core-audit")
def backtest_core_audit(
    day: str = Query(default=None, description="Audit day (YYYY-MM-DD); default today."),
) -> dict[str, Any]:
    """Core-holding operation audit (2026-08-21): did manual trades follow the
    rules? Pyramid ADDs are checked against the pre-trade blended cost via
    reverse replay; SELLs against the -5% stop / panic de-risk; ETF ADDs
    against the sleeve MA200. Warns are surfaced as a violations list.
    """
    from data_sync_service.service.core_holding_audit import audit_core_holdings

    audit_day = day or date.today().isoformat()
    try:
        return {"ok": True, **audit_core_holdings(day=audit_day)}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"core audit failed: {exc}") from exc


@router.get("/timeline")
def backtest_timeline(
    start: str | None = Query(None, description="Start YYYY-MM-DD, default 1y ago"),
    end: str | None = Query(None, description="End YYYY-MM-DD, default today"),
    strategy: str = Query(
        "homeport_m30",
        description=(
            "harbor | homeport | homeport_m30 | starport | starship | starship_robust "
            "| starship_b | twin_star | pick_strong | state_bucket"
        ),
    ),
    refresh: bool = Query(
        False,
        description=(
            "Bypass the memory + file caches and rebuild (post-close refresh). "
            "A pre-close warmup is also invalidated automatically after 15:00."
        ),
    ),
) -> dict[str, Any]:
    """Past-year / walk-forward timeline.

    Query ``start``/``end`` select the window. UI labels trailing vs product
    past-year vs OOS2/train/valid (gate) vs holdout (read-only).

    - strategy=harbor: 港湾 (S-3 core + idle-cash ETF parking, H2 2pt; sunset base).
    - strategy=homeport: 母港 M50 = 港湾 x B3 risk-budget 50/50 (frozen; starport's base).
    - strategy=homeport_m30: 母港 M30 defensive tier = 港湾 70% x B3 30% (H-MIX-TUNE, current baseline).
    - strategy=starport: 星港 = 母港 x satellite overlay w=1/3 (H-B3-SAT, display only).
    - strategy=starship: 星舰 = satellite standalone + H2 ETF parking (research
      display; current H2 pure-sleeve arm).
    - strategy=starship_robust: 稳健星舰 H2-a25 = satellite + 25% H2 ETF + 75% B3
      (canonical research/display arm; K3 risk is reported, not hidden).
    - strategy=starship_b: 星舰 B = satellite + idle cash 100% parked in the
      3-leg inverse-vol {国债, 黄金, 纳指} blend (research/display; shallower DD,
      stronger 2022-23 stress than H2-a25 at lower long return).
    - strategy=twin_star: 双子星 = 港湾 x satellite overlay w=1/2 (parallel
      comparison entry, kept 2026-09-14; not Live).
    - strategy=pick_strong: legacy 择强单轨 ``mom_compare`` (research only).
    - strategy=state_bucket: 状态分桶 research timeline (独立腿).
    """
    from datetime import date as date_type
    from datetime import timedelta

    today = date_type.today().isoformat()
    if not start:
        start = (date_type.today() - timedelta(days=365)).isoformat()
    end = end or today
    _validate_window(start, end)
    allowed = TIMELINE_STRATEGIES
    if strategy not in allowed:
        raise HTTPException(
            status_code=400,
            detail=f"unknown strategy={strategy!r}; use {'|'.join(allowed)}",
        )
    if refresh is True:
        result, _engine = _get_or_build_timeline(start, end, strategy=strategy, force=True)
    else:
        result, _engine = _get_or_build_timeline(start, end, strategy=strategy)
    return result


def _timeline_brief_parking(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    """Last parking leg held / traded, from an already-cached timeline payload."""
    if not payload:
        return None
    rows = [r for r in (payload.get("rows") or []) if isinstance(r, dict)]
    row = rows[-1] if rows else {}
    out: dict[str, Any] = {
        "key": row.get("pick"),
        "symbol": row.get("pickTs"),
        "sides": row.get("parkedSides"),
        "trail": row.get("parkedTrail"),
        "returnPct": row.get("parkedRetPct"),
        "idlePct": row.get("idlePct"),
    }
    held = payload.get("parkedHeld")
    if isinstance(held, dict):
        out["held"] = {
            k: held.get(k)
            for k in ("key", "name", "ts", "since", "weight")
            if held.get(k) is not None
        }
    blotter = payload.get("parkedBlotter")
    if isinstance(blotter, list) and blotter:
        out["blotterTail"] = [b for b in blotter[-5:] if isinstance(b, dict)]
    trimmed = {k: v for k, v in out.items() if v not in (None, {}, [])}
    return trimmed or None


def build_timeline_brief(
    *,
    strategy: str | None = None,
    start: str | None = None,
    end: str | None = None,
) -> dict[str, Any]:
    """Compact, cache-first timeline note for the decision agent (read-only).

    Resolves the strategy from settings when omitted (settings ``homeport`` →
    timeline ``homeport_m30``; the timeline key ``homeport`` is the frozen M50
    base). Reads only the in-process / file timeline caches — NEVER runs a
    cold simulate, so the caller never blocks for minutes. On a miss it says
    so and points at the warmup / full timeline endpoint.
    """
    from data_sync_service.service.agent_contract import (
        MAPPING_NOTE,
        MODE_LABELS,
        TIMELINE_LABELS,
        timeline_strategy_for_mode,
    )

    settings_mode = ""
    try:
        from data_sync_service.api.settings_routes import selected_strategy_mode

        settings_mode = str(selected_strategy_mode() or "")
    except Exception:  # noqa: BLE001 — the brief must stay readable
        settings_mode = ""

    timeline = str(strategy or "").strip() or timeline_strategy_for_mode(settings_mode)
    if timeline not in TIMELINE_STRATEGIES:
        raise HTTPException(
            status_code=400,
            detail=f"unknown strategy={timeline!r}; use {'|'.join(TIMELINE_STRATEGIES)}",
        )
    if start or end:
        if not start or not end:
            raise HTTPException(
                status_code=422,
                detail="start and end must be provided together (YYYY-MM-DD)",
            )
        _validate_window(start, end)
        requested = True
        window_start, window_end = start, end
    else:
        requested = False
        window_start, window_end = default_timeline_window()

    payload, cache_meta = read_timeline_cache(strategy=timeline, start=start, end=end)
    served_start = str(
        (payload or {}).get("start") or cache_meta.get("cacheKey", [window_start])[0]
    )
    served_end = str(
        (payload or {}).get("end") or cache_meta.get("cacheKey", [None, window_end])[1]
    )

    out: dict[str, Any] = {
        "ok": True,
        "available": payload is not None,
        "generatedAt": datetime.now(UTC).isoformat(),
        "settingsMode": settings_mode or None,
        "settingsLabel": MODE_LABELS.get(settings_mode) if settings_mode else None,
        "timelineStrategy": timeline,
        "timelineLabel": TIMELINE_LABELS.get(timeline),
        "window": {
            "start": served_start if payload is not None else window_start,
            "end": served_end if payload is not None else window_end,
            "requested": requested,
            "source": "requested"
            if requested
            else (cache_meta.get("fallback") or "default_trailing_year"),
        },
        "cache": cache_meta,
        "mappingNote": MAPPING_NOTE,
        "curve": {
            "endpoint": "/api/backtest/timeline",
            "query": {
                "strategy": timeline,
                "start": served_start if payload is not None else window_start,
                "end": served_end if payload is not None else window_end,
            },
        },
        "note": (
            "Compact view of the LIVE timeline cache window. The frozen "
            "COMPARE/vintage table in the judgment brief (backtest.comparison) "
            "is a different source — do not mix the two without labeling."
        ),
    }
    if payload is None:
        out["reason"] = (
            "timeline cache cold for this window (memory + file miss); "
            "warm_default_timelines() / close-sync force refresh fills it — "
            "the brief never runs a cold simulate"
        )
        out["warmup"] = "warm_default_timelines"
        return out

    rows = [r for r in (payload.get("rows") or []) if isinstance(r, dict)]
    last = rows[-1] if rows else {}
    prev = rows[-2] if len(rows) >= 2 else {}
    last_nav = _row_nav(last) if last else None
    prev_nav = _row_nav(prev) if prev else None
    peak = max((_row_nav(r) or 0.0) for r in rows) if rows else 0.0
    summary = payload.get("summary") or {}
    out["summary"] = {
        k: summary.get(k)
        for k in (
            "fusedPct",
            "basePct",
            "maxDdFusedPct",
            "harborPct",
            "harborWeight",
            "riskPct",
            "riskMaxDdPct",
            "parkingTrades",
            "trailExits",
            "parkedTrades",
            "parkedTrailExits",
        )
        if summary.get(k) is not None
    }
    out["lastDay"] = {
        k: v
        for k, v in {
            "date": last.get("date"),
            "nav": round(last_nav, 6) if last_nav else None,
            "returnPct": (
                round((last_nav / prev_nav - 1.0) * 100.0, 2) if last_nav and prev_nav else None
            ),
            "drawdownFromPeakPct": (
                round((peak - last_nav) / peak * 100.0, 2) if peak and last_nav else None
            ),
            "idlePct": last.get("idlePct"),
            "deployedPct": last.get("deployedPct"),
            "positions": last.get("positions"),
        }.items()
        if v is not None
    }
    out["parking"] = _timeline_brief_parking(payload)
    out["timelineMode"] = payload.get("mode") or _TIMELINE_MODE
    return {k: v for k, v in out.items() if v is not None}


@router.get("/strategy-catalog")
def backtest_strategy_catalog() -> dict[str, Any]:
    """Frozen strategy-family catalog for the Backtest page 策略总览 tab.

    Display only (2026-09-14 clean caliber, costs included); every row cites its
    authoritative doc. Baseline is 母港M30 (Yuan 2026-10-04); 星舰B is observation.
    """
    from data_sync_service.service.strategy_catalog import strategy_catalog

    return {"ok": True, "strategies": strategy_catalog()}


@router.get("/b3-state")
def backtest_b3_state() -> dict[str, Any]:
    """Current-month B3 target/drift/rebalance list (母港/星港 decision aid).

    Display only; B3 paper/live accounting is pending OPT-186.
    """
    from data_sync_service.service.strategy_today import b3_state

    return b3_state()


@router.get("/starship-b-state")
def backtest_starship_b_state() -> dict[str, Any]:
    """Current-month 星舰 B park-leg target/drift/rebalance list (3-leg inv-vol).

    Same math as ``b3_state`` but over {国债, 黄金, 纳指}. Display only.
    """
    from data_sync_service.service.strategy_today import starship_b_state

    return starship_b_state()


def _hsgt_money_to_yi(value: float) -> float:
    """Tushare ``moneyflow_hsgt`` money fields → 亿元.

    ``north_money`` / ``hgt`` / ``sgt`` are published in **百万元** (official
    docs + ``tushare/stock/reference.py``), so 22448.99 → 224.5 亿元
    (2024-04-26, the single-day northbound net-buy record).
    """
    return round(value / 1e2, 1)


def _load_flow_rows(start: str, end: str) -> list[dict[str, Any]]:
    """TIP-017 资金流全景 rows (display only, fail-open): one per trading day.

    Fields: etfShareYi 亿份 (4 码 per-code forward-fill SUM, same convention
    as the B-gate), etfShareD20Pct %, gateOn bool (B 闸: 沪深300 < MA200 且
    份额 20 日净增 ≤ 0 → 国家队撤退态), marginTrillion 万亿元, marginD20Pct %,
    northDailyYi 亿元, northD20Yi 亿元 (20-session cumulative), smNetPct %
    (小单净买 / turnover, 2023+). natDailyYi = Σ Δshare×NAV (亿元, via daily-table
    ETF closes), natD20Yi / marginD20Yi / northD20Yi = 20-session cumulative
    net flow (亿元, 1:1:1 money trio).
    400-day warm lookback feeds every MA/window; T+1 publications (margin/north)
    forward-fill per field (5-session staleness cap).

    Data-quality guards (2026-10 datafix):
    - margin: only dates with all 3 exchanges (SSE/SZSE/BSE) feed totals;
      partial days (e.g. 2026-09-30 SSE-only) are dropped from marg_map so
      the full-market SUM never halves overnight. Dropped dates surface as
      ``marginIncomplete`` on the following complete row's tooltip context
      via the /flow-series meta (see backtest_flow_series).
    - north: 2024-08-19 caliber break — pre-break north_money is daily net
      inflow (百万元), post-break it is turnover scale (always positive).
      Post-break rows carry ``northIsTurnover=True`` and the 20-session
      cumulative restarts at the break (never mixes net + turnover).
    """
    try:
        from datetime import date as date_type
        from datetime import timedelta

        from data_sync_service.db import get_connection
        from data_sync_service.service import risk_state_gate as rsg
        from data_sync_service.service.cn_risk_state_sync import BROAD_ETF_CODES

        warm = (date_type.fromisoformat(start) - timedelta(days=400)).isoformat()

        def _levels(sql: str, params: tuple) -> list[tuple[str, float]]:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    return [(str(r[0]), float(r[1])) for r in cur.fetchall() if r[1] is not None]

        shares = rsg.load_etf_share_series(list(BROAD_ETF_CODES), warm, end)
        share_maps = {c: dict(s) for c, s in shares.items()}
        index_series = rsg.load_close_series("index_daily", "000300.SH", warm, end)
        # ETF closes for the money conversion (万份 × 元 → 亿元).
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT trade_date, ts_code, close FROM daily WHERE ts_code = ANY(%s) "
                    "AND trade_date BETWEEN %s AND %s ORDER BY trade_date",
                    (list(BROAD_ETF_CODES), warm, end),
                )
                nav_maps: dict[str, dict[str, float]] = {c: {} for c in BROAD_ETF_CODES}
                for d, ts, c in cur.fetchall():
                    if c is not None:
                        nav_maps[str(ts)][str(d)] = float(c)
        # 国家队 daily net subscription (亿元): Σ_c Δshare_c × NAV_c / 1e4.
        nat_daily: dict[str, float] = {}
        for c in BROAD_ETF_CODES:
            s_map = share_maps.get(c, {})
            n_map = nav_maps.get(c, {})
            days = sorted(s_map)
            for i in range(1, len(days)):
                prev_d, cur_d = days[i - 1], days[i]
                nav = n_map.get(cur_d)
                if nav is None:
                    continue
                # skip Δ across data gaps (>10 calendar days) to avoid fake spikes
                if (date_type.fromisoformat(cur_d) - date_type.fromisoformat(prev_d)).days > 10:
                    continue
                nat_daily[cur_d] = (
                    nat_daily.get(cur_d, 0.0) + (s_map[cur_d] - s_map[prev_d]) * nav / 1e4
                )
        nat_daily = {d: round(v, 1) for d, v in nat_daily.items()}
        nat_days = sorted(nat_daily)
        nat20: dict[str, float] = {}
        for i in range(19, len(nat_days)):
            nat20[nat_days[i]] = round(sum(nat_daily[d] for d in nat_days[i - 19 : i + 1]), 1)
        marg = _levels(
            "SELECT trade_date, SUM(rzye) FROM cn_margin_total WHERE trade_date "
            "BETWEEN %s::date - INTERVAL '400 days' AND %s::date "
            "AND trade_date IN (SELECT trade_date FROM cn_margin_total "
            "GROUP BY trade_date HAVING COUNT(DISTINCT exchange_id) >= 3) "
            "GROUP BY trade_date ORDER BY trade_date",
            (start, end),
        )
        hsgt = _levels(
            "SELECT trade_date, north_money FROM cn_moneyflow_hsgt WHERE trade_date "
            "BETWEEN %s::date - INTERVAL '400 days' AND %s::date ORDER BY trade_date",
            (start, end),
        )
        # Incomplete margin days (partial publication) for the UI warning.
        try:
            with get_connection() as _conn:
                with _conn.cursor() as _cur:
                    _cur.execute(
                        "SELECT trade_date FROM cn_margin_total WHERE trade_date "
                        "BETWEEN %s::date AND %s::date "
                        "GROUP BY trade_date HAVING COUNT(DISTINCT exchange_id) < 3",
                        (start, end),
                    )
                    margin_incomplete_dates = sorted(str(r[0]) for r in _cur.fetchall())
        except Exception:  # noqa: BLE001
            margin_incomplete_dates = []

        # Per-code forward fill → totals (identical to the B-gate convention).
        last_by_code: dict[str, float] = {}
        totals: dict[str, float] = {}
        for d in sorted({x for s in share_maps.values() for x in s}):
            for c, s in share_maps.items():
                if d in s:
                    last_by_code[c] = s[d]
            if last_by_code:
                totals[d] = sum(last_by_code.values())
        tot_days = sorted(totals)
        etf20: dict[str, float] = {}
        for i in range(20, len(tot_days)):
            base = totals[tot_days[i - 20]]
            if base:
                etf20[tot_days[i]] = round((totals[tot_days[i]] / base - 1) * 100, 2)

        marg_map = dict(marg)
        marg20: dict[str, float] = {}
        for i in range(20, len(marg)):
            base = marg[i - 20][1]
            if base:
                marg20[marg[i][0]] = round((marg[i][1] / base - 1) * 100, 2)
        # 两融 daily net buy (亿元): Δrzye between consecutive observations.
        margin_daily: dict[str, float] = {}
        for i in range(1, len(marg)):
            prev_d, cur_d = marg[i - 1][0], marg[i][0]
            if (date_type.fromisoformat(cur_d) - date_type.fromisoformat(prev_d)).days > 10:
                continue
            margin_daily[cur_d] = round((marg[i][1] - marg[i - 1][1]) / 1e8, 1)
        # 20-session cumulative net buy (亿元): exact level difference rzye[t]-rzye[t-20].
        marg20sum: dict[str, float] = {}
        for i in range(20, len(marg)):
            marg20sum[marg[i][0]] = round((marg[i][1] - marg[i - 20][1]) / 1e8, 1)

        north_daily = {d: _hsgt_money_to_yi(v) for d, v in hsgt}  # 百万元 → 亿元
        # Caliber break 2024-08-19: post-break north_money is turnover, not
        # net inflow. The 20-session cumulative must not mix the two regimes,
        # so it restarts at the first post-break observation.
        try:
            from data_sync_service.db.cn_risk_state import HSGT_BREAK_DATE
        except Exception:  # noqa: BLE001
            HSGT_BREAK_DATE = "2024-08-19"
        north20: dict[str, float] = {}
        run: list[tuple[str, float]] = []
        for d, v in hsgt:
            if run and d >= HSGT_BREAK_DATE > run[0][0]:
                run = []
            run.append((d, v))
            if len(run) >= 20:
                # Only emit when the 20-day window sits in one regime.
                window = run[-20:]
                if (window[0][0] >= HSGT_BREAK_DATE) == (window[-1][0] >= HSGT_BREAK_DATE):
                    north20[d] = _hsgt_money_to_yi(sum(x for _, x in window))
        north_is_turnover = {d: (d >= HSGT_BREAK_DATE) for d, _ in hsgt}

        sm_net_pct: dict[str, float] = {}
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT trade_date, sm_net, turnover FROM cn_flow_daily "
                    "WHERE trade_date BETWEEN %s::date AND %s::date ORDER BY trade_date",
                    (start, end),
                )
                for d, sm, to in cur.fetchall():
                    if sm is not None and to:
                        sm_net_pct[str(d)] = round(float(sm) / float(to) * 100, 2)

        calendar = sorted(
            {d for d in tot_days if start <= d <= end}
            | {d for d in marg_map if start <= d <= end}
            | {d for d in north_daily if start <= d <= end}
        )
        gate = rsg.national_team_state_by_day(index_series, shares, calendar)

        rows: list[dict[str, Any]] = []
        etf_yi: float | None = None
        marg_tn: float | None = None
        last20 = {
            "etfShareD20Pct": None,
            "marginD20Pct": None,
            "northD20Yi": None,
            "smNetPct": None,
            "natD20Yi": None,
            "marginD20Yi": None,
        }
        since = {k: 99 for k in last20}
        src = {
            "etfShareD20Pct": etf20,
            "marginD20Pct": marg20,
            "northD20Yi": north20,
            "smNetPct": sm_net_pct,
            "natD20Yi": nat20,
            "marginD20Yi": marg20sum,
        }
        etf_age = marg_age = 99
        north20_regime: str | None = None  # regime of last consumed north20 value
        for d in calendar:
            if totals.get(d) is not None:
                etf_yi = round(totals[d] / 1e4, 1)
                etf_age = 0
            else:
                etf_age += 1
            if marg_map.get(d) is not None:
                marg_tn = round(marg_map[d] / 1e12, 3)
                marg_age = 0
            else:
                marg_age += 1
            for k, m in src.items():
                if d in m:
                    last20[k] = m[d]
                    since[k] = 0
                    if k == "northD20Yi":
                        north20_regime = "turnover" if d >= HSGT_BREAK_DATE else "net"
                else:
                    since[k] += 1
            # Never forward-fill the 20d cumulative across the caliber break:
            # a post-break day showing a pre-break net-buy sum (or vice versa)
            # would mix two different series.
            cur_regime = "turnover" if d >= HSGT_BREAK_DATE else "net"
            if north20_regime is not None and north20_regime != cur_regime:
                stale_north = None
            else:
                stale_north = last20["northD20Yi"] if since["northD20Yi"] <= 5 else None
            stale = {k: (v if since[k] <= 5 else None) for k, v in last20.items()}
            stale["northD20Yi"] = stale_north
            rows.append(
                {
                    "date": d,
                    "etfShareYi": etf_yi if etf_age <= 5 else None,
                    "etfShareD20Pct": stale["etfShareD20Pct"],
                    "gateOn": gate.get(d),
                    "marginTrillion": marg_tn if marg_age <= 5 else None,
                    "marginD20Pct": stale["marginD20Pct"],
                    "northDailyYi": north_daily.get(d),
                    "northD20Yi": stale["northD20Yi"],
                    "northIsTurnover": bool(north_is_turnover.get(d, d >= "2024-08-19")),
                    "marginIncompleteDate": d in set(margin_incomplete_dates),
                    "smNetPct": stale["smNetPct"],
                    "natDailyYi": nat_daily.get(d),
                    "natD20Yi": stale["natD20Yi"],
                    "marginDailyYi": margin_daily.get(d),
                    "marginD20Yi": stale["marginD20Yi"],
                }
            )
        # Attach quality meta on the function for the endpoint (no signature change).
        rows_meta = {  # type: ignore[attr-defined]
            "marginIncompleteDates": margin_incomplete_dates,
            "hsgtBreakDate": "2024-08-19",
        }
        try:
            _load_flow_rows.meta = rows_meta  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            pass
        return rows
    except Exception:  # noqa: BLE001 — display-only; panel renders empty on failure
        return []


def _load_flow_by_day(start: str, end: str) -> dict[str, dict[str, float | None]]:
    """TIP-017 flow layer for the timeline (display only, fail-open)."""
    try:
        return {
            r["date"]: {
                "etfShareD20Pct": r["etfShareD20Pct"],
                "marginD20Pct": r["marginD20Pct"],
                "northD20": r["northD20Yi"],
                "smNetPct": r["smNetPct"],
            }
            for r in _load_flow_rows(start, end)
        }
    except Exception:  # noqa: BLE001 — display-only; timeline renders without flow
        return {}


@router.get("/flow-series")
def backtest_flow_series(
    start: str | None = Query(None, description="Start YYYY-MM-DD, default 1y ago"),
    end: str | None = Query(None, description="End YYYY-MM-DD, default today"),
) -> dict[str, Any]:
    """TIP-017 资金流全景 (display only): 国家队(宽基份额+B闸) / 两融 / 北向 / 散户."""
    from datetime import date as date_type
    from datetime import timedelta

    if not start:
        start = (date_type.today() - timedelta(days=365)).isoformat()
    end = end or date_type.today().isoformat()
    _validate_window(start, end)
    rows = _load_flow_rows(start, end)
    meta = getattr(_load_flow_rows, "meta", None) or {
        "marginIncompleteDates": [],
        "hsgtBreakDate": "2024-08-19",
    }
    return {"ok": True, "start": start, "end": end, "rows": rows, **meta}


def _base_refresh_force(start: str, end: str, strategy: str, force: bool) -> bool:
    """Force a derived strategy's base only when it is not already fresh.

    A forced warmup (close sync) must not rebuild the S-3 engine once per
    derived tab: the first plan rebuilds ``harbor``, later plans reuse that
    fresh memory entry.
    """
    if not force:
        return False
    return _timeline_mem_get((start, end, strategy, _TIMELINE_MODE)) is None


def _remember_timeline(
    cache_key: tuple, result: dict[str, Any], engine: dict[str, Any] | None = None
) -> None:
    """Cache a freshly built timeline in memory AND (per strategy) on disk.

    File persistence is what makes ``backtest.recent`` / the agent timeline
    brief survivable across a process restart without re-running the engine.
    """
    _timeline_mem_put(cache_key, result, engine)
    if len(cache_key) >= 3 and cache_key[2] in _FILE_CACHE_STRATEGIES:
        _save_timeline_file(cache_key[0], cache_key[1], result, cache_key[2])


def _get_or_build_timeline(
    start: str,
    end: str,
    *,
    strategy: str = "homeport_m30",
    need_engine: bool = False,
    force: bool = False,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Return (timeline_result, engine_ctx|None). Rebuilds when engine needed but missing.

    ``force=True`` bypasses every cache layer for an explicit post-close refresh.
    """
    cache_key = (start, end, strategy, _TIMELINE_MODE)
    if force:
        _timeline_mem_drop(cache_key)
    cached = None if force else _timeline_mem_get(cache_key)
    engine = _timeline_engine_cache.get(cache_key)
    if cached is not None and (not need_engine or engine is not None):
        return cached, engine
    if cached is None and strategy in _FILE_CACHE_STRATEGIES:
        file_cached = None if force else _load_timeline_file(start, end, strategy)
        if file_cached is not None and not need_engine:
            _timeline_mem_put(cache_key, file_cached)
            return file_cached, None

    # Standalone state-bucket: no S-3 / pick-strong dependency.
    # ``starship`` is the same standalone leg under its 2026-09-14 name (display alias),
    # but must replay the Live/habit caliber so it matches the frozen standalone doc.
    if strategy in ("state_bucket", "starship", "starship_robust", "starship_b"):
        from data_sync_service.service.state_bucket_track import (
            A25_PARKING_MODE,
            STARSIP_B_PARKING_MODE,
            build_state_bucket_timeline,
        )

        parked = strategy in ("starship", "starship_robust", "starship_b")
        parked_mode = {
            "starship_robust": A25_PARKING_MODE,
            "starship_b": STARSIP_B_PARKING_MODE,
        }.get(strategy, "sleeve")
        try:
            result = build_state_bucket_timeline(
                start=start,
                end=end,
                recipe="habit" if parked else "frozen",
                parked_display=parked,
                parked_mode=parked_mode,
            )
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(
                status_code=500, detail=f"timeline {strategy} failed: {exc}"
            ) from exc
        if strategy == "starship":
            result = {
                **result,
                "mode": "starship",
                "strategy": "星舰",
                "satWeight": 1.0,
                "baseKey": "satellite",
                "parkingMode": "h2",
            }
        elif strategy == "starship_robust":
            result = {
                **result,
                "mode": "starship_robust",
                "strategy": "稳健星舰 H2-a25",
                "satWeight": 1.0,
                "baseKey": "satellite",
                "parkingMode": "h2_a25",
            }
        elif strategy == "starship_b":
            result = {
                **result,
                "mode": "starship_b",
                "strategy": "星舰 B",
                "satWeight": 1.0,
                "baseKey": "satellite",
                "parkingMode": "starship_b",
            }
        _remember_timeline(cache_key, result, None)
        return result, None

    # Homeport derives from the cached Harbor timeline (no second engine run).
    if strategy == "homeport":
        from data_sync_service.service.homeport import blend_homeport_timeline

        harbor_result, engine_ctx = _get_or_build_timeline(
            start,
            end,
            strategy="harbor",
            need_engine=need_engine,
            force=_base_refresh_force(start, end, "harbor", force),
        )
        try:
            result = blend_homeport_timeline(harbor_result)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=f"timeline homeport failed: {exc}") from exc
        _remember_timeline(cache_key, result, engine_ctx)
        return result, engine_ctx

    # Homeport M30 defensive tier (H-MIX-TUNE 2026-09-16): same legs as
    # homeport, harbor weight 0.7 instead of frozen 0.5. The M50 line above
    # stays frozen (starport's base).
    if strategy == "homeport_m30":
        from data_sync_service.service.homeport import blend_homeport_timeline

        harbor_result, engine_ctx = _get_or_build_timeline(
            start,
            end,
            strategy="harbor",
            need_engine=need_engine,
            force=_base_refresh_force(start, end, "harbor", force),
        )
        try:
            result = blend_homeport_timeline(harbor_result, w_harbor=0.7)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(
                status_code=500, detail=f"timeline homeport_m30 failed: {exc}"
            ) from exc
        _remember_timeline(cache_key, result, engine_ctx)
        return result, engine_ctx

    # Starport derives from the cached Homeport timeline + standalone satellite rows.
    if strategy == "starport":
        from data_sync_service.service.starport import blend_starport_timeline
        from data_sync_service.service.state_bucket_track import build_state_bucket_timeline

        homeport_result, engine_ctx = _get_or_build_timeline(
            start,
            end,
            strategy="homeport",
            need_engine=need_engine,
            force=_base_refresh_force(start, end, "homeport", force),
        )
        try:
            sat_result = build_state_bucket_timeline(start=start, end=end, recipe="habit")
            result = blend_starport_timeline(homeport_result, sat_result)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=f"timeline starport failed: {exc}") from exc
        _remember_timeline(cache_key, result, engine_ctx)
        return result, engine_ctx

    # Twin Star (parallel comparison entry) derives from Harbor + satellite w=1/2.
    if strategy == "twin_star":
        from data_sync_service.service.starport import blend_twin_star_timeline
        from data_sync_service.service.state_bucket_track import build_state_bucket_timeline

        harbor_result, engine_ctx = _get_or_build_timeline(
            start,
            end,
            strategy="harbor",
            need_engine=need_engine,
            force=_base_refresh_force(start, end, "harbor", force),
        )
        try:
            sat_result = build_state_bucket_timeline(start=start, end=end, recipe="habit")
            result = blend_twin_star_timeline(harbor_result, sat_result)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(
                status_code=500, detail=f"timeline twin_star failed: {exc}"
            ) from exc
        _remember_timeline(cache_key, result, engine_ctx)
        return result, engine_ctx

    import sys

    from data_sync_service.service.backtest_engine import BacktestConfig, BacktestData, simulate
    from data_sync_service.service.pick_strong_track import build_mom_compare_timeline

    sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))
    from run_walk_forward import S3_CONFIG  # noqa: E402  # pyright: ignore[reportMissingImports]

    cfg = BacktestConfig(start_date=start, end_date=end, **S3_CONFIG)
    try:
        data = BacktestData(cfg)
        run = simulate(cfg, data)
        if strategy != "harbor":
            try:
                from run_walk_forward import (  # noqa: E402  # pyright: ignore[reportMissingImports]
                    HK_S3_CONFIG,
                )

                cfg_hk = BacktestConfig(start_date=start, end_date=end, **HK_S3_CONFIG)  # type: ignore[arg-type]
                data_hk = BacktestData(cfg_hk)
                run_hk = simulate(cfg_hk, data_hk)
                hk_by_day = {str(s.get("date")): s for s in run_hk.positions_by_day}
                for s in run.positions_by_day:
                    day = str(s.get("date"))
                    hk_s = hk_by_day.get(day)
                    if hk_s:
                        s["positions"] = (s.get("positions") or []) + (hk_s.get("positions") or [])
                for ts, mp in data_hk.close_by_ts_day.items():
                    if ts not in data.close_by_ts_day:
                        data.close_by_ts_day[ts] = mp
                    else:
                        data.close_by_ts_day[ts].update(mp)
                data.calendar = sorted(set(data.calendar) | set(data_hk.calendar))
            except Exception:
                pass
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"timeline S-3 failed: {exc}") from exc

    try:
        if strategy == "harbor":
            from data_sync_service.service.harbor import build_harbor_timeline
            from data_sync_service.service.portfolio_nav_sim import engine_nav_by_day_from_run

            built = build_harbor_timeline(
                calendar=data.calendar,
                positions_by_day=run.positions_by_day,
                engine_nav_by_day=engine_nav_by_day_from_run(data.calendar, run.nav_curve),
            )
        else:
            built = build_mom_compare_timeline(
                calendar=data.calendar,
                positions_by_day=run.positions_by_day,
                close_by_ts_day=data.close_by_ts_day,
            )
        result = {
            "ok": True,
            "start": start,
            "end": end,
            "mode": built.get("mode") or _TIMELINE_MODE,
            "strategy": built.get("strategy") or "择强单轨",
            "summary": built.get("summary"),
            "rows": built.get("rows") or [],
            "trailPct": built.get("trailPct"),
        }
        engine_ctx = {
            "calendar": data.calendar,
            "positions_by_day": run.positions_by_day,
            "close_by_ts_day": data.close_by_ts_day,
        }
        _remember_timeline(cache_key, result, engine_ctx)
        return result, engine_ctx
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"timeline multi failed: {exc}") from exc


@router.get("/return-attribution")
def backtest_return_attribution(
    start: str | None = Query(None, description="Start YYYY-MM-DD, default 1y ago"),
    end: str | None = Query(None, description="End YYYY-MM-DD, default today"),
    books: str = Query(
        "pick_strong,user",
        description="Comma books: pick_strong,user",
    ),
    top_k: int = Query(10, ge=1, le=50, description="Top |dayRet| days"),
) -> dict[str, Any]:
    """涨跌归因: pick-strong NAV by asset + optional user_trades realized pnl."""
    from datetime import date as date_type
    from datetime import timedelta

    from data_sync_service.service.return_attribution import (
        attribute_pick_strong,
        attribute_user_trades,
        build_stock_legs_by_day,
        day_returns_from_nav,
    )

    today = date_type.today().isoformat()
    if not start:
        start = (date_type.today() - timedelta(days=365)).isoformat()
    end = end or today
    _validate_window(start, end)
    book_set = {b.strip() for b in books.split(",") if b.strip()}

    out: dict[str, Any] = {
        "ok": True,
        "start": start,
        "end": end,
        "note": (
            "pickStrong = 100% hard-switch day attribution; "
            "userTrades = realized SELL gross pnl (not same NAV path)."
        ),
    }

    if "pick_strong" in book_set:
        try:
            timeline, engine = _get_or_build_timeline(start, end, need_engine=True)
            rows = timeline.get("rows") or []
            day_rows = day_returns_from_nav(rows)
            stock_legs = None
            if engine is not None:
                stock_legs = build_stock_legs_by_day(
                    day_rows=day_rows,
                    positions_by_day=engine["positions_by_day"],
                    close_by_ts_day=engine["close_by_ts_day"],
                    calendar=engine["calendar"],
                )
            pick_pkg = attribute_pick_strong(rows, stock_legs_by_day=stock_legs, top_k=top_k)
            # Align totalGeo with timeline summary when available
            summary = timeline.get("summary") or {}
            fused = summary.get("fusedPct")
            if fused is not None:
                pick_pkg["timelineFusedPct"] = fused
            out["pickStrong"] = pick_pkg
        except HTTPException:
            raise
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(
                status_code=500, detail=f"pick_strong attribution failed: {exc}"
            ) from exc

    if "user" in book_set:
        try:
            from data_sync_service.db.user_trades import fetch_sell_rows

            sells = fetch_sell_rows()
            out["userTrades"] = attribute_user_trades(sells, start=start, end=end)
        except Exception as exc:  # noqa: BLE001
            out["userTrades"] = {
                "closedCount": 0,
                "bySymbol": {},
                "byBucket": {},
                "insufficient": True,
                "error": str(exc),
            }

    return out


@router.get("/latest-report")
def backtest_latest_report() -> dict[str, Any]:
    """Return the most recent CLI report (data/backtest_reports/latest.json).

    404 when the CLI has never been run. This is the human/AI-agent entry
    point for "what did the last sensitivity sweep say".
    """
    if not LATEST_REPORT.exists():
        raise HTTPException(
            status_code=404,
            detail="no backtest report yet — run scripts/run_backtest.py first",
        )
    try:
        return {"ok": True, "report": json.loads(LATEST_REPORT.read_text(encoding="utf-8"))}
    except (OSError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=500, detail=f"report unreadable: {exc}") from exc


@router.get("/exit-attribution")
def backtest_exit_attribution(
    days: int = Query(5, ge=1, le=30, description="Forward trading days after close."),
    limit: int = Query(500, ge=1, le=2000, description="Max closed trades to examine."),
) -> dict[str, Any]:
    """Exit attribution (L3-P3): forward return by close reason."""
    from data_sync_service.service.exit_attribution import analyze_exit_attribution

    try:
        result = analyze_exit_attribution(days=days, limit=limit)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"exit attribution failed: {exc}") from exc
    if "error" in result:
        raise HTTPException(status_code=500, detail=str(result["error"]))
    return {"ok": True, **result}


@router.get("/weekly-review")
def weekly_review(
    end: str = Query(
        default=None,
        description="ISO week end date (YYYY-MM-DD, inclusive). Defaults to today.",
    ),
) -> dict[str, Any]:
    """Weekly decision-quality review (L3-P4 / decision Agent M2 v0).

    Aggregates one ISO week of decision volume, NET paper outcomes, exit
    attribution and funnel health into a structured payload + Chinese
    markdown report (copy-paste into an AI agent for commentary).
    """
    from data_sync_service.service.weekly_review import build_weekly_review

    end_date = end or date.today().isoformat()
    try:
        result = build_weekly_review(end_date=end_date)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"weekly review failed: {exc}") from exc
    return result


class WeeklyPlanRequest(BaseModel):
    markdown: str


@router.post("/weekly-plan")
def weekly_plan_store(req: WeeklyPlanRequest) -> dict[str, Any]:
    """Store the decision agent's next-week action plan (morning_briefs
    brief_type='weekly-plan', keyed by this Monday). Frontend / history read
    it via GET /api/backtest/weekly-plan."""
    from datetime import UTC, datetime, timedelta

    from data_sync_service.db.morning_brief import upsert_brief

    today = datetime.now(tz=UTC).date()
    monday = today - timedelta(days=today.weekday())
    brief = upsert_brief(
        brief_date=monday.isoformat(),
        brief_type="weekly-plan",
        items=[],
        macro_overview=None,
        model_version="weekly_plan_v1",
        source_item_ids=None,
        markdown=req.markdown,
    )
    return {"ok": True, "brief": brief}


@router.get("/weekly-plan")
def weekly_plan_latest() -> dict[str, Any]:
    """Latest stored next-week action plan (brief_type='weekly-plan')."""
    from data_sync_service.db.morning_brief import fetch_latest_brief

    brief = fetch_latest_brief(brief_type="weekly-plan")
    return {"ok": True, "plan": brief}


@router.get("/correlation-status")
def correlation_status(
    include_matrix: bool = Query(
        False, description="Also compute the empirical 20d correlation matrix."
    ),
) -> dict[str, Any]:
    """Portfolio correlation firewall status (L3-P5 / V7.0-01).

    Semantic factor-cluster exposure of the CURRENT watchlist book +
    over-limit clusters (new BUY/ADD blocked there) + optional empirical
    top correlation pairs. Read-only.
    """
    from data_sync_service.db.watchlist_automation import list_registry
    from data_sync_service.service.correlation import (
        em_industry_for_ts_code,
        evaluate_correlation_cap,
    )

    try:
        registry = list_registry()
        positions: list[dict[str, Any]] = []
        industries: dict[str, str] = {}
        for r in registry:
            # list_registry flattens the payload into the row (positionPct
            # is a top-level key).
            sym = str(r.get("symbol") or "")
            try:
                pct = float(r.get("positionPct") or 0)
            except (TypeError, ValueError):
                pct = 0.0
            positions.append({"symbol": sym, "positionPct": pct})
            if sym.startswith("CN:"):
                from data_sync_service.service.paper_trading import _resolve_ts_code

                resolved = _resolve_ts_code(sym)
                if resolved:
                    ind = em_industry_for_ts_code(resolved[1])
                    if ind:
                        industries[sym] = ind
        result = evaluate_correlation_cap(
            positions, industries=industries, include_matrix=include_matrix
        )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"correlation status failed: {exc}") from exc
    return {"ok": True, **result}
