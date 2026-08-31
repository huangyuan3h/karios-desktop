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
from datetime import date
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


@router.get("/twin-star/action")
def twin_star_action() -> dict[str, Any]:
    """双子星 (Twin-Star) 今日操作信号: core pick-strong target + S-gap 卫星闸/候选.

    Signals from the latest completed close (t-1) -> next open execution, same
    semantics as the frozen strategy (docs/backtests/state-bucket-algo-2026-08-31.md §7).
    """
    from data_sync_service.service.twin_star_daily import build_twin_star_daily_action

    try:
        return {"ok": True, **build_twin_star_daily_action()}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"twin-star action failed: {exc}") from exc

_timeline_cache: dict[tuple[str, str], dict[str, Any]] = {}
# Engine context for stock-leg attribution (not file-persisted).
_timeline_engine_cache: dict[tuple[str, str], dict[str, Any]] = {}
TIMELINE_CACHE_DIR = Path(__file__).resolve().parents[3] / "data" / "backtest_reports" / "timeline_cache"
TIMELINE_CACHE_TTL_HOURS = 24
# Bump when pick / trail logic changes so stale file caches are ignored.
_TIMELINE_MODE = "mom_compare_t8"


def _timeline_file(start: str, end: str) -> Path:
    safe = f"{start}_{end}_{_TIMELINE_MODE}.json"
    return TIMELINE_CACHE_DIR / safe


def _load_timeline_file(start: str, end: str) -> dict[str, Any] | None:
    p = _timeline_file(start, end)
    if not p.exists():
        return None
    try:
        import time

        if time.time() - p.stat().st_mtime > TIMELINE_CACHE_TTL_HOURS * 3600:
            return None
        data = json.loads(p.read_text(encoding="utf-8"))
        if data.get("mode") != _TIMELINE_MODE:
            return None
        return data
    except Exception:
        return None

def _save_timeline_file(start: str, end: str, data: dict[str, Any]) -> None:
    try:
        TIMELINE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        _timeline_file(start, end).write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
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
        "2021": 341.0, "2022": 93.0, "2023": -263.0,
        "2024": 606.0, "2025": 956.0, "2026": 1325.0,
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


@router.get("/behavior-audit/latest")
def behavior_audit_latest(limit: int = Query(2, ge=1, le=10)) -> dict[str, Any]:
    """OPT-106: latest REAL-book vs backtest behavior audit (watchlist).

    Compares the user's actual registry holdings against the S-3 backtest
    "should hold" set: extra = 买了不该买 / 该卖没卖, missing = 该持没买.
    Refreshed by POST /backtest/behavior-audit/refresh (simulate ~minutes)
    or the daily close cron.
    """
    from data_sync_service.db.behavior_audit import latest_audit

    return {"ok": True, "items": latest_audit(limit=limit)}


@router.post("/behavior-audit/refresh")
def behavior_audit_refresh(
    tradeDate: str | None = Query(default=None, description="Audit day (YYYY-MM-DD); default today."),
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
        m: {"expected": v["expected"], "actual": v["actual"],
            "extra": v.get("extraList", []), "missing": v.get("missingList", [])}
        for m, v in out["markets"].items() if v.get("available")
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
    trailing_stop_pct: float = Query(0.0, le=0, description="Peak-pullback trailing stop (<=0; 0 disables)."),
    position_pct: float = Query(0.05, gt=0, le=1, description="Per-trade position size (default 0.05 = 5% sleeve)."),
    max_positions: int = Query(10, ge=1, le=100, description="Max simultaneous positions (default 10)."),
    rs_rank_min: float = Query(0.0, ge=0, le=1, description="Min whole-market RS percentile (0 disables; 0.8 = top 20% 20d relative strength)."),
    diverging_scale: float = Query(0.0, ge=0, le=1, description="Position size when regime=Diverging (0 = no entries, 0.5 = half size)."),
    drawdown_circuit_pct: float = Query(0.0, le=0, description="Halt new entries when trailing 30d realized pnl <= this (<=0; 0 disables)."),
    panic_cooldown_days: int = Query(0, ge=0, le=30, description="Days after a sentiment-panic day with no new entries (default 0; S-3 uses 3)."),
    slippage_pct: float = Query(0.0, ge=0, le=2, description="One-way slippage % deducted at entry and exit (default 0; S-3 honest view uses 0.05)."),
    trend_score_min: float = Query(0.0, ge=0, le=100, description="A2 trend-quality score minimum (0 disables; 60 = MA-aligned, near-high, strong RS stocks only)."),
    exclude_boards: str = Query("", description="Comma-separated 3-digit board prefixes to exclude (e.g. '300' = ChiNext; empty = no filter)."),
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
    return {"ok": True, "configs": len(grid), "benchmarks": benchmarks, "results": [r.to_dict() for r in results]}


@router.get("/paper-vs-backtest")
def backtest_paper_vs_backtest() -> dict[str, Any]:
    """C4 paper-vs-backtest report (data/backtest_reports/
    paper_vs_backtest_latest.json), generated by
    scripts/paper_vs_backtest_report.py.

    Reconciles every closed S-3 paper trade against the backtest engine's
    twin trade. 404 when the report script has never been run; the verdict
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


@router.get("/sleeve-nav")
def backtest_sleeve_nav() -> dict[str, Any]:
    """T6 third-asset sleeve NAV report (data/backtest_reports/
    sleeve_nav_latest.json), generated by scripts/sleeve_nav_sim.py.

    Three-window delta of the sleeve-on-idle-cash NAV vs the idle-0% baseline.
    404 when the script has never been run.
    """
    report_path = REPORTS_DIR / "sleeve_nav_latest.json"
    if not report_path.exists():
        raise HTTPException(
            status_code=404,
            detail="no sleeve NAV report yet — run scripts/sleeve_nav_sim.py first",
        )
    try:
        return {"ok": True, "report": json.loads(report_path.read_text(encoding="utf-8"))}
    except (OSError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=500, detail=f"report unreadable: {exc}") from exc


@router.get("/timeline")
def backtest_timeline(
    start: str | None = Query(None, description="Start YYYY-MM-DD, default 1y ago"),
    end: str | None = Query(None, description="End YYYY-MM-DD, default today"),
    strategy: str = Query(
        "pick_strong",
        description="pick_strong (单轨) | twin_star (双子星: 择强核心+S-gap卫星 50/50)",
    ),
) -> dict[str, Any]:
    """Past-year timeline.

    - strategy=pick_strong: 择强单轨 ``mom_compare`` (equal-asset pool).
    - strategy=twin_star: 双子星 — 择强单轨作核心 (50%) + S-gap 状态分桶卫星 (50%)。
    """
    from datetime import date as date_type
    from datetime import timedelta

    today = date_type.today().isoformat()
    if not start:
        start = (date_type.today() - timedelta(days=365)).isoformat()
    end = end or today
    _validate_window(start, end)
    result, _engine = _get_or_build_timeline(start, end, strategy=strategy)
    return result


def _get_or_build_timeline(
    start: str, end: str, *, strategy: str = "pick_strong", need_engine: bool = False
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Return (timeline_result, engine_ctx|None). Rebuilds when engine needed but missing."""
    cache_key = (start, end, strategy)
    cached = _timeline_cache.get(cache_key)
    engine = _timeline_engine_cache.get(cache_key)
    if cached is not None and (not need_engine or engine is not None):
        return cached, engine
    if cached is None and strategy == "pick_strong":
        file_cached = _load_timeline_file(start, end)
        if file_cached is not None and not need_engine:
            _timeline_cache[cache_key] = file_cached
            return file_cached, None

    from data_sync_service.service.backtest_engine import BacktestConfig, BacktestData, simulate
    from data_sync_service.service.pick_strong_track import build_mom_compare_timeline

    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))
    from run_walk_forward import S3_CONFIG  # noqa: E402

    cfg = BacktestConfig(start_date=start, end_date=end, **S3_CONFIG)
    try:
        data = BacktestData(cfg)
        run = simulate(cfg, data)
        try:
            from run_walk_forward import HK_S3_CONFIG  # noqa: E402

            cfg_hk = BacktestConfig(
                start_date=start, end_date=end, **HK_S3_CONFIG
            )  # type: ignore[arg-type]
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
        if strategy == "twin_star":
            from data_sync_service.service.pick_strong_track import build_twin_star_timeline
            from data_sync_service.service.state_bucket_track import build_sgap_timeline

            sat = build_sgap_timeline(start=start, end=end)
            built = build_twin_star_timeline(
                core_rows=result["rows"],
                core_summary=result["summary"],
                sat_rows=sat["rows"],
            )
            result = {
                "ok": True,
                "start": start,
                "end": end,
                "mode": built.get("mode") or "twin_star",
                "strategy": built.get("strategy") or "双子星 (Twin-Star)",
                "summary": built.get("summary"),
                "rows": built.get("rows") or [],
                "coreMode": built.get("coreMode"),
                "coreWeight": built.get("coreWeight"),
                "satWeight": built.get("satWeight"),
                "satSummary": sat.get("summary"),
                "trailPct": built.get("trailPct"),
            }
        _timeline_cache[cache_key] = result
        _timeline_engine_cache[cache_key] = engine_ctx
        if strategy == "pick_strong":
            _save_timeline_file(start, end, result)
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
            pick_pkg = attribute_pick_strong(
                rows, stock_legs_by_day=stock_legs, top_k=top_k
            )
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
    include_matrix: bool = Query(False, description="Also compute the empirical 20d correlation matrix."),
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
        result = evaluate_correlation_cap(positions, industries=industries, include_matrix=include_matrix)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"correlation status failed: {exc}") from exc
    return {"ok": True, **result}
