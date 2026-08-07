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

from data_sync_service.service.backtest_engine import (
    BacktestConfig,
    default_sensitivity_grid,
    run_sensitivity,
    simulate,
)

router = APIRouter(prefix="/api/backtest", tags=["backtest"])

LATEST_REPORT = Path(__file__).resolve().parents[3] / "data" / "backtest_reports" / "latest.json"


def _validate_window(start: str, end: str) -> None:
    if not start or not end:
        raise HTTPException(status_code=422, detail="start and end are required (YYYY-MM-DD)")
    if start > end:
        raise HTTPException(status_code=422, detail="start must be <= end")


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
) -> dict[str, Any]:
    """Run one backtest configuration (v0: signals = historical TrendOK scores)."""
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
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    try:
        run = simulate(config)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"backtest failed: {exc}") from exc
    return {"ok": True, "summary": run.summary.to_dict()}


@router.get("/sensitivity")
def backtest_sensitivity(
    start: str = Query(..., description="Window start (YYYY-MM-DD)."),
    end: str = Query(..., description="Window end (YYYY-MM-DD)."),
) -> dict[str, Any]:
    """Run the default v0 grid (score x hold x stop) and return summaries."""
    _validate_window(start, end)
    try:
        grid = default_sensitivity_grid(start, end)
        results = run_sensitivity(grid)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"sensitivity failed: {exc}") from exc
    return {"ok": True, "configs": len(grid), "results": [r.to_dict() for r in results]}


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
    from data_sync_service.service.correlation import em_industry_for_ts_code, evaluate_correlation_cap

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
