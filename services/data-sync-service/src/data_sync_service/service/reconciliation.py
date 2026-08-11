"""Weekly backtest-vs-paper reconciliation service (2026-08-11).

Answers for one trading day: what the S-3 backtest (CN + HK lines) says we
SHOULD hold at that day's close (engine end-of-day snapshots) vs what the
paper book ACTUALLY holds — per market, with entry-date alignment checks.
The Monday cron runs it for last Friday and persists the snapshot
(db/reconciliation), so drift between the backtest world and the real book
is measured weekly instead of silently diverging.

This is the "矫正操作" loop: any missing/extra/entry-skew row is a decision
point for the weekly review / decision agent, not a surprise.
"""

from __future__ import annotations

import logging
from typing import Any

from data_sync_service.db.paper_trading import list_paper_trades
from data_sync_service.service.backtest_engine import BacktestConfig, BacktestData, simulate

logger = logging.getLogger(__name__)

# Same three fixed windows as run_walk_forward (S-3 audit standard).
WINDOWS: dict[str, tuple[str, str]] = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
}

S3_CONFIG: dict[str, float | int | str] = {
    "score_threshold": 65.0,
    "max_hold_days": 60,
    "stop_loss_pct": -5.0,
    "target_pnl_pct": 100.0,
    "score_floor": 0.0,
    "market": "CN",
    "gates": "full",
    "trailing_stop_pct": -8.0,
    "position_pct": 0.10,
    "max_positions": 20,
    "rs_rank_min": 0.5,
    "diverging_scale": 1.0,
    "panic_cooldown_days": 3,
    "slippage_pct": 0.05,
    "pyramid_trigger_pct": 2.5,
    "pyramid_add_scale": 0.5,
    "pyramid_max_adds": 1,
    "exclude_boards": "300",
}

HK_S3_CONFIG: dict[str, float | int | str] = {
    "score_threshold": 65.0,
    "max_hold_days": 60,
    "stop_loss_pct": -5.0,
    "target_pnl_pct": 100.0,
    "score_floor": 0.0,
    "market": "HK",
    "gates": "regime",
    "trailing_stop_pct": -12.0,
    "position_pct": 0.10,
    "max_positions": 20,
    "rs_rank_min": 0.6,
    "diverging_scale": 1.0,
    "panic_cooldown_days": 3,
    "slippage_pct": 0.05,
    "pyramid_trigger_pct": 2.5,
    "pyramid_add_scale": 0.5,
    "pyramid_max_adds": 1,
    "exclude_boards": "",
}


def _mk_config(market: str, start: str, end: str) -> BacktestConfig:
    base = HK_S3_CONFIG if market == "HK" else S3_CONFIG
    return BacktestConfig(start_date=start, end_date=end, **base)


def _entry(row: dict) -> str:
    """paper rows expose entryDate (camelCase via _row_to_dict)."""
    return str(row.get("entryDate") or row.get("entry_date") or "")


def _paper_holdings_on(day: str) -> dict[str, dict]:
    """symbol -> row for paper trades open on `day`."""
    out: dict[str, dict] = {}
    for row in list_paper_trades():
        if row.get("status") == "open" and _entry(row) <= day:
            out[str(row.get("symbol"))] = row
        elif (
            row.get("status") == "closed"
            and _entry(row) <= day
            and (not row.get("closeDate") or str(row.get("closeDate")) > day)
        ):
            out[str(row.get("symbol"))] = row
    return out


def reconcile_day(day: str, *, window: str = "valid", end_date: str | None = None) -> dict[str, Any]:
    """Full reconciliation for one trading day, per market (CN + HK).

    ``window`` picks the S-3 window config; ``end_date`` extends the window
    (for reconciling recent days beyond the fixed window end, e.g. today).
    Returns {reconDate, window, markets: {CN: {...}, HK: {...}}}.
    """
    if window not in WINDOWS:
        raise ValueError(f"unknown window {window!r} (valid: {list(WINDOWS)})")
    start, w_end = WINDOWS[window]
    end = max(end_date, w_end) if end_date else w_end
    if not (start <= day <= end):
        raise ValueError(f"{day} not in window {start}..{end}")

    paper = _paper_holdings_on(day)
    markets: dict[str, Any] = {}
    for market in ("CN", "HK"):
        cfg = _mk_config(market, start, end)
        data = BacktestData(cfg)
        run = simulate(cfg, data=data)
        snap = next((s for s in run.positions_by_day if s["date"] == day), None)
        if snap is None:
            markets[market] = {"available": False, "reason": f"no snapshot for {day}"}
            continue
        expect = {p["symbol"]: p for p in snap["positions"]}
        actual = {k: v for k, v in paper.items() if str(v.get("market") or "CN") == market}
        aligned, missing_h, extra = [], [], []
        for s in sorted(set(expect) & set(actual)):
            aligned.append({
                "symbol": s,
                "entry": expect[s]["entry_date"],
                "paperEntry": _entry(actual[s]),
                "entrySkew": _entry(actual[s]) != expect[s]["entry_date"],
                "score": expect[s].get("score_at_entry"),
            })
        for s in sorted(set(expect) - set(actual)):
            p = expect[s]
            missing_h.append({
                "symbol": s,
                "entry": p["entry_date"],
                "score": p.get("score_at_entry"),
                "positionPct": p.get("position_pct"),
            })
        for s in sorted(set(actual) - set(expect)):
            a = actual[s]
            extra.append({
                "symbol": s,
                "entry": _entry(a),
                "source": a.get("source"),
            })
        markets[market] = {
            "available": True,
            "expected": len(expect),
            "actual": len(actual),
            "aligned": len(aligned),
            "missing": len(missing_h),
            "extra": len(extra),
            "alignedList": aligned,
            "missingList": missing_h,
            "extraList": extra,
        }
    return {"reconDate": day, "window": window, "markets": markets}


def run_and_persist(day: str, *, window: str = "valid") -> dict[str, Any]:
    """reconcile_day + persist (idempotent per day+market). Cron entry point."""
    from data_sync_service.db.reconciliation import insert_recon

    out = reconcile_day(day, window=window)
    for market, m in out["markets"].items():
        if not m.get("available"):
            continue
        detail = [
            {"type": "missing", **x} for x in m.get("missingList", [])
        ] + [{"type": "extra", **x} for x in m.get("extraList", [])]
        insert_recon(
            recon_date=out["reconDate"],
            market=market,
            window=out["window"],
            expected=m["expected"],
            actual=m["actual"],
            aligned=m["aligned"],
            missing=m["missing"],
            extra=m["extra"],
            detail=detail,
        )
    return out
