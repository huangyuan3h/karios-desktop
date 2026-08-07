"""Exit attribution (OPT-064 / L3-P3) — how GOOD are our sells?

For every CLOSED paper trade, measure the forward return N trading days
AFTER the close date. That tells us, per close reason, whether the exit
logic is actually selling at the right time:

- ``exit_early`` — forward return >= EARLY_THRESHOLD_PCT (+2%): the position
  kept running after we sold; the sell was premature.
- ``exit_well`` — forward return <= WELL_THRESHOLD_PCT (-1%): the price
  went down after we sold; the sell was timely.
- ``neutral`` — everything between.

Aggregated by close_reason (stop_hit / target_hit / max_hold / score_floor /
pool_exit / end_of_window), plus a portfolio-exposure view for the satellite
book caps review (single-stock / sector concentration lower bounds derived
from the max number of simultaneously-held paper positions).

Data discipline: forward returns come from the ``daily`` table (same bars
the live paper cron uses); trades closed too recently to have N forward
trading days are excluded from the forward stats but still counted in the
exposure view. Small samples produce empty aggregates with a hint, not
misleading numbers.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

from data_sync_service.db.daily import fetch_ohlcv_batch_between
from data_sync_service.db.paper_trading import list_paper_trades
from data_sync_service.service.paper_trading import _resolve_ts_code

logger = logging.getLogger(__name__)

EARLY_THRESHOLD_PCT = 2.0
WELL_THRESHOLD_PCT = -1.0

BUCKET_EARLY = "exit_early"
BUCKET_WELL = "exit_well"
BUCKET_NEUTRAL = "neutral"

CLOSE_REASON_LABELS = {
    "stop_hit": "止损",
    "target_hit": "止盈",
    "max_hold": "最长持有",
    "score_floor": "分数跌破",
    "pool_exit": "移出池子",
    "end_of_window": "窗口结束",
}


def _close_reason_label(reason: str) -> str:
    return CLOSE_REASON_LABELS.get(reason, reason or "未知")


def _bucket(fwd_pct: float) -> str:
    if fwd_pct >= EARLY_THRESHOLD_PCT:
        return BUCKET_EARLY
    if fwd_pct <= WELL_THRESHOLD_PCT:
        return BUCKET_WELL
    return BUCKET_NEUTRAL


def analyze_exit_attribution(*, days: int = 5, limit: int = 500) -> dict[str, Any]:
    """Attribution of closed paper trades by close reason (L3-P3).

    Returns:
      {
        "days": N,
        "closedCount": total closed examined,
        "withForwardCount": closed trades having N forward trading days,
        "excludedReason": "too_recent" | None,
        "overall": {bucket counts + earlyRate/wellRate over forward sample},
        "byReason": {reason: {label, count, withForward, avgFwdPct, earlyCount,
                              wellCount, neutralCount, earlyRate, wellRate}},
        "exposure": {maxSimultaneous, singleStockWeightFloorPct, note},
        "insufficient": true/false — paper book still accumulating
      }
    """
    try:
        closed = [
            t
            for t in list_paper_trades(status="closed", limit=limit)
            if (t.get("closeDate") or t.get("close_date"))
            and (t.get("closePrice") or t.get("close_price"))
        ]
    except Exception as exc:  # noqa: BLE001
        logger.warning("exit_attribution list_paper_trades failed: %s", exc)
        return {"error": f"list_paper_trades failed: {exc}"}

    if not closed:
        return {
            "days": days,
            "closedCount": 0,
            "withForwardCount": 0,
            "overall": _empty_overall(),
            "byReason": {},
            "exposure": _empty_exposure(),
            "insufficient": True,
            "hint": "paper 平仓记录为空——系统刚起步，继续积累后这里才会出数字",
        }

    # 1) Forward bars: window from min close_date to max close_date + buffer.
    close_dates = sorted(str(t.get("closeDate") or t.get("close_date") or "") for t in closed)
    start = close_dates[0]
    end = close_dates[-1]
    try:
        buffer_end = (date.fromisoformat(end) + timedelta(days=days * 7 + 10)).isoformat()
    except ValueError:
        buffer_end = end

    ts_codes: list[str] = []
    for t in closed:
        resolved = _resolve_ts_code(str(t.get("symbol") or ""))
        if resolved:
            ts_codes.append(resolved[1])
    bars_by_ts: dict[str, list[tuple[str, str, str, str, str, str]]] = {}
    if ts_codes:
        try:
            bars_by_ts = fetch_ohlcv_batch_between(ts_codes, start, buffer_end)
        except Exception as exc:  # noqa: BLE001
            logger.warning("exit_attribution fetch bars failed: %s", exc)

    # ts_code -> {date: close}
    close_by_ts_day: dict[str, dict[str, float]] = {}
    for ts, bars in bars_by_ts.items():
        closes: dict[str, float] = {}
        for bar in bars:
            try:
                closes[str(bar[0])] = float(bar[4])
            except (TypeError, ValueError):
                continue
        close_by_ts_day[ts] = closes

    def forward_return(ts: str, close_date: str) -> float | None:
        closes = close_by_ts_day.get(ts)
        if not closes:
            return None
        # trading days AFTER close_date (strictly greater), ASC
        after = [d for d in sorted(closes) if d > close_date]
        if len(after) < days:
            return None
        entry_px = closes.get(close_date)
        exit_px = closes[after[days - 1]]
        if not entry_px or entry_px <= 0:
            return None
        return (exit_px - entry_px) / entry_px * 100.0

    # 2) Per-trade attribution.
    per_trade: list[dict[str, Any]] = []
    exposure_days: dict[str, int] = {}
    for t in closed:
        sym = str(t.get("symbol") or "")
        cd = str(t.get("closeDate") or t.get("close_date") or "")
        resolved = _resolve_ts_code(sym)
        ts = resolved[1] if resolved else None
        fwd = forward_return(ts, cd) if ts else None
        # Simultaneous positions are a portfolio fact, independent of whether
        # forward bars exist yet.
        if cd:
            exposure_days[cd] = exposure_days.get(cd, 0) + 1
        per_trade.append(
            {
                "symbol": sym,
                "closeReason": str(t.get("closeReason") or t.get("close_reason") or ""),
                "closeReasonLabel": _close_reason_label(
                    str(t.get("closeReason") or t.get("close_reason") or "")
                ),
                "closeDate": cd,
                "pnlPct": t.get("pnlPct") if t.get("pnlPct") is not None else t.get("pnl_pct"),
                "forwardPct": round(fwd, 3) if fwd is not None else None,
                "bucket": _bucket(fwd) if fwd is not None else None,
            }
        )

    # 3) Aggregates.
    with_fwd = [p for p in per_trade if p["forwardPct"] is not None]
    overall = _empty_overall()
    for p in with_fwd:
        overall["count"] += 1
        overall["sumFwd"] = overall.get("sumFwd", 0.0) + float(p["forwardPct"])
        count_key = {"exit_early": "earlyCount", "exit_well": "wellCount", "neutral": "neutralCount"}[
            p["bucket"]
        ]
        overall[count_key] += 1
    if overall["count"]:
        overall["avgFwdPct"] = round(overall["sumFwd"] / overall["count"], 3)
        overall["earlyRate"] = round(overall["earlyCount"] / overall["count"], 3)
        overall["wellRate"] = round(overall["wellCount"] / overall["count"], 3)
    overall.pop("sumFwd", None)

    by_reason: dict[str, dict[str, Any]] = {}
    for p in with_fwd:
        r = p["closeReason"]
        b = by_reason.setdefault(
            r,
            {
                "label": p["closeReasonLabel"],
                "count": 0,
                "withForward": 0,
                "avgFwdPct": None,
                "earlyCount": 0,
                "wellCount": 0,
                "neutralCount": 0,
                "earlyRate": None,
                "wellRate": None,
            },
        )
        b["withForward"] += 1
        b.setdefault("sumFwd", 0.0)
        b["sumFwd"] += float(p["forwardPct"])
        count_key = {"exit_early": "earlyCount", "exit_well": "wellCount", "neutral": "neutralCount"}[
            p["bucket"]
        ]
        b[count_key] += 1
    for r, b in by_reason.items():
        if b["withForward"]:
            b["avgFwdPct"] = round(b["sumFwd"] / b["withForward"], 3)
            b["earlyRate"] = round(b["earlyCount"] / b["withForward"], 3)
            b["wellRate"] = round(b["wellCount"] / b["withForward"], 3)
        b.pop("sumFwd", None)

    # Also count closed rows per reason (including too-recent ones).
    for p in per_trade:
        r = p["closeReason"]
        by_reason.setdefault(r, {"label": p["closeReasonLabel"], "count": 0})["count"] += 1

    max_sim = max(exposure_days.values()) if exposure_days else 0
    single_floor = round(100.0 / max_sim, 2) if max_sim else None

    return {
        "days": days,
        "closedCount": len(closed),
        "withForwardCount": len(with_fwd),
        "excluded": len(closed) - len(with_fwd),
        "insufficient": len(with_fwd) < 10,
        "hint": (
            "平仓样本仍少（<10 笔有完整前向窗口）——继续积累，数字会更有意义"
            if len(with_fwd) < 10
            else None
        ),
        "overall": overall,
        "byReason": by_reason,
        "exposure": {
            "maxSimultaneous": max_sim,
            "singleStockWeightFloorPct": single_floor,
            "note": (
                "单票权重下界 = 100% / 最多同时持仓数。与卫星仓红线对照："
                "单票 ≤15%、板块 ≤30%、sleeve 5%。暴露超标时优先减仓而非改参数。"
            ),
        },
    }


def _empty_overall() -> dict[str, Any]:
    return {
        "count": 0,
        "avgFwdPct": None,
        "earlyCount": 0,
        "wellCount": 0,
        "neutralCount": 0,
        "earlyRate": None,
        "wellRate": None,
    }


def _empty_exposure() -> dict[str, Any]:
    return {
        "maxSimultaneous": 0,
        "singleStockWeightFloorPct": None,
        "note": "无平仓数据，暴露统计为空。",
    }
