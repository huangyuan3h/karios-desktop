"""Notification aggregation service (2026-08-12).

Aggregates "things to do / things to know" from existing products —
portfolio-health (stop/trail alerts + EXIT), reconciliation (missing
backtest holdings), sync_job_record (today's cron failures), rolling OOS
warning. Pure aggregation: no new data collection, one endpoint the UI
polls. Each item carries a watchlist-page anchor so the UI can scroll to
the relevant block.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

ALERT_MARGIN_PT = 1.5

REPORTS_DIR = Path(__file__).resolve().parents[3] / "data" / "backtest_reports"

# Which jobs matter to the user's trading day (intraday + post-close chain).
TRADING_JOB_TYPES = {
    "close_sync",
    "watchlist_automation",
    "paper_s3_intake_CN",
    "paper_s3_intake_HK",
    "paper_trading_update",
    "paper_chain_watchdog",
    "cn_industry_post_close_sync",
    "index_basic_sync",
}


def _anchor_blocks() -> dict[str, dict[str, Any]]:
    from data_sync_service.service.portfolio_health import build_portfolio_health

    try:
        h = build_portfolio_health(trade_date=None, markets=("CN", "HK"))
    except Exception as exc:  # noqa: BLE001
        logger.warning("notifications: portfolio health failed: %s", exc)
        return {}
    out: dict[str, dict[str, Any]] = {}
    for key, market in (("", "CN"), ("hkHealth", "HK")):
        block = h if key == "" else h.get("hkHealth") or {}
        if block:
            out[market] = block
    return out


def _stop_trail_alerts() -> list[dict[str, Any]]:
    """Holdings near their stop/trail line or flagged EXIT (anchor: holdings)."""
    out: list[dict[str, Any]] = []
    for market, block in _anchor_blocks().items():
        for hold in block.get("holdings") or []:
            symbol = str(hold.get("symbol") or "")
            name = str(hold.get("name") or symbol)
            if not symbol:
                continue
            pnl = hold.get("pnlPct")
            try:
                pnl_f = float(pnl)
            except (TypeError, ValueError):
                pnl_f = None
            if hold.get("action") == "EXIT":
                out.append({
                    "id": f"exit:{market}:{symbol}",
                    "type": "exit",
                    "severity": "high",
                    "title": f"建议退出 · {name}",
                    "detail": f"{symbol} 触发 {hold.get('reason') or '退出规则'}（{market}）",
                    "anchor": "holdings",
                    "createdAt": datetime.now(UTC).isoformat(),
                })
                continue
            if pnl_f is None:
                continue
            ops = hold.get("lineOps") or {}
            expire_date = ops.get("expireDate") or hold.get("expireDate")
            if "trail_up" in ops:
                prev_v, cur_v = ops["trail_up"]
                out.append({
                    "id": f"lineup:{market}:{symbol}:trail",
                    "type": "line_update",
                    "severity": "medium",
                    "title": f"需调单 · 移动线上调 · {name}",
                    "detail": (
                        f"{symbol} 移动线 {prev_v} → {cur_v}（峰值上移）——"
                        "请上调券商固定价条件单，或改用移动止损单"
                    ),
                    "anchor": "holdings",
                    "createdAt": datetime.now(UTC).isoformat(),
                })
            if "stop_up" in ops:
                prev_v, cur_v = ops["stop_up"]
                out.append({
                    "id": f"lineup:{market}:{symbol}:stop",
                    "type": "line_update",
                    "severity": "medium",
                    "title": f"需调单 · 止损线上调 · {name}",
                    "detail": (
                        f"{symbol} 止损线 {prev_v} → {cur_v}（金字塔加仓）——"
                        "请上调券商止损条件单"
                    ),
                    "anchor": "holdings",
                    "createdAt": datetime.now(UTC).isoformat(),
                })
            if "expire_soon" in ops:
                out.append({
                    "id": f"expire:{market}:{symbol}",
                    "type": "expire_soon",
                    "severity": "medium",
                    "title": f"临近到期 · {name}",
                    "detail": (
                        f"{symbol} 持有期剩 {ops['expire_soon']} 天"
                        + (f"（到期 {expire_date}）" if expire_date else "")
                        + "——券商条件单不会自动到期卖出，请记日历"
                    ),
                    "anchor": "holdings",
                    "createdAt": datetime.now(UTC).isoformat(),
                })
            for line_name, line in (("stop", hold.get("stopLossLine")),
                                    ("trailing", hold.get("trailingLine"))):
                try:
                    line_f = float(line)
                except (TypeError, ValueError):
                    continue
                distance = abs(pnl_f - line_f)
                if distance <= ALERT_MARGIN_PT:
                    out.append({
                        "id": f"near:{market}:{symbol}:{line_name}",
                        "type": "near_line",
                        "severity": "high" if distance <= 0.5 else "medium",
                        "title": f"接近{('止损' if line_name == 'stop' else '移动')}线 · {name}",
                        "detail": f"{symbol} 距{'止损' if line_name == 'stop' else '移动'}线 {distance:.2f}pt（现 {pnl_f:.2f}% / 线 {line_f}）",
                        "anchor": "holdings",
                        "createdAt": datetime.now(UTC).isoformat(),
                    })
    return out


def _recon_alerts() -> list[dict[str, Any]]:
    """Backtest vs paper missing positions (anchor: recon)."""
    from data_sync_service.db.reconciliation import latest_recon

    try:
        rows = latest_recon(limit=2)
    except Exception as exc:  # noqa: BLE001
        logger.warning("notifications: recon failed: %s", exc)
        return []
    out: list[dict[str, Any]] = []
    for r in rows:
        if r.get("missing", 0) <= 0:
            continue
        market = "港股" if r.get("market") == "HK" else "A股"
        out.append({
            "id": f"recon:{r.get('reconDate')}:{r.get('market')}",
            "type": "recon_missing",
            "severity": "medium",
            "title": f"回测口径 · {market}缺 {r.get('missing')} 只持仓",
            "detail": f"回测应持 {r.get('expected')} · 实持 {r.get('actual')}（对账 {r.get('reconDate')}）——展开缺票可提醒买入",
            "anchor": "recon",
            "createdAt": datetime.now(UTC).isoformat(),
        })
    return out


def _cron_failures() -> list[dict[str, Any]]:
    """Today's trading-chain cron failures (anchor: scheduler)."""
    from data_sync_service.db.sync_job_record import list_recent_failures

    try:
        rows = list_recent_failures(hours=24)
    except Exception as exc:  # noqa: BLE001
        logger.warning("notifications: cron failures failed: %s", exc)
        return []
    out: list[dict[str, Any]] = []
    for r in rows:
        job = str(r.get("job_type") or "")
        if job not in TRADING_JOB_TYPES:
            continue
        out.append({
            "id": f"cron:{job}:{r.get('sync_at')}",
            "type": "cron_failed",
            "severity": "high",
            "title": f"任务失败 · {job}",
            "detail": f"{job} 失败：{str(r.get('error_message') or '')[:160] or '未知错误'}",
            "anchor": "scheduler",
            "createdAt": str(r.get("sync_at") or datetime.now(UTC).isoformat()),
        })
    return out[:5]


def _rolling_oos_warning() -> list[dict[str, Any]]:
    """Rolling OOS window flagged (anchor: backtest)."""
    p = REPORTS_DIR / "rolling_oos_latest.json"
    if not p.exists():
        return []
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if not data.get("warning"):
        return []
    warnings = data.get("warnings") or []
    detail = " · ".join(str(w) for w in warnings[:3])
    return [{
        "id": f"oos:{data.get('windowEnd')}",
        "type": "oos_warning",
        "severity": "medium",
        "title": "滚动 OOS 预警（近 90 天窗）",
        "detail": f"{data.get('windowStart')} ~ {data.get('windowEnd')}：{detail}",
        "anchor": "backtest",
        "createdAt": datetime.now(UTC).isoformat(),
    }]


def build_notifications() -> list[dict[str, Any]]:
    """All actionable notifications, most severe first."""
    items = (
        _stop_trail_alerts()
        + _cron_failures()
        + _recon_alerts()
        + _rolling_oos_warning()
    )
    order = {"high": 0, "medium": 1}
    items.sort(key=lambda x: order.get(str(x.get("severity")), 2))
    return items
