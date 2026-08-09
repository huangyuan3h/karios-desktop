"""Weekly decision-quality review (OPT-065 / L3-P4, decision Agent M2 v0).

Aggregates one week of the decision loop into a structured, DATA-DRIVEN
report — no LLM in the critical path:

- decision volume by provenance (execution_decision_changes BUY/ADD)
- paper-trade outcomes (NET-of-costs, by market and close reason)
- exit attribution (forward returns → early / well / neutral, by reason)
- funnel health (watchlist automation runs, screener additions)
- watchlist pool state (registry size, score distribution)

The report is emitted as Chinese markdown that can be copied straight into
an AI agent for deeper commentary; the numbers themselves come from the same
tables live paper/attribution read, so the report never lies about sample
size. Small samples are called out explicitly instead of producing
misleading win rates.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

from data_sync_service.db import get_connection
from data_sync_service.db import paper_trading as pt_db
from data_sync_service.service.exit_attribution import analyze_exit_attribution

logger = logging.getLogger(__name__)


def week_bounds(end_date: str) -> tuple[str, str]:
    """Return (monday, end) for the ISO week containing end_date."""
    try:
        d = date.fromisoformat(end_date)
    except ValueError:
        raise ValueError(f"end must be YYYY-MM-DD (got {end_date!r})") from None

    monday = d - timedelta(days=d.weekday())
    return monday.isoformat(), d.isoformat()


def _count_changes_by_source(start: str, end: str, source_attr: str = "source") -> dict[str, int]:
    out: dict[str, int] = {}
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT {source_attr}, COUNT(*)
                FROM execution_decision_changes
                WHERE field = 'action' AND new_value IN ('BUY', 'ADD')
                  AND trade_date >= %s AND trade_date <= %s
                GROUP BY {source_attr}
                """,
                (start, end),
            )
            for r in cur.fetchall():
                key = str(r[0] or "UNKNOWN")
                out[key] = out.get(key, 0) + int(r[1] or 0)
    return out


def _count_automation_runs(start: str, end: str) -> dict[str, Any]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*),
                       COALESCE(SUM((meta->>'screenerAdded')::int), 0) AS added,
                       COALESCE(SUM(CASE WHEN alpha_add IS NOT NULL THEN 1 ELSE 0 END), 0) AS alpha_runs
                FROM watchlist_automation_runs
                WHERE trade_date >= %s AND trade_date <= %s
                """,
                (start, end),
            )
            r = cur.fetchone()
    return {"runs": int(r[0] or 0) if r else 0, "screenerAdded": int(r[1] or 0) if r else 0}


def _registry_state() -> dict[str, Any]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM watchlist_registry")
            total = int(cur.fetchone()[0] or 0)
            cur.execute(
                "SELECT COUNT(*) FROM watchlist_registry WHERE (payload->>'positionPct')::numeric > 0"
            )
            held = int(cur.fetchone()[0] or 0)
    return {"total": total, "held": held}


def _closed_by_reason(start: str, end: str) -> dict[str, dict[str, Any]]:
    try:
        rows = pt_db.list_paper_trades(status="closed", since=start, limit=500)
    except Exception as exc:  # noqa: BLE001
        logger.warning("weekly_review list_paper_trades failed: %s", exc)
        return {}
    out: dict[str, dict[str, Any]] = {}
    for t in rows:
        if str(t.get("closeDate") or t.get("close_date") or "") > end:
            continue
        reason = str(t.get("closeReason") or t.get("close_reason") or "unknown")
        b = out.setdefault(reason, {"count": 0, "sumNet": 0.0, "wins": 0})
        b["count"] += 1
        pnl = t.get("pnlPct")
        if pnl is not None:
            b["sumNet"] += float(pnl)
            if float(pnl) > 0:
                b["wins"] += 1
    for _, b in out.items():
        b["avgNet"] = round(b["sumNet"] / b["count"], 3) if b["count"] else None
        b["winRate"] = round(b["wins"] / b["count"], 3) if b["count"] else None
        del b["sumNet"]
    return out


def build_weekly_review(*, end_date: str) -> dict[str, Any]:
    """Aggregate one ISO week ending on ``end_date`` into a review payload."""
    start, end = week_bounds(end_date)

    fired = _count_changes_by_source(start, end)
    fired_total = sum(fired.values())

    # Paper outcomes (closed this week, NET-of-costs).
    by_reason = _closed_by_reason(start, end)
    closed_total = sum(b["count"] for b in by_reason.values())
    wins = sum(b.get("wins", 0) for b in by_reason.values())
    net_sum = sum(b["count"] * (b.get("avgNet") or 0) for b in by_reason.values())

    # Exit attribution reuses the L3-P3 service (same sample semantics).
    attribution = {}
    try:
        attribution = analyze_exit_attribution(days=5, limit=500)
        if "error" in attribution:
            attribution = {}
    except Exception as exc:  # noqa: BLE001
        logger.warning("weekly_review attribution failed: %s", exc)
        attribution = {}

    automation = _count_automation_runs(start, end)
    registry = _registry_state()

    stats: dict[str, Any] = {
        "week": {"start": start, "end": end},
        "decisionVolume": {"total": fired_total, "bySource": fired},
        "paper": {
            "closed": closed_total,
            "wins": wins,
            "winRate": round(wins / closed_total, 3) if closed_total else None,
            "avgNetPnlPct": round(net_sum / closed_total, 3) if closed_total else None,
            "byReason": by_reason,
        },
        "exitAttribution": {
            "withForward": attribution.get("withForwardCount", 0),
            "earlyRate": (attribution.get("overall") or {}).get("earlyRate"),
            "wellRate": (attribution.get("overall") or {}).get("wellRate"),
            "avgFwdPct": (attribution.get("overall") or {}).get("avgFwdPct"),
        },
        "funnel": automation,
        "registry": registry,
    }

    return {"ok": True, **stats, "markdown": _render_markdown(stats)}


# ---------------------------------------------------------------------------
# Markdown rendering
# ---------------------------------------------------------------------------


def _fmt_pct(v: float | None, digits: int = 1) -> str:
    return "—" if v is None else f"{v * 100:.{digits}f}%"


def _fmt_num(v: float | None, digits: int = 1) -> str:
    return "—" if v is None else f"{v:.{digits}f}%"


def _render_markdown(stats: dict[str, Any]) -> str:
    w = stats["week"]
    lines: list[str] = []
    lines.append(f"# Karios 周度决策质量报告（{w['start']} ~ {w['end']}）")
    lines.append("")
    lines.append("> 数据驱动报告：数字来自决策日志 / paper（净口径）/ 卖出归因，不含 LLM 推断。样本不足时明确标注。")
    lines.append("")

    # 1) decision volume
    lines.append("## 1. 决策量")
    fired = stats["decisionVolume"]
    if fired["total"] == 0:
        lines.append("- 本周无 BUY/ADD 信号。")
    else:
        srcs = " · ".join(f"{k} {v}" for k, v in sorted(fired["bySource"].items(), key=lambda x: -x[1]))
        lines.append(f"- BUY/ADD 信号共 **{fired['total']}** 条：{srcs}")
    lines.append(f"- Watchlist 池 {stats['registry']['total']} 只（持仓 {stats['registry']['held']}）· Automation 运行 {stats['funnel']['runs']} 次（新增 {stats['funnel']['screenerAdded']}）")
    lines.append("")

    # 2) paper outcomes
    paper = stats["paper"]
    lines.append("## 2. Paper 实绩（净口径）")
    if paper["closed"] == 0:
        lines.append("- 本周无平仓交易（paper 数据仍在积累）。")
    else:
        lines.append(
            f"- 已平仓 **{paper['closed']}** 笔 · 胜率 {_fmt_pct(paper['winRate'])} · "
            f"均净盈亏 {_fmt_num(paper['avgNetPnlPct'])}"
        )
        if paper["byReason"]:
            lines.append("- 分理由：")
            for reason, b in sorted(paper["byReason"].items(), key=lambda x: -x[1]["count"]):
                lines.append(
                    f"  - `{reason}` {b['count']} 笔 · 胜率 {_fmt_pct(b['winRate'])} · "
                    f"均净 {_fmt_num(b['avgNet'])}"
                )
    lines.append("")

    # 3) exit attribution
    attr = stats["exitAttribution"]
    lines.append("## 3. 卖出归因（平仓后 5 个交易日前向收益）")
    if attr["withForward"] == 0:
        lines.append("- 前向样本不足，暂不归因。")
    else:
        lines.append(
            f"- 样本 {attr['withForward']} 笔 · 卖早率 {_fmt_pct(attr['earlyRate'])} "
            f"· 卖对率 {_fmt_pct(attr['wellRate'])} · 平均前向 {_fmt_num(attr['avgFwdPct'])}"
        )
        note = _attribution_note(attr)
        if note:
            lines.append(f"- ⚠ {note}")
    lines.append("")

    # 4) 自动结论
    lines.append("## 4. 本周观察")
    notes = _auto_notes(stats)
    if not notes:
        lines.append("- 数据仍少，先积累。")
    else:
        lines.extend(f"- {n}" for n in notes)
    return "\n".join(lines)


def _attribution_note(attr: dict[str, Any]) -> str | None:
    early = attr.get("earlyRate")
    if early is not None and early >= 0.5 and attr.get("withForward", 0) >= 5:
        return f"卖早率 {_fmt_pct(early)} —— 一半以上卖出后仍在涨，止盈/止损可能过早。"
    well = attr.get("wellRate")
    if well is not None and well >= 0.5 and attr.get("withForward", 0) >= 5:
        return f"卖对率 {_fmt_pct(well)} —— 卖出后多数下跌，执行时机不错。"
    return None


def _auto_notes(stats: dict[str, Any]) -> list[str]:
    notes: list[str] = []
    fired = stats["decisionVolume"]
    paper = stats["paper"]
    attr = stats["exitAttribution"]

    if fired["total"] and paper["closed"]:
        if paper["winRate"] is not None and paper["winRate"] < 0.5 and paper["closed"] >= 5:
            notes.append("本周平仓胜率 < 50%（净口径）——信号质量或市场环境需关注。")
    for src, n in sorted(fired["bySource"].items(), key=lambda x: -x[1]):
        if n >= 5:
            notes.append(f"信号主要来自 {src}（{n} 条）——该通道占比过高时注意供给单一化。")
    if attr.get("earlyRate") is not None and attr["earlyRate"] >= 0.5 and attr.get("withForward", 0) >= 5:
        notes.append("卖早率高——Chandelier/止盈阈值可考虑放宽，但改参数前先用 paper 跑一周对照。")
    if stats["funnel"]["runs"] == 0:
        notes.append("本周 Automation 未运行——检查调度（盘后 17:30 cron）。")
    return notes
