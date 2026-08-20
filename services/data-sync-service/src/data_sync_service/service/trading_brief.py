"""Trading-session briefs (2026-08-11) — three snapshot cards for the user's
real trading rhythm (10:00 open / 12:00 midday / 14:30 action).

The user consumes each time point in ~30 seconds and gets back to work:
  - open   (10:00): regime + panic, S-3 candidates, overnight news top5.
  - midday (12:00): candidate changes vs open, price drift, held-line checks.
  - action (14:30): BUY cards (candidate + suggested size) + conditional-stop
    list for the broker side (fixed -5% / trailing -8%, HK -12%) + alerts.

Everything is assembled from EXISTING blocks — portfolio_health (regime /
holdings / stop lines), paper_s3.build_s3_candidates, morning_brief news
selection — stored in the SAME morning_briefs table (UNIQUE(brief_date,
brief_type) already supports arbitrary types) with a rendered markdown
column for the front-end card. No new mechanisms, just composition.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from data_sync_service.db.morning_brief import upsert_brief
from data_sync_service.service.morning_brief import select_brief_items

logger = logging.getLogger(__name__)

MODEL_VERSION = "trading-brief-v1"

# "action" brief always shows the conditional-stop list; open/midday only
# when a held name is within 1.5pt of its stop/trail line.
ALERT_MARGIN_PT = 1.5

BRIEF_TYPES = ("open", "midday", "action")


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d")


def _market_label(market: str) -> str:
    return "A股" if market == "CN" else "港股"


def _health() -> dict[str, Any]:
    """Full CN+HK health block (buy/hold/sell + regime + panic)."""
    from data_sync_service.service.portfolio_health import build_portfolio_health

    return build_portfolio_health(trade_date=None, markets=("CN", "HK"))


def _candidates(market: str) -> list[dict[str, Any]]:
    from data_sync_service.service.paper_s3 import build_s3_candidates

    try:
        return build_s3_candidates(trade_date=None, market=market) or []
    except Exception as exc:  # noqa: BLE001
        logger.warning("trading_brief candidates %s failed: %s", market, exc)
        return []


# ---------------------------------------------------------------------------
# Sections
# ---------------------------------------------------------------------------


def _regime_section(h: dict[str, Any]) -> list[dict[str, Any]]:
    out = []
    for key, label in (("", "A股"), ("hkHealth", "港股")):
        block = h if key == "" else h.get("hkHealth") or {}
        if not block:
            continue
        out.append({
            "type": "regime",
            "market": label,
            "regime": block.get("regime"),
            "strength": block.get("strength"),
            "sentiment": block.get("sentiment"),
            "panicActive": bool(block.get("panicCooldown", {}).get("active")),
            "panicCooldownEnd": block.get("panicCooldown", {}).get("cooldownEndDate"),
            "candidateTotal": block.get("s3CandidateTotal"),
        })
    return out


def _candidates_section(h: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for market in ("CN", "HK"):
        for c in _candidates(market):
            rows.append({
                "type": "candidate",
                "market": market,
                "symbol": c.get("symbol"),
                "name": c.get("name"),
                "industry": c.get("industry"),
                "score": c.get("score"),
                "rs": c.get("rs"),
            })
    # Score freshness: portfolio-health already exposes it; fall back to text.
    return rows


def _holdings_section(h: dict[str, Any]) -> list[dict[str, Any]]:
    """Held names with their conditional-stop lines (fixed/trailing/expiry)."""
    rows = []
    for key, market in (("", "CN"), ("hkHealth", "HK")):
        block = h if key == "" else h.get("hkHealth") or {}
        for hold in block.get("holdings") or []:
            stop = hold.get("stopLossLine")
            trail = hold.get("trailingLine")
            rows.append({
                "type": "holding",
                "market": market,
                "symbol": hold.get("symbol"),
                "name": hold.get("name"),
                "action": hold.get("action"),  # EXIT / HOLD
                "reason": hold.get("reason"),
                "stopLossLine": stop,
                "trailingLine": trail,
                "pnlPct": hold.get("pnlPct"),
                "expireDate": hold.get("expireDate"),
                "lineOps": hold.get("lineOps") or {},
            })
    return rows


def _alerts_section(h: dict[str, Any]) -> list[dict[str, Any]]:
    """Held names near their stop/trail line (within ALERT_MARGIN_PT)."""
    out = []
    for key, market in (("", "CN"), ("hkHealth", "HK")):
        block = h if key == "" else h.get("hkHealth") or {}
        for hold in block.get("holdings") or []:
            for line_name, line in (("stop", hold.get("stopLossLine")),
                                    ("trailing", hold.get("trailingLine"))):
                if not line or not hold.get("pnlPct"):
                    continue
                try:
                    distance = abs(float(hold["pnlPct"]) - float(line))
                except (TypeError, ValueError):
                    continue
                if distance <= ALERT_MARGIN_PT and hold.get("action") != "EXIT":
                    out.append({
                        "type": "alert",
                        "market": market,
                        "symbol": hold.get("symbol"),
                        "name": hold.get("name"),
                        "line": line_name,
                        "lineValue": line,
                        "pnlPct": hold.get("pnlPct"),
                        "distancePct": round(distance, 2),
                    })
    return out


def _news_section(top: int = 5) -> list[dict[str, Any]]:
    items = select_brief_items(hours=24)
    out = []
    for it in items[:top]:
        out.append({
            "type": "news",
            "id": str(it.get("id") or it.get("title") or ""),
            "title": it.get("title"),
            "category": it.get("category"),
            "importance": it.get("importance"),
            "tickers": it.get("tickers") or [],
            "aiSummary": it.get("aiSummary"),
            "score": it.get("score"),
        })
    return out


def _recon_section(top: int = 5) -> list[dict[str, Any]]:
    """Latest backtest-vs-paper snapshot (weekly Monday cron) so the action
    card speaks the backtest dialect: which backtest-held names the paper
    book is missing (2026-08-12). Rows carry score so the reader knows what
    the backtest was thinking at entry."""
    from data_sync_service.db.reconciliation import latest_recon

    # limit=2: the latest snapshot has one row per market (CN then HK by
    # market sort); 1 row would silently drop the second market.
    rows = latest_recon(limit=2)
    out: list[dict[str, Any]] = []
    for r in rows:
        missing = [
            x for x in (r.get("detail") or []) if x.get("type") == "missing"
        ]
        out.append({
            "type": "recon",
            "reconDate": r.get("reconDate"),
            "market": r.get("market"),
            "expected": r.get("expected"),
            "actual": r.get("actual"),
            "missing": r.get("missing"),
            "extra": r.get("extra"),
            "alignedReturnDiffPct": r.get("alignedReturnDiffPct"),
            "missingTop": sorted(
                missing,
                key=lambda x: -(float(x.get("score") or 0)),
            )[:top],
        })
    return out


# ---------------------------------------------------------------------------
# Markdown rendering (compact, ~30s read)
# ---------------------------------------------------------------------------


def _third_asset_section() -> list[dict[str, Any]]:
    """T6 (2026-08-19): 513100 idle-cash sleeve hint, evaluated on the PAPER
    book so the daily briefing stays aligned with the backtest best result."""
    from data_sync_service.service.third_asset_sleeve import build_third_asset_sleeve_for_paper

    try:
        sleeve = build_third_asset_sleeve_for_paper(day=_now().split("T")[0])
    except Exception as exc:  # noqa: BLE001
        logger.warning("trading_brief third-asset section failed: %s", exc)
        return []
    if not sleeve.get("active"):
        return []
    return [{"type": "third_asset", **sleeve}]


def render_markdown(sections: list[dict[str, Any]], brief_type: str) -> str:
    lines: list[str] = []
    regime = [s for s in sections if s["type"] == "regime"]
    cands = [s for s in sections if s["type"] == "candidate"]
    holds = [s for s in sections if s["type"] == "holding"]
    alerts = [s for s in sections if s["type"] == "alert"]
    news = [s for s in sections if s["type"] == "news"]
    recon = [s for s in sections if s["type"] == "recon"]

    if regime:
        lines.append("**Regime**")
        for r in regime:
            p = "· panic 冷却" if r["panicActive"] else ""
            lines.append(
                f"- {r['market']}: {r['regime']}（强度 {r['strength']}）{p}"
            )

    if cands:
        lines.append("")
        lines.append(f"**S-3 候选（{len(cands)}）**")
        for c in sorted(cands, key=lambda x: -float(x["score"] or 0)):
            lines.append(
                f"- [{_market_label(c['market'])}] {c['symbol']} {c.get('name') or ''}"
                f" · score {c['score']} · RS {c.get('rs')}"
            )
    elif brief_type == "action":
        lines.append("")
        lines.append("**S-3 候选：无**（regime 非可投 / 恐慌冷却 / 无达标）")

    if holds:
        lines.append("")
        lines.append("**持仓 / 条件单**")
        for h in sorted(holds, key=lambda x: -(float(x["pnlPct"]) if x["pnlPct"] not in (None, "") else -99)):
            stop = f" 止损 {h['stopLossLine']}" if h["stopLossLine"] else ""
            trail = f" 移动 {h['trailingLine']}" if h["trailingLine"] else ""
            exp = f" 到期 {h['expireDate']}" if h["expireDate"] else ""
            tag = "🔴退出" if h["action"] == "EXIT" else "✅持有"
            pnl = h["pnlPct"] if h["pnlPct"] not in (None, "") else "—"
            ops = h.get("lineOps") or {}
            marks: list[str] = []
            if "trail_up" in ops:
                prev_v, cur_v = ops["trail_up"]
                marks.append(f"🛠移动线上调 {prev_v}→{cur_v}")
            if "stop_up" in ops:
                prev_v, cur_v = ops["stop_up"]
                marks.append(f"🛠止损线上调 {prev_v}→{cur_v}")
            if "expire_soon" in ops:
                marks.append(f"⏰剩 {ops['expire_soon']} 天到期")
            mark = f" {' '.join(marks)}" if marks else ""
            lines.append(
                f"- [{_market_label(h['market'])}] {h['symbol']} {h.get('name') or ''}"
                f" · {pnl}% {tag}{stop}{trail}{exp}{mark}"
            )

    if alerts:
        lines.append("")
        lines.append("**⚠ 接近止损线**")
        for a in alerts:
            lines.append(
                f"- {a['symbol']} {a['name']} 距{a['line']}线 {a['distancePct']}pt"
                f"（现 {a['pnlPct']}% / 线 {a['lineValue']}）"
            )

    third = [s for s in sections if s["type"] == "third_asset"]
    if third:
        t = third[0]
        lines.append("")
        lines.append(f"**第三资产套筒（{t.get('label') or t.get('action') or ''}）**")
        lines.append(f"- {t.get('message')}")
        details = []
        if t.get("price") is not None:
            details.append(f"现价 {t['price']}")
        if t.get("ma200") is not None:
            details.append(f"MA200 {t['ma200']}")
        if t.get("idlePct") is not None:
            details.append(f"闲置 {t['idlePct']}%")
        if t.get("asOfDate"):
            details.append(f"asOf {t['asOfDate']}")
        if details:
            lines.append(f"  _({' · '.join(details)})")

    if recon:
        lines.append("")
        lines.append(f"**回测口径（对账 {recon[0]['reconDate']}）**")
        for r in recon:
            mkt = _market_label(r["market"])
            lines.append(
                f"- {mkt}：回测应持 {r['expected']} · 实持 {r['actual']}"
                f" · 缺 {r['missing']} · 多 {r['extra']}"
            )
            for m in r["missingTop"]:
                pct = m.get("positionPct")
                pct_label = f"{round(float(pct) * 100)}%" if pct else "10%"
                lines.append(
                    f"  缺票 {m['symbol']}（入场 score {m.get('score')} · 建议 {pct_label}"
                    f"· {m.get('entry') or '—'} 入场）"
                )

    if news:
        lines.append("")
        lines.append("**新闻 Top5**")
        for n in news:
            lines.append(f"- {n['title']}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def generate_trading_brief(brief_type: str) -> dict[str, Any]:
    """Build + store one session brief (open/midday/action). Reuses the
    morning_briefs table; returns the stored row (with markdown)."""
    if brief_type not in BRIEF_TYPES:
        raise ValueError(f"unknown brief_type {brief_type!r} (valid: {BRIEF_TYPES})")

    h = _health()
    sections: list[dict[str, Any]] = []
    sections += _regime_section(h)
    sections += _candidates_section(h)
    sections += _holdings_section(h)
    sections += _third_asset_section()
    if brief_type in ("midday", "action"):
        sections += _alerts_section(h)
        # E4 (webhook design §2): held names near their stop/trail line push
        # events as soon as the session brief computes them (once per symbol
        # + line + day via dedupe_key).
        from data_sync_service.db.webhook import emit_event

        for alert in (a for a in sections if a.get("type") == "alert"):
            emit_event(
                "near_stop",
                {
                    "symbol": alert.get("symbol"),
                    "market": alert.get("market"),
                    "line": alert.get("line"),
                    "pnl_pct": alert.get("pnlPct"),
                    "distance_pct": alert.get("distancePct"),
                },
                dedupe_key=(
                    f"near_stop:{alert.get('symbol')}:{alert.get('line')}:"
                    f"{_now().split('T')[0]}"
                ),
            )
    if brief_type == "action":
        sections += _recon_section(5)
        # OPT-113: the 14:00 execution card pushes to webhook subscribers so
        # the user's phone gets the buy list + exit flags + gate state at
        # their actual trading time (once per day via dedupe_key).
        from data_sync_service.db.webhook import emit_event
        from data_sync_service.service.third_asset_sleeve import build_third_asset_sleeve_for_paper

        try:
            _sleeve = build_third_asset_sleeve_for_paper(day=_now().split("T")[0])
        except Exception as exc:  # noqa: BLE001
            logger.warning("trading_brief execution-card sleeve failed: %s", exc)
            _sleeve = {}
        # Only actionable sleeve actions reach the phone push; DONT_BUY stays
        # on the watchlist banner (a closed-gate day must never push "buy").
        _sleeve_pushed = (
            {k: _sleeve.get(k) for k in ("action", "label", "message", "price", "ma200", "idlePct", "asOfDate")}
            if _sleeve.get("active") and _sleeve.get("action") not in ("NONE", "DONT_BUY")
            else None
        )
        emit_event(
            "execution_card",
            {
                "day": _now().split("T")[0],
                "gate": {
                    str(r.get("market")): {
                        "regime": r.get("regime"),
                        "strength": r.get("strength"),
                        "sentiment": r.get("sentiment"),
                        "panicActive": r.get("panicActive"),
                        "panicCooldownEnd": r.get("panicCooldownEnd"),
                        "candidateTotal": r.get("candidateTotal"),
                    }
                    for r in sections
                    if r.get("type") == "regime"
                },
                "candidates": [
                    {
                        "market": c.get("market"),
                        "symbol": c.get("symbol"),
                        "name": c.get("name"),
                        "score": c.get("score"),
                        "rs": c.get("rs"),
                    }
                    for c in sections
                    if c.get("type") == "candidate"
                ],
                "exits": [
                    {
                        "market": h.get("market"),
                        "symbol": h.get("symbol"),
                        "name": h.get("name"),
                        "pnlPct": h.get("pnlPct"),
                        "stopLossLine": h.get("stopLossLine"),
                        "trailingLine": h.get("trailingLine"),
                        "expireDate": h.get("expireDate"),
                    }
                    for h in sections
                    if h.get("type") == "holding" and h.get("action") == "EXIT"
                ],
                "thirdAssetSleeve": _sleeve_pushed,
            },
            dedupe_key=f"execution_card:{_now().split('T')[0]}",
        )
    if brief_type in ("open", "midday"):
        sections += _news_section(5)
    markdown = render_markdown(sections, brief_type)

    brief = upsert_brief(
        brief_date=_now(),
        brief_type=f"trading-{brief_type}",
        items=sections,
        macro_overview=None,
        model_version=MODEL_VERSION,
        source_item_ids=None,
        markdown=markdown,
    )
    logger.info(
        "Generated trading-%s brief: %d sections",
        brief_type,
        len(sections),
    )
    return brief
