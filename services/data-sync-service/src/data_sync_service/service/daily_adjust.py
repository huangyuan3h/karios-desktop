"""Fixed daily 14:30 「今日调整」 composer (2026-11-05).

Why: the 14:30 satellite panel push (``satellite_live_job._emit_action``) only
ran for ``SATELLITE_MODES``. The default/live mode is ``homeport`` (母港 M30),
so a Harbor parking trail exit (e.g. ETF:513350 on 2026-11-03) never produced
a Bark push. This module composes ONE daily payload per selected mode:

- satellite modes: reuse ``satellite_actions.compose_satellite_action``.
- ``harbor`` / ``homeport``: compose from ``build_multi_asset_sleeve`` (the
  parking leg, including the sticky trail exit).
- Empty day: the body explicitly says 「今日无需调整」 (never silence).

Pure functions: the scheduler supplies the sleeve/panel/holdings/whip. Omitted
params fall back to a read-only, fail-open registry / whip-memory read; this
module never writes settings, the whip memory or an order.
"""

from __future__ import annotations

import logging
from typing import Any

from data_sync_service.service.agent_risk_gates import (
    STOP_WARNING_PREFIX,
    build_risk_gates,
    rebuy_advice,
)
from data_sync_service.service.satellite_actions import (
    SATELLITE_MODES,
    STRATEGY_LABELS,
    compose_satellite_action,
)

logger = logging.getLogger(__name__)

NO_CHANGE_TEXT = "今日无需调整"


def _day(day: str | None) -> str:
    from data_sync_service.service.trade_calendar_utils import shanghai_today_iso

    return str(day or shanghai_today_iso())[:10]


def load_live_sleeve(day: str) -> dict[str, Any]:
    """Build the Live Harbor parking hint from registry holdings (fail-open)."""
    try:
        from data_sync_service.db.watchlist_automation import list_registry
        from data_sync_service.service.multi_asset_sleeve import build_multi_asset_sleeve

        raw_holdings = [
            {
                "symbol": str(r.get("symbol") or "").upper(),
                "positionPct": (r.get("payload") or {}).get("positionPct", r.get("positionPct")),
                "costPrice": (r.get("payload") or {}).get("costPrice", r.get("costPrice")),
                "entryDate": (r.get("payload") or {}).get("entryDate", r.get("entryDate")),
                "name": (r.get("payload") or {}).get("name", r.get("name")),
                "ts_code": r.get("ts_code"),
            }
            for r in list_registry()
            if str(r.get("symbol") or "").upper().startswith(("CN:", "HK:", "ETF:"))
        ]
        return build_multi_asset_sleeve(
            day=day, cn_block={"holdings": raw_holdings}, holdings_override=raw_holdings
        )
    except Exception as exc:  # noqa: BLE001
        return {
            "active": False,
            "action": "NONE",
            "message": f"停车场状态不可用：{exc}",
            "note": str(exc),
        }


def _sleeve_lines(sleeve: dict[str, Any]) -> tuple[list[str], list[dict[str, Any]]]:
    """Harbor/homeport: (lines, adjustments) from the Live sleeve payload."""
    action = str(sleeve.get("action") or "NONE").upper()
    symbol = str(sleeve.get("symbol") or "").upper()
    message = str(sleeve.get("message") or "")
    hint = str(sleeve.get("executeHint") or "")
    pending = bool(sleeve.get("pendingOpenSell"))
    signal_date = str(sleeve.get("signalDate") or "")
    lines: list[str] = []
    adjustments: list[dict[str, Any]] = []

    def _adj(side: str, sym: str, reason: str, *, to: str = "", hint_text: str = "") -> None:
        adjustments.append(
            {
                "symbol": sym,
                "side": side,
                "to": to,
                "reason": reason,
                "signalDate": signal_date,
                "pendingOpenSell": pending,
                "executeHint": hint_text or hint,
            }
        )

    if action == "SELL_TO_REPO":
        sym = symbol or "停车 ETF"
        hint_text = hint or "次日开盘卖出转逆回购"
        # Keep the drawdown fact only: the sleeve `message` already carries the
        # clock + signal date, and repeating them three times reads badly on a
        # phone push ("卖出：X → 逆回购（峰值回撤…；待开盘…）（信号日…）").
        reason_core = message.split("→")[0].strip() if "→" in message else message
        reason_core = reason_core or "停车腿退出"
        tail = f"（信号日 {signal_date}）" if pending and signal_date else ""
        lines.append(f"卖出：{sym} → 逆回购（{reason_core}）· {hint_text}{tail}")
        _adj("SELL", sym, reason_core, to="逆回购", hint_text=hint_text)
    elif action == "ROTATE":
        sym = symbol or "停车 ETF"
        pick = (sleeve.get("pick") or {}).get("symbol") or (sleeve.get("pick") or {}).get("key")
        lines.append(f"轮动：{message or f'卖出 {sym} → {pick}'}（{hint or '次日开盘执行轮动'}）")
        _adj("SELL", sym, message or "停车轮动", to=str(pick or ""), hint_text=hint)
        if pick:
            _adj("BUY", str(pick), f"停车轮动买入 {pick}", hint_text=hint)
    elif action == "SELL_TO_A_SHARE":
        sym = symbol or "停车 ETF"
        lines.append(f"卖出：{sym} → 股票篮（{message or '停车场让位股票核心'}；{hint}）")
        _adj("SELL", sym, message or "停车场让位股票核心", to="股票篮", hint_text=hint)
    elif action == "BUY":
        sym = symbol or str((sleeve.get("pick") or {}).get("symbol") or "")
        lines.append(
            f"买入：{sym}（{message or '闲置现金停入停车 ETF'}；{hint or '次日开盘买入'}）"
        )
        _adj("BUY", sym, message or "闲置现金停车", hint_text=hint)
    elif action == "HOLD":
        pick = (sleeve.get("pick") or {}).get("symbol") or (sleeve.get("pick") or {}).get("key")
        warn_pre = sleeve.get("intradayWarn") or {}
        if warn_pre.get("active"):
            # Steer: once the print is at/under the line, never print 持有 —
            # the warning below is the instruction (close confirms the exit).
            pass
        else:
            lines.append(f"持有：{pick or symbol}（{message or '停车腿持有'}）")
        if pending:
            # Defensive: a sticky trail must never be hidden behind HOLD.
            lines = [
                f"卖出：{symbol or '停车 ETF'} → 逆回购（{message or '峰值回撤触发'}；"
                f"{hint or '待开盘执行'}）"
            ]
            _adj("SELL", symbol or "停车 ETF", message or "峰值回撤触发", to="逆回购")
    else:  # NONE / DONT_BUY
        if message:
            lines.append(message)
    # 14:30 early warning (steer 2026-11-05): a print at/under the trail line is
    # a conditional Must-watch. It must never be silent and never imply 持有/买.
    warn = sleeve.get("intradayWarn") or {}
    if warn.get("active") and action != "SELL_TO_REPO":
        warn_sym = str(warn.get("symbol") or symbol or "停车 ETF").upper()
        warn_hint = str(warn.get("executeHint") or "若收盘仍低于门槛价 → 明日开盘卖出转逆回购")
        lines.append(f"{warn.get('message') or f'{warn_sym}现价触线'}")
        lines.append(warn_hint)
        adjustments.append(
            {
                "symbol": warn_sym,
                "side": "SELL",
                "to": "逆回购",
                "reason": "14:30 现价触线（待收盘确认）",
                "conditional": True,
                "signalDate": str(warn.get("tradeDate") or ""),
                "pendingOpenSell": False,
                "executeHint": warn_hint,
                "px": warn.get("px"),
                "thresholdPx": warn.get("thresholdPx"),
                "peak": warn.get("peak"),
                "drawdownPct": warn.get("drawdownPct"),
            }
        )
    return lines, adjustments


def _whip_state(
    sleeve: dict[str, Any],
    day: str,
    whip: dict[str, Any] | None,
) -> dict[str, Any]:
    """Same-key rebuy state (fail-open; never saves memory from this composer).

    ``observe=False``: the daily 14:30 push must not write the whip memory as a
    side effect of composing text. The caller may pass the state it already
    observed (the judgment brief does); otherwise we read the existing memory.
    """
    if isinstance(whip, dict) and whip:
        return whip
    try:
        from data_sync_service.service.agent_whip import evaluate_whip

        return evaluate_whip(sleeve or None, None, day=day, observe=False)
    except Exception as exc:  # noqa: BLE001 — advice only, never break the push
        logger.warning("daily adjust whip state unavailable: %s", exc)
        return {}


def _held_rows(
    sleeve: dict[str, Any],
    holdings: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    """Held rows for the stop check: caller holdings, else the sleeve's own leg."""
    rows = [r for r in (holdings or []) if isinstance(r, dict)]
    if rows:
        return rows
    if not sleeve.get("holding"):
        return []
    pick = sleeve.get("pick") or {}
    symbol = str(sleeve.get("symbol") or pick.get("symbol") or "").upper()
    if not symbol:
        return []
    return [
        {
            "symbol": symbol,
            "key": pick.get("key") or sleeve.get("pickKey"),
            "name": pick.get("name") or sleeve.get("label"),
            "stopLossLine": sleeve.get("stopLossLine"),
            "trailingLine": sleeve.get("trailingLine"),
            "stopRule": sleeve.get("stopRule"),
        }
    ]


def _annotate_rebuys(
    adjustments: list[dict[str, Any]],
    risk: dict[str, Any],
) -> list[dict[str, Any]]:
    """Mark every BUY / ROTATE target that would rebuy a just-stopped key.

    Advice only: the row stays visible and Must-act SELL keeps its position
    ahead of it. Returns the matched annotations (also mirrored on the row).
    """
    rebuy = risk.get("sameKeyRebuy") or {}
    last_by_key = rebuy.get("recentExits") or {}
    copy = rebuy.get("copy")
    matches: list[dict[str, Any]] = []
    for row in adjustments:
        if str(row.get("side") or "").upper() != "BUY":
            continue
        advice = rebuy_advice(symbol=row.get("symbol"), key=row.get("key"), whip={"context": rebuy})
        if advice is None:
            continue
        copy_value = copy or advice["copy"]
        row["sameKeyRebuy"] = True
        row["deferRebuy"] = True
        row["annotation"] = copy_value
        row["lastExitDate"] = advice["lastExitDate"]
        matches.append(
            {
                **advice,
                "symbol": row.get("symbol") or advice.get("symbol"),
                "key": advice.get("key") or row.get("key"),
                "copy": copy_value,
                "lastExitDate": advice["lastExitDate"] or last_by_key.get(advice.get("key")),
            }
        )
    return matches


def compose_daily_adjust(
    mode: str,
    *,
    trade_date: str | None = None,
    sleeve: dict[str, Any] | None = None,
    panel: dict[str, Any] | None = None,
    parking: dict[str, Any] | None = None,
    holdings: list[dict[str, Any]] | None = None,
    whip: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """One guaranteed daily adjustment payload for ``mode``.

    ``noChange`` is True when there is nothing to execute; ``lines`` then
    starts with 「今日无需调整」 so the Bark body is never empty/silent.

    Two advice-only gates are annotated on top (2026-11-11): a risky holding /
    proposed buy with no stop gets a 止损缺口 warning line (never flips
    ``mustAct`` / ``noChange``), and a BUY / ROTATE into a recently trailed
    parking key is kept visible but flagged ``sameKeyRebuy`` / ``deferRebuy``.
    """
    day = _day(trade_date)
    label = STRATEGY_LABELS.get(mode, mode)
    adjustments: list[dict[str, Any]] = []
    lines: list[str] = []
    base: dict[str, Any] = {}
    if mode in SATELLITE_MODES:
        if panel and panel.get("decisionAvailable"):
            sat = compose_satellite_action(panel, mode, parking=parking)
            base = dict(sat)
            lines = list(sat.get("lines") or [])
            for ts in sat.get("exits") or []:
                adjustments.append(
                    {
                        "symbol": str(ts),
                        "side": "SELL",
                        "to": str(sat.get("proceeds") or ""),
                        "reason": "卫星腿 14:30 到期",
                        "executeHint": "14:30 卖出",
                    }
                )
            for buy in sat.get("buys") or []:
                adjustments.append(
                    {
                        "symbol": str(buy.get("ts") or ""),
                        "side": "BUY",
                        "to": "",
                        "reason": "卫星闸开买入",
                        "executeHint": "14:30 买入",
                    }
                )
        else:
            lines = ["卫星现场面板缺失，今日卫星腿无法判定；请检查 14:30 任务"]
        # The Live account can ALSO hold a Harbor parking leg (default book) while
        # a satellite mode is selected — the trail exit must never be hidden by
        # the mode switch (2026-11-05: default book still held 63.5% OIL).
        # The scheduler passes the live sleeve; pure satellite unit tests pass
        # none (no DB read from a composer).
        if sleeve is not None:
            harbor_lines, harbor_adjust = _sleeve_lines(sleeve)
            if harbor_adjust:
                lines.extend(f"港湾停车腿 · {ln}" for ln in harbor_lines)
                adjustments.extend(harbor_adjust)
    else:
        sleeve = sleeve or load_live_sleeve(day)
        lines, adjustments = _sleeve_lines(sleeve)

    live_sleeve = sleeve if isinstance(sleeve, dict) else {}
    whip_state = _whip_state(live_sleeve, day, whip)
    proposed_buys = [a for a in adjustments if str(a.get("side") or "").upper() == "BUY"]
    risk = build_risk_gates(
        whip=whip_state,
        holdings=_held_rows(live_sleeve, holdings),
        proposed_buys=proposed_buys,
    )
    rebuy_matches = _annotate_rebuys(adjustments, risk)
    missing_by_symbol = {
        str(row.get("symbol") or "").upper(): row
        for row in (risk.get("stops") or {}).get("missing") or []
        if row.get("source") == "buy"
    }
    for row in proposed_buys:
        info = missing_by_symbol.get(str(row.get("symbol") or "").upper())
        if info:
            row["stopMissing"] = True
            row["stopWarning"] = info.get("message")
    for match in rebuy_matches:
        sym = match.get("symbol") or match.get("key")
        lines.append(
            f"⚠️ {sym}：{match.get('copy')}（{match.get('key')} {match.get('lastExitDate')} 止损）"
        )
    for warning in (risk.get("stops") or {}).get("warnings") or []:
        lines.append(f"⚠️ {STOP_WARNING_PREFIX}：{warning}")

    no_change = not adjustments
    if no_change:
        lines = [NO_CHANGE_TEXT] + [ln for ln in lines if NO_CHANGE_TEXT not in ln]
    must_act = any(a.get("side") == "SELL" and not a.get("conditional") for a in adjustments)
    return {
        **base,
        "tradeDate": day,
        "mode": mode,
        "strategyLabel": label,
        "dailyAdjust": True,
        "noChange": no_change,
        "mustAct": must_act,
        "mustWatch": any(a.get("conditional") for a in adjustments),
        "adjustments": adjustments,
        "lines": lines,
        "summary": "；".join(lines),
        "riskGates": risk,
        "warnings": list(risk.get("warnings") or []),
    }


def emit_trail_if_active(
    day: str, *, sleeve: dict[str, Any] | None = None
) -> dict[str, Any] | None:
    """Emit a Bark 「港湾停车止损」 event when the sleeve says SELL_TO_REPO.

    Called after the post-close sleeve rebuild (``sleeve_paper_auto`` 18:20),
    so the close-of-signal day is pushed the SAME evening — not only at the
    next day's 14:30 adjust. While the leg remains held the sticky pending
    signal re-emits once per day (dedupe day+symbol+action). Never raises.
    """
    try:
        sleeve = sleeve if sleeve is not None else load_live_sleeve(day)
        action = str(sleeve.get("action") or "NONE").upper()
        if action != "SELL_TO_REPO":
            return None
        symbol = str(sleeve.get("symbol") or "停车ETF").upper()
        pending = bool(sleeve.get("pendingOpenSell"))
        signal_date = str(sleeve.get("signalDate") or day)
        hint = str(sleeve.get("executeHint") or "次日开盘卖出转逆回购")
        message = str(sleeve.get("message") or f"{symbol} 停车腿退出")
        payload = {
            "tradeDate": day,
            "mode": "harbor",
            "strategyLabel": "港湾停车",
            "dailyAdjust": False,
            "trailAlert": True,
            "mustAct": True,
            "symbol": symbol,
            "pendingOpenSell": pending,
            "signalDate": signal_date,
            "executeHint": hint,
            "lines": [
                f"卖出：{symbol} → 逆回购",
                message,
                hint + (f"（信号日 {signal_date}）" if pending else ""),
            ],
            "summary": f"{symbol} 卖出转逆回购（{message}；{hint}）",
        }
        from data_sync_service.db.webhook import emit_event

        emit_event(
            "satellite_action",
            payload,
            dedupe_key=f"harbor_trail:{day}:{symbol}:SELL_TO_REPO",
        )
        return payload
    except Exception:  # noqa: BLE001
        return None
