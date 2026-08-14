"""Bark push formatting for webhook events (2026-08-14 · OPT-115).

Converts a webhook event into a Bark push payload (title + body, Chinese,
phone-friendly). Generic subscribers still receive the raw HMAC-signed event
JSON; only provider='bark' subscriptions are formatted.
"""

from __future__ import annotations

from typing import Any

PUSH_GROUP = "Karios"


def _lines(*parts: Any) -> str:
    return "\n".join(str(p) for p in parts if p not in (None, "", [], {}))


def _gate_line(name: str, gate: dict[str, Any]) -> str:
    regime = str(gate.get("regime") or "—")
    panic = bool(gate.get("panicActive"))
    cand_total = gate.get("candidateTotal")
    if panic:
        state = "不可买·恐慌冷却"
    elif cand_total:
        state = f"可买（候选 {cand_total}）"
    else:
        state = "可买（无候选）"
    return f"{name} {regime} · {state}"


def format_bark(event_type: str, payload: dict[str, Any]) -> dict[str, str]:
    """event_type + payload -> Bark push (title, body)."""
    p = payload or {}
    if event_type == "execution_card":
        gates = p.get("gate") or {}
        candidates = p.get("candidates") or []
        exits = p.get("exits") or []
        body = _lines(
            _lines(*(_gate_line(k, v) for k, v in gates.items())),
            *([""] + [f"  {c.get('name') or c.get('symbol')} score={c.get('score')}"
                      for c in candidates] or []),
            *([""] + [f"  🚩退出 {e.get('name') or e.get('symbol')} {e.get('pnlPct')}%"
                      for e in exits] or []),
        )
        return {"title": f"📋 执行卡 {p.get('day', '')}".strip(), "body": body}

    if event_type == "audit_issues":
        markets = p.get("markets") or {}
        lines = []
        for m, v in markets.items():
            parts = []
            for e in (v.get("extra") or []):
                kind = "该卖没卖" if e.get("kind") == "exited" else "买了不该买"
                parts.append(f"  {kind} {e.get('symbol')}")
            for sym in (v.get("missing") or []):
                parts.append(f"  该持没买 {sym}")
            if parts:
                lines.append(f"{m}（回测应持 {v.get('expected')} / 实持 {v.get('actual')}）")
                lines.extend(parts)
        return {
            "title": f"⚠️ 行为对账 {p.get('day', '')}".strip(),
            "body": _lines(*lines) or "无明细",
        }

    if event_type == "near_stop":
        sym = p.get("symbol") or ""
        line = str(p.get("line") or "")
        pnl = p.get("pnl_pct")
        dist = p.get("distance_pct")
        return {
            "title": f"⚠️ {sym} 接近{line}",
            "body": _lines(
                f"现价距{line} {dist}%" if dist is not None else None,
                f"浮盈 {pnl}%" if pnl is not None else None,
            ),
        }

    if event_type == "intraday_drawdown":
        sym = p.get("symbol") or ""
        return {
            "title": f"🔴 {sym} 跌破 -8%",
            "body": _lines(
                f"入场 {p.get('entry_price')} → 现价 {p.get('price')}",
                f"回撤 {p.get('drawdown_pct')}%",
            ),
        }

    if event_type == "candidate_added":
        added = p.get("added") or []
        market = str(p.get("market") or "")
        return {
            "title": f"🆕 {market} S-3 新候选（{len(added)}）",
            "body": _lines(*[f"  {s}" for s in added]) or "无",
        }

    if event_type == "recon_missing":
        markets = p.get("markets") or []
        return {
            "title": "📊 周对账缺票",
            "body": _lines(*[f"  {m}" for m in markets]) or "无",
        }

    if event_type == "job_failed":
        job = p.get("job_type") or p.get("jobType") or "?"
        return {
            "title": f"🔧 任务失败 {job}",
            "body": _lines(str(p.get("error_message") or "")) or "见控制台",
        }

    if event_type == "test":
        return {"title": "✅ Karios 连通测试", "body": "Webhook 链路正常"}

    # Generic fallback: keep it readable.
    return {
        "title": f"Karios · {event_type}",
        "body": _lines(str(p)[:300] or ""),
    }
