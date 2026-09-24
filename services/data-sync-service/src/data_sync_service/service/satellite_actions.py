"""Strategy-aware 14:30 action composition (OPT-223, 2026-09-17).

Turns the raw live panel (+ exits) into the per-strategy instruction the card,
Bark push and in-app notification show. Pure functions, no DB/network — the
same payload feeds `format_bark` and `notifications.py` so all three channels
cannot drift.
"""

from __future__ import annotations

from typing import Any

STRATEGY_LABELS: dict[str, str] = {
    "harbor": "港湾",
    "homeport": "母港 M30",
    "starport": "星港",
    "starship": "星舰 v2",
    "starship_robust": "稳健星舰 H2-a25",
    "starship_b": "星舰 B",
    "twin_star": "双子星",
}
# Strategies that carry the satellite leg (only these get the 14:30 action).
SATELLITE_MODES = ("starport", "starship", "starship_robust", "starship_b", "twin_star")
MAX_BUYS = 4

# The satellite-standalone family parks its idle cash in the H2 sleeve itself
# (the overlays return proceeds to their base leg instead).
_STANDALONE_MODES = ("starship", "starship_robust")

# Where the satellite sale proceeds go, per structure (mirrors the card copy).
_CORE_LABEL = {"starport": "母港腿（港湾×B3）", "twin_star": "港湾腿"}


def _pct(v: Any) -> str:
    try:
        return f"{float(v) * 100:.1f}%"
    except (TypeError, ValueError):
        return "—"


def parking_label(parking: dict[str, Any] | None) -> str:
    if not parking:
        return "现金/逆回购"
    name = str(parking.get("name") or parking.get("key") or "")
    ts = str(parking.get("ts") or "")
    return f"{name} {ts}".strip() or "现金/逆回购"


def compose_satellite_action(
    panel: dict[str, Any],
    mode: str,
    *,
    parking: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Compose today's satellite action for ``mode`` from the live panel.

    ``parking`` = the H2 want (``multi_asset_sleeve._pick``) — the destination
    of the satellite's idle/sale cash.
    """
    labels = STRATEGY_LABELS
    label = labels.get(mode, mode)
    gate_open = panel.get("gateOpen") is True
    ranked = panel.get("ranked") or []
    candidates = [
        e
        for e in ranked
        if e.get("inBucket") and not e.get("skipReason") and e.get("fillable")
    ][:MAX_BUYS]
    exits = [str(x.get("ts") or "") for x in (panel.get("exits") or []) if x.get("ts")]
    held = [
        str(x.get("ts") or "")
        for x in (panel.get("heldLegs") or [])
        if x.get("ts")
    ]
    park = parking_label(parking)
    proceeds = (
        f"卖出资金停入 H2 停车腿（{park}）"
        if mode in _STANDALONE_MODES
        else f"卖出资金回到{_CORE_LABEL.get(mode, '港湾腿')}（该腿闲置现金停 H2 停车腿：{park}）"
    )
    lines: list[str] = []
    if exits:
        lines.append(f"卖出：{'、'.join(t.split('.')[0] for t in exits)}（14:30 到期）")
        lines.append(proceeds)
    else:
        lines.append("今日无到期卖出" + (f"（继续持有 {'、'.join(t.split('.')[0] for t in held)}）" if held else ""))
    if gate_open:
        buys = "、".join(str(e.get("ts", "")).split(".")[0] for e in candidates) or "无合规候选"
        lines.append(f"闸开（广度 {_pct(panel.get('breadth1430'))}）→ 买入：{buys}")
    else:
        would = "、".join(str(e.get("ts", "")).split(".")[0] for e in candidates)
        tail = f"（若开闸会买：{would}）" if would else ""
        lines.append(f"闸关（广度 {_pct(panel.get('breadth1430'))}）→ 只卖不买{tail}")
    if mode == "starship":
        lines.append(f"闲钱停车：持有 {park}（H2 迟滞，2pt 领先才换）")
    elif mode == "starship_robust":
        lines.append(
            f"闲钱停车：25% 停 {park}（H2 迟滞，2pt 领先才换）+ 75% 停 B3 风险预算（5 资产逆波动率，月初再平衡）"
        )
    elif mode == "starship_b":
        lines.append(
            "闲钱停车：100% 停 {国债+黄金+纳指} 逆波动率（3 腿月频，月初再平衡）"
        )
    return {
        "tradeDate": panel.get("tradeDate"),
        "mode": mode,
        "strategyLabel": label,
        "gateOpen": panel.get("gateOpen"),
        "breadth1430": panel.get("breadth1430"),
        "gapCount": panel.get("gapCount"),
        "exits": exits,
        "held": held,
        "buys": [
            {
                "ts": e.get("ts"),
                "gapPct": e.get("gapPct"),
                "amp1430Pct": e.get("amp1430Pct"),
                "px1430": e.get("px1430"),
            }
            for e in candidates
        ],
        "proceeds": proceeds,
        "parking": parking,
        "parkingLabel": park,
        "lines": lines,
        "summary": "；".join(lines),
    }


def action_severity(action: dict[str, Any]) -> str:
    """Bark/notification severity: exits or an open gate are actionable."""
    if action.get("exits") or action.get("gateOpen") is True:
        return "high"
    return "medium"
