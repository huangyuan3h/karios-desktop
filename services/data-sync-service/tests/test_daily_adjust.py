"""Fixed 14:30 「今日调整」 composer (2026-11-05).

Covers the two miss modes from the 513350 incident:
- homeport/harbor must get a daily push even when nothing fires
  (「今日无需调整」, never silence);
- a pending Harbor trail exit must be listed as 卖出→逆回购 / 次日开盘.
"""

from __future__ import annotations

from data_sync_service.service.daily_adjust import (
    NO_CHANGE_TEXT,
    compose_daily_adjust,
)


def _trail_sleeve(**over) -> dict:
    sleeve = {
        "active": True,
        "action": "SELL_TO_REPO",
        "label": "卖出转repo(止损)",
        "message": "ETF:513350峰值回撤8.6% ≥8% → 次日开盘卖转逆回购（信号日 2026-11-03，待开盘执行）",
        "symbol": "ETF:513350",
        "trailExit": True,
        "pendingOpenSell": True,
        "signalDate": "2026-11-03",
        "executeHint": "待开盘卖出转逆回购（信号已触发，未执行）",
        "pick": {"key": "OIL", "symbol": "ETF:513350"},
    }
    sleeve.update(over)
    return sleeve


def test_homeport_pending_trail_lists_sell_to_repo() -> None:
    out = compose_daily_adjust(
        "homeport", trade_date="2026-11-05", sleeve=_trail_sleeve()
    )
    assert out["dailyAdjust"] is True
    assert out["noChange"] is False
    assert out["mustAct"] is True
    joined = "\n".join(out["lines"])
    assert "ETF:513350" in joined
    assert "逆回购" in joined
    assert "待开盘" in joined
    assert NO_CHANGE_TEXT not in joined
    assert out["adjustments"][0]["side"] == "SELL"
    assert out["adjustments"][0]["symbol"] == "ETF:513350"
    assert out["adjustments"][0]["pendingOpenSell"] is True


def test_homeport_hold_emits_explicit_no_change() -> None:
    out = compose_daily_adjust(
        "homeport",
        trade_date="2026-11-05",
        sleeve={
            "active": True,
            "action": "HOLD",
            "message": "港湾停车：持有 ETF:513350",
            "pick": {"key": "OIL", "symbol": "ETF:513350"},
        },
    )
    assert out["noChange"] is True
    assert out["mustAct"] is False
    assert out["lines"][0] == NO_CHANGE_TEXT
    assert out["adjustments"] == []


def test_harbor_rotate_lists_sell_and_buy() -> None:
    out = compose_daily_adjust(
        "harbor",
        trade_date="2026-11-05",
        sleeve={
            "active": True,
            "action": "ROTATE",
            "message": "港湾停车轮动：卖出 ETF:513350 → ETF:518880",
            "symbol": "ETF:513350",
            "pick": {"key": "GOLD", "symbol": "ETF:518880"},
            "executeHint": "次日开盘执行轮动",
        },
    )
    sides = [a["side"] for a in out["adjustments"]]
    assert sides == ["SELL", "BUY"]
    assert out["mustAct"] is True
    assert "次日开盘执行轮动" in "\n".join(out["lines"])


def test_satellite_panel_no_exits_no_gate_is_no_change() -> None:
    panel = {
        "tradeDate": "2026-11-05",
        "decisionAvailable": True,
        "gateOpen": False,
        "breadth1430": 0.2,
        "exits": [],
        "heldLegs": [],
        "ranked": [],
    }
    out = compose_daily_adjust("starship", trade_date="2026-11-05", panel=panel)
    assert out["noChange"] is True
    assert out["lines"][0] == NO_CHANGE_TEXT


def test_satellite_panel_exits_are_adjustments() -> None:
    panel = {
        "tradeDate": "2026-11-05",
        "decisionAvailable": True,
        "gateOpen": False,
        "breadth1430": 0.2,
        "exits": [{"ts": "000978.SZ", "exitDue": "2026-11-05"}],
        "heldLegs": [],
        "ranked": [],
    }
    out = compose_daily_adjust("starship", trade_date="2026-11-05", panel=panel)
    assert out["noChange"] is False
    assert out["mustAct"] is True
    assert out["adjustments"][0]["symbol"] == "000978.SZ"
    assert out["adjustments"][0]["side"] == "SELL"


def test_satellite_panel_missing_still_emits() -> None:
    out = compose_daily_adjust("starport", trade_date="2026-11-05", panel=None)
    assert out["noChange"] is True
    assert NO_CHANGE_TEXT in out["lines"][0]
    assert any("面板缺失" in ln for ln in out["lines"])


def test_homeport_intraday_warn_is_conditional_not_silent() -> None:
    """14:30 print at/under the trail line → warning line, clock unchanged."""
    sleeve = {
        "active": True,
        "action": "HOLD",
        "label": "持有",
        "message": "港湾停车：持有 ETF:513350（mom60 4.98%）",
        "pick": {"key": "OIL", "symbol": "ETF:513350"},
        "intradayWarn": {
            "active": True,
            "status": "breached",
            "symbol": "ETF:513350",
            "px": 1.281,
            "thresholdPx": 1.289,
            "peak": 1.401,
            "drawdownPct": 8.6,
            "message": "ETF:513350现价1.281 ≤ 门槛价1.289（峰值1.401 回撤8.6%）→ 现价已达卖出条件",
            "executeHint": "若收盘仍低于门槛价 → 明日开盘卖出转逆回购（不盘中卖）",
            "tradeDate": "2026-11-05",
        },
    }
    out = compose_daily_adjust("homeport", trade_date="2026-11-05", sleeve=sleeve)
    joined = "\n".join(out["lines"])
    assert "现价已达卖出条件" in joined
    assert "门槛价1.289" in joined
    assert "若收盘仍低于门槛价" in joined
    assert "持有：" not in joined  # steer: never 持有 once the print is at/under
    assert NO_CHANGE_TEXT not in joined
    assert out["noChange"] is False
    assert out["mustWatch"] is True
    # Conditional: never a confirmed must-act (the close still decides).
    assert out["mustAct"] is False
    adj = out["adjustments"][0]
    assert adj["side"] == "SELL" and adj["to"] == "逆回购"
    assert adj["conditional"] is True
    assert adj["thresholdPx"] == 1.289


def test_satellite_mode_still_surfaces_the_live_harbor_trail() -> None:
    """A satellite mode selected must not hide the Harbor parking exit."""
    panel = {
        "tradeDate": "2026-11-05",
        "decisionAvailable": True,
        "gateOpen": False,
        "breadth1430": 0.2,
        "exits": [],
        "heldLegs": [],
        "ranked": [],
    }
    out = compose_daily_adjust(
        "starship_b", trade_date="2026-11-05", panel=panel, sleeve=_trail_sleeve()
    )
    joined = "\n".join(out["lines"])
    assert "港湾停车腿 · 卖出：ETF:513350 → 逆回购" in joined
    assert out["mustAct"] is True
    assert any(a["symbol"] == "ETF:513350" for a in out["adjustments"])
