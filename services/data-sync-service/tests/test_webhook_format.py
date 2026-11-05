"""Bark webhook formatting tests (OPT-115)."""

from __future__ import annotations

from data_sync_service.service.webhook_format import format_bark


def test_execution_card_formats_gate_candidates_exits() -> None:
    out = format_bark(
        "execution_card",
        {
            "day": "2026-08-14",
            "gate": {
                "A股": {"regime": "Diverging", "panicActive": True, "candidateTotal": 0},
                "港股": {"regime": "Weak", "panicActive": False, "candidateTotal": 2},
            },
            "candidates": [{"symbol": "CN:600801", "name": "华新建材", "score": 67.4}],
            "exits": [{"symbol": "CN:300628", "name": "亿联网络", "pnlPct": -5.4}],
        },
    )
    assert out["title"] == "📋 执行卡·单轨对照 2026-08-14"
    assert "不可买·恐慌冷却" in out["body"]
    assert "港股 Weak · 可买（候选 2）" in out["body"]
    assert "华新建材" in out["body"]
    assert "🚩退出 亿联网络 -5.4%" in out["body"]


def test_execution_card_renders_pyramid_triggers() -> None:
    """Execution card pushes candidate/exits/pyramid; the legacy T6 sleeve
    hint was retired with third_asset_sleeve (OPT-226)."""
    out = format_bark(
        "execution_card",
        {
            "day": "2026-08-19",
            "gate": {"A股": {"regime": "Strong", "panicActive": False, "candidateTotal": 3}},
        },
    )
    assert "择强单轨" not in out["body"]

    # pyramid trigger rendered when present
    out3 = format_bark(
        "execution_card",
        {
            "day": "2026-08-20",
            "gate": {"A股": {"regime": "Diverging", "panicActive": False, "candidateTotal": 0}},
            "pyramidTriggers": [
                {
                    "market": "CN",
                    "symbol": "CN:300628",
                    "name": "亿联网络",
                    "lastClose": 42.01,
                    "triggerLine": 40.897,
                },
            ],
        },
    )
    assert "金字塔加仓触发" in out3["body"]
    assert "亿联网络" in out3["body"]
    assert "加半仓" in out3["body"]


def test_audit_issues_formats_kinds() -> None:
    out = format_bark(
        "audit_issues",
        {
            "day": "2026-08-14",
            "markets": {
                "CN": {
                    "expected": 0,
                    "actual": 2,
                    "extra": [
                        {"symbol": "CN:300628", "kind": "never_entered"},
                        {"symbol": "CN:600002", "kind": "exited"},
                    ],
                    "missing": [],
                },
                "HK": {"expected": 19, "actual": 0, "extra": [], "missing": ["HK:02099"]},
            },
        },
    )
    assert "该卖没卖 CN:600002" in out["body"]
    assert "买了不该买 CN:300628" in out["body"]
    assert "该持没买 HK:02099" in out["body"]


def test_near_stop_and_drawdown() -> None:
    near = format_bark(
        "near_stop", {"symbol": "HK:00700", "line": "trail", "pnl_pct": -7.2, "distance_pct": 0.4}
    )
    assert near["title"] == "⚠️ HK:00700 接近trail"
    dd = format_bark(
        "intraday_drawdown",
        {"symbol": "CN:600801", "entry_price": 25.0, "price": 22.9, "drawdown_pct": -8.4},
    )
    assert dd["title"] == "🔴 CN:600801 跌破 -8%"
    assert "回撤 -8.4%" in dd["body"]


def test_test_event_and_fallback() -> None:
    t = format_bark("test", {})
    assert t["title"] == "✅ Karios 连通测试"
    fb = format_bark("unknown_event", {"a": 1})
    assert fb["title"] == "Karios · unknown_event"


def test_job_failed_includes_error_body() -> None:
    # Regression (2026-08-14): emit sites use "error", formatter read
    # "error_message" -> Bark received an empty message.
    r = format_bark(
        "job_failed",
        {"job_type": "option_iv_daily", "error": "no_iv_data", "last_ts_code": None},
    )
    assert r["title"] == "🔧 任务失败 option_iv_daily"
    assert r["body"] == "no_iv_data"
    # legacy callers using error_message still work
    legacy = format_bark("job_failed", {"job_type": "x", "error_message": "legacy err"})
    assert legacy["body"] == "legacy err"
    # last_ts_code included when present
    with_code = format_bark(
        "job_failed", {"job_type": "y", "error": "boom", "last_ts_code": "CN:600000"}
    )
    assert "last_ts_code CN:600000" in with_code["body"]
    # OPT-144: peripheral streak shown when present
    streak = format_bark(
        "job_failed", {"job_type": "news_fetch_job", "error": "timeout", "streak": 3}
    )
    assert "连败 3 次" in streak["body"]


def test_satellite_action_push_lines() -> None:
    """OPT-223: the 14:30 push carries the selected strategy's action."""
    r = format_bark(
        "satellite_action",
        {
            "tradeDate": "2026-09-17",
            "strategyLabel": "星舰 v2",
            "gateOpen": False,
            "breadth1430": 0.279,
            "lines": [
                "卖出：000978、301024（14:30 到期）",
                "卖出资金停入 H2 停车腿（富国油气QDII 513350.SH）",
                "闸关（广度 27.9%）→ 只卖不买（若开闸会买：002128、000338）",
                "闲钱停车：持有 富国油气QDII 513350.SH（H2 迟滞，2pt 领先才换）",
            ],
        },
    )
    assert r["title"] == "🛰 星舰 v2 · 2026.09.17 14:30"
    assert "卖出资金停入 H2 停车腿" in r["body"]
    assert "闸关" in r["body"]


def test_factor_status_up_push() -> None:
    """2026-10-25 必达 MVP: cold→watch upgrade pages the phone."""
    r = format_bark(
        "factor_status_up",
        {
            "factor_id": "starship_b",
            "name": "星舰B默认",
            "from": "cold",
            "to": "watch",
            "percentile": 55.2,
            "net": -3.5,
            "date": "2026-10-26",
        },
    )
    assert r["title"] == "🧬 星舰B默认 冷库→观察"
    assert "分位 55.2" in r["body"]
    assert "净 -3.5" in r["body"]


def test_factor_revived_push() -> None:
    r = format_bark(
        "factor_revived",
        {
            "factor_id": "harbor",
            "name": "港湾 (参考)",
            "from": "watch",
            "to": "revived",
            "percentile": 99.8,
            "net": 37.5,
            "date": "2026-10-26",
        },
    )
    assert r["title"] == "🟢 因子复活 港湾 (参考)"
    assert "分位 99.8" in r["body"]


def test_sgap_revival_push() -> None:
    r = format_bark("sgap_revival", {"old_pos": 10, "new_pos": 20, "date": "2026-10-26"})
    assert r["title"] == "🟢 S-gap 复活位 10→20"
    assert "星舰B" in r["body"]


def test_daily_adjust_push_no_change_and_sell() -> None:
    """2026-11-05: fixed 14:30 今日调整 for homeport/harbor."""
    empty = format_bark(
        "satellite_action",
        {
            "tradeDate": "2026-11-05",
            "strategyLabel": "母港 M30",
            "dailyAdjust": True,
            "noChange": True,
            "lines": ["今日无需调整"],
        },
    )
    assert empty["title"] == "📋 母港 M30 · 今日调整 2026.11.05"
    assert "今日无需调整" in empty["body"]

    sell = format_bark(
        "satellite_action",
        {
            "tradeDate": "2026-11-05",
            "strategyLabel": "母港 M30",
            "dailyAdjust": True,
            "noChange": False,
            "lines": [
                "卖出：ETF:513350 → 逆回购（峰值回撤8.6%；待开盘卖出转逆回购）",
            ],
        },
    )
    assert sell["title"] == "📋 母港 M30 · 今日调整 2026.11.05"
    assert "513350" in sell["body"]
    assert "逆回购" in sell["body"]


def test_harbor_trail_alert_push() -> None:
    r = format_bark(
        "satellite_action",
        {
            "tradeDate": "2026-11-03",
            "strategyLabel": "港湾停车",
            "trailAlert": True,
            "mustAct": True,
            "lines": [
                "卖出：ETF:513350 → 逆回购",
                "峰值回撤8.6% ≥8%",
                "次日开盘卖出转逆回购",
            ],
        },
    )
    assert r["title"] == "🛑 港湾停车止损 · 2026.11.03"
    assert "ETF:513350" in r["body"]
    assert "次日开盘" in r["body"]

