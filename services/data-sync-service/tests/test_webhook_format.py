"""Bark webhook formatting tests (OPT-115)."""

from __future__ import annotations

from data_sync_service.service.webhook_format import format_bark


def test_execution_card_formats_gate_candidates_exits() -> None:
    out = format_bark(
        "execution_card",
        {
            "day": "2026-08-14",
            "gate": {
                "A股": {"regime": "Diverging", "panicActive": True,
                        "candidateTotal": 0},
                "港股": {"regime": "Weak", "panicActive": False,
                         "candidateTotal": 2},
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


def test_execution_card_renders_sleeve_when_actionable() -> None:
    """T6 (2026-08-19): the Bark push includes the sleeve hint for actionable
    actions only (BUY / SELL) — never for DONT_BUY on a closed-gate day."""
    out = format_bark(
        "execution_card",
        {
            "day": "2026-08-19",
            "gate": {"A股": {"regime": "Strong", "panicActive": False, "candidateTotal": 3}},
            "thirdAssetSleeve": {
                "action": "BUY_513100", "label": "建议买入 513100",
                "message": "闲置资金 90% 且 ETF:513100 在200日线上 → 建议买入",
                "price": 2.239, "ma200": 1.983, "idlePct": 90.0,
            },
        },
    )
    assert "择强单轨：建议买入 513100" in out["body"]
    assert "闲置资金 90% 且 ETF:513100 在200日线上" in out["body"]
    assert "现价 2.239 · MA200 1.983" in out["body"]

    # DONT_BUY (gate closed) must NOT be pushed as a buy prompt
    out2 = format_bark(
        "execution_card",
        {
            "day": "2026-08-19",
            "gate": {"A股": {"regime": "Weak", "panicActive": True, "candidateTotal": 0}},
            "thirdAssetSleeve": None,
        },
    )
    assert "择强单轨" not in out2["body"]

    # pyramid trigger rendered when present
    out3 = format_bark(
        "execution_card",
        {
            "day": "2026-08-20",
            "gate": {"A股": {"regime": "Diverging", "panicActive": False, "candidateTotal": 0}},
            "pyramidTriggers": [
                {"market": "CN", "symbol": "CN:300628", "name": "亿联网络",
                 "lastClose": 42.01, "triggerLine": 40.897},
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
                    "expected": 0, "actual": 2,
                    "extra": [
                        {"symbol": "CN:300628", "kind": "never_entered"},
                        {"symbol": "CN:600002", "kind": "exited"},
                    ],
                    "missing": [],
                },
                "HK": {"expected": 19, "actual": 0, "extra": [],
                        "missing": ["HK:02099"]},
            },
        },
    )
    assert "该卖没卖 CN:600002" in out["body"]
    assert "买了不该买 CN:300628" in out["body"]
    assert "该持没买 HK:02099" in out["body"]


def test_near_stop_and_drawdown() -> None:
    near = format_bark("near_stop", {"symbol": "HK:00700", "line": "trail",
                                     "pnl_pct": -7.2, "distance_pct": 0.4})
    assert near["title"] == "⚠️ HK:00700 接近trail"
    dd = format_bark("intraday_drawdown",
                     {"symbol": "CN:600801", "entry_price": 25.0,
                      "price": 22.9, "drawdown_pct": -8.4})
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


def test_twin_star_reminder_readable() -> None:
    out = format_bark(
        "twin_star_reminder",
        {
            "title": "机会双子星 · 14:30 操作",
            "detail": "今日14:30卖 300413.SZ · 核心50%: OIL (HOLD) · 卫星: R-wide 开闸 (breadth 0.6) · 10 只缺口票 · 买 000001.SZ(amp1%)",
        },
    )
    assert "14:30" in out["title"]
    assert "今日14:30卖 300413.SZ" in out["body"]
    assert "买 000001.SZ(amp1%)" in out["body"]
    assert "{" not in out["body"]


def test_audit_issues_renders_sat_leg_as_info() -> None:
    """OPT-140: satellite leg is info (engine-book对照), never 买了不该买."""
    out = format_bark(
        "audit_issues",
        {
            "day": "2026-09-04",
            "markets": {
                "CN": {
                    "expected": 0,
                    "actual": 1,
                    "extra": [],
                    "missing": [],
                    "sat": {
                        "expected": 2,
                        "actual": 1,
                        "extra": ["CN:600099"],
                        "missing": ["CN:600088"],
                    },
                },
            },
        },
    )
    assert "买了不该买" not in out["body"]
    assert "🛰 卫星腿（引擎应持 2 / 实持 1" in out["body"]
