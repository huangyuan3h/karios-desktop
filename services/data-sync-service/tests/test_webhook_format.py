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
    assert out["title"] == "📋 执行卡 2026-08-14"
    assert "不可买·恐慌冷却" in out["body"]
    assert "港股 Weak · 可买（候选 2）" in out["body"]
    assert "华新建材" in out["body"]
    assert "🚩退出 亿联网络 -5.4%" in out["body"]


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
