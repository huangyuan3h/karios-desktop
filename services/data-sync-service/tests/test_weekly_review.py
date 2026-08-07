"""OPT-065 tests: weekly review aggregation + report rendering.

The aggregate queries hit the DB; tests patch the DB-facing helpers and the
service boundaries so they run anywhere. Rendering is pure — covered here.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient  # type: ignore[import-not-found]

from data_sync_service.main import app  # type: ignore[import-not-found]

client = TestClient(app)


@pytest.fixture(autouse=True)
def _reset_settings_cache() -> None:
    from data_sync_service import config

    config.get_settings.cache_clear()
    yield
    config.get_settings.cache_clear()


def test_week_bounds() -> None:
    from data_sync_service.service.weekly_review import week_bounds

    assert week_bounds("2026-08-08") == ("2026-08-03", "2026-08-08")  # Sat of ISO week
    assert week_bounds("2026-08-03") == ("2026-08-03", "2026-08-03")  # Monday
    assert week_bounds("2026-08-05") == ("2026-08-03", "2026-08-05")  # Wednesday
    with pytest.raises(ValueError):
        week_bounds("not-a-date")


def _fake_stats() -> dict:
    return {
        "week": {"start": "2026-08-03", "end": "2026-08-08"},
        "decisionVolume": {"total": 10, "bySource": {"ALPHA": 8, "TV": 2}},
        "paper": {
            "closed": 4,
            "wins": 2,
            "winRate": 0.5,
            "avgNetPnlPct": 1.25,
            "byReason": {
                "target_hit": {"count": 2, "avgNet": 3.0, "winRate": 1.0},
                "stop_hit": {"count": 2, "avgNet": -0.5, "winRate": 0.0},
            },
        },
        "exitAttribution": {
            "withForward": 0,
            "earlyRate": None,
            "wellRate": None,
            "avgFwdPct": None,
        },
        "funnel": {"runs": 3, "screenerAdded": 5},
        "registry": {"total": 40, "held": 4},
    }


def test_render_markdown_mentions_numbers() -> None:
    from data_sync_service.service.weekly_review import _render_markdown

    md = _render_markdown(_fake_stats())
    assert "# Karios 周度决策质量报告" in md
    assert "BUY/ADD 信号共 **10** 条" in md
    assert "胜率 50.0%" in md
    assert "`target_hit` 2 笔" in md
    assert "暂不归因" in md  # forward sample too small


def test_render_markdown_high_early_rate_note() -> None:
    from data_sync_service.service.weekly_review import _render_markdown

    stats = _fake_stats()
    stats["exitAttribution"] = {
        "withForward": 10,
        "earlyRate": 0.6,
        "wellRate": 0.2,
        "avgFwdPct": 3.2,
    }
    md = _render_markdown(stats)
    assert "卖早率" in md
    assert "卖早率高" in md  # auto note triggered


def test_render_markdown_empty_book() -> None:
    from data_sync_service.service.weekly_review import _render_markdown

    stats = _fake_stats()
    stats["paper"] = {
        "closed": 0,
        "wins": 0,
        "winRate": None,
        "avgNetPnlPct": None,
        "byReason": {},
    }
    md = _render_markdown(stats)
    assert "本周无平仓交易" in md
    # notes still fire on decision-volume observations
    assert "信号主要来自 ALPHA" in md


def test_weekly_review_api_endpoint() -> None:
    with patch(
        "data_sync_service.service.weekly_review.build_weekly_review",
        return_value={"ok": True, "week": {"start": "2026-08-03", "end": "2026-08-08"}, "markdown": "# 报告"},
    ):
        resp = client.get("/api/backtest/weekly-review?end=2026-08-08")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["markdown"] == "# 报告"


def test_weekly_review_rejects_bad_end() -> None:
    resp = client.get("/api/backtest/weekly-review?end=banana")
    assert resp.status_code == 422
