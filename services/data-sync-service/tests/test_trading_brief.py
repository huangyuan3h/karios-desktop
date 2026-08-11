"""Trading-session brief tests (2026-08-11) — 10:00/12:00/14:30 composition.

The brief is pure composition of existing blocks (portfolio_health, S-3
candidates, news) — these tests pin the section shapes and the markdown
rendering without touching the DB (network/data sources are patched).
"""

from __future__ import annotations

from unittest.mock import patch

from data_sync_service.service import trading_brief as tb


def _fake_health() -> dict:
    return {
        "tradeDate": "2026-08-11",
        "regime": "Weak",
        "strength": 22.25,
        "sentiment": "extreme_caution",
        "panicCooldown": {"active": True, "cooldownEndDate": "2026-08-14"},
        "s3CandidateTotal": 0,
        "holdings": [
            {
                "symbol": "HK:00700",
                "name": "腾讯控股",
                "action": "HOLD",
                "reason": None,
                "stopLossLine": 452.2,
                "trailingLine": 433.136,
                "pnlPct": 1.13,
                "expireDate": "2026-09-27",
            },
            {
                "symbol": "CN:300628",
                "name": "亿联网络",
                "action": "EXIT",
                "reason": "stop_loss",
                "stopLossLine": 37.905,
                "trailingLine": 36.828,
                "pnlPct": -5.4,
                "expireDate": "2026-10-03",
            },
            {
                "symbol": "HK:01277",
                "name": "力量发展",
                "action": "HOLD",
                "reason": None,
                "stopLossLine": None,
                "trailingLine": None,
                "pnlPct": None,
                "expireDate": None,
            },
        ],
        "hkHealth": {
            "regime": "Strong",
            "strength": 100.0,
            "panicCooldown": {"active": True, "cooldownEndDate": "2026-08-14"},
            "s3CandidateTotal": 3,
            "holdings": [],
        },
    }


def test_section_shapes() -> None:
    h = _fake_health()
    regime = tb._regime_section(h)
    assert [r["market"] for r in regime] == ["A股", "港股"]
    assert regime[0]["panicActive"] is True
    assert regime[1]["regime"] == "Strong"

    cands = tb._candidates_section(h)
    assert isinstance(cands, list)

    holds = tb._holdings_section(h)
    assert len(holds) == 3
    by_sym = {x["symbol"]: x for x in holds}
    assert by_sym["HK:00700"]["stopLossLine"] == 452.2
    assert by_sym["HK:00700"]["trailingLine"] == 433.136
    assert by_sym["CN:300628"]["action"] == "EXIT"
    assert by_sym["HK:01277"]["pnlPct"] is None

    alerts = tb._alerts_section(h)
    # CN:300628 at -5.4 vs stop line -5 → within 1.5pt, but action==EXIT (skipped)
    assert all(a["symbol"] != "CN:300628" for a in alerts)


def test_render_markdown_compact() -> None:
    h = _fake_health()
    sections = (
        tb._regime_section(h)
        + tb._candidates_section(h)
        + tb._holdings_section(h)
        + tb._alerts_section(h)
        + tb._news_section(3)
    )
    md = tb.render_markdown(sections, "action")
    assert "**Regime**" in md
    assert "A股: Weak" in md
    assert "港股: Strong" in md
    assert "**持仓 / 条件单**" in md
    assert "HK:00700 腾讯控股 · 1.13% ✅持有 止损 452.2 移动 433.136" in md
    assert "力量发展 · —% ✅持有" in md  # None pnl renders as —
    assert "🔴退出" in md
    assert "**新闻 Top5**" in md


def test_generate_action_brief_without_candidates() -> None:
    h = _fake_health()
    sections = (
        tb._regime_section(h)
        + tb._candidates_section(h)
        + tb._holdings_section(h)
        + tb._alerts_section(h)
    )
    md = tb.render_markdown(sections, "action")
    assert "**S-3 候选：无**" in md  # action brief must say no-candidates explicitly


def test_generate_trading_brief_rejects_unknown_type() -> None:
    import pytest

    with pytest.raises(ValueError):
        tb.generate_trading_brief("lunch")


def test_generate_trading_brief_stores_and_returns_markdown() -> None:
    with (
        patch("data_sync_service.service.trading_brief._health", return_value=_fake_health()),
        patch("data_sync_service.service.trading_brief._candidates", return_value=[]),
        patch("data_sync_service.service.trading_brief._news_section", return_value=[]),
        patch("data_sync_service.service.trading_brief.upsert_brief") as upsert,
    ):
        upsert.return_value = {"briefDate": "2026-08-11", "items": []}
        tb.generate_trading_brief("action")
    assert upsert.call_count == 1
    kw = upsert.call_args.kwargs
    assert kw["brief_type"] == "trading-action"
    assert kw["markdown"]
    assert "**Regime**" in kw["markdown"]
