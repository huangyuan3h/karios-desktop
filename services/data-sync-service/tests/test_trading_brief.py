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
                "lineOps": {"trail_up": [433.136, 440.2], "expire_soon": 4,
                            "expireDate": "2026-09-27"},
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
    with patch(
        "data_sync_service.service.trading_brief._news_section",
        return_value=[
            {"type": "news", "id": "n1", "title": "测试新闻标题", "category": "宏观"},
        ],
    ):
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
    assert "🛠移动线上调 433.136→440.2" in md
    assert "⏰剩 4 天到期" in md
    assert "力量发展 · —% ✅持有" in md  # None pnl renders as —
    assert "🔴退出" in md
    assert "**新闻 Top5**" in md


def test_generate_action_brief_without_candidates() -> None:
    h = _fake_health()
    # _candidates reads live DB state (build_s3_candidates) — pin it to empty
    # so this test is deterministic regardless of what's in Postgres.
    with patch("data_sync_service.service.trading_brief._candidates", return_value=[]):
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
        patch("data_sync_service.service.trading_brief._recon_section", return_value=[]),
        patch("data_sync_service.service.trading_brief._third_asset_section", return_value=[]),
        patch("data_sync_service.service.trading_brief.upsert_brief") as upsert,
        patch("data_sync_service.db.webhook.emit_event"),
    ):
        upsert.return_value = {"briefDate": "2026-08-11", "items": []}
        tb.generate_trading_brief("action")
    assert upsert.call_count == 1
    kw = upsert.call_args.kwargs
    assert kw["brief_type"] == "trading-action"
    assert kw["markdown"]
    assert "**Regime**" in kw["markdown"]


def test_third_asset_section_renders_when_active() -> None:
    """T6 (2026-08-19): the brief carries the 513100 sleeve hint."""
    sections = [
        {"type": "third_asset", "active": True, "action": "BUY_513100",
         "label": "建议买入 513100", "message": "闲置资金 90% 且 ETF:513100 在200日线上",
         "price": 2.239, "ma200": 1.983, "idlePct": 90.0, "asOfDate": "2026-08-18"},
    ]
    md = tb.render_markdown(sections, "action")
    assert "**择强单轨·对照非实盘（建议买入 513100）**" in md
    assert "闲置资金 90% 且 ETF:513100 在200日线上" in md
    assert "现价 2.239" in md
    assert "MA200 1.983" in md
    assert "闲置 90.0%" in md


def test_third_asset_section_inactive_returns_empty() -> None:
    # _third_asset_section is a deprecated alias of _pick_strong_section,
    # which reads portfolio_health.multiAssetSleeve (not third_asset_sleeve).
    with patch(
        "data_sync_service.service.portfolio_health.build_portfolio_health",
        return_value={"multiAssetSleeve": {"active": False, "action": "NONE"}},
    ) as build_fn:
        out = tb._third_asset_section()
    build_fn.assert_called_once()
    assert out == []


def test_action_brief_renders_backtest_recon_section() -> None:
    sections = [
        {
            "type": "recon",
            "reconDate": "2026-08-07",
            "market": "HK",
            "expected": 19,
            "actual": 0,
            "missing": 19,
            "extra": 0,
            "alignedReturnDiffPct": None,
            "missingTop": [
                {
                    "symbol": "HK:02099",
                    "entry": "2026-08-05",
                    "score": 88.0,
                    "positionPct": 0.1,
                }
            ],
        }
    ]
    md = tb.render_markdown(sections, "action")
    assert "**回测口径（对账 2026-08-07）**" in md
    assert "港股：回测应持 19 · 实持 0 · 缺 19 · 多 0" in md
    assert "缺票 HK:02099（入场 score 88.0 · 建议 10%· 2026-08-05 入场）" in md


def test_action_brief_emits_execution_card_webhook() -> None:
    """OPT-113: the 14:00 action brief must push an execution_card event
    (gate state + buy candidates + EXIT holdings) once per day."""
    from unittest.mock import MagicMock

    emit = MagicMock()
    with (
        patch("data_sync_service.service.trading_brief._health", return_value=_fake_health()),
        patch("data_sync_service.service.trading_brief._candidates", return_value=[
            {
                "market": "CN",
                "symbol": "CN:600801",
                "name": "华新建材",
                "industry": "建筑材料",
                "score": 67.4,
                "rs": 0.937,
            },
        ]),
        patch("data_sync_service.service.trading_brief._news_section", return_value=[]),
        patch("data_sync_service.service.trading_brief._recon_section", return_value=[]),
        patch("data_sync_service.service.trading_brief.upsert_brief") as upsert,
        patch("data_sync_service.db.webhook.emit_event", emit),
    ):
        upsert.return_value = {"briefDate": "2026-08-11", "items": []}
        tb.generate_trading_brief("action")

    assert emit.call_count == 1
    event_type = emit.call_args.args[0]
    payload = emit.call_args.args[1]
    _dedupe = emit.call_args.kwargs["dedupe_key"]
    assert event_type == "execution_card"
    assert payload["day"]  # YYYY-MM-DD
    assert payload["gate"]["A股"]["regime"] == "Weak"
    assert payload["gate"]["A股"]["panicActive"] is True
    assert payload["gate"]["港股"]["regime"] == "Strong"
    assert payload["candidates"][0]["symbol"] == "CN:600801"
    # EXIT holding is surfaced; HOLD holdings are not.
    exit_syms = [e["symbol"] for e in payload["exits"]]
    assert exit_syms == ["CN:300628"]
    assert payload["exits"][0]["pnlPct"] == -5.4
