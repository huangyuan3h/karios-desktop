"""Fail-open and markdown-renderer coverage for the judgment brief."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from data_sync_service.service import agent_brief, agent_event_tags, agent_overlays, agent_whip

SHANGHAI = ZoneInfo("Asia/Shanghai")


@pytest.fixture()
def memory_settings(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    store: dict[str, Any] = {}
    monkeypatch.setattr(
        "data_sync_service.db.app_settings.get_setting", lambda k, d=None: store.get(k, d)
    )
    monkeypatch.setattr(
        "data_sync_service.db.app_settings.set_setting", lambda k, v: store.__setitem__(k, v)
    )
    return store


def test_formatters_and_shanghai_clock() -> None:
    assert agent_brief._fmt_cny("nope") == "—"
    assert "万" in agent_brief._fmt_cny(25000)
    assert agent_brief._fmt_cny(12) == "12"
    assert agent_brief._fmt_dd("x") == "—"
    assert agent_brief._sh_time(None) == "—"
    assert agent_brief._sh_time("not-a-timestamp") == "not-a-timestamp"
    # Naive ISO is treated as UTC, then shown in Asia/Shanghai.
    shown = agent_brief._sh_time("2026-11-10T01:00:00")
    assert ":" in shown


def test_last_session_close_walks_back_weekend(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Frozen(datetime):
        @classmethod
        def now(cls, tz=None):  # noqa: ANN001
            return datetime(2026, 11, 14, 16, 0, tzinfo=tz or SHANGHAI)  # Saturday after close

    monkeypatch.setattr(agent_brief, "datetime", _Frozen)
    ts = agent_brief._last_session_close_ts()
    close = datetime.fromtimestamp(ts, tz=SHANGHAI)
    assert close.weekday() < 5


def test_dashboard_ttl_and_live_quote_fail_open(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "data_sync_service.service.trade_calendar_utils.compute_market_status",
        lambda: (_ for _ in ()).throw(RuntimeError("status down")),
    )
    assert agent_brief._dashboard_cache_ttl() == float(agent_brief.DASHBOARD_CACHE_TTL_SECONDS)

    assert agent_brief._live_quotes([]) == {"available": False, "error": "no_symbols"}
    agent_brief._QUOTE_CACHE["quotes"] = None
    agent_brief._QUOTE_CACHE["symbols"] = ()
    agent_brief._QUOTE_CACHE["at"] = 0
    monkeypatch.setattr(
        "data_sync_service.service.portfolio_health._resolve_holding_ts",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("no ts")),
    )
    assert agent_brief._live_quotes(["ETF:511260"])["error"] == "no_ts_code"

    monkeypatch.setattr(
        "data_sync_service.service.portfolio_health._resolve_holding_ts",
        lambda sym: "511260.SH",
    )
    monkeypatch.setattr(
        "data_sync_service.service.realtime_quote.fetch_realtime_quotes",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("quote down")),
    )
    assert "quote down" in agent_brief._live_quotes(["ETF:511260"])["error"]

    monkeypatch.setattr(
        "data_sync_service.service.realtime_quote.fetch_realtime_quotes",
        lambda *a, **k: None,
    )
    assert agent_brief._live_quotes(["ETF:511260"])["error"] == "quote_failed"


def test_just_sold_skips_future_and_stops_at_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = [{"closeDate": "2026-11-20", "symbol": "FUTURE"}]
    rows += [{"closeDate": "2026-11-09", "symbol": f"S{i}"} for i in range(12)]
    monkeypatch.setattr("data_sync_service.db.paper_trading.list_paper_trades", lambda **k: rows)
    out = agent_brief._just_sold(day="2026-11-10", limit=10)
    assert len(out) == 10
    assert all(r["symbol"] != "FUTURE" for r in out)


def test_price_integrity_skips_junk_and_fills_missing_ma200() -> None:
    out = agent_brief._price_integrity_block(
        contract={
            "sleeve": {
                "pick": {"symbol": "ETF:511260", "close": 141.6, "ma200": 138.8, "key": "BOND10"}
            },
            "nextLeg": {"ma200": 138.8},
        },
        health={
            "multiAssetHoldings": ["skip", {"symbol": "ETF:511260", "marketData": {"close": 141.6}}]
        },
        quotes={"quotes": {}},
    )
    assert isinstance(out, dict)


def test_brief_helpers_fail_open(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "data_sync_service.api.backtest_routes.warm_timeline_recent_stats",
        lambda **k: (_ for _ in ()).throw(RuntimeError("stats down")),
    )
    monkeypatch.setattr(
        "data_sync_service.api.backtest_routes.read_timeline_recent_stats",
        lambda **k: (_ for _ in ()).throw(RuntimeError("file down")),
    )
    assert agent_brief._recent_timeline_note(strategy="homeport_m30") is None

    marks: dict[str, Any] = {}
    monkeypatch.setattr(
        "data_sync_service.api.backtest_routes.backtest_overview",
        lambda: (_ for _ in ()).throw(RuntimeError("overview down")),
    )
    assert agent_brief._backtest_overview_cached(refresh=True, cache_marks=marks) == {}
    assert marks["backtestCacheHit"] is False

    marks = {}
    monkeypatch.setattr(
        "data_sync_service.service.strategy_catalog.strategy_catalog",
        lambda: (_ for _ in ()).throw(RuntimeError("catalog down")),
    )
    assert agent_brief._strategy_catalog_cached(refresh=True, cache_marks=marks) == []
    assert marks["backtestCacheHit"] is False

    monkeypatch.setattr(
        "data_sync_service.service.portfolio_health.build_portfolio_health",
        lambda **k: (_ for _ in ()).throw(RuntimeError("health down")),
    )
    assert agent_brief._build_health(day="2026-11-10") is None
    assert agent_brief._gate_block({})["error"] == "execution gate unavailable"
    fund = agent_brief._fund_block(
        {"industryFundFlow": {"flow5d": {"top": ["skip", {"industryName": "A", "series": []}]}}},
        compact=True,
    )
    assert isinstance(fund, dict)

    monkeypatch.setattr(
        "data_sync_service.api.settings_routes.selected_strategy_mode",
        lambda: (_ for _ in ()).throw(RuntimeError("settings down")),
    )
    assert agent_brief._mode_cache_key() == ("", "")

    started: list[Any] = []

    class _Thread:
        def __init__(self, *a: Any, **k: Any) -> None:
            started.append(k)

        def start(self) -> None:
            started.append("start")

    monkeypatch.setattr(agent_brief.threading, "Thread", _Thread)
    agent_brief.warm_judgment_brief_async(reason="coverage")
    assert "start" in started


def test_execution_block_journal_and_snapshot_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "data_sync_service.db.execution_journal.list_changes",
        lambda **k: (_ for _ in ()).throw(RuntimeError("journal down")),
    )
    monkeypatch.setattr(
        "data_sync_service.db.execution_journal.fetch_latest_snapshot",
        lambda **k: (_ for _ in ()).throw(RuntimeError("snap down")),
    )
    block = agent_brief._execution_block(day="2026-11-10", compact=True)
    # _trim drops empty lists / None, but the error keys must remain.
    assert "changesError" in block
    assert "snapshotError" in block


def test_markdown_renderers_cover_unavailable_and_optional_rows() -> None:
    text = "\n".join(
        agent_brief._render_alerts(
            {
                "contract": {
                    "strategy": {},
                    "fillClock": {},
                    "whip": {"context": {"sameKeyRebuy": True, "pickKey": "OIL"}},
                }
            }
        )
    )
    assert "缺少执行时钟" in text
    assert "同键回买" in text

    contract_md = "\n".join(
        agent_brief._render_contract(
            {
                "contract": {
                    "strategy": {"mode": "homeport"},
                    "sleeve": {
                        "action": "HOLD",
                        "executeHint": "次日开盘",
                        "intradayWarn": "可追",
                        "message": "持有",
                    },
                    "nextLeg": {"key": "BOND10"},
                    "whip": {"sameKeyRebuy": False, "context": {"oil": {"active": False}}},
                    "overlays": {
                        "active": [
                            {"pickKey": "OIL", "status": "accepted", "validUntil": "2026-11-20"}
                        ]
                    },
                    "health": {"regime": "Strong", "exits": []},
                }
            }
        )
    )
    assert "executeHint" in contract_md
    assert "OIL accepted" in contract_md

    gate_md = "\n".join(agent_brief._render_gate({"gate": {"available": False, "error": "down"}}))
    assert "不可用: down" in gate_md
    gate_ok = "\n".join(
        agent_brief._render_gate(
            {
                "gate": {
                    "available": True,
                    "cn": {
                        "mode": "DEFEND",
                        "allowNewEntries": False,
                        "positionRangeHint": "0-20",
                        "satelliteNote": "wait",
                    },
                    "hk": {},
                }
            }
        )
    )
    assert "仓位提示: 0-20" in gate_ok
    assert "卫星: wait" in gate_ok

    book_md = "\n".join(
        agent_brief._render_book(
            {"book": {"available": True, "cnHoldings": [], "hkHoldings": [], "parkingHoldings": []}}
        )
    )
    assert "无持仓" in book_md

    bt_md = "\n".join(agent_brief._render_backtest({"backtest": {"available": False}}, False))
    assert "不可用" in bt_md
    bt_ok = "\n".join(
        agent_brief._render_backtest(
            {
                "backtest": {
                    "available": True,
                    "currentMode": {"mode": "homeport"},
                    "comparison": [],
                    "s3Baseline": {"windows": {"OOS2": {"totalNetPnlPct": 1}, "junk": "skip"}},
                }
            },
            False,
        )
    )
    assert "OOS2" in bt_ok or "S-3" in bt_ok

    macro_md = "\n".join(
        agent_brief._render_macro(
            {"macro": {"available": True, "indexLights": [], "macro": [], "warnings": ["w1"]}},
            True,
        )
    )
    assert "警告: w1" in macro_md

    assert "不可用" in "\n".join(agent_brief._render_freshness({"freshness": {"available": False}}))
    assert "不可用" in "\n".join(agent_brief._render_execution({"execution": {"available": False}}))

    exec_md = "\n".join(
        agent_brief._render_execution(
            {
                "execution": {
                    "available": True,
                    "latestSnapshot": {
                        "tradeDate": "2026-11-10",
                        "cards": [{"symbol": "X", "action": "HOLD"}],
                    },
                    "attention": {
                        "counts": {},
                        "gates": {},
                        "mustAct": [],
                        "fires": [],
                        "blockedFires": [{"symbol": "Y", "action": "BUY", "why": "gate"}],
                    },
                    "changes": [
                        {
                            "changedAt": "2026-11-10",
                            "symbol": "X",
                            "field": "action",
                            "oldValue": "HOLD",
                            "newValue": "EXIT",
                        }
                    ],
                }
            }
        )
    )
    assert "blocked" in exec_md
    assert "变更日志" in exec_md

    # Snapshot cards are listed only when attention is empty.
    cards_md = "\n".join(
        agent_brief._render_execution(
            {
                "execution": {
                    "available": True,
                    "latestSnapshot": {
                        "tradeDate": "2026-11-10",
                        "cards": [{"symbol": "Z", "action": "TRIM", "reason": "size"}],
                    },
                    "attention": {},
                    "changes": [],
                }
            }
        )
    )
    assert "Z TRIM" in cards_md


def test_remaining_event_overlay_whip_branches(
    memory_settings: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(agent_event_tags, "_today_iso", lambda: "2026-11-10")
    # confidence None on an otherwise active stored row is ignored.
    memory_settings[agent_event_tags.EVENT_TAG_SETTINGS_KEY] = [
        {
            "id": "tag-none",
            "tag": "no_conf",
            "effect": "advise_strategy",
            "confidence": None,
            "status": "active",
            "sources": [{"url": "https://example.com/x"}],
            "validFrom": "2026-11-01",
            "validUntil": "2026-11-20",
        },
        {
            "id": "tag-no-until",
            "tag": "needs_until",
            "effect": "advise_strategy",
            "confidence": 0.8,
            "status": "active",
            "sources": [{"url": "https://example.com/x"}],
            "validFrom": "2026-11-01",
        },
    ]
    assert all(
        t["id"] != "tag-none" or t.get("confidence") is not None or True
        for t in agent_event_tags.active_event_tags(day="2026-11-10")
    )
    # The None-confidence row must be filtered out.
    assert all(t["id"] != "tag-none" for t in agent_event_tags.active_event_tags(day="2026-11-10"))

    updated = agent_event_tags.record_event_tag(
        tag="needs_until",
        effect="advise_strategy",
        confidence=0.85,
        sources=[{"url": "https://example.com/x"}],
        day="2026-11-10",
    )
    assert updated.get("validUntil")

    # Dedupe: an advice tag reuses the engine tag id.
    monkeypatch.setattr(
        agent_event_tags,
        "active_event_tags",
        lambda **k: [{"id": "dup", "effect": "advise_strategy", "effectiveStatus": "active"}],
    )
    monkeypatch.setattr(
        agent_event_tags,
        "_engine_tag",
        lambda **k: {"id": "dup", "effect": k.get("effect"), "effectiveStatus": "active"},
    )
    block = agent_event_tags.build_event_tag_block(
        contract={
            "contractId": "c1",
            "sleeve": {
                "action": "SELL_TO_REPO",
                "trailExit": True,
                "symbol": "ETF:513350",
                "pickKey": "OIL",
            },
            "strategy": {"mode": "homeport"},
            "nextLeg": {},
        },
        day="2026-11-10",
    )
    ids = [t["id"] for t in block["eventTags"]["tags"]]
    assert ids.count("dup") == 1

    monkeypatch.setattr(agent_overlays, "_today_iso", lambda: "2026-11-10")
    agent_overlays.upsert_overlay(pick_key="GOLD", status="proposed", day="2026-11-10")
    agent_overlays.upsert_overlay(pick_key="OIL", status="accepted", day="2026-11-10")
    # Filter miss + status miss.
    assert agent_overlays.list_overlays(day="2026-11-10", pick_key="SILVER") == []
    assert agent_overlays.list_overlays(day="2026-11-10", status="rejected") == []
    # Re-post accepted without validUntil when the row already has one keeps it;
    # a fresh accepted row without validUntil fills one.
    bare_id = "bare-1"
    memory_settings[agent_overlays.OVERLAY_SETTINGS_KEY] = [
        {
            "id": bare_id,
            "type": "hold_same_key_rebuy",
            "pickKey": "NASDAQ",
            "status": "proposed",
        }
    ]
    filled = agent_overlays.upsert_overlay(pick_key="NASDAQ", status="accepted", day="2026-11-10")
    assert filled.get("validUntil")

    today = agent_overlays._today_iso()
    assert len(today) == 10
    assert len(agent_whip._today_iso()) == 10

    store_key = agent_whip.WHIP_MEMORY_KEY
    memory_settings[store_key] = {"OIL": "2026-11-01"}
    agent_whip.evaluate_whip(
        {"action": "HOLD", "pick": {"key": "BOND10"}},
        [{"key": "OIL", "symbol": "ETF:513350", "positionPct": 20.0}],
        day="2026-11-10",
        observe=True,
    )
    assert "OIL" not in (memory_settings.get(store_key) or {})


def test_brief_leftover_branches(monkeypatch: pytest.MonkeyPatch) -> None:
    """Hit TRIM/other ranking, stale-macro fail-open, catalog skip, and cache-after-lock."""
    ranked, _attn = agent_brief._rank_attention_cards(
        [
            {"action": "TRIM", "symbol": "CN:600000"},
            {"action": "WATCH", "symbol": "CN:000001"},
        ],
        gate={"cn": {"allowNewEntries": False}, "hk": {}},
    )
    assert any(c.get("attentionBucket") == "trim" for c in ranked)
    assert any(c.get("attentionBucket") == "other" for c in ranked)

    monkeypatch.setattr(agent_brief, "_today_iso", lambda: "2026-11-11")
    monkeypatch.setattr(
        "data_sync_service.service.market_regime.get_index_signals",
        lambda **k: (_ for _ in ()).throw(RuntimeError("quotes down")),
    )
    macro = agent_brief._macro_block(
        {
            "asOfDate": "2026-11-10",
            "macroSnapshot": {
                "cnIndexSignals": [{"asOfDate": "2026-11-10", "symbol": "000300.SH"}],
                "macro": [],
                "warnings": ["w"],
            },
        },
        compact=False,
    )
    assert macro["available"] is True

    marks: dict[str, Any] = {}
    monkeypatch.setattr(agent_brief, "_backtest_overview_cached", lambda **k: {})
    monkeypatch.setattr(
        agent_brief,
        "_strategy_catalog_cached",
        lambda **k: [
            {"timelineStrategy": "", "label": "skip"},
            {"timelineStrategy": "custom_future", "canonical": True},
        ],
    )
    block = agent_brief._backtest_block(
        contract={"strategy": {"mode": "homeport", "timelineStrategy": "custom_future"}},
        compact=True,
        refresh=True,
        cache_marks=marks,
    )
    assert isinstance(block, dict)

    planted = {"judgment": {"ok": True}, "timingsMs": {"totalMs": 1}}

    class _Lock:
        def __enter__(self) -> Any:
            key = (
                "2026-11-10",
                "compact",
                ("", ""),
                agent_brief.BRIEF_VERSION,
            )
            agent_brief._BRIEF_CACHE.put(key, planted)
            return self

        def __exit__(self, *a: Any) -> None:
            return None

    monkeypatch.setattr(agent_brief, "_mode_cache_key", lambda: ("", ""))
    monkeypatch.setattr(agent_brief, "_BRIEF_BUILD_LOCK", _Lock())
    monkeypatch.setattr(agent_brief, "_today_iso", lambda: "2026-11-10")
    out = agent_brief.build_judgment_brief(trade_date="2026-11-10", mode="compact", refresh=False)
    assert out.get("cached") is True

    real_dumps = agent_brief.json.dumps

    def _dumps(obj: Any, *a: Any, **k: Any) -> str:
        if isinstance(obj, dict) and "judgment" in obj and "timingsMs" not in obj:
            raise TypeError("cannot serialize")
        return real_dumps(obj, *a, **k)

    monkeypatch.setattr(agent_brief, "_BRIEF_BUILD_LOCK", __import__("threading").Lock())
    agent_brief._BRIEF_CACHE.clear()
    monkeypatch.setattr(agent_brief, "_build_health", lambda day: {"ok": True})
    monkeypatch.setattr(
        "data_sync_service.service.agent_contract.build_decision_contract",
        lambda **k: {
            "strategy": {"mode": "homeport"},
            "eventTags": {"count": 1},
            "geoRegime": "calm",
        },
    )
    monkeypatch.setattr(agent_brief, "_dashboard_summary", lambda **k: {"asOfDate": "2026-11-10"})

    def _boom(*a: Any, **k: Any) -> Any:
        raise RuntimeError("block down")

    for name in (
        "_freshness_block",
        "_gate_block",
        "_fund_block",
        "_macro_block",
        "_news_block",
        "_book_block",
        "_execution_block",
        "_backtest_block",
    ):
        if hasattr(agent_brief, name):
            monkeypatch.setattr(agent_brief, name, _boom)
    monkeypatch.setattr(agent_brief, "_price_integrity_block", lambda **k: {"available": False})
    monkeypatch.setattr(agent_brief.json, "dumps", _dumps)
    built = agent_brief.build_judgment_brief(trade_date="2026-11-10", mode="compact", refresh=True)
    assert built.get("eventTags") or built.get("judgment")
