"""Agent judgment brief tests (no Postgres, no network).

The brief is the ONE call the decision agent makes: contract + gate + book +
market facts + the backtest comparison. 研报 / Alpha Radar / Auto-QA are
intentionally out (only ``omitted`` names them).
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from data_sync_service.main import app
from data_sync_service.service import agent_brief

client = TestClient(app)


@pytest.fixture(autouse=True)
def _reset_brief_cache() -> Any:
    """Every test starts from a cold in-process cache (the module caches by day)."""
    agent_brief.reset_judgment_brief_cache()
    yield
    agent_brief.reset_judgment_brief_cache()


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


def _fake_health(*, trade_date: str | None = None, markets: tuple[str, ...] = ("CN",)) -> dict:
    sleeve = {
        "active": True,
        "action": "SELL_TO_REPO",
        "label": "卖出转repo(止损)",
        "message": "ETF:513350峰值回撤8.5% ≥8% → 次日开盘卖转逆回购",
        "holding": True,
        "symbol": "ETF:513350",
        "trailExit": True,
        "pendingOpenSell": False,
        "signalDate": "2026-11-09",
        "idlePct": 0,
        "parkPct": 63.5,
        "pick": {
            "key": "OIL",
            "symbol": "ETF:513350",
            "name": "富国油气QDII",
            "mom60": 4.8,
            "close": 1.28,
            "ma200": 1.5,
            "above_ma200": True,
            "all_mom": {"OIL": 4.8, "BOND10": 1.1},
            "all_above": {"OIL": True, "BOND10": True},
        },
        "etfPick": {"key": "OIL", "symbol": "ETF:513350"},
    }
    return {
        "tradeDate": trade_date or "2026-11-10",
        "regime": "Strong",
        "sentiment": "hot",
        "panicCooldown": {"active": False},
        "circuitBlocked": False,
        "holdings": [
            {
                "symbol": "CN:600000",
                "name": "测试股",
                "positionPct": 10,
                "costPrice": 10.0,
                "entryDate": "2026-10-09",
                "lastClose": 10.8,
                "lastDate": "2026-11-09",
                "evaluatedPrice": 10.8,
                "pnlPct": 8.0,
                "drawdownFromPeakPct": -3.0,
                "holdingDays": 32,
                "action": "HOLD",
                "trailingLine": 9.9,
                "stopLossLine": 9.5,
                "expireDate": "2026-12-08",
            }
        ],
        "hkHealth": {"holdings": []},
        "s3Candidates": [{"symbol": "CN:600000"}],
        "scoreFresh": True,
        "multiAssetSleeve": sleeve,
        "multiAssetHoldings": [
            {
                "symbol": "ETF:513350",
                "key": "OIL",
                "positionPct": 63.5,
                "trailExit": True,
                "signalDate": "2026-11-09",
            }
        ],
    }


def _fake_dashboard_summary(**_: Any) -> dict[str, Any]:
    return {
        "asOfDate": "2026-11-10",
        "marketEnvironmentZh": "大盘强势，注意拥挤",
        "industryFundFlow": {
            "asOfDate": "2026-11-10",
            "days": 5,
            "dates": ["2026-11-06", "2026-11-10"],
            "topByDate": [{"date": "2026-11-10", "top": ["半导体", "算力"]}],
            "flow5d": {
                "dates": ["2026-11-06", "2026-11-10"],
                "top": [
                    {"industryName": "半导体", "sum5d": 50e8, "series": []},
                    {"industryName": "算力", "sum5d": 40e8, "series": []},
                ],
            },
            "flow5dOut": {
                "dates": ["2026-11-06", "2026-11-10"],
                "top": [{"industryName": "白酒", "sum5d": -30e8, "series": []}],
            },
        },
        "marketSentiment": {
            "asOfDate": "2026-11-10",
            "items": [
                {
                    "date": "2026-11-10",
                    "upCount": 3200,
                    "downCount": 1800,
                    "riskMode": "confirmed_uptrend",
                    "marketTurnoverCny": 1.7e12,
                    "rules": ["breadth_ok"],
                }
            ],
            "indexSignals": [],
            "executionGate": {
                "mode": "ATTACK",
                "allowNewEntries": True,
                "marketRegime": "Strong",
                "indexLight": "green",
                "reasons": ["REGIME_STRONG"],
                "hkGate": {
                    "mode": "HOLD_ONLY",
                    "allowNewEntries": False,
                    "marketRegime": "Diverging",
                    "indexLight": "yellow",
                    "reasons": [],
                },
            },
            "etfFundFlow": {
                "asOfDate": "2026-11-10",
                "items": [
                    {
                        "name": "沪深300ETF",
                        "symbol": "510300",
                        "netFlow1d": 12e8,
                        "netFlow3d": 30e8,
                        "signalDisplay": "净流入",
                    }
                ],
            },
            "etfFlowSignal": {
                "verdict": "confirm",
                "broadDirection": "inflow",
                "sectorDirection": "inflow",
                "incomplete": False,
            },
            "srvIndex": {"level": "High", "overlapCount": 2},
        },
        "macroSnapshot": {
            "cnIndexSignals": [
                {
                    "name": "沪深300",
                    "tsCode": "000300.SH",
                    "signal": "green",
                    "positionRange": "50%-60%",
                    "close": 4200.0,
                    "pctChg": 0.8,
                    "asOfDate": "2026-11-07",
                }
            ],
            "macro": [
                {
                    "name": "美元指数",
                    "seriesId": "DXY",
                    "category": "fx",
                    "close": 101.2,
                    "pctChg": -0.2,
                    "signal": "neutral",
                    "asOfDate": "2026-11-10",
                }
            ],
            "warnings": [],
        },
        "news": {
            "hours": 24,
            "total": 40,
            "items": [
                {
                    "title": "算力需求超预期",
                    "sourceId": "eastmoney",
                    "publishedAt": "2026-11-10T08:00:00+08:00",
                    "link": "https://example.com/n1",
                    "aiSummary": "长文AI点评不应进入简报",
                    "importance": 2,
                    "relevanceScore": 40,
                }
            ],
        },
    }


@pytest.fixture()
def patched(monkeypatch: pytest.MonkeyPatch, memory_settings: dict[str, Any]) -> None:
    monkeypatch.setattr(
        "data_sync_service.api.settings_routes.selected_strategy_mode", lambda: "homeport"
    )
    monkeypatch.setattr(
        "data_sync_service.service.portfolio_health.build_portfolio_health", _fake_health
    )
    monkeypatch.setattr(
        "data_sync_service.service.dashboard.dashboard_summary", _fake_dashboard_summary
    )
    monkeypatch.setattr(
        "data_sync_service.api.health_routes.datasource_freshness",
        lambda: [
            {
                "source": "market",
                "label": "行情",
                "lastSyncedAt": "2026-11-10T07:10:00+00:00",
                "ageMinutes": 120,
                "thresholdMinutes": 1440,
                "stale": False,
            },
            {
                "source": "research",
                "label": "研报",
                "lastSyncedAt": "2026-11-09T07:10:00+00:00",
                "ageMinutes": 1500,
                "thresholdMinutes": 1440,
                "stale": True,
            },
        ],
    )
    monkeypatch.setattr(
        "data_sync_service.api.backtest_routes.backtest_overview",
        lambda: {
            "ok": True,
            "cnBaseline": {
                "tag": "s3-20260812",
                "generatedAt": "2026-08-12T00:00:00+00:00",
                "windows": {
                    "OOS2": {
                        "totalNetPnlPct": 55.1,
                        "maxDrawdownPct": -14.2,
                        "sharpe": 2.2,
                        "trades": 120,
                        "winRate": 0.44,
                    }
                },
            },
            "longWindowCN": {
                "window": "2021-08-01 ~ 2026-08-11",
                "totalNetPnlPct": 250.8,
                "maxDrawdownPct": 40.9,
                "sharpe": 2.65,
                "trades": 1401,
            },
            "rollingOos": {
                "windowStart": "2026-08-04",
                "windowEnd": "2026-11-02",
                "markets": {"CN": {"totalNetPnlPct": 3.1, "maxDrawdownPct": -4.2}},
            },
        },
    )
    monkeypatch.setattr(
        "data_sync_service.service.strategy_catalog.strategy_catalog",
        lambda: [
            {
                "key": "harbor",
                "name": "港湾",
                "status": "live",
                "statusLabel": "Live · 日落",
                "timelineStrategy": "harbor",
                "tag": "harbor-p1-20260913",
                "updated": "2026-09-14",
                "doc": "docs/backtests/stable/etf-parking-baseline-2026-09-13.md",
                "windows": {
                    "OOS2": {"total": 54.4, "mdd": -14.3, "sharpe": 1.71},
                    "valid": {"total": 50.2, "mdd": -21.8, "sharpe": 2.13},
                    "long": {"total": 197.1, "mdd": -22.9, "sharpe": 0.98},
                },
            },
            {
                "key": "homeport",
                "name": "母港",
                "canonical": False,
                "status": "product_candidate",
                "statusLabel": "基线 · M30",
                "timelineStrategy": "homeport_m30",
                "windows": {
                    "OOS2": {"total": 43.4, "mdd": -11.0, "sharpe": 1.79},
                    "valid": {"total": 35.4, "mdd": -15.7, "sharpe": 2.18},
                    "long": {"total": 146.6, "mdd": -16.4, "sharpe": 1.06},
                },
            },
            {
                "key": "starship_b",
                "name": "星舰 B",
                "status": "product_candidate",
                "statusLabel": "观察 · 待复活",
                "timelineStrategy": "starship_b",
                "windows": {
                    "OOS2": {"total": 231.7, "mdd": -4.0, "sharpe": 6.65},
                    "valid": {"total": 5.4, "mdd": -10.0, "sharpe": 0.66},
                    "long": {"total": 669.6, "mdd": -5.5, "sharpe": 3.9},
                },
            },
        ],
    )
    monkeypatch.setattr(
        "data_sync_service.api.backtest_routes.warm_timeline_recent_stats",
        lambda strategy, sessions=20: (
            {
                "start": "2026-10-10",
                "end": "2026-11-07",
                "sessions": 20,
                "returnPct": 2.5,
            }
            if strategy == "homeport_m30"
            else None
        ),
    )
    monkeypatch.setattr(
        "data_sync_service.service.realtime_quote.fetch_realtime_quotes",
        lambda codes: {
            "ok": True,
            "items": [
                {
                    "ts_code": "600000.SH",
                    "price": "11.20",
                    "pct_chg": "1.5",
                    "pre_close": "11.03",
                    "trade_time": "2026-11-10 14:30:00",
                }
            ],
        },
    )
    monkeypatch.setattr(
        "data_sync_service.db.paper_trading.list_paper_trades",
        lambda **kwargs: [
            {
                "symbol": "CN:600519",
                "market": "CN",
                "source": "S3",
                "entryDate": "2026-10-01",
                "entryPrice": 1400.0,
                "closeDate": "2026-11-07",
                "closePrice": 1330.0,
                "pnlPct": -5.0,
                "holdingDays": 25,
                "closeReason": "stop_hit",
            }
        ],
    )
    monkeypatch.setattr("data_sync_service.db.execution_journal.list_changes", lambda **kw: [])
    monkeypatch.setattr(
        "data_sync_service.db.execution_journal.fetch_latest_snapshot", lambda **kw: None
    )


def test_brief_shape_has_contract_and_backtest(patched: None) -> None:
    out = agent_brief.build_judgment_brief(trade_date="2026-11-10")

    assert out["ok"] is True
    assert out["asOfDate"] == "2026-11-10"
    assert out["timezone"] == "Asia/Shanghai"
    assert out["briefId"].startswith("jb:2026-11-10:homeport_m30:")
    assert out["briefVersion"] == agent_brief.BRIEF_VERSION

    contract = out["contract"]
    assert contract["strategy"]["mode"] == "homeport"
    assert contract["strategy"]["timelineStrategy"] == "homeport_m30"
    assert contract["strategy"]["timelineLabel"] == "母港 M30"
    assert contract["sleeve"]["action"] == "SELL_TO_REPO"

    # Gate: CN + HK both present.
    assert out["gate"]["cn"]["mode"] == "ATTACK"
    assert out["gate"]["hk"]["mode"] == "HOLD_ONLY"

    # Book: holdings + live quotes + just sold.
    assert out["book"]["cnHoldings"][0]["symbol"] == "CN:600000"
    assert out["book"]["liveQuotes"]["quotes"]["CN:600000"]["price"] == "11.20"
    assert out["book"]["justSold"][0]["symbol"] == "CN:600519"

    # Backtest: the required comparison, plus the current-mode mapping.
    bt = out["backtest"]
    assert bt["available"] is True
    keys = [row["timelineStrategy"] for row in bt["comparison"]]
    assert keys == ["harbor", "homeport_m30", "starship_b"]
    assert [row["isCurrentMode"] for row in bt["comparison"]] == [False, True, False]
    assert bt["comparison"][1]["windows"]["long"]["total"] == 146.6
    assert bt["currentMode"]["mode"] == "homeport"
    assert bt["currentMode"]["timelineStrategy"] == "homeport_m30"
    assert "M30" in bt["currentMode"]["mappingNote"]
    assert bt["s3Baseline"]["longWindowCN"]["totalNetPnlPct"] == 250.8
    assert bt["recent"]["perStrategy"]["homeport_m30"]["returnPct"] == 2.5
    # No equity curve inlined.
    assert "rows" not in bt

    # Market facts.
    assert out["fund"]["inflow5d"][0]["industryName"] == "半导体"
    assert out["fund"]["etfFlow"]["verdict"] == "confirm"
    assert out["sentiment"]["riskMode"] == "confirmed_uptrend"
    assert out["macro"]["macro"][0]["name"] == "美元指数"
    assert out["news"]["items"][0]["title"] == "算力需求超预期"
    assert out["freshness"]["staleSources"] == ["research"]


def test_brief_excludes_alpha_and_research_bodies(patched: None) -> None:
    out = agent_brief.build_judgment_brief(trade_date="2026-11-10")
    blob = str(out)

    assert out["omitted"] == [
        "research_reports",
        "alpha_radar_trends",
        "alpha_catalyst_stocks",
        "alpha_auto_qa",
    ]
    # No report/alpha/alessay bodies anywhere in the payload.
    assert "aiSummary" not in blob
    assert "长文AI点评不应进入简报" not in blob
    assert "targetPrice" not in blob
    assert "alphaEvents" not in blob
    assert "Alpha Radar · Structured Trends" not in blob
    assert "researchAlerts" not in blob
    for key in ("alphaRadar", "researchReports", "catalyst", "autoQa"):
        assert key not in out


def test_brief_is_bounded(patched: None) -> None:
    out = agent_brief.build_judgment_brief(trade_date="2026-11-10")
    assert out["payloadBytes"] < 120_000


def test_compact_mode_is_a_subset(patched: None) -> None:
    full = agent_brief.build_judgment_brief(trade_date="2026-11-10", mode="full")
    compact = agent_brief.build_judgment_brief(trade_date="2026-11-10", mode="compact")

    assert full["mode"] == "full"
    assert compact["mode"] == "compact"
    assert compact["contract"]["contractId"] == full["contract"]["contractId"]
    assert len(compact["news"]["items"]) <= len(full["news"]["items"])
    assert compact["backtest"].get("recent") is None
    # Same comparison table either way — that is the meat.
    assert [r["timelineStrategy"] for r in compact["backtest"]["comparison"]] == [
        r["timelineStrategy"] for r in full["backtest"]["comparison"]
    ]


def test_routes_json_and_markdown(patched: None) -> None:
    resp = client.get("/v1/agent/judgment-brief?tradeDate=2026-11-10")
    assert resp.status_code == 200
    body = resp.json()
    assert body["contract"]["contractId"].startswith("dc:2026-11-10:homeport_m30:")
    assert [r["timelineStrategy"] for r in body["backtest"]["comparison"]][:3] == [
        "harbor",
        "homeport_m30",
        "starship_b",
    ]

    md = client.get("/v1/agent/judgment-brief.md?tradeDate=2026-11-10")
    assert md.status_code == 200
    assert md.headers["content-type"].startswith("text/markdown")
    text = md.text
    assert "# 决策简报 judgment brief" in text
    assert "## 4. 回测 backtest" in text
    assert "homeport_m30" in text
    assert "## 11. 故意省略 omitted" in text
    assert "Alpha Radar · Structured Trends" not in text
    assert "研报" not in text.split("## 9. 数据新鲜度")[0]

    via_query = client.get("/v1/agent/judgment-brief?tradeDate=2026-11-10&format=md")
    assert via_query.status_code == 200
    assert via_query.text.startswith("# 决策简报 judgment brief")


def test_route_rejects_bad_mode(patched: None) -> None:
    resp = client.get("/v1/agent/judgment-brief?mode=everything")
    assert resp.status_code == 422


def test_route_degrades_when_dashboard_summary_fails(
    patched: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _boom(**_: Any) -> dict[str, Any]:
        raise RuntimeError("db down")

    monkeypatch.setattr("data_sync_service.service.dashboard.dashboard_summary", _boom)
    out = agent_brief.build_judgment_brief(trade_date="2026-11-10")
    # Contract + backtest still served; facts degrade honestly.
    assert out["contract"]["ok"] is True
    assert out["backtest"]["available"] is True
    assert out["gate"]["available"] is False
    assert out["sentiment"]["available"] is False
    assert "db down" in out["gate"]["error"]


# ---------------------------------------------------------------------------
# cache + timings (2026-11-10 speed pass)
# ---------------------------------------------------------------------------


def _counting_builders(monkeypatch: pytest.MonkeyPatch) -> dict[str, int]:
    """Count how often the expensive builders actually run (they are stubs)."""
    import data_sync_service.api.backtest_routes as br
    import data_sync_service.service.strategy_catalog as sc

    calls = {"dashboard": 0, "health": 0, "overview": 0, "catalog": 0}

    def _dashboard(**kwargs: Any) -> dict[str, Any]:
        calls["dashboard"] += 1
        return _fake_dashboard_summary(**kwargs)

    def _health(**kwargs: Any) -> dict:
        calls["health"] += 1
        return _fake_health(**kwargs)

    base_overview = br.backtest_overview
    base_catalog = sc.strategy_catalog

    def _overview() -> dict[str, Any]:
        calls["overview"] += 1
        return base_overview()

    def _catalog() -> list[dict[str, Any]]:
        calls["catalog"] += 1
        return base_catalog()

    monkeypatch.setattr("data_sync_service.service.dashboard.dashboard_summary", _dashboard)
    monkeypatch.setattr(
        "data_sync_service.service.portfolio_health.build_portfolio_health", _health
    )
    monkeypatch.setattr(br, "backtest_overview", _overview)
    monkeypatch.setattr(sc, "strategy_catalog", _catalog)
    return calls


def test_brief_cache_hit_skips_expensive_builders(
    patched: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = _counting_builders(monkeypatch)

    first = agent_brief.build_judgment_brief(trade_date="2026-11-10")
    assert first["cached"] is False
    assert calls == {"dashboard": 1, "health": 1, "overview": 1, "catalog": 1}

    # Per-block wall times are part of the payload now.
    timings = first["timingsMs"]
    for key in (
        "contractMs",
        "dashboardSummaryMs",
        "freshnessMs",
        "executionMs",
        "backtestMs",
        "quotesMs",
        "totalMs",
    ):
        assert key in timings, key
        assert timings[key] >= 0

    second = agent_brief.build_judgment_brief(trade_date="2026-11-10")

    # No builder ran again: this is a real cache hit.
    assert calls == {"dashboard": 1, "health": 1, "overview": 1, "catalog": 1}
    assert second["cached"] is True
    assert second["cache"]["hit"] is True
    assert second["timingsMs"]["cacheLookupMs"] >= 0

    # Same meaning, byte-for-byte on the meat (only cache/timings metadata moves).
    assert second["contract"] == first["contract"]
    assert second["book"] == first["book"]
    assert second["backtest"] == first["backtest"]
    assert second["omitted"] == first["omitted"]
    assert second["payloadBytes"] == first["payloadBytes"]

    # Research / Alpha bodies still absent on a cache hit.
    blob = str(second)
    assert "长文AI点评不应进入简报" not in blob
    assert "aiSummary" not in blob
    assert "targetPrice" not in blob


def test_brief_refresh_bypasses_cache(patched: None, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _counting_builders(monkeypatch)

    first = agent_brief.build_judgment_brief(trade_date="2026-11-10")
    assert first["cached"] is False

    refreshed = agent_brief.build_judgment_brief(trade_date="2026-11-10", refresh=True)

    assert refreshed["cached"] is False
    assert refreshed["cache"]["hit"] is False
    assert calls == {"dashboard": 2, "health": 2, "overview": 2, "catalog": 2}
    assert refreshed["contract"]["contractId"] == first["contract"]["contractId"]


def test_brief_cache_expires_after_ttl(patched: None, monkeypatch: pytest.MonkeyPatch) -> None:
    clock = {"now": 1_000_000.0}
    monkeypatch.setattr(agent_brief._BRIEF_CACHE, "_clock", lambda: clock["now"])

    assert agent_brief.build_judgment_brief(trade_date="2026-11-10")["cached"] is False

    clock["now"] += agent_brief.BRIEF_CACHE_TTL_SECONDS - 1
    assert agent_brief.build_judgment_brief(trade_date="2026-11-10")["cached"] is True

    clock["now"] += 2  # past the TTL
    assert agent_brief.build_judgment_brief(trade_date="2026-11-10")["cached"] is False


def test_brief_cache_expires_across_close_boundary(
    patched: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A pre-close snapshot must not be served as the evening brief."""
    close_ts = 2_000_010.0
    clock = {"now": close_ts - 5.0}
    monkeypatch.setattr(agent_brief, "_last_session_close_ts", lambda: close_ts)
    monkeypatch.setattr(agent_brief._BRIEF_CACHE, "_clock", lambda: clock["now"])

    assert agent_brief.build_judgment_brief(trade_date="2026-11-10")["cached"] is False

    clock["now"] = close_ts + 1.0  # inside the 75s TTL, but after the close
    rebuilt = agent_brief.build_judgment_brief(trade_date="2026-11-10")
    assert rebuilt["cached"] is False


def test_compact_reuses_warm_full_dashboard_and_catalog(
    patched: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = _counting_builders(monkeypatch)

    agent_brief.build_judgment_brief(trade_date="2026-11-10", mode="full")
    compact = agent_brief.build_judgment_brief(trade_date="2026-11-10", mode="compact")

    # Compact is its own brief entry, but reuses the full run's slow inputs.
    assert compact["cached"] is False
    assert compact["cache"]["dashboardHit"] is True
    assert compact["cache"]["backtestHit"] is True
    assert calls == {"dashboard": 1, "health": 2, "overview": 1, "catalog": 1}
    assert compact["backtest"].get("recent") is None


def test_invalidation_drops_cached_brief(patched: None) -> None:
    assert agent_brief.build_judgment_brief(trade_date="2026-11-10")["cached"] is False
    assert agent_brief.build_judgment_brief(trade_date="2026-11-10")["cached"] is True

    agent_brief.invalidate_judgment_brief_cache("test")
    assert agent_brief.build_judgment_brief(trade_date="2026-11-10")["cached"] is False


def test_dashboard_cache_ttl_is_session_aware(monkeypatch: pytest.MonkeyPatch) -> None:
    import data_sync_service.service.trade_calendar_utils as tcu

    monkeypatch.setattr(tcu, "compute_market_status", lambda now=None: {"isMarketOpen": True})
    assert agent_brief._dashboard_cache_ttl() == agent_brief.DASHBOARD_CACHE_TTL_SECONDS

    monkeypatch.setattr(tcu, "compute_market_status", lambda now=None: {"isMarketOpen": False})
    assert agent_brief._dashboard_cache_ttl() == agent_brief.DASHBOARD_IDLE_TTL_SECONDS


def test_dashboard_cache_keeps_summary_warm_off_session(
    patched: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = {"dashboard": 0}
    clock = {"now": 1_000_000.0}

    def _dashboard(**kwargs: Any) -> dict[str, Any]:
        calls["dashboard"] += 1
        return _fake_dashboard_summary(**kwargs)

    monkeypatch.setattr("data_sync_service.service.dashboard.dashboard_summary", _dashboard)
    monkeypatch.setattr(agent_brief._DASHBOARD_CACHE, "_clock", lambda: clock["now"])
    # Off-session TTL (no market open): 15 minutes.
    monkeypatch.setattr(agent_brief._DASHBOARD_CACHE, "_ttl", lambda: 900.0)

    agent_brief._dashboard_summary()
    clock["now"] += 300  # well past the in-session 75s, still inside the idle TTL
    agent_brief._dashboard_summary()

    assert calls["dashboard"] == 1


def test_warmup_populates_todays_brief(patched: None) -> None:
    assert agent_brief.warm_judgment_brief(reason="test") is True

    warm = agent_brief.build_judgment_brief()
    assert warm["cached"] is True
    assert warm["asOfDate"] == agent_brief._today_iso()


def test_warmup_is_fail_open(patched: None, monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(**_: Any) -> dict[str, Any]:
        raise RuntimeError("contract down")

    monkeypatch.setattr("data_sync_service.service.agent_contract.build_decision_contract", _boom)
    # No raise; just a False + a warning log.
    assert agent_brief.warm_judgment_brief(reason="test") is False


def test_route_supports_refresh(patched: None) -> None:
    first = client.get("/v1/agent/judgment-brief?tradeDate=2026-11-10")
    assert first.status_code == 200
    assert first.json()["cached"] is False

    assert first.json()["timingsMs"]["totalMs"] >= 0

    warm = client.get("/v1/agent/judgment-brief?tradeDate=2026-11-10")
    assert warm.json()["cached"] is True

    forced = client.get("/v1/agent/judgment-brief?tradeDate=2026-11-10&refresh=true")
    assert forced.status_code == 200
    assert forced.json()["cached"] is False

    md = client.get("/v1/agent/judgment-brief.md?tradeDate=2026-11-10&refresh=true")
    assert md.status_code == 200
    assert md.text.startswith("# 决策简报 judgment brief")
