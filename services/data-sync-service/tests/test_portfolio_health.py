"""Tests for service/portfolio_health.py (S-3-aligned holdings health).

The trailing-stop / peak logic must mirror the backtest engine (CLOSE-based
peaks, not high) and the S-3 constants from db/paper_trading.py.
"""

from __future__ import annotations

from unittest.mock import patch

from data_sync_service.service import portfolio_health as ph

BARS = [
    ("2026-08-04", 40.5, 39.6, 39.65),
    ("2026-08-05", 41.2, 40.05, 40.1),
    ("2026-08-06", 40.0, 39.55, 39.6),
    ("2026-08-07", 39.8, 39.45, 39.5),
]


class FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params):
        if params[0].startswith("513180.SH"):
            self._rows = [("2026-08-05", 0.630, 0.574, 0.624), ("2026-08-06", 0.622, 0.564, 0.614)]

    def fetchall(self):
        return self._rows


class FakeConn:
    def __init__(self, rows):
        self._cursor = FakeCursor(rows)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def cursor(self):
        return self._cursor


def _check(*, cost: float, entry: str, ts: str, rows: list, trade_date: str = "2026-08-07"):
    with patch("data_sync_service.db.get_connection", return_value=FakeConn(rows)):
        return ph._holding_check(
            name="X", cost=cost, entry_date=entry, ts=ts, trade_date=trade_date,
        )


def test_peak_uses_close_not_high():
    """Trailing stop must be CLOSE-based like the backtest engine."""
    out = _check(cost=39.9, entry="2026-08-04", ts="300628.SZ", rows=BARS)
    assert out["peakPrice"] == 40.1
    assert out["peakDate"] == "2026-08-05"
    assert out["drawdownFromPeakPct"] == -1.5
    assert out["action"] == "HOLD"


def test_realtime_breach_warns_but_action_stays_close_caliber_opt101():
    """OPT-101: the action is ALWAYS close-caliber (backtest behavior). A
    realtime breach of the stop line only produces a warning when the close
    has not confirmed it — the user follows the exact backtest action."""
    rows = [("2026-08-12", 204.0, 202.75, 202.8)]
    with patch("data_sync_service.db.get_connection", return_value=FakeConn(rows)):
        rt = ph._holding_check(
            name="X", cost=202.8, entry_date="2026-08-12", ts="02099.HK",
            trade_date="2026-08-13", realtime_price=190.8,
        )
    assert rt["action"] == "HOLD"  # close 202.8 > stop line 192.66 → HOLD
    assert rt["realtime"] is True
    assert rt["evaluatedPrice"] == 190.8
    assert rt["peakPrice"] == 202.8  # close peak unchanged
    assert rt["realtimeWarning"] is True
    assert "待收盘确认" in rt["realtimeAlert"]


def test_realtime_and_close_both_breach_exits_opt101():
    """Close below the stop line → EXIT regardless of realtime."""
    rows = [("2026-08-12", 204.0, 190.45, 190.5)]
    with patch("data_sync_service.db.get_connection", return_value=FakeConn(rows)):
        rt = ph._holding_check(
            name="X", cost=202.8, entry_date="2026-08-12", ts="02099.HK",
            trade_date="2026-08-13", realtime_price=189.0,
        )
    assert rt["action"] == "EXIT"
    assert "stop_loss" in rt["reason"]
    assert rt.get("realtimeWarning") is None  # close already confirmed


def test_trailing_stop_fires_on_close_pullback():
    """A close pullback >= 8% from the close-peak fires EXIT."""
    rows = [
        ("2026-08-03", 40.0, 39.85, 39.9),
        ("2026-08-04", 40.5, 39.95, 40.0),
        ("2026-08-05", 41.2, 40.45, 40.5),
        ("2026-08-06", 39.8, 37.25, 37.3),
        ("2026-08-07", 37.0, 36.45, 36.5),
    ]
    out = _check(cost=39.9, entry="2026-08-03", ts="300628.SZ", rows=rows)
    assert out["action"] == "EXIT"
    assert "trailing_stop" in out["reason"]
    assert out["drawdownFromPeakPct"] <= -8.0


def test_fixed_stop_fires():
    rows = [
        ("2026-08-04", 40.0, 39.85, 39.9),
        ("2026-08-05", 39.5, 38.15, 38.2),
        ("2026-08-06", 38.0, 37.65, 37.7),
    ]
    out = _check(cost=39.9, entry="2026-08-04", ts="300628.SZ", rows=rows)
    assert out["action"] == "EXIT"
    assert "stop_loss" in out["reason"]


def test_expire_date_is_entry_plus_60():
    out = _check(cost=10.0, entry="2026-08-04", ts="300628.SZ", rows=BARS)
    assert out["maxHoldDate"] == out["expireDate"] == "2026-10-03"
    assert out["holdingDays"] == 3


def test_no_price_data_holds():
    with patch("data_sync_service.db.get_connection", return_value=FakeConn([])):
        out = ph._holding_check(
            name="X", cost=10.0, entry_date="2026-08-04",
            ts="300628.SZ", trade_date="2026-08-07",
        )
    assert out["action"] == "HOLD"
    assert out["status"] == "no-price-data"


REGISTRY = [
    {"symbol": "CN:300628", "name": "亿联网络", "positionPct": 5.93,
     "costPrice": 39.9, "entryDate": "2026-08-04"},
]


def test_build_attaches_candidates_and_pyramid_flags():
    cands = [{"symbol": "CN:600111", "ts_code": "600111.SH",
              "industry": "有色", "score": 71.0, "rs": 0.62, "regime": "Strong"}]
    with (
        patch("data_sync_service.db.get_connection", return_value=FakeConn(BARS)),
        patch("data_sync_service.db.paper_trading.today_iso", return_value="2026-08-07"),
        patch.object(ph, "list_registry", return_value=REGISTRY),
        patch.object(ph, "_pyramided_symbols", return_value={"CN:300628"}),
        patch("data_sync_service.service.paper_s3.build_s3_candidates", return_value=cands),
        patch("data_sync_service.service.backtest_engine._load_regime_by_day",
              return_value={"2026-08-07": "Strong"}),
        patch("data_sync_service.service.market_sentiment.get_cn_sentiment",
              return_value={"items": [{"riskMode": "hot"}]}),
        patch("data_sync_service.service.market_sentiment.get_panic_cooldown",
              return_value={"lastPanicDate": None, "cooldownEndDate": None, "active": False}),
        patch.object(ph, "_lookup_stock_basic", return_value=({"600111.SH": "北方稀土"}, {})),
    ):
        out = ph.build_portfolio_health(trade_date="2026-08-07")

    assert out["regime"] == "Strong"
    assert out["s3Candidates"][0]["symbol"] == "CN:600111"
    assert out["s3Candidates"][0]["name"] == "北方稀土"
    assert out["s3Candidates"][0]["alphaEvents"] == []
    h = out["holdings"][0]
    assert h["pyramidAdded"] is True
    assert h["pyramidTriggerLine"] == 40.897
    assert out["s3Rules"]["pyramidTriggerPct"] == 2.5


def test_detect_line_ops_trail_up_first_baseline_no_op() -> None:
    assert ph._detect_line_ops(
        prev_trail=None, cur_trail=36.828, prev_stop=None, cur_stop=37.905,
        max_hold_days=60, holding_days=8, expire_date="2026-10-03",
    ) == {"expireDate": "2026-10-03"}


def test_detect_line_ops_trail_climb_and_expiry() -> None:
    ops = ph._detect_line_ops(
        prev_trail=36.828, cur_trail=37.52, prev_stop=37.905, cur_stop=37.905,
        max_hold_days=60, holding_days=57,
    )
    assert ops["trail_up"] == [36.828, 37.52]
    assert "stop_up" not in ops
    assert ops["expire_soon"] == 3


def test_detect_line_ops_stop_up_on_pyramid() -> None:
    ops = ph._detect_line_ops(
        prev_trail=36.828, cur_trail=36.828, prev_stop=37.905, cur_stop=39.2,
        max_hold_days=60, holding_days=10,
    )
    assert ops["stop_up"] == [37.905, 39.2]
    assert "trail_up" not in ops


def test_detect_line_ops_no_change_no_ops() -> None:
    assert ph._detect_line_ops(
        prev_trail=36.828, cur_trail=36.828, prev_stop=37.905, cur_stop=37.905,
        max_hold_days=60, holding_days=20,
    ) == {}


def test_build_holdings_flags_line_ops_and_persists_baseline(monkeypatch) -> None:
    """Peak climb moves the trailing line -> lineOps set + baseline persisted."""
    reg = [
        {"symbol": "CN:300628", "positionPct": 5.93, "costPrice": 39.9,
         "entryDate": "2026-08-04", "name": "亿联网络",
         "payload": {"conditionalOps": {"trail": 36.828, "stop": 37.905}}},
    ]
    persisted: list[tuple] = []

    def fake_update(sym: str, fields: dict) -> None:
        persisted.append((sym, fields))

    monkeypatch.setattr(ph, "list_registry", lambda: reg)
    monkeypatch.setattr(ph, "_resolve_holding_ts", lambda s: "300628.SZ")
    monkeypatch.setattr(
        "data_sync_service.db.watchlist_automation.update_registry_payload", fake_update)
    monkeypatch.setattr(ph, "_holding_check", lambda **kw: {
        "stopLossLine": 37.905, "trailingLine": 37.52, "holdingDays": 12,
        "expireDate": "2026-10-03", "action": "HOLD", "pnlPct": 1.2,
    })

    out = ph._build_holdings_block(market="CN", day="2026-08-12")
    assert out[0]["lineOps"]["trail_up"] == [36.828, 37.52]
    assert out[0]["lineOps"]["expireDate"] == "2026-10-03"
    assert persisted == [("CN:300628", {
        "conditionalOps": {"trail": 37.52, "stop": 37.905},
    })]


def test_build_holdings_first_sighting_persists_without_flag(monkeypatch) -> None:
    reg = [
        {"symbol": "CN:300628", "positionPct": 5.93, "costPrice": 39.9,
         "entryDate": "2026-08-04", "name": "亿联网络", "payload": {}},
    ]
    persisted: list[tuple] = []

    def fake_update(sym: str, fields: dict) -> None:
        persisted.append((sym, fields))

    monkeypatch.setattr(ph, "list_registry", lambda: reg)
    monkeypatch.setattr(ph, "_resolve_holding_ts", lambda s: "300628.SZ")
    monkeypatch.setattr(
        "data_sync_service.db.watchlist_automation.update_registry_payload", fake_update)
    monkeypatch.setattr(ph, "_holding_check", lambda **kw: {
        "stopLossLine": 37.905, "trailingLine": 36.828, "holdingDays": 8,
        "expireDate": "2026-10-03", "action": "HOLD", "pnlPct": 0.5,
    })

    out = ph._build_holdings_block(market="CN", day="2026-08-12")
    assert "trail_up" not in out[0]["lineOps"]
    assert persisted and persisted[0][0] == "CN:300628"


def test_alpha_sym_key_normalizes_hk_padding() -> None:
    assert ph._alpha_sym_key("HK:2099") == "HK:02099"
    assert ph._alpha_sym_key("HK:09880") == "HK:09880"
    assert ph._alpha_sym_key("CN:300628") == "CN:300628"


def test_alpha_events_for_symbols_matches_and_ranks(monkeypatch) -> None:
    items = [
        {"trendName": "黄金牛市", "catalystGrade": "A", "mappingConfidence": 0.9,
         "riskStatus": "waiting_v2_flow", "eventFocus": "美联储降息预期升温",
         "documentPublishedAt": "2026-08-10T08:00:00+08:00", "createdAt": "x",
         "cnSymbols": [], "hkSymbols": [{"symbol": "HK:02099", "confidence": 0.9}]},
        {"trendName": "低置信事件", "catalystGrade": "C", "mappingConfidence": 0.3,
         "riskStatus": "risk", "eventFocus": "", "documentPublishedAt": None,
         "createdAt": "x", "cnSymbols": [{"symbol": "CN:300628", "confidence": 0.3}],
         "hkSymbols": []},
        {"trendName": "通信设备景气", "catalystGrade": "B", "mappingConfidence": 0.8,
         "riskStatus": "ok", "eventFocus": "5G 资本开支超预期", "documentPublishedAt": None,
         "createdAt": "2026-08-11T00:00:00Z",
         "cnSymbols": [{"symbol": "CN:300628", "confidence": 0.85}], "hkSymbols": []},
    ]
    monkeypatch.setattr("data_sync_service.db.alpha_radar.fetch_trends",
                        lambda limit=200, max_age_days=14: (3, items))
    out = ph._alpha_events_for_symbols(["CN:300628", "HK:2099"])
    assert len(out["HK:02099"]) == 1
    assert out["HK:02099"][0]["trend"] == "黄金牛市"
    assert out["CN:300628"][0]["trend"] == "通信设备景气"  # confidence 0.8 > 0.3 → top
    assert out["CN:300628"][1]["trend"] == "低置信事件"
    assert len(out["CN:300628"]) == 2


def test_industry_flow_map_ranks_industries(monkeypatch) -> None:

    dates = ["2026-08-06", "2026-08-07", "2026-08-10", "2026-08-11", "2026-08-12"]
    rows = []
    for i, name in enumerate(("通信", "有色金属", "电子")):
        for d in dates:
            rows.append({"date": d, "industry_code": f"BK{i}", "industry_name": name,
                         "net_inflow": (100.0 - i * 20.0) * 1e8})
    monkeypatch.setattr("data_sync_service.db.industry_fund_flow.get_dates_upto",
                        lambda day, days: dates)
    monkeypatch.setattr("data_sync_service.db.industry_fund_flow.get_rows_for_dates",
                        lambda ds: rows)
    out = ph._industry_flow_map("2026-08-12")
    assert out["通信"]["rank5d"] == 1
    assert out["通信"]["netInflow5d"] == 500.0
    assert out["电子"]["rank5d"] == 3
    assert out["通信"]["total"] == 3


def test_build_holdings_attaches_alpha_and_flow(monkeypatch) -> None:
    reg = [
        {"symbol": "CN:300628", "positionPct": 5.93, "costPrice": 39.9,
         "entryDate": "2026-08-04", "name": "亿联网络",
         "payload": {"conditionalOps": {"trail": 36.828, "stop": 37.905}}},
    ]
    monkeypatch.setattr(ph, "list_registry", lambda: reg)
    monkeypatch.setattr(ph, "_resolve_holding_ts", lambda s: "300628.SZ")
    monkeypatch.setattr(ph, "_holding_check", lambda **kw: {
        "stopLossLine": 37.905, "trailingLine": 36.828, "holdingDays": 8,
        "expireDate": "2026-10-03", "action": "HOLD", "pnlPct": 1.2,
    })
    monkeypatch.setattr(ph, "_pyramided_symbols", lambda: set())
    monkeypatch.setattr(
        "data_sync_service.db.watchlist_automation.update_registry_payload",
        lambda *a, **k: None)
    monkeypatch.setattr(ph, "_alpha_events_for_symbols", lambda syms: {
        "CN:300628": [{"trend": "通信设备景气", "grade": "B", "confidence": 0.85,
                       "daysAgo": 1, "riskStatus": "ok", "focus": ""}],
    })
    monkeypatch.setattr(ph, "_l1_industry_for_symbols", lambda syms: {"CN:300628": "通信"})
    monkeypatch.setattr(ph, "_industry_flow_map", lambda day: {
        "通信": {"industry": "通信", "netInflow5d": -47.69, "rank5d": 26, "total": 31},
    })
    out = ph._build_holdings_block(market="CN", day="2026-08-12")
    h = out[0]
    assert h["alphaEvents"][0]["trend"] == "通信设备景气"
    assert h["industryFlow"] == {"industry": "通信", "netInflow5d": -47.69,
                                 "rank5d": 26, "total": 31}


def test_health_block_candidate_info_and_summary(monkeypatch) -> None:
    """P2: candidates carry alpha/flow info; block carries infoSummary."""
    cands = [{"symbol": "CN:600111", "ts_code": "600111.SH", "industry": "有色金属",
              "score": 71.0, "rs": 0.62, "regime": "Strong"}]
    holdings_payload = [
        {"symbol": "CN:300628", "positionPct": 5.93, "costPrice": 39.9,
         "entryDate": "2026-08-04", "name": "亿联网络",
         "payload": {"conditionalOps": {"trail": 36.828, "stop": 37.905}}},
    ]
    monkeypatch.setattr(ph, "list_registry", lambda: holdings_payload)
    monkeypatch.setattr(ph, "_market_holdings_symbols", lambda m: ["CN:300628"])
    monkeypatch.setattr(ph, "_alpha_events_for_symbols", lambda syms: {
        "CN:600111": [{"trend": "稀土催化", "grade": "A", "confidence": 0.9,
                       "daysAgo": 1, "riskStatus": "ok", "focus": ""}],
    })
    monkeypatch.setattr(ph, "_l1_industry_for_symbols", lambda syms: {
        "CN:300628": "通信", "CN:600111": "有色金属",
    })
    monkeypatch.setattr(ph, "_industry_flow_map", lambda day: {
        "通信": {"industry": "通信", "netInflow5d": -47.69, "rank5d": 26, "total": 31},
        "有色金属": {"industry": "有色金属", "netInflow5d": 8.2, "rank5d": 2, "total": 31},
    })
    monkeypatch.setattr(ph, "_resolve_holding_ts", lambda s: "300628.SZ")
    monkeypatch.setattr(ph, "_holding_check", lambda **kw: {
        "stopLossLine": 37.905, "trailingLine": 36.828, "holdingDays": 8,
        "expireDate": "2026-10-03", "action": "HOLD", "pnlPct": -2.36,
    })
    monkeypatch.setattr(ph, "_pyramided_symbols", lambda: set())
    monkeypatch.setattr(ph, "_lookup_stock_basic",
                        lambda ts: ({"600111.SH": "北方稀土"}, {}))
    monkeypatch.setattr(ph, "_score_data_as_of", lambda market, day: day)
    monkeypatch.setattr("data_sync_service.service.backtest_engine._load_regime_by_day",
                        lambda cfg, days: {days[0]: "Strong"})
    monkeypatch.setattr("data_sync_service.service.market_sentiment.get_cn_sentiment",
                        lambda days, as_of_date: {"items": [{"riskMode": "hot"}]})
    monkeypatch.setattr("data_sync_service.service.market_sentiment.get_panic_cooldown",
                        lambda days, cooldown_days, as_of_date:
                        {"lastPanicDate": None, "cooldownEndDate": None, "active": False})
    monkeypatch.setattr("data_sync_service.service.paper_s3.build_s3_candidates",
                        lambda trade_date, max_positions: cands)
    monkeypatch.setattr("data_sync_service.service.paper_s3._circuit_blocked",
                        lambda as_of: False)
    monkeypatch.setattr(
        "data_sync_service.db.watchlist_automation.update_registry_payload",
        lambda *a, **k: None)


    monkeypatch.setattr("data_sync_service.service.market_regime.regime_strength_score",
                        lambda as_of_date, market: {"strength": 30.0})

    out = ph._health_block(market="CN", day="2026-08-12")
    c = out["s3Candidates"][0]
    assert c["alphaEvents"][0]["trend"] == "稀土催化"
    assert c["industryFlow"] == {"industry": "有色金属", "netInflow5d": 8.2,
                                 "rank5d": 2, "total": 31}
    assert out["infoSummary"] == {"holdingsCount": 1, "eventHoldings": 0,
                                  "industryOutflow": 1, "industryInflow": 0}
    assert out["holdings"][0]["industryFlow"]["netInflow5d"] == -47.69


def test_build_includes_etf_holdings_in_cn_block(monkeypatch):
    """2026-08-10: A-share ETFs (ETF:XXXXXX) belong to the CN line's holdings."""
    from data_sync_service.service import portfolio_health as ph

    reg = [
        {
            "symbol": "ETF:513180", "positionPct": 27.87, "costPrice": 0.613,
            "entryDate": "2026-08-02", "name": "华夏恒生科技ETF",
        },
    ]

    def fake_registry():
        return reg

    monkeypatch.setattr(ph, "list_registry", fake_registry)
    monkeypatch.setattr(ph, "_resolve_holding_ts", lambda s: "513180.SH")
    monkeypatch.setattr(
        ph, "_holding_check",
        lambda **kw: {"symbol": kw["name"], "action": "HOLD", "pnlPct": 1.2},
    )
    with patch("data_sync_service.service.market_sentiment.get_cn_sentiment",
               return_value={"items": [{"riskMode": "hot"}]}):
        out = ph.build_portfolio_health(trade_date="2026-08-07", markets=("CN",))

    syms = [h["symbol"] for h in out["holdings"]]
    assert "ETF:513180" in syms


def test_strong_regime_uses_atr_stop_rule_opt105():
    """CN Strong regime → entry-locked ATR line + stopRule label (UI check)."""
    rows = [  # 8 pre-entry bars TR 0.5 → atr_pct 5% → ATR stop -10%
        ("2026-07-20", 10.2, 9.95, 10.0),
        ("2026-07-21", 10.2, 9.95, 10.0),
        ("2026-07-22", 10.2, 9.95, 10.0),
        ("2026-07-23", 10.2, 9.95, 10.0),
        ("2026-07-24", 10.2, 9.95, 10.0),
        ("2026-07-27", 10.2, 9.95, 10.0),
        ("2026-07-28", 10.2, 9.95, 10.0),
        ("2026-07-29", 10.2, 9.95, 10.0),
        ("2026-08-03", 10.5, 10.25, 10.3),  # entry day close 10.3
    ]
    with patch("data_sync_service.db.get_connection", return_value=FakeConn(rows)):
        out = ph._holding_check(
            name="X", cost=10.0, entry_date="2026-08-03", ts="600111.SH",
            trade_date="2026-08-07", regime="Strong",
        )
    assert out["stopRule"] == "atr"
    # ATR line is never TIGHTER than the fixed -5% line (the whole point of
    # Strong: don't cut winners with a fixed tight stop).
    assert out["stopLossLine"] >= round(10.0 * 0.95, 3)
    assert "ATR" in out["stopRuleDetail"]


def test_weak_regime_uses_fixed_stop_rule_opt105():
    rows = [("2026-08-03", 10.5, 10.25, 10.3)]
    with patch("data_sync_service.db.get_connection", return_value=FakeConn(rows)):
        out = ph._holding_check(
            name="X", cost=10.0, entry_date="2026-08-03", ts="600111.SH",
            trade_date="2026-08-07", regime="Weak",
        )
    assert out["stopRule"] == "fixed"
    assert out["stopLossLine"] == round(10.0 * 0.95, 3)  # fixed -5%
