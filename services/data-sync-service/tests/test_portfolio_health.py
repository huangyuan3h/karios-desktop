"""Tests for service/portfolio_health.py (S-3-aligned holdings health).

The trailing-stop / peak logic must mirror the backtest engine (CLOSE-based
peaks, not high) and the S-3 constants from db/paper_trading.py.
"""

from __future__ import annotations

from unittest.mock import patch

from data_sync_service.service import portfolio_health as ph

BARS = [
    ("2026-08-04", 40.5, 39.65),
    ("2026-08-05", 41.2, 40.1),
    ("2026-08-06", 40.0, 39.6),
    ("2026-08-07", 39.8, 39.5),
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
            self._rows = [("2026-08-05", 0.630, 0.624), ("2026-08-06", 0.622, 0.614)]

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


def test_trailing_stop_fires_on_close_pullback():
    """A close pullback >= 8% from the close-peak fires EXIT."""
    rows = [
        ("2026-08-03", 40.0, 39.9),
        ("2026-08-04", 40.5, 40.0),
        ("2026-08-05", 41.2, 40.5),
        ("2026-08-06", 39.8, 37.3),
        ("2026-08-07", 37.0, 36.5),
    ]
    out = _check(cost=39.9, entry="2026-08-03", ts="300628.SZ", rows=rows)
    assert out["action"] == "EXIT"
    assert "trailing_stop" in out["reason"]
    assert out["drawdownFromPeakPct"] <= -8.0


def test_fixed_stop_fires():
    rows = [
        ("2026-08-04", 40.0, 39.9),
        ("2026-08-05", 39.5, 38.2),
        ("2026-08-06", 38.0, 37.7),
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
    assert out["s3Candidates"] == [{"symbol": "CN:600111", "ts_code": "600111.SH",
                                    "industry": "有色", "score": 71.0, "rs": 0.62,
                                    "regime": "Strong", "name": "北方稀土"}]
    h = out["holdings"][0]
    assert h["pyramidAdded"] is True
    assert h["pyramidTriggerLine"] == 40.897
    assert out["s3Rules"]["pyramidTriggerPct"] == 2.5


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
