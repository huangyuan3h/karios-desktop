"""query_routes endpoint tests (mocked services/DB)."""

from __future__ import annotations

from unittest.mock import patch

from fastapi.testclient import TestClient  # type: ignore[import-not-found]

from data_sync_service.main import app  # type: ignore[import-not-found]

client = TestClient(app)


class _FakeCur:
    def __init__(self, rows) -> None:
        self._rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def execute(self, *a, **kw) -> None:
        pass

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self, rows) -> None:
        self._rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return None

    def cursor(self):
        return _FakeCur(self._rows)


def _patch_resolve_db(monkeypatch, rows) -> None:
    import data_sync_service.db as dbmod
    import data_sync_service.db.stock_basic as sbmod

    monkeypatch.setattr(dbmod, "get_connection", lambda: _FakeConn(rows))
    monkeypatch.setattr(sbmod, "ensure_table", lambda: None)


# ---- resolve_symbols_endpoint (52 missed) ----------------------------------

def test_resolve_symbols_empty(monkeypatch) -> None:
    assert client.get("/market/stocks/resolve").json() == []
    assert client.get("/market/stocks/resolve?symbols=").json() == []


def test_resolve_symbols_invalid_symbols(monkeypatch) -> None:
    _patch_resolve_db(monkeypatch, [])
    out = client.get("/market/stocks/resolve?symbols=CN:abc&symbols=HK:&symbols=garbage").json()
    assert out == []


def test_resolve_symbols_caps_at_500(monkeypatch) -> None:
    rows = [("600000.SH", "600000", "浦发银行")]
    _patch_resolve_db(monkeypatch, rows)
    syms = "&symbols=".join([f"CN:600000"] * 501)
    out = client.get(f"/market/stocks/resolve?symbols={syms}").json()
    assert len(out) == 1


def test_resolve_symbols_cn_hk_etf(monkeypatch) -> None:
    rows = [
        ("600000.SH", "600000", "浦发银行"),
        ("00700.HK", "00700", "腾讯控股"),
        ("510300.SH", "510300", "沪深300ETF"),
        ("159915.SZ", "159915", "创业板ETF"),
    ]
    _patch_resolve_db(monkeypatch, rows)
    out = client.get(
        "/market/stocks/resolve?symbols=CN:600000&symbols=HK:700&symbols=ETF:510300&symbols=ETF:159915"
    ).json()
    assert out[0] == {
        "symbol": "CN:600000",
        "market": "CN",
        "ticker": "600000",
        "name": "浦发银行",
        "currency": "CNY",
    }
    assert out[1]["symbol"] == "HK:700" and out[1]["market"] == "HK" and out[1]["ticker"] == "00700"
    assert out[1]["currency"] == "HKD"
    assert out[2]["market"] == "ETF" and out[2]["currency"] == "CNY"
    assert out[3]["market"] == "ETF"


def test_resolve_symbols_misses_db(monkeypatch) -> None:
    _patch_resolve_db(monkeypatch, [])
    out = client.get("/market/stocks/resolve?symbols=CN:600000&symbols=HK:00700").json()
    assert out == []


def test_resolve_symbols_db_error_500(monkeypatch) -> None:
    import data_sync_service.db as dbmod
    import data_sync_service.db.stock_basic as sbmod

    def boom():
        raise RuntimeError("db exploded")

    monkeypatch.setattr(dbmod, "get_connection", boom)
    monkeypatch.setattr(sbmod, "ensure_table", lambda: None)
    resp = client.get("/market/stocks/resolve?symbols=CN:600000")
    assert resp.status_code == 500
    assert "db exploded" in resp.json()["detail"]


# ---- get_market_stocks_quotes_endpoint (24 missed) -------------------------

def test_quotes_empty_symbols() -> None:
    assert client.get("/market/stocks/quotes").json() == {"quotes": {}}


def test_quotes_caps_and_maps(monkeypatch) -> None:
    with patch("data_sync_service.api.query_routes.symbol_to_ts_code", side_effect=lambda s: f"{s}.SZ") as m_conv, patch(
        "data_sync_service.api.query_routes.get_market_quotes_batch"
    ) as m_batch:
        m_batch.return_value = {
            "CN:000001.SZ": {"price": "10.5", "changePct": "1.2", "volume": "100", "turnover": "1000"}
        }
        syms = "&symbols=".join(["CN:000001"] * 501)
        out = client.get(f"/market/stocks/quotes?symbols={syms}").json()
    assert len(out["quotes"]) == 1  # 500 identical symbols collapse to one key
    assert m_conv.call_count == 500


def test_quotes_mixed_valid_invalid(monkeypatch) -> None:
    with patch("data_sync_service.api.query_routes.symbol_to_ts_code", side_effect=lambda s: None if s.startswith("HK") else "X.SZ"), patch(
        "data_sync_service.api.query_routes.get_market_quotes_batch"
    ) as m_batch:
        m_batch.return_value = {"X.SZ": {"price": "1.0", "changePct": "0.0", "volume": "2", "turnover": "3"}}
        out = client.get("/market/stocks/quotes?symbols=CN:000001&symbols=HK:00700&symbols=HK:00001").json()
    assert out["quotes"]["CN:000001"]["price"] == "1.0"
    assert out["quotes"]["HK:00700"] == {"price": None, "changePct": None, "volume": None, "turnover": None}
    assert out["quotes"]["HK:00001"]["price"] is None


def test_quotes_all_invalid(monkeypatch) -> None:
    with patch("data_sync_service.api.query_routes.symbol_to_ts_code", lambda s: None):
        out = client.get("/market/stocks/quotes?symbols=HK:00700").json()
    assert out == {"quotes": {}}


def test_quotes_whitespace_stripped(monkeypatch) -> None:
    with patch("data_sync_service.api.query_routes.symbol_to_ts_code", side_effect=lambda s: None if s.strip() == "" else "X.SZ"), patch(
        "data_sync_service.api.query_routes.get_market_quotes_batch"
    ) as m_batch:
        m_batch.return_value = {"X.SZ": {"price": "1.0", "changePct": "0.0", "volume": "2", "turnover": "3"}}
        out = client.get("/market/stocks/quotes?symbols=CN:1&symbols=%20%20").json()
    assert set(out["quotes"]) == {"CN:1"}


# ---- small endpoints -------------------------------------------------------

def test_quote_endpoint_single_and_list(monkeypatch) -> None:
    with patch("data_sync_service.api.query_routes.fetch_realtime_quotes") as m:
        m.return_value = {"000001.SZ": {"price": "10"}}
        assert client.get("/quote?ts_code=000001.SZ").json() == {"000001.SZ": {"price": "10"}}
        client.get("/quote?ts_code=000001.SZ&ts_codes=600000.SH,%20%20").json()
    m.assert_called_with(["000001.SZ", "600000.SH"])


def test_macro_history_endpoint(monkeypatch) -> None:
    with patch("data_sync_service.api.query_routes.fetch_macro_daily") as m:
        m.return_value = [{"a": 1}]
        out = client.get("/macro/history?series_id=SPX&limit=10").json()
    assert out == {"seriesId": "SPX", "data": [{"a": 1}]}
    m.assert_called_once()


def test_index_signals_history_endpoint(monkeypatch) -> None:
    with patch("data_sync_service.api.query_routes.fetch_index_daily") as m:
        m.return_value = [{"o": 1}]
        out = client.get("/index/signals/history?ts_code=000001.SH").json()
    assert out == {"tsCode": "000001.SH", "data": [{"o": 1}]}


def test_index_basic_history_endpoint(monkeypatch) -> None:
    with patch("data_sync_service.api.query_routes.fetch_index_basic") as m:
        m.return_value = [{"x": 2}]
        out = client.get("/index/basic/history?ts_code=000001.SH").json()
    assert out == {"tsCode": "000001.SH", "data": [{"x": 2}]}


def test_market_bars_endpoint_ok_and_error(monkeypatch) -> None:
    with patch("data_sync_service.api.query_routes.get_market_bars") as m:
        m.return_value = {"symbol": "CN:000001"}
        assert client.get("/market/stocks/CN:000001/bars?days=60").json() == {"symbol": "CN:000001"}

    with patch("data_sync_service.api.query_routes.get_market_bars", side_effect=RuntimeError("bar crash")):
        resp = client.get("/market/stocks/CN:000001/bars?days=60")
    assert resp.status_code == 500
    assert resp.json()["detail"] == "bar crash"


def test_market_chips_endpoint_ok_and_http_exception(monkeypatch) -> None:
    with patch("data_sync_service.api.query_routes.get_market_chips") as m:
        m.return_value = {"chips": []}
        assert client.get("/market/stocks/CN:000001/chips").json() == {"chips": []}

    from fastapi import HTTPException

    with patch("data_sync_service.api.query_routes.get_market_chips", side_effect=HTTPException(status_code=404, detail="no")):
        assert client.get("/market/stocks/CN:000001/chips").status_code == 404

    with patch("data_sync_service.api.query_routes.get_market_chips", side_effect=RuntimeError("chip crash")):
        resp = client.get("/market/stocks/CN:000001/chips")
    assert resp.status_code == 500


def test_market_fund_flow_endpoint(monkeypatch) -> None:
    with patch("data_sync_service.api.query_routes.get_market_fund_flow") as m:
        m.return_value = {"fundFlow": []}
        assert client.get("/market/stocks/CN:000001/fund-flow").json() == {"fundFlow": []}

    from fastapi import HTTPException

    with patch(
        "data_sync_service.api.query_routes.get_market_fund_flow",
        side_effect=HTTPException(status_code=422, detail="bad"),
    ):
        assert client.get("/market/stocks/CN:000001/fund-flow").status_code == 422

    with patch("data_sync_service.api.query_routes.get_market_fund_flow", side_effect=RuntimeError("ff crash")):
        resp = client.get("/market/stocks/CN:000001/fund-flow")
    assert resp.status_code == 500


def test_market_stocks_endpoint(monkeypatch) -> None:
    with patch("data_sync_service.api.query_routes.fetch_market_stocks") as m:
        m.return_value = (42, [{"symbol": "CN:000001"}])
        out = client.get("/market/stocks?market=CN&q=000001&offset=10&limit=5&use_realtime=true").json()
    assert out == {"items": [{"symbol": "CN:000001"}], "total": 42, "offset": 10, "limit": 5}
    m.assert_called_once_with(market="CN", q="000001", offset=10, limit=5, use_realtime=True)


def test_search_stocks_empty_and_ok(monkeypatch) -> None:
    assert client.get("/search/stocks?q=%20%20").json() == {"items": []}
    with patch("data_sync_service.api.query_routes.fetch_market_stocks") as m:
        m.return_value = (1, [{"symbol": "CN:000001"}])
        out = client.get("/search/stocks?q=000001&market=CN&limit=8").json()
    assert out == {"items": [{"symbol": "CN:000001"}]}
    m.assert_called_once_with(market="CN", q="000001", offset=0, limit=8, use_realtime=False)


def test_watchlist_v5_alerts_endpoint(monkeypatch) -> None:
    with patch("data_sync_service.api.query_routes.compute_watchlist_v5_alerts") as m:
        m.return_value = [{"symbol": "CN:000001"}]
        out = client.post("/market/stocks/watchlist/v5-alerts", json={"items": [{"symbol": "CN:000001", "position_pct": 0.1}]}).json()
    assert out == [{"symbol": "CN:000001"}]
    m.assert_called_once_with([{"symbol": "CN:000001", "position_pct": 0.1}])


def test_watchlist_v5_plan_endpoint(monkeypatch) -> None:
    with patch("data_sync_service.api.query_routes.compute_watchlist_v5_plan") as m:
        m.return_value = {"plan": []}
        assert client.post("/market/stocks/watchlist/v5-plan", json={}).json() == {"plan": []}


def test_watchlist_momentum_alerts_endpoint(monkeypatch) -> None:
    with patch("data_sync_service.api.query_routes.compute_watchlist_momentum_alerts") as m:
        m.return_value = [{"x": 1}]
        out = client.post("/market/stocks/watchlist/momentum-alerts?realtime=true", json={"items": [{"symbol": "CN:1"}]}).json()
    assert out == [{"x": 1}]


def test_misc_status_endpoints(monkeypatch) -> None:
    import data_sync_service.api.query_routes as qr

    for path, fn, ret in [
        ("/stock-basic", "get_stock_basic_list", [{"a": 1}]),
        ("/stock-basic/status", "get_stock_basic_sync_status", {"success": True}),
        ("/daily", "get_daily_from_db", []),
        ("/index-daily", "fetch_index_daily", []),
        ("/daily/status", "get_daily_sync_status", {"ok": True}),
        ("/adj-factor/status", "get_adj_factor_sync_status", {"ok": True}),
        ("/close/status", "get_close_sync_status", {"ok": True}),
        ("/market/status", "get_market_status", {"total": 100}),
    ]:
        with patch(f"data_sync_service.api.query_routes.{fn}", return_value=ret) as m:
            assert client.get(path).json() == ret
            assert m.called


def test_healthz_degraded(monkeypatch) -> None:
    with patch("data_sync_service.api.query_routes.check_db", return_value=(False, "down")):
        out = client.get("/healthz").json()
    assert out == {"status": "degraded", "db": False, "db_error": "down"}


def test_trendok_endpoint(monkeypatch) -> None:
    with patch("data_sync_service.api.query_routes.compute_trendok_for_symbols") as m:
        m.return_value = [{"symbol": "CN:000001"}]
        assert client.get("/market/stocks/trendok?symbols=CN:000001&realtime=true").json() == [
            {"symbol": "CN:000001"}
        ]
    m.assert_called_once_with(["CN:000001"], True)


def test_macro_snapshot_endpoint(monkeypatch) -> None:
    with patch("data_sync_service.api.query_routes.build_macro_snapshot") as m:
        m.return_value = {"macro": []}
        assert client.get("/macro/snapshot").json() == {"macro": []}
