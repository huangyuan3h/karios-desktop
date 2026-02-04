from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

import main


def _seed_bars(tmp_path, symbols: list[str]) -> None:
    """
    Seed minimal daily bars for symbols so 3D/5D returns and decay signals can be computed offline.
    """
    with main._connect() as conn:
        ts = main.now_iso()
        base = datetime(2026, 1, 1, tzinfo=UTC).date()
        for sym in symbols:
            for i in range(10):
                d = (base + timedelta(days=i)).isoformat()
                # Simple uptrend.
                close = 10.0 + i * 0.2
                conn.execute(
                    """
                    INSERT INTO market_bars(symbol, date, open, high, low, close, volume, amount, updated_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (sym, d, "10", "10", "10", str(close), "1000", str(2e8), ts),
                )
        conn.commit()


def test_mainline_generate_and_read(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    client = TestClient(main.app)
    acc = client.post("/broker/accounts", json={"broker": "pingan", "title": "Main"}).json()
    account_id = acc["id"]

    _seed_bars(tmp_path, ["CN:000001", "CN:000002", "CN:000003"])

    # Mock spot + boards + limitups + membership.
    def fake_spot():
        return [
            main.StockRow(
                symbol="CN:000001",
                market="CN",
                ticker="000001",
                name="Alpha",
                currency="CNY",
                quote={"change_pct": "9.9", "vol_ratio": "5.0", "turnover": "120000000"},
            ),
            main.StockRow(
                symbol="CN:000002",
                market="CN",
                ticker="000002",
                name="Beta",
                currency="CNY",
                quote={"change_pct": "7.2", "vol_ratio": "3.2", "turnover": "90000000"},
            ),
            main.StockRow(
                symbol="CN:000003",
                market="CN",
                ticker="000003",
                name="Gamma",
                currency="CNY",
                quote={"change_pct": "5.3", "vol_ratio": "2.2", "turnover": "80000000"},
            ),
        ]

    monkeypatch.setattr(main, "fetch_cn_a_spot", fake_spot)
    monkeypatch.setattr(main, "fetch_cn_industry_boards_spot", lambda: [{"name": "CommercialSpace", "change_pct": "5.0", "turnover": "1e9"}])
    monkeypatch.setattr(main, "fetch_cn_concept_boards_spot", lambda: [{"name": "SpaceTheme", "change_pct": "6.0", "turnover": "1e9"}])
    monkeypatch.setattr(
        main,
        "fetch_cn_limitup_pool",
        lambda _d: [{"ticker": "000001"}, {"ticker": "000002"}, {"ticker": "000003"}],
    )
    monkeypatch.setattr(main, "fetch_cn_industry_members", lambda _name: ["000001", "000002", "000003"])
    # Make concept theme weak so Top1 is clearly separated (avoid "rotation" no-mainline rule).
    monkeypatch.setattr(main, "fetch_cn_concept_members", lambda _name: [])
    monkeypatch.setattr(main, "_market_cn_industry_fund_flow_top_by_date", lambda **_k: {"topByDate": []})

    def fake_ai(payload):
        themes = payload.get("themes") if isinstance(payload, dict) else None
        arr = themes if isinstance(themes, list) else []
        out = []
        for t in arr:
            out.append(
                {
                    "kind": t.get("kind"),
                    "name": t.get("name"),
                    "logicScore": 92,
                    "logicGrade": "S",
                    "logicSummary": "Policy + industry trend align; catalysts may sustain multi-day momentum.",
                }
            )
        return {"date": payload.get("date", ""), "themes": out, "model": "test"}

    monkeypatch.setattr(main, "_ai_mainline_explain", lambda *, payload: fake_ai(payload))

    as_of_ts = "2026-01-10T01:00:00+00:00"
    resp = client.post(
        "/leader/mainline/generate",
        json={"accountId": account_id, "asOfTs": as_of_ts, "force": True, "topK": 3, "universeVersion": "v0"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["accountId"] == account_id
    assert data["tradeDate"] == "2026-01-10"
    assert data["selected"] is not None
    assert len(data["themesTopK"]) >= 1

    # Read back.
    resp2 = client.get(f"/leader/mainline?accountId={account_id}&tradeDate=2026-01-10&universeVersion=v0")
    assert resp2.status_code == 200
    got = resp2.json()
    assert got["tradeDate"] == "2026-01-10"


def test_mainline_no_clear_mainline(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    client = TestClient(main.app)
    acc = client.post("/broker/accounts", json={"broker": "pingan", "title": "Main"}).json()
    account_id = acc["id"]

    _seed_bars(tmp_path, ["CN:000001", "CN:000002", "CN:000003"])

    monkeypatch.setattr(main, "fetch_cn_a_spot", lambda: [])
    monkeypatch.setattr(main, "fetch_cn_industry_boards_spot", lambda: [{"name": "X", "change_pct": "1.0", "turnover": "1"}])
    monkeypatch.setattr(main, "fetch_cn_concept_boards_spot", lambda: [{"name": "Y", "change_pct": "1.0", "turnover": "1"}])
    monkeypatch.setattr(main, "fetch_cn_limitup_pool", lambda _d: [])
    monkeypatch.setattr(main, "fetch_cn_industry_members", lambda _name: ["000001"])
    monkeypatch.setattr(main, "fetch_cn_concept_members", lambda _name: ["000002"])
    monkeypatch.setattr(main, "_market_cn_industry_fund_flow_top_by_date", lambda **_k: {"topByDate": []})

    def fake_ai(payload):
        themes = payload.get("themes") if isinstance(payload, dict) else None
        arr = themes if isinstance(themes, list) else []
        out = []
        for t in arr:
            out.append({"kind": t.get("kind"), "name": t.get("name"), "logicScore": 40, "logicGrade": "B"})
        return {"date": payload.get("date", ""), "themes": out, "model": "test"}

    monkeypatch.setattr(main, "_ai_mainline_explain", lambda *, payload: fake_ai(payload))

    resp = client.post(
        "/leader/mainline/generate",
        json={"accountId": account_id, "asOfTs": "2026-01-10T01:00:00+00:00", "force": True, "topK": 3, "universeVersion": "v0"},
    )
    assert resp.status_code == 200
    data = resp.json()
    # Should still return Top1 as a "best theme", but mark it as not clear (rotation/multi-line).
    assert data["selected"] is not None
    assert isinstance(data.get("debug"), dict)
    assert (data.get("debug") or {}).get("selectedClear") is False


def test_leader_daily_uses_mainline_candidate_pool(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    client = TestClient(main.app)
    client.post("/broker/accounts", json={"broker": "pingan", "title": "Main"}).json()

    _seed_bars(tmp_path, ["CN:000001", "CN:000002", "CN:000003"])

    # Make TV empty so mainline pool dominates.
    monkeypatch.setattr(main, "_list_enabled_tv_screeners", lambda limit=6: [])
    monkeypatch.setattr(main, "_latest_tv_snapshot_for_screener", lambda _sid: None)

    monkeypatch.setattr(main, "fetch_cn_a_spot", lambda: [main.StockRow(symbol="CN:000001", market="CN", ticker="000001", name="Alpha", currency="CNY", quote={"change_pct": "9.9", "vol_ratio": "5.0", "turnover": "120000000"})])
    monkeypatch.setattr(main, "fetch_cn_industry_boards_spot", lambda: [{"name": "CommercialSpace", "change_pct": "5.0", "turnover": "1e9"}])
    monkeypatch.setattr(main, "fetch_cn_concept_boards_spot", lambda: [])
    monkeypatch.setattr(main, "fetch_cn_limitup_pool", lambda _d: [{"ticker": "000001"}, {"ticker": "000002"}, {"ticker": "000003"}])
    monkeypatch.setattr(main, "fetch_cn_industry_members", lambda _name: ["000001", "000002", "000003"])
    monkeypatch.setattr(main, "_market_cn_industry_fund_flow_top_by_date", lambda **_k: {"topByDate": []})

    monkeypatch.setattr(
        main,
        "_ai_mainline_explain",
        lambda *, payload: {
            "date": payload.get("date", ""),
            "themes": [{"kind": "industry", "name": "CommercialSpace", "logicScore": 95, "logicGrade": "S"}],
            "model": "test",
        },
    )

    # Avoid expensive per-stock fetches.
    class _Bars:
        def __init__(self):
            self.bars = []

    class _Items:
        def __init__(self):
            self.items = []

    monkeypatch.setattr(main, "market_stock_bars", lambda *_a, **_k: _Bars())
    monkeypatch.setattr(main, "market_stock_chips", lambda *_a, **_k: _Items())
    monkeypatch.setattr(main, "market_stock_fund_flow", lambda *_a, **_k: _Items())
    monkeypatch.setattr(main, "_refresh_leader_live_scores", lambda **_k: None)

    # AI leader picks one inside pool and one outside; outside should be filtered out.
    monkeypatch.setattr(
        main,
        "_ai_leader_daily",
        lambda *, payload: {"date": payload.get("date", ""), "leaders": [{"symbol": "CN:999999"}, {"symbol": "CN:000001", "market": "CN", "ticker": "000001", "name": "Alpha", "score": 88, "reason": "test", "whyBullets": ["a", "b", "c"], "expectedDurationDays": 3, "buyZone": {"low": 1, "high": 2}, "triggers": [{"kind": "breakout", "condition": "x"}], "invalidation": "y", "targetPrice": {"primary": 3}, "probability": 3, "risks": ["r1", "r2"], "riskPoints": []}]} ,
    )

    resp = client.post("/leader/daily", json={"force": True, "useMainline": True, "maxCandidates": 50})
    assert resp.status_code == 200
    data = resp.json()
    tickers = [x["ticker"] for x in data.get("leaders") or []]
    assert "000001" in tickers
    assert "999999" not in tickers

