from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient

import main


def test_infer_intraday_slot() -> None:
    tz = main.ZoneInfo("Asia/Shanghai")
    t1 = datetime(2026, 1, 2, 9, 40, tzinfo=tz)
    assert main._infer_intraday_slot(t1) == "0930_1030"
    t2 = datetime(2026, 1, 2, 10, 40, tzinfo=tz)
    assert main._infer_intraday_slot(t2) == "1030_1130"
    t3 = datetime(2026, 1, 2, 13, 10, tzinfo=tz)
    assert main._infer_intraday_slot(t3) == "1300_1400"
    t4 = datetime(2026, 1, 2, 14, 20, tzinfo=tz)
    assert main._infer_intraday_slot(t4) == "1400_1445"


def test_intraday_rank_generate_and_read(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    client = TestClient(main.app)
    acc = client.post("/broker/accounts", json={"broker": "pingan", "title": "Main"}).json()
    account_id = acc["id"]

    # Mock spot and minute bars.
    def fake_spot():
        return [
            main.StockRow(
                symbol="CN:000001",
                market="CN",
                ticker="000001",
                name="Alpha",
                currency="CNY",
                quote={"change_pct": "3.2", "vol_ratio": "4.8", "turnover": "100000000"},
            ),
            main.StockRow(
                symbol="CN:000002",
                market="CN",
                ticker="000002",
                name="Beta",
                currency="CNY",
                quote={"change_pct": "1.2", "vol_ratio": "2.0", "turnover": "90000000"},
            ),
        ]

    def fake_minute(ticker: str, *, trade_date: str, interval: str = "1"):
        # 20 minutes bars, simple up trend.
        out = []
        for i in range(20):
            out.append(
                {
                    "ts": f"{trade_date} 09:{30+i:02d}",
                    "open": 10 + i * 0.01,
                    "high": 10 + i * 0.02,
                    "low": 10 + i * 0.01,
                    "close": 10 + i * 0.02,
                    "volume": 1000 + i * 10,
                    "amount": (10 + i * 0.02) * (1000 + i * 10),
                }
            )
        return out

    monkeypatch.setattr(main, "fetch_cn_a_spot", fake_spot)
    monkeypatch.setattr(main, "fetch_cn_a_minute_bars", fake_minute)

    as_of_ts = datetime(2026, 1, 2, 1, 40, tzinfo=UTC).isoformat()
    resp = client.post(
        "/rank/cn/intraday/generate",
        json={"accountId": account_id, "asOfTs": as_of_ts, "force": True, "limit": 30, "universeVersion": "v0"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["accountId"] == account_id
    assert data["tradeDate"] == "2026-01-02"
    assert data["slot"] in ("0930_1030", "1030_1130", "1300_1400", "1400_1445")
    assert isinstance(data.get("items"), list)
    assert len(data["items"]) >= 1
    assert data["items"][0]["ticker"] in ("000001", "000002")

    # Read latest snapshot.
    resp2 = client.get(f"/rank/cn/intraday?accountId={account_id}&limit=30&universeVersion=v0")
    assert resp2.status_code == 200
    got = resp2.json()
    assert got["accountId"] == account_id

    # Observations API.
    resp3 = client.get("/rank/cn/intraday/observations?date=2026-01-02")
    assert resp3.status_code == 200
    obs = resp3.json()
    assert obs["tradeDate"] == "2026-01-02"
    assert isinstance(obs.get("items"), list)


def test_intraday_rank_prune_keeps_last_10_days(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    client = TestClient(main.app)
    acc = client.post("/broker/accounts", json={"broker": "pingan", "title": "Main"}).json()
    account_id = acc["id"]

    # Seed 12 trade dates (direct DB upsert).
    base = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    for i in range(12):
        dt = base + timedelta(days=i)
        trade_date = dt.date().isoformat()
        out = {
            "asOfTs": dt.isoformat(),
            "tradeDate": trade_date,
            "slot": "0930_1030",
            "accountId": account_id,
            "universeVersion": "v0",
            "items": [],
            "observations": [],
        }
        main._upsert_cn_intraday_rank_snapshot(
            account_id=account_id,
            as_of_ts=dt.isoformat(),
            trade_date=trade_date,
            slot="0930_1030",
            universe_version="v0",
            ts=dt.isoformat(),
            output=out,
        )

    main._prune_cn_intraday_rank_snapshots(account_id=account_id, keep_days=10)
    # Oldest 2 days should be pruned.
    with main._connect() as conn:
        rows = conn.execute(
            "SELECT DISTINCT trade_date FROM cn_intraday_rank_snapshots WHERE account_id = ? ORDER BY trade_date ASC",
            (account_id,),
        ).fetchall()
        dates = [str(r[0]) for r in rows]
    assert len(dates) == 10
    assert "2026-01-01" not in dates
    assert "2026-01-02" not in dates


