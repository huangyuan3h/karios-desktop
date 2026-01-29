from fastapi.testclient import TestClient

import main


def test_sentiment_sync_does_not_overwrite_on_upstream_failure(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.sqlite3"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    # Seed a previous successful day.
    main._upsert_cn_sentiment_daily(
        date="2026-01-23",
        as_of_date="2026-01-23",
        up=100,
        down=200,
        flat=10,
        up_down_ratio=0.5,
        market_turnover_cny=1.2e12,
        market_volume=123.0,
        premium=1.0,
        failed_rate=25.0,
        risk_mode="hot",
        rules=["fixture"],
        updated_at="2026-01-23T00:00:00Z",
        raw={},
    )

    # Make upstream fail (e.g. RemoteDisconnected / captcha HTML).
    monkeypatch.setattr(main, "fetch_cn_market_breadth_eod", lambda _as_of: (_ for _ in ()).throw(RuntimeError("aborted")))

    # Seed a bad placeholder row for today (what older logic used to write).
    main._upsert_cn_sentiment_daily(
        date="2026-01-28",
        as_of_date="2026-01-28",
        up=0,
        down=0,
        flat=0,
        up_down_ratio=0.0,
        market_turnover_cny=0.0,
        market_volume=0.0,
        premium=0.0,
        failed_rate=0.0,
        risk_mode="caution",
        rules=["breadth_failed: aborted"],
        updated_at="2026-01-28T00:00:00Z",
        raw={},
    )

    client = TestClient(main.app)
    resp = client.post("/market/cn/sentiment/sync", json={"date": "2026-01-28", "force": True})
    assert resp.status_code == 200
    data = resp.json()
    assert data["asOfDate"] == "2026-01-28"
    # Should return cached latest (2026-01-23), not insert a zero row for 2026-01-28.
    assert data["items"][0]["date"] == "2026-01-23"
    assert data["items"][0]["upCount"] == 100

    # Placeholder row for 2026-01-28 should be cleaned up.
    resp2 = client.get("/market/cn/sentiment?days=10&asOfDate=2026-01-28")
    assert resp2.status_code == 200
    dates = [x["date"] for x in resp2.json()["items"]]
    assert "2026-01-28" not in dates

