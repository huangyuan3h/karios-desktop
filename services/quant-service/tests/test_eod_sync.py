from __future__ import annotations

import time
from datetime import date, timedelta

from fastapi.testclient import TestClient  # type: ignore[import-not-found]

import main  # type: ignore[import-not-found]


def _today() -> str:
    return main._today_cn_date_str()


def _mk_bars(days: int) -> list[main.BarRow]:
    n = max(10, int(days))
    end = date.fromisoformat(_today())
    out: list[main.BarRow] = []
    for i in range(n):
        d = (end - timedelta(days=(n - 1 - i))).isoformat()
        out.append(
            main.BarRow(
                date=d,
                open="10.0",
                high="11.0",
                low="9.5",
                close="10.5",
                volume="1000000",
                amount="10000000",
            )
        )
    return out


def test_eod_sync_trigger_runs_in_background(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.duckdb"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))

    # Seed market universe quickly (avoid network).
    monkeypatch.setattr(
        main,
        "fetch_cn_a_spot",
        lambda: [
            main.StockRow(
                symbol="CN:000001",
                market="CN",
                ticker="000001",
                name="Ping An Bank",
                currency="CNY",
                quote={"price": "10.00", "change_pct": "1.23"},
            )
        ],
    )
    monkeypatch.setattr(main, "fetch_hk_spot", lambda: [])

    # Patch providers used by the EOD sync pipeline.
    monkeypatch.setattr(main, "fetch_cn_a_daily_bars", lambda _ticker, days=120: _mk_bars(days))
    monkeypatch.setattr(
        main,
        "fetch_cn_a_chip_summary",
        lambda _ticker, days=60: [
            {
                "date": _today(),
                "profitRatio": "0.5",
                "avgCost": "10.0",
                "cost90Low": "9.0",
                "cost90High": "11.0",
                "cost90Conc": "0.2",
                "cost70Low": "9.5",
                "cost70High": "10.5",
                "cost70Conc": "0.1",
            }
        ],
    )
    monkeypatch.setattr(
        main,
        "fetch_cn_a_fund_flow",
        lambda _ticker, days=60: [
            {
                "date": _today(),
                "close": "10.0",
                "changePct": "1.0",
                "mainNetAmount": "100",
                "mainNetRatio": "2.0",
                "superNetAmount": "40",
                "superNetRatio": "1.0",
                "largeNetAmount": "30",
                "largeNetRatio": "0.8",
                "mediumNetAmount": "20",
                "mediumNetRatio": "0.5",
                "smallNetAmount": "10",
                "smallNetRatio": "0.2",
            }
        ],
    )
    monkeypatch.setattr(
        main,
        "fetch_cn_industry_fund_flow_eod",
        lambda _as_of: [
            {
                "date": _today(),
                "industry_code": "BK_A",
                "industry_name": "Power",
                "net_inflow": 5_000_000_000.0,
                "raw": {"x": 1},
            }
        ],
    )
    monkeypatch.setattr(main, "fetch_cn_industry_fund_flow_hist", lambda _name, days=10: [])

    client = TestClient(main.app)

    # Seed market cache so EOD target fallback has something to use.
    r0 = client.post("/market/sync")
    assert r0.status_code == 200
    assert r0.json()["ok"] is True

    # Trigger EOD sync.
    resp = client.post("/sync/trigger", json={"force": True})
    assert resp.status_code == 200
    out = resp.json()
    assert out["ok"] is True
    assert isinstance(out.get("runId"), str) and out["runId"]
    assert out["status"] in ("queued", "ok", "partial", "failed", "skipped")

    # Poll status until finished.
    for _ in range(120):
        st = client.get("/sync/status")
        assert st.status_code == 200
        last = st.json().get("lastRun")
        if last and last.get("id") == out["runId"]:
            if last.get("status") not in ("queued", "running"):
                break
        time.sleep(0.05)
    else:
        raise AssertionError("Sync did not finish in time.")

    st2 = client.get("/sync/status").json()
    last2 = st2.get("lastRun") or {}
    assert last2.get("id") == out["runId"]
    assert last2.get("kind") == "eod"
    assert last2.get("status") in ("ok", "partial", "failed", "skipped")
    steps = last2.get("steps") or []
    names = {x.get("step") for x in steps if isinstance(x, dict)}
    assert {"quotes", "bars", "chips", "fund_flow", "industry_fund_flow"}.issubset(names)

