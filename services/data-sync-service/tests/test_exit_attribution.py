"""OPT-064 tests: exit attribution (forward return by close reason).

Pure logic tests mock the DB layer; API tests patch the service.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient  # type: ignore[import-not-found]

from data_sync_service.main import app  # type: ignore[import-not-found]

client = TestClient(app)


@pytest.fixture(autouse=True)
def _reset_settings_cache() -> None:
    from data_sync_service import config

    config.get_settings.cache_clear()
    yield
    config.get_settings.cache_clear()


def _closed_trade(symbol="CN:600001", reason="target_hit", close_date="2026-08-01", pnl=5.0) -> dict:
    return {
        "symbol": symbol,
        "entryDate": "2026-07-28",
        "side": "BUY",
        "status": "closed",
        "closeDate": close_date,
        "closePrice": 10.5,
        "pnlPct": pnl,
        "closeReason": reason,
        "market": "CN",
    }


def _bars(price_map: dict[str, float]) -> list[tuple[str, str, str, str, str, str]]:
    out = []
    for d in sorted(price_map):
        px = str(price_map[d])
        out.append((d, px, px, px, px, "1000"))
    return out


def test_attribution_empty_book() -> None:
    with patch(
        "data_sync_service.service.exit_attribution.list_paper_trades",
        return_value=[],
    ):
        from data_sync_service.service.exit_attribution import analyze_exit_attribution

        out = analyze_exit_attribution(days=5)
    assert out["closedCount"] == 0
    assert out["insufficient"] is True
    assert out["byReason"] == {}


def test_attribution_buckets_early_well_neutral() -> None:
    """Three closed trades, same symbol:
    - close A on 08-01, forward 5d = +3%  → exit_early
    - close B on 08-01, forward 5d = -2%  → exit_well
    - close C on 08-01, forward 5d = +0.5% → neutral
    """
    trades = [
        _closed_trade(reason="target_hit", close_date="2026-08-01"),
        _closed_trade(reason="stop_hit", close_date="2026-08-01"),
        _closed_trade(reason="max_hold", close_date="2026-08-01"),
    ]
    # entry 08-01 close 10.0 → 08-08 close 10.3 (+3%), etc. — but forward
    # is computed per trade via the same bars; we cheat by giving each trade
    # its own symbol so the bars differ.
    # 6 trading days so days=5 forward window exists; forward return = close
    # on day 6 vs close on close_date.
    bars = {
        "600001.SH": _bars(
            {"2026-08-01": 10.0, "2026-08-03": 10.1, "2026-08-04": 10.1, "2026-08-05": 10.2, "2026-08-06": 10.2, "2026-08-07": 10.3}
        ),  # +3.0%
        "600002.SH": _bars(
            {"2026-08-01": 10.0, "2026-08-03": 9.9, "2026-08-04": 9.9, "2026-08-05": 9.8, "2026-08-06": 9.8, "2026-08-07": 9.8}
        ),  # -2.0%
        "600003.SH": _bars(
            {"2026-08-01": 10.0, "2026-08-03": 10.0, "2026-08-04": 10.0, "2026-08-05": 10.05, "2026-08-06": 10.05, "2026-08-07": 10.05}
        ),  # +0.5%
    }
    trades[0]["symbol"] = "CN:600001"
    trades[1]["symbol"] = "CN:600002"
    trades[2]["symbol"] = "CN:600003"

    with patch(
        "data_sync_service.service.exit_attribution.list_paper_trades",
        return_value=trades,
    ), patch(
        "data_sync_service.service.exit_attribution.fetch_ohlcv_batch_between",
        return_value=bars,
    ):
        from data_sync_service.service.exit_attribution import analyze_exit_attribution

        out = analyze_exit_attribution(days=5)
    assert out["closedCount"] == 3
    assert out["withForwardCount"] == 3
    assert out["insufficient"] is True  # <10 sample → hint
    assert out["overall"]["earlyCount"] == 1
    assert out["overall"]["wellCount"] == 1
    assert out["overall"]["neutralCount"] == 1
    assert out["overall"]["avgFwdPct"] == pytest.approx(0.5)
    # byReason: each reason maps to its own bucket via its symbol.
    assert out["byReason"]["target_hit"]["earlyCount"] == 1
    assert out["byReason"]["stop_hit"]["wellCount"] == 1
    assert out["byReason"]["max_hold"]["neutralCount"] == 1


def test_attribution_insufficient_forward_data() -> None:
    """Close too recent to have N forward days → excluded, with count."""
    trades = [_closed_trade(close_date="2026-08-06")]  # only 1 forward day available
    bars = {"600001.SH": _bars({"2026-08-06": 10.0, "2026-08-07": 10.1})}
    with patch(
        "data_sync_service.service.exit_attribution.list_paper_trades",
        return_value=trades,
    ), patch(
        "data_sync_service.service.exit_attribution.fetch_ohlcv_batch_between",
        return_value=bars,
    ):
        from data_sync_service.service.exit_attribution import analyze_exit_attribution

        out = analyze_exit_attribution(days=5)
    assert out["closedCount"] == 1
    assert out["withForwardCount"] == 0
    assert out["excluded"] == 1
    assert out["overall"]["count"] == 0


def test_attribution_exposure_max_simultaneous() -> None:
    """Two trades closing on the same day count as 2 simultaneous positions."""
    trades = [
        _closed_trade(symbol="CN:600001", close_date="2026-08-01"),
        _closed_trade(symbol="CN:600002", close_date="2026-08-01"),
    ]
    bars = {
        "600001.SH": _bars({"2026-08-01": 10.0, "2026-08-03": 10.3}),
        "600002.SH": _bars({"2026-08-01": 10.0, "2026-08-03": 10.3}),
    }
    with patch(
        "data_sync_service.service.exit_attribution.list_paper_trades",
        return_value=trades,
    ), patch(
        "data_sync_service.service.exit_attribution.fetch_ohlcv_batch_between",
        return_value=bars,
    ):
        from data_sync_service.service.exit_attribution import analyze_exit_attribution

        out = analyze_exit_attribution(days=5)
    assert out["exposure"]["maxSimultaneous"] == 2
    assert out["exposure"]["singleStockWeightFloorPct"] == 50.0


def test_attribution_api_endpoint() -> None:
    with patch(
        "data_sync_service.service.exit_attribution.analyze_exit_attribution",
        return_value={
            "days": 5,
            "closedCount": 0,
            "withForwardCount": 0,
            "excluded": 0,
            "insufficient": True,
            "hint": None,
            "overall": {},
            "byReason": {},
            "exposure": {"maxSimultaneous": 0, "singleStockWeightFloorPct": None, "note": ""},
        },
    ):
        resp = client.get("/api/backtest/exit-attribution?days=5")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["insufficient"] is True
