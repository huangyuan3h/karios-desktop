from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo


def _bars_with_today(
    *,
    today: str,
    prev_close: float,
    today_close: float,
    prev_high: float,
    today_low: float,
    days: int = 40,
) -> list[tuple[str, str, str, str, str, str]]:
    out: list[tuple[str, str, str, str, str, str]] = []
    base_price = prev_close
    for i in range(days - 2):
        date = f"2025-03-{i + 1:02d}"
        out.append(
            (
                date,
                f"{base_price:.3f}",
                f"{base_price:.3f}",
                f"{base_price:.3f}",
                f"{base_price:.3f}",
                "1000.000",
            )
        )
    yday = "2025-03-29"
    out.append(
        (
            yday,
            f"{prev_close:.3f}",
            f"{prev_high:.3f}",
            f"{prev_close * 0.99:.3f}",
            f"{prev_close:.3f}",
            "1000.000",
        )
    )
    out.append(
        (
            today,
            f"{prev_close:.3f}",
            f"{today_close:.3f}",
            f"{today_low:.3f}",
            f"{today_close:.3f}",
            "1000.000",
        )
    )
    return out


def test_compute_day_risk_metrics_normal() -> None:
    import data_sync_service.service.trendok as trendok  # type: ignore[import-not-found]

    today = datetime.now(tz=ZoneInfo("Asia/Shanghai")).date().isoformat()
    bars = _bars_with_today(
        today=today,
        prev_close=100.0,
        today_close=105.0,
        prev_high=101.0,
        today_low=100.5,
    )
    dates = [b[0] for b in bars]
    highs = [float(b[2]) for b in bars]
    lows = [float(b[3]) for b in bars]
    closes = [float(b[4]) for b in bars]

    metrics = trendok._compute_day_risk_metrics(dates, highs, lows, closes, today=today)  # type: ignore[attr-defined]
    assert metrics["intradayChgPct"] == 5.0
    assert metrics["gapUp"] is False


def test_compute_day_risk_metrics_stale_bar_still_computes() -> None:
    import data_sync_service.service.trendok as trendok  # type: ignore[import-not-found]

    bars = _bars_with_today(
        today="2026-05-28",
        prev_close=100.0,
        today_close=107.0,
        prev_high=101.0,
        today_low=106.0,
    )
    dates = [b[0] for b in bars]
    highs = [float(b[2]) for b in bars]
    lows = [float(b[3]) for b in bars]
    closes = [float(b[4]) for b in bars]

    metrics = trendok._compute_day_risk_metrics(  # type: ignore[attr-defined]
        dates, highs, lows, closes, today="2026-05-29"
    )
    assert metrics["intradayChgPct"] == 7.0
    assert metrics["gapUp"] is True
    assert metrics["riskMetricsLive"] is False


def test_stale_surge_does_not_block_buy() -> None:
    import data_sync_service.service.trendok as trendok  # type: ignore[import-not-found]

    today = datetime.now(tz=ZoneInfo("Asia/Shanghai")).date().isoformat()
    bars = _bars_with_today(
        today="2026-05-28",
        prev_close=100.0,
        today_close=107.0,
        prev_high=101.0,
        today_low=106.0,
    )
    if today == "2026-05-28":
        return
    res = trendok._trendok_one(  # type: ignore[attr-defined]
        symbol="CN:000001",
        name="Test",
        industry=None,
        bars=bars,
        flow_ctx=None,
        market_regime="Strong",
    )
    assert res.get("intradayChgPct") == 7.0
    assert res.get("riskMetricsLive") is False
    assert res.get("buyAction") != "avoid"


def test_intraday_surge_blocks_buy() -> None:
    import data_sync_service.service.trendok as trendok  # type: ignore[import-not-found]

    today = datetime.now(tz=ZoneInfo("Asia/Shanghai")).date().isoformat()
    bars = _bars_with_today(
        today=today,
        prev_close=100.0,
        today_close=107.0,
        prev_high=101.0,
        today_low=106.0,
    )
    res = trendok._trendok_one(  # type: ignore[attr-defined]
        symbol="CN:000001",
        name="Test",
        industry=None,
        bars=bars,
        flow_ctx=None,
        market_regime="Strong",
    )
    assert res.get("intradayChgPct") == 7.0
    assert res.get("buyAction") == "avoid"
    assert res.get("buyChecks", {}).get("blocked_intraday_surge") is True
    assert any(a.get("code") == "intraday_surge" for a in (res.get("riskAlerts") or []))


def test_gap_up_weak_blocks_momentum_buy(monkeypatch) -> None:
    import data_sync_service.service.trendok as trendok  # type: ignore[import-not-found]

    today = datetime.now(tz=ZoneInfo("Asia/Shanghai")).date().isoformat()
    monkeypatch.setattr(trendok, "_shanghai_today_iso", lambda: today)
    bars = _bars_with_today(
        today=today,
        prev_close=100.0,
        today_close=103.0,
        prev_high=100.0,
        today_low=101.5,
    )

    res = trendok._trendok_one(  # type: ignore[attr-defined]
        symbol="CN:000001",
        name="Test",
        industry=None,
        bars=bars,
        flow_ctx=None,
        market_regime="Weak",
    )
    assert res.get("gapUp") is True
    checks = res.get("buyChecks") or {}
    assert checks.get("blocked_gap_up_weak_market") is True
    assert any(a.get("code") == "gap_up_weak_market" for a in (res.get("riskAlerts") or []))


def test_gap_up_strong_does_not_force_avoid() -> None:
    import data_sync_service.service.trendok as trendok  # type: ignore[import-not-found]

    today = datetime.now(tz=ZoneInfo("Asia/Shanghai")).date().isoformat()
    bars = _bars_with_today(
        today=today,
        prev_close=100.0,
        today_close=103.0,
        prev_high=100.0,
        today_low=101.5,
    )
    res = trendok._trendok_one(  # type: ignore[attr-defined]
        symbol="CN:000001",
        name="Test",
        industry=None,
        bars=bars,
        flow_ctx=None,
        market_regime="Strong",
    )
    assert res.get("gapUp") is True
    assert not (res.get("buyChecks") or {}).get("blocked_gap_up_weak_market")
    assert not any(a.get("code") == "gap_up_weak_market" for a in (res.get("riskAlerts") or []))


def test_inst_retail_chase_blocks_buy(monkeypatch) -> None:
    import data_sync_service.service.trendok as trendok  # type: ignore[import-not-found]

    today = datetime.now(tz=ZoneInfo("Asia/Shanghai")).date().isoformat()
    monkeypatch.setattr(trendok, "_shanghai_today_iso", lambda: today)
    bars = _bars_with_today(
        today=today,
        prev_close=100.0,
        today_close=107.0,
        prev_high=101.0,
        today_low=106.0,
    )
    inst_summary = {
        "trade_date": today,
        "on_board": True,
        "inst_net_buy_yi": -1.5,
        "seat_label": "机构净卖/拉萨主买",
        "lhasa_dominant": True,
    }
    res = trendok._trendok_one(  # type: ignore[attr-defined]
        symbol="CN:300308",
        name="Test",
        industry=None,
        bars=bars,
        flow_ctx=None,
        market_regime="Strong",
        inst_summary=inst_summary,
    )
    assert res.get("instFlow", {}).get("display") == "-1.5亿 (机构净卖/拉萨主买)"
    assert res.get("buyAction") == "avoid"
    assert (res.get("buyChecks") or {}).get("blocked_inst_retail_chase") is True
    assert any(a.get("code") == "inst_retail_chase" for a in (res.get("riskAlerts") or []))
    if res.get("score") is not None:
        assert float(res["score"]) <= 60.0
