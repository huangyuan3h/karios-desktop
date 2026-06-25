from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo


def _flat_bars(*, days: int = 65, start_price: float = 100.0, end_price: float | None = None) -> list[tuple[str, str, str, str, str, str]]:
    out: list[tuple[str, str, str, str, str, str]] = []
    final_price = end_price if end_price is not None else start_price
    for i in range(days):
        if days == 1:
            price = final_price
        else:
            price = start_price + (final_price - start_price) * (i / (days - 1))
        date = f"2025-01-{i + 1:02d}" if i < 31 else f"2025-02-{i - 30:02d}"
        ps = f"{price:.3f}"
        out.append((date, ps, ps, ps, ps, "1000.000"))
    return out


def test_rs_leader_flag_when_outperforming_in_weak_market(monkeypatch) -> None:
    import data_sync_service.service.trendok as trendok  # type: ignore[import-not-found]

    today = datetime.now(tz=ZoneInfo("Asia/Shanghai")).date().isoformat()
    bars = _flat_bars(days=65, start_price=100.0, end_price=125.0)
    bars[-1] = (today, "124.000", "126.000", "123.000", "125.000", "1000.000")
    bars[-2] = (bars[-2][0], "118.000", "119.000", "117.000", "118.000", "1000.000")

    res = trendok._trendok_one(  # type: ignore[attr-defined]
        symbol="CN:600519",
        name="Test",
        industry=None,
        bars=bars,
        flow_ctx=None,
        market_regime="Weak",
        index_20d_ret=-5.0,
        index_ema20_down=True,
    )

    assert res.get("rs") is not None
    assert float(res["rs"]) > 10.0
    assert res.get("checks", {}).get("rs_leader") is True
    assert any(a.get("code") == "rs_leader" for a in (res.get("riskAlerts") or []))
    assert res.get("buyAction") != "avoid"


def test_intraday_distribution_forces_avoid(monkeypatch) -> None:
    import data_sync_service.service.trendok as trendok  # type: ignore[import-not-found]

    today = datetime.now(tz=ZoneInfo("Asia/Shanghai")).date().isoformat()
    monkeypatch.setattr(trendok, "_shanghai_today_iso", lambda: today)
    bars = _flat_bars(days=65, start_price=100.0, end_price=110.0)
    bars[-2] = (bars[-2][0], "100.000", "101.000", "99.500", "100.000", "1000.000")
    bars[-1] = (today, "100.500", "103.500", "100.000", "103.000", "1000.000")

    res = trendok._trendok_one(  # type: ignore[attr-defined]
        symbol="CN:600519",
        name="Test",
        industry=None,
        bars=bars,
        flow_ctx=None,
        market_regime="Strong",
        rt_vwap=105.0,
    )

    assert res.get("intradayChgPct") == 3.0
    assert res.get("checks", {}).get("intraday_distribution") is True
    assert res.get("buyAction") == "avoid"
    assert any(a.get("code") == "intraday_distribution" for a in (res.get("riskAlerts") or []))
    assert res.get("values", {}).get("rtVwap") == 105.0
