from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import data_sync_service.service.market_regime as mr


def _series(days: int = 80) -> list[tuple[str, float, float]]:
    start = date(2026, 1, 1)
    return [
        ((start + timedelta(days=i)).isoformat(), 100.0 + i, 1_000_000.0 + i * 1_000.0)
        for i in range(days)
    ]


def _quote_item(code: str, price: float, pre_close: float, trade_time: str) -> dict[str, Any]:
    return {
        "ts_code": code,
        "price": str(price),
        "pre_close": str(pre_close),
        "volume": "1000000",
        "trade_time": trade_time,
    }


def test_hsi_quote_failure_does_not_block_cn_realtime(monkeypatch) -> None:
    mr.clear_index_signals_cache()
    series = _series()

    def fake_quotes(codes: list[str]) -> dict[str, Any]:
        if codes == ["000001.SH", "399006.SZ", "000905.SH"]:
            return {
                "ok": True,
                "items": [
                    _quote_item("000001.SH", 190.0, 179.0, "2026-03-21 14:30:00"),
                    _quote_item("399006.SZ", 290.0, 279.0, "2026-03-21 14:30:01"),
                    _quote_item("000905.SH", 390.0, 379.0, "2026-03-21 14:30:02"),
                ],
            }
        if codes == ["HSI", "HSTECH"]:
            return {"ok": False, "error": "list index out of range"}
        if codes == ["HSI"] or codes == ["HSTECH"]:
            return {"ok": False, "error": "list index out of range"}
        raise AssertionError(f"unexpected quote request: {codes}")

    monkeypatch.setattr(mr, "_is_shanghai_sync_window", lambda: True)
    monkeypatch.setattr(mr, "fetch_realtime_quotes", fake_quotes)
    monkeypatch.setattr(
        mr,
        "fetch_last_closes_vol_batch",
        lambda codes, days=80, as_of_date=None: {code: series for code in codes},
    )
    monkeypatch.setattr(mr, "fetch_macro_last_closes", lambda series_id, days=80, **kwargs: [(d, c) for d, c, _ in series])
    monkeypatch.setattr(mr, "fetch_hk_index_on_demand", lambda series_id: ({}, None))

    signals = mr.get_index_signals(include_breadth=False)
    by_code = {str(x["tsCode"]): x for x in signals}

    assert by_code["000001.SH"]["realtime"] is True
    assert by_code["000001.SH"]["source"] == "tushare.realtime_quote"
    assert by_code["000001.SH"]["close"] == 190.0
    assert by_code["399006.SZ"]["realtime"] is True
    assert by_code["399006.SZ"]["close"] == 290.0
    assert by_code["000905.SH"]["realtime"] is True
    assert by_code["000905.SH"]["close"] == 390.0
    assert by_code["HSI"]["realtime"] is False
    assert by_code["HSI"]["quoteError"] == "list index out of range"


def test_cn_batch_failure_falls_back_to_single_symbol_quotes(monkeypatch) -> None:
    mr.clear_index_signals_cache()
    series = _series()

    def fake_quotes(codes: list[str]) -> dict[str, Any]:
        if codes == ["000001.SH", "399006.SZ", "000905.SH"]:
            return {"ok": False, "error": "batch failure"}
        if codes == ["000001.SH"]:
            return {"ok": True, "items": [_quote_item("000001.SH", 190.0, 179.0, "2026-03-21 14:30:00")]}
        if codes == ["399006.SZ"]:
            return {"ok": False, "error": "symbol failure"}
        if codes == ["000905.SH"]:
            return {"ok": False, "error": "symbol failure"}
        if codes == ["HSI", "HSTECH"]:
            return {"ok": False, "error": "unsupported"}
        if codes == ["HSI"] or codes == ["HSTECH"]:
            return {"ok": False, "error": "unsupported"}
        raise AssertionError(f"unexpected quote request: {codes}")

    monkeypatch.setattr(mr, "_is_shanghai_sync_window", lambda: True)
    monkeypatch.setattr(mr, "fetch_realtime_quotes", fake_quotes)
    monkeypatch.setattr(
        mr,
        "fetch_last_closes_vol_batch",
        lambda codes, days=80, as_of_date=None: {code: series for code in codes},
    )
    monkeypatch.setattr(mr, "fetch_macro_last_closes", lambda series_id, days=80, **kwargs: [(d, c) for d, c, _ in series])
    monkeypatch.setattr(mr, "fetch_hk_index_on_demand", lambda series_id: ({}, None))

    signals = mr.get_index_signals(include_breadth=False)
    by_code = {str(x["tsCode"]): x for x in signals}

    assert by_code["000001.SH"]["realtime"] is True
    assert by_code["000001.SH"]["close"] == 190.0
    assert by_code["399006.SZ"]["realtime"] is False
    assert by_code["399006.SZ"]["source"] == "db.index_daily"
    assert by_code["399006.SZ"]["quoteError"] == "symbol failure"

