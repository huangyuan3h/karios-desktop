from unittest.mock import patch

import data_sync_service.service.market_regime as mr


def test_compute_index_signals_uses_single_batch_read(monkeypatch) -> None:
    calls: list[tuple] = []

    def _batch(ts_codes, *, days, as_of_date=None):
        calls.append((list(ts_codes), days, as_of_date))
        return {
            code: [(f"2026-06-{i:02d}", 100.0 + i, 1.0) for i in range(1, 25)] for code in ts_codes
        }

    monkeypatch.setattr(mr, "fetch_last_closes_vol_batch", _batch)
    monkeypatch.setattr(mr, "_is_shanghai_sync_window", lambda: False)
    monkeypatch.setattr(mr, "INDEX_SIGNALS", [{"ts_code": "000001.SH", "name": "上证指数"}])
    monkeypatch.setattr(mr, "HK_INDEX_SIGNALS", [])
    monkeypatch.setattr(mr, "_get_breadth_above_ma20_ratio", lambda **_: {"ratio": 0.5})
    monkeypatch.setattr(
        mr,
        "_get_market_liquidity_and_mainline",
        lambda **_: {
            "total_turnover_cny": 0.0,
            "max_industry_inflow": 0.0,
            "turnover_above_1_5T": False,
            "mainline_inflow_above_5B": False,
        },
    )

    out = mr._compute_index_signals(as_of_date="2026-06-18", include_breadth=True)
    assert len(calls) == 1
    assert out
