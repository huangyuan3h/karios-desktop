"""Coverage for pure helpers (no DB, no network): date parsers, factor math,
allocation week arithmetic."""

from __future__ import annotations

import numpy as np


def test_cn_date_helpers():
    from data_sync_service.db import (
        cn_financial,
        cn_hk_hold,
        cn_holder,
        cn_margin_detail,
        cn_moneyflow,
    )

    for mod in (cn_holder, cn_hk_hold, cn_margin_detail, cn_moneyflow, cn_financial):
        assert mod._date(None) is None
        assert mod._date("") is None
        assert mod._date("20260805") == "2026-08-05"
        assert mod._date("2026-08-05") == "2026-08-05"


def test_forecast_iso_date():
    from data_sync_service.db.stock_forecast import _iso_date

    assert _iso_date("20260805") == "2026-08-05"
    assert _iso_date("2026-08-05") == "2026-08-05"


def test_factor_signal_math():
    from data_sync_service.service.factor_signals_service import _probability, _rollmean

    assert _probability(0.60, 1.5) == 0.922
    assert _probability(0.45, 1.5) == 0.894
    assert _probability(0.35, 1.5) == 0.854
    assert _probability(0.60, 1.0) == 0.865
    assert _probability(0.45, 1.0) == 0.830
    assert _probability(0.31, 1.0) == 0.787
    ma = _rollmean(np.array([1.0, 2.0, 3.0, 4.0]), 2)
    assert np.isnan(ma[0])
    assert ma[1] == 1.5 and ma[3] == 3.5


def test_allocation_week_start():
    from data_sync_service.service.allocation import week_start_for

    assert week_start_for("2026-09-04") == "2026-08-31"  # Friday -> Monday
    assert week_start_for("2026-08-31") == "2026-08-31"  # Monday itself
    assert week_start_for("2026-09-06") == "2026-08-31"  # Sunday same week
