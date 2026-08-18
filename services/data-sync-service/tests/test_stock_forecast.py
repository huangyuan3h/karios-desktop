"""stock_forecast module tests — P14 PEAD data layer."""

from __future__ import annotations

from data_sync_service.db import stock_forecast as sf


def test_iso_date_formats_compact() -> None:
    assert sf._iso_date("20260805") == "2026-08-05"
    assert sf._iso_date("2026-08-05") == "2026-08-05"
    assert sf._iso_date("x") == "x"


def test_positive_types_set() -> None:
    assert sf.POSITIVE_TYPES == frozenset({"预增", "扭亏", "略增", "续盈"})
    assert "预减" not in sf.POSITIVE_TYPES
