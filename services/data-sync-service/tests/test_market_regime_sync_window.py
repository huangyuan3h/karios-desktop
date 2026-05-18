from datetime import datetime
from zoneinfo import ZoneInfo


def test_sync_window_includes_lunch_break() -> None:
    import data_sync_service.service.market_regime as mr  # type: ignore[import-not-found]

    tz = ZoneInfo("Asia/Shanghai")
    lunch = datetime(2026, 2, 24, 12, 0, 0, tzinfo=tz)
    assert mr._is_shanghai_sync_window_at(lunch) is True


def test_sync_window_includes_trading_time() -> None:
    import data_sync_service.service.market_regime as mr  # type: ignore[import-not-found]

    tz = ZoneInfo("Asia/Shanghai")
    trading = datetime(2026, 2, 24, 10, 15, 0, tzinfo=tz)
    assert mr._is_shanghai_sync_window_at(trading) is True


def test_sync_window_excludes_night() -> None:
    import data_sync_service.service.market_regime as mr  # type: ignore[import-not-found]

    tz = ZoneInfo("Asia/Shanghai")
    night = datetime(2026, 2, 24, 20, 0, 0, tzinfo=tz)
    assert mr._is_shanghai_sync_window_at(night) is False
