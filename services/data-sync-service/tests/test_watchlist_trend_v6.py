from data_sync_service.testback.strategies.base import Bar
from data_sync_service.testback.strategies.watchlist_trend_v6 import WatchlistTrendV6Strategy


def _bar(
    ts_code: str,
    trade_date: str,
    open_price: float,
    high: float,
    low: float,
    close: float,
) -> Bar:
    avg_price = (open_price + high + low + close) / 4.0
    return Bar(
        ts_code=ts_code,
        trade_date=trade_date,
        open=open_price,
        high=high,
        low=low,
        close=close,
        avg_price=avg_price,
        volume=0.0,
        amount=0.0,
    )


def test_v6_stop_price_prefers_tightest() -> None:
    strategy = WatchlistTrendV6Strategy(stop_loss_pct=0.1, atr_stop_mult=2.0, trailing_atr_mult=2.0)
    stop_price = strategy._stop_price(entry_price=100.0, entry_atr=5.0, peak_price=120.0, atr_now=4.0)
    assert stop_price == 112.0


def test_v6_atr_calculation() -> None:
    strategy = WatchlistTrendV6Strategy(atr_window=3)
    history = strategy._history["000001.SZ"]
    history.append(_bar("000001.SZ", "2024-01-01", 10.0, 11.0, 9.0, 10.0))
    history.append(_bar("000001.SZ", "2024-01-02", 11.0, 12.0, 10.0, 11.0))
    history.append(_bar("000001.SZ", "2024-01-03", 10.5, 11.0, 9.5, 10.5))
    history.append(_bar("000001.SZ", "2024-01-04", 11.5, 13.0, 11.0, 12.0))
    atr_val = strategy._calc_atr(history)
    assert abs(atr_val - 2.0) < 1e-6
