"""D1 fix: fetch_last_ohlcv_batch as_of must not see future closes."""

import pytest

from data_sync_service.db import get_connection
from data_sync_service.db.daily import TABLE_NAME, fetch_last_ohlcv_batch


@pytest.mark.requires_postgres
def test_fetch_last_ohlcv_batch_as_of_filters_future():
    ts = "TEST_ASOF.XX"
    # Clean up
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {TABLE_NAME} WHERE ts_code = %s", (ts,))
        conn.commit()
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {TABLE_NAME} (ts_code, trade_date, close, vol) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (ts, "2024-01-01", 10, 1000),
            )
            cur.execute(
                f"INSERT INTO {TABLE_NAME} (ts_code, trade_date, close, vol) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (ts, "2026-12-31", 999, 1000),
            )
        conn.commit()

    all_rows = fetch_last_ohlcv_batch([ts], days=10)
    assert all_rows[ts][-1][0] == "2026-12-31"
    assert all_rows[ts][-1][4] == "999"

    hist = fetch_last_ohlcv_batch([ts], days=10, as_of="2024-06-01")
    assert hist[ts][-1][0] == "2024-01-01"
    assert hist[ts][-1][4] == "10"

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {TABLE_NAME} WHERE ts_code = %s", (ts,))
        conn.commit()
