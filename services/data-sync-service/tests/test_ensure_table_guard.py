import pytest

pytestmark = pytest.mark.requires_postgres

from unittest.mock import MagicMock, patch

import data_sync_service.db.daily as daily_mod
from data_sync_service.db._ensure_guard import reset_ensured_for_tests


def test_fetch_last_ohlcv_batch_calls_ensure_table_once_per_process() -> None:
    reset_ensured_for_tests()
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    cursor.fetchall.return_value = []

    with patch.object(daily_mod, "get_connection") as mock_conn:
        mock_conn.return_value.__enter__.return_value = conn
        daily_mod.fetch_last_ohlcv_batch(["000001.SZ"], days=10)
        daily_mod.fetch_last_ohlcv_batch(["000001.SZ"], days=10)

    ddl_calls = [
        call
        for call in cursor.execute.call_args_list
        if call.args and "CREATE TABLE" in str(call.args[0])
    ]
    assert len(ddl_calls) == 1
