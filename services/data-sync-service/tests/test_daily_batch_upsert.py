from __future__ import annotations

import pytest

pytestmark = pytest.mark.requires_postgres

from unittest.mock import MagicMock, patch

import pandas as pd

from data_sync_service.db import daily as daily_mod


def test_upsert_from_dataframe_uses_executemany() -> None:
    df = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "trade_date": "20240601",
                "open": 10.0,
                "high": 11.0,
                "low": 9.5,
                "close": 10.5,
                "pre_close": 10.0,
                "change": 0.5,
                "pct_chg": 5.0,
                "vol": 1000.0,
                "amount": 2000.0,
            },
            {
                "ts_code": "000002.SZ",
                "trade_date": "20240601",
                "open": 20.0,
                "high": 21.0,
                "low": 19.5,
                "close": 20.5,
                "pre_close": 20.0,
                "change": 0.5,
                "pct_chg": 2.5,
                "vol": 2000.0,
                "amount": 4000.0,
            },
        ]
    )
    mock_cur = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    mock_conn.__enter__.return_value = mock_conn

    with (
        patch.object(daily_mod, "ensure_table"),
        patch.object(daily_mod, "get_connection", return_value=mock_conn),
    ):
        count = daily_mod.upsert_from_dataframe(df)

    assert count == 2
    mock_cur.executemany.assert_called_once()
    mock_cur.execute.assert_not_called()
    rows = mock_cur.executemany.call_args[0][1]
    assert len(rows) == 2
    assert rows[0][0] == "000001.SZ"
    assert rows[1][0] == "000002.SZ"
