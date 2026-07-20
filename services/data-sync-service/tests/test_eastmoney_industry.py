from __future__ import annotations

import pytest

pytestmark = pytest.mark.requires_postgres

import warnings
from unittest.mock import patch

from data_sync_service.service.eastmoney_industry import (  # type: ignore[import-not-found]
    _symbol_to_ts_code,
    _ts_code_to_secid,
    ensure_em_industries_for_ts_codes,
    fetch_em_industries_for_ts_codes,
    get_eastmoney_industry_sync_status,
    lookup_em_industries_for_ts_codes,
    sync_eastmoney_industry,
    sync_eastmoney_industry_incremental,
)


def test_ts_code_to_secid() -> None:
    assert _ts_code_to_secid("000021.SZ") == "0.000021"
    assert _ts_code_to_secid("600000.SH") == "1.600000"


def test_symbol_to_ts_code() -> None:
    assert _symbol_to_ts_code("CN:000021") == "000021.SZ"
    assert _symbol_to_ts_code("CN:600000") == "600000.SH"


def test_fetch_em_industries_for_ts_codes() -> None:
    with patch(
        "data_sync_service.service.eastmoney_industry._fetch_em_industry_for_ts_code",
        side_effect=lambda code: "消费电子" if code == "000021.SZ" else None,
    ):
        out = fetch_em_industries_for_ts_codes(["000021.SZ", "600000.SH"], sleep_s=0)
    assert out == {"000021.SZ": "消费电子"}


def test_sync_eastmoney_industry_symbols_path() -> None:
    with (
        patch(
            "data_sync_service.service.eastmoney_industry.fetch_em_industries_for_ts_codes",
            return_value={"000021.SZ": "消费电子"},
        ),
        patch("data_sync_service.service.eastmoney_industry.upsert_rows", return_value=1) as upsert,
        patch("data_sync_service.service.eastmoney_industry.count_rows", return_value=1),
        patch(
            "data_sync_service.service.eastmoney_industry.coverage_stats",
            return_value={"totalCnStocks": 10, "emMapped": 1, "missingCount": 9},
        ),
    ):
        out = sync_eastmoney_industry(symbols=["CN:000021"], sleep_s=0)
    assert out["ok"] is True
    assert out["resolved"] == 1
    assert out["coveragePct"] == 10.0
    assert upsert.call_count == 1


def test_lookup_em_industries_for_ts_codes_db_only() -> None:
    with (
        patch(
            "data_sync_service.service.eastmoney_industry.lookup_by_ts_codes",
            return_value={"600000.SH": "银行"},
        ) as lookup,
        patch(
            "data_sync_service.service.eastmoney_industry.fetch_em_industries_for_ts_codes",
        ) as fetch,
    ):
        out = lookup_em_industries_for_ts_codes(["600000.SH", "000021.SZ"])
    assert out == {"600000.SH": "银行"}
    lookup.assert_called_once_with(["600000.SH", "000021.SZ"])
    fetch.assert_not_called()


def test_ensure_em_industries_for_ts_codes_only_missing() -> None:
    with (
        patch(
            "data_sync_service.service.eastmoney_industry.lookup_by_ts_codes",
            return_value={"600000.SH": "银行"},
        ),
        patch(
            "data_sync_service.service.eastmoney_industry.fetch_em_industries_for_ts_codes",
            return_value={"000021.SZ": "消费电子"},
        ) as fetch,
        patch("data_sync_service.service.eastmoney_industry.upsert_rows", return_value=1) as upsert,
        warnings.catch_warnings(record=True),
    ):
        ensure_em_industries_for_ts_codes(["600000.SH", "000021.SZ"])
    fetch.assert_called_once()
    assert fetch.call_args[0][0] == ["000021.SZ"]
    assert upsert.call_count == 1


def test_sync_eastmoney_industry_incremental_missing() -> None:
    with (
        patch(
            "data_sync_service.service.eastmoney_industry.get_today_run",
            return_value=None,
        ),
        patch(
            "data_sync_service.service.eastmoney_industry.list_missing_cn_ts_codes",
            return_value=["000021.SZ", "600000.SH"],
        ) as list_missing,
        patch(
            "data_sync_service.service.eastmoney_industry.fetch_em_industries_for_ts_codes",
            return_value={"000021.SZ": "消费电子"},
        ) as fetch,
        patch("data_sync_service.service.eastmoney_industry.upsert_rows", return_value=1) as upsert,
        patch("data_sync_service.service.eastmoney_industry.insert_record") as insert,
        patch(
            "data_sync_service.service.eastmoney_industry.coverage_stats",
            return_value={"totalCnStocks": 100, "emMapped": 50, "missingCount": 50},
        ),
        patch("data_sync_service.service.eastmoney_industry.count_rows", return_value=50),
    ):
        out = sync_eastmoney_industry_incremental(mode="missing", batch_size=500, max_batches=1, sleep_s=0)
    assert out["ok"] is True
    assert out["requested"] == 2
    assert out["resolved"] == 1
    assert out["updated"] == 1
    assert out["coveragePct"] == 50.0
    list_missing.assert_called_once()
    fetch.assert_called_once()
    upsert.assert_called_once()
    insert.assert_called_once()


def test_sync_eastmoney_industry_incremental_skips_when_empty() -> None:
    with (
        patch("data_sync_service.service.eastmoney_industry.get_today_run", return_value=None),
        patch("data_sync_service.service.eastmoney_industry.list_missing_cn_ts_codes", return_value=[]),
        patch(
            "data_sync_service.service.eastmoney_industry.coverage_stats",
            return_value={"totalCnStocks": 100, "emMapped": 100, "missingCount": 0},
        ),
        patch("data_sync_service.service.eastmoney_industry.count_rows", return_value=100),
    ):
        out = sync_eastmoney_industry_incremental(mode="missing", batch_size=500)
    assert out["ok"] is True
    assert out["skipped"] is True
    assert out["missingCount"] == 0


def test_get_eastmoney_industry_sync_status() -> None:
    with (
        patch(
            "data_sync_service.service.eastmoney_industry.coverage_stats",
            return_value={"totalCnStocks": 200, "emMapped": 180, "missingCount": 20},
        ),
        patch("data_sync_service.service.eastmoney_industry.count_rows", return_value=180),
        patch(
            "data_sync_service.service.eastmoney_industry.get_today_run",
            return_value={"success": True, "last_ts_code": None},
        ),
    ):
        out = get_eastmoney_industry_sync_status()
    assert out["ok"] is True
    assert out["coveragePct"] == 90.0
    assert out["missingCount"] == 20
    assert out["todayRun"]["success"] is True
