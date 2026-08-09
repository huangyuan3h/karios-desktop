from __future__ import annotations

import json
import warnings
from unittest.mock import patch

from data_sync_service.service.eastmoney_industry import (  # type: ignore[import-not-found]
    _em2016_to_board_name,
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


def test_em2016_to_board_name() -> None:
    assert _em2016_to_board_name("医药生物-化学制药-化学制剂") == "化学制药"
    assert _em2016_to_board_name("金融-银行-股份制与城商行") == "银行"
    assert _em2016_to_board_name("银行") == "银行"
    assert _em2016_to_board_name("") is None
    assert _em2016_to_board_name(None) is None


class _FakeResp:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def __enter__(self) -> _FakeResp:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self) -> bytes:
        return self._payload


class _RaisingResp:
    def __enter__(self) -> _RaisingResp:
        raise RemoteDisconnected("Remote end closed connection without response")

    def __exit__(self, *args: object) -> None:
        return None

    def read(self) -> bytes:  # pragma: no cover - never reached
        return b""


from http.client import RemoteDisconnected  # noqa: E402


def test_fetch_falls_back_to_emweb_when_push2_down() -> None:
    """B7: push2 unreachable must fall back to emweb F10 EM2016 (second level)."""
    emweb_payload = json.dumps(
        {
            "jbzl": [
                {
                    "SECURITY_CODE": "603259",
                    "SECURITY_NAME_ABBR": "药明康德",
                    "EM2016": "医药生物-化学制药-化学制剂",
                }
            ]
        }
    ).encode()

    def fake_urlopen(req, timeout=15):  # noqa: ANN001
        url = req.full_url
        if "push2.eastmoney.com" in url:
            return _RaisingResp()
        if "emweb.securities.eastmoney.com" in url:
            assert "code=SH603259" in url
            return _FakeResp(emweb_payload)
        raise AssertionError(f"unexpected url {url}")

    with patch(
        "data_sync_service.service.eastmoney_industry.urllib.request.urlopen",
        side_effect=fake_urlopen,
    ):
        from data_sync_service.service.eastmoney_industry import (
            _fetch_em_industry_for_ts_code,
        )

        assert _fetch_em_industry_for_ts_code("603259.SH") == "化学制药"


def test_incremental_marks_failure_when_nothing_resolved() -> None:
    """A batch that resolves zero industries must NOT be recorded as success."""
    with (
        patch(
            "data_sync_service.service.eastmoney_industry.list_missing_cn_ts_codes",
            return_value=["000021.SZ", "600000.SH"],
        ),
        patch(
            "data_sync_service.service.eastmoney_industry.fetch_em_industries_for_ts_codes",
            return_value={},
        ),
        patch("data_sync_service.service.eastmoney_industry.upsert_rows", return_value=0),
        patch("data_sync_service.service.eastmoney_industry.insert_record") as insert,
        patch(
            "data_sync_service.service.eastmoney_industry.coverage_stats",
            return_value={"totalCnStocks": 10, "emMapped": 0, "missingCount": 10},
        ),
        patch("data_sync_service.service.eastmoney_industry.count_rows", return_value=1),
    ):
        out = sync_eastmoney_industry_incremental(mode="missing", batch_size=500, max_batches=1)

    assert out["ok"] is False
    calls = [c.kwargs for c in insert.call_args_list]
    assert calls[-1]["success"] is False
    assert "no industry resolved" in (calls[-1]["error_message"] or "")
