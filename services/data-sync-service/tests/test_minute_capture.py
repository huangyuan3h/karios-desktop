"""minute_capture tests — pure unit (no network, no DB writes)."""

from __future__ import annotations

from unittest.mock import patch

from data_sync_service.service import minute_capture as mc


def test_fetch_tencent_parses_hk_rows() -> None:
    raw = (
        "min_data_hk02099={\"code\":0,\"msg\":\"\",\"data\":{\"hk02099\":{\"data\":"
        "{\"data\":[\"0930 192.000 70200 13478400.000\","
        "\"0931 199.400 361000 69886670.000\"]}}}}\n"
    )

    class _Resp:
        def read(self):
            return raw.encode()

    with patch("urllib.request.urlopen", return_value=_Resp()):
        rows = mc._fetch_tencent("hk02099", "hk")
    assert rows is not None
    assert len(rows) == 2
    assert rows[0] == {
        "time": "0930", "open": 192.0, "high": 192.0, "low": 192.0,
        "close": 192.0, "vol": 70200.0, "amount": 13478400.0,
    }
    assert rows[1]["close"] == 199.4


def test_fetch_tencent_handles_json_not_jsonp() -> None:
    raw = '{"code":0,"msg":"","data":{"sz000001":{"data":{"data":["0930 11.22 2852 3199944.00"]}}}}'
    class _Resp:
        def read(self):
            return raw.encode()

    with patch("urllib.request.urlopen", return_value=_Resp()):
        rows = mc._fetch_tencent("sz000001", "cn")
    assert rows is not None
    assert rows[0]["close"] == 11.22


def test_capture_symbols_bad_kind() -> None:
    out = mc.capture_day_minute(
        ts_code="12345.XX", trade_date="2026-08-14", kind="xx",
    )
    assert out["ok"] is False
    assert out["skipped"] is True


def test_capture_symbols_handles_network_failure() -> None:
    with patch(
        "urllib.request.urlopen",
        side_effect=RuntimeError("connection reset"),
    ):
        out = mc.capture_day_minute(
            ts_code="02099.HK", trade_date="2026-08-14", kind="hk",
        )
    assert out["ok"] is False
    assert "connection reset" in out["reason"]
