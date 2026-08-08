"""service/hk_basic.py coverage."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import Mock

import pandas as pd

from data_sync_service.service import hk_basic as hb


class TestHelpers:
    def test_parse_iso(self) -> None:
        assert hb._parse_iso_datetime(None) is None
        assert hb._parse_iso_datetime("") is None
        dt = datetime(2026, 8, 7, 10, 0, tzinfo=UTC)
        assert hb._parse_iso_datetime(dt) is dt
        assert hb._parse_iso_datetime("2026-08-07T10:00:00+00:00") == dt
        assert hb._parse_iso_datetime("bad") is None

    def test_same_month(self) -> None:
        assert hb._is_same_utc_month(datetime(2026, 8, 1, tzinfo=UTC), datetime(2026, 8, 31, tzinfo=UTC))
        assert not hb._is_same_utc_month(datetime(2026, 7, 1, tzinfo=UTC), datetime(2026, 8, 1, tzinfo=UTC))
        assert hb._is_same_utc_month(datetime(2026, 8, 1, tzinfo=UTC), datetime(2026, 8, 2, tzinfo=UTC))


class TestMap:
    def test_empty(self) -> None:
        out = hb.map_hk_basic_to_stock_basic_df(None)
        assert out.empty
        out2 = hb.map_hk_basic_to_stock_basic_df(pd.DataFrame())
        assert list(out2.columns) == ["ts_code", "symbol", "name", "industry", "market", "list_date", "delist_date"]

    def test_mapping(self) -> None:
        df = pd.DataFrame([
            {"ts_code": "00700.HK", "name": "腾讯控股", "list_date": "20040616", "delist_date": None},
            {"ts_code": None, "name": "x", "list_date": None, "delist_date": None},
            {"ts_code": ".HK", "name": "y", "list_date": None, "delist_date": None},
            {"ts_code": "", "name": "z", "list_date": None, "delist_date": None},
        ])
        out = hb.map_hk_basic_to_stock_basic_df(df)
        assert out["symbol"].iloc[0] == "00700"
        assert pd.isna(out["symbol"].iloc[1])
        assert pd.isna(out["symbol"].iloc[2])
        assert pd.isna(out["symbol"].iloc[3])
        assert out["market"].iloc[0] == "HK"
        assert out["industry"].iloc[0] is None

    def test_missing_columns(self) -> None:
        df = pd.DataFrame([{"ts_code": "00941.HK"}])
        out = hb.map_hk_basic_to_stock_basic_df(df)
        assert out["name"].iloc[0] is None
        assert out["symbol"].iloc[0] == "00941"


class TestSync:
    def test_bad_status(self) -> None:
        out = hb.sync_hk_basic(list_status="X")
        assert out == {"ok": False, "error": "list_status must be one of: L, D, P"}

    def test_skip_month(self, monkeypatch) -> None:
        monkeypatch.setattr(hb, "get_last_success", lambda job: {"sync_at": datetime.now(UTC).isoformat()})
        out = hb.sync_hk_basic()
        assert out["skipped"] is True
        monkeypatch.setattr(hb, "get_last_success", lambda job: {})
        monkeypatch.setattr(hb, "get_settings", lambda: Mock(tu_share_api_key=""))
        out = hb.sync_hk_basic()
        assert out["ok"] is False and "TU_SHARE_API_KEY" in out["error"]
        monkeypatch.setattr(hb, "get_last_success", lambda job: {"sync_at": "bad-date"})
        out = hb.sync_hk_basic()
        assert out["ok"] is False

    def test_no_key(self, monkeypatch) -> None:
        monkeypatch.setattr(hb, "get_last_success", lambda job: None)
        monkeypatch.setattr(hb, "get_settings", lambda: Mock(tu_share_api_key=""))
        seen = {}
        monkeypatch.setattr(hb, "insert_record", lambda **kw: seen.update(kw))
        out = hb.sync_hk_basic()
        assert out["ok"] is False
        assert seen["success"] is False

    def test_empty_df(self, monkeypatch) -> None:
        monkeypatch.setattr(hb, "get_last_success", lambda job: None)
        monkeypatch.setattr(hb, "get_settings", lambda: Mock(tu_share_api_key="k"))
        pro = Mock()
        pro.hk_basic.return_value = pd.DataFrame()
        monkeypatch.setattr(hb.ts, "pro_api", lambda k: pro)
        seen = {}
        monkeypatch.setattr(hb, "insert_record", lambda **kw: seen.update(kw))
        out = hb.sync_hk_basic()
        assert out == {"ok": True, "updated": 0, "message": "no data from tushare"}
        assert seen["success"] is True

    def test_success(self, monkeypatch) -> None:
        monkeypatch.setattr(hb, "get_last_success", lambda job: None)
        monkeypatch.setattr(hb, "get_settings", lambda: Mock(tu_share_api_key="k"))
        pro = Mock()
        pro.hk_basic.return_value = pd.DataFrame([{"ts_code": "00700.HK", "name": "腾讯", "list_date": "20040616", "delist_date": None}])
        monkeypatch.setattr(hb.ts, "pro_api", lambda k: pro)
        seen = {}
        monkeypatch.setattr(hb, "insert_record", lambda **kw: seen.update(kw))
        monkeypatch.setattr(hb, "upsert_from_dataframe", lambda df, keep_industry=True: 1)
        out = hb.sync_hk_basic(ts_code="00700.HK", list_status="L", force=True)
        assert out["ok"] is True and out["updated"] == 1 and out["list_status"] == "L"
        assert pro.hk_basic.call_args.kwargs["ts_code"] == "00700.HK"
        assert seen["success"] is True

    def test_exception(self, monkeypatch) -> None:
        monkeypatch.setattr(hb, "get_last_success", lambda job: None)
        monkeypatch.setattr(hb, "get_settings", lambda: Mock(tu_share_api_key="k"))
        pro = Mock()
        pro.hk_basic.side_effect = RuntimeError("boom")
        monkeypatch.setattr(hb.ts, "pro_api", lambda k: pro)
        seen = {}
        monkeypatch.setattr(hb, "insert_record", lambda **kw: seen.update(kw))
        out = hb.sync_hk_basic(force=True)
        assert out == {"ok": False, "error": "boom"}
        assert seen["success"] is False
