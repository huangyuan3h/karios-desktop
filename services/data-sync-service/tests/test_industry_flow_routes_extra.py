"""api/industry_flow_routes.py coverage."""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from data_sync_service.api import industry_flow_routes as ifr


class TestFundFlow:
    def test_get_ok(self, monkeypatch) -> None:
        monkeypatch.setattr(ifr, "get_cn_industry_fund_flow", lambda days, top_n, as_of_date: {"items": []})
        assert ifr.market_cn_industry_fund_flow(days=10, topN=30, asOfDate=None) == {"items": []}

    def test_get_error(self, monkeypatch) -> None:
        monkeypatch.setattr(ifr, "get_cn_industry_fund_flow", lambda days, top_n, as_of_date: (_ for _ in ()).throw(RuntimeError("boom")))
        with pytest.raises(HTTPException) as exc:
            ifr.market_cn_industry_fund_flow(days=10, topN=30, asOfDate=None)
        assert exc.value.status_code == 500 and exc.value.detail == "boom"

    def test_sync_defaults(self, monkeypatch) -> None:
        seen = {}

        def fake(days, top_n, force):  # noqa: ANN001
            seen.update(days=days, top_n=top_n, force=force)
            return {"ok": True}

        monkeypatch.setattr(ifr, "sync_cn_industry_fund_flow", fake)
        assert ifr.market_cn_industry_fund_flow_sync({})["ok"] is True
        assert seen == {"days": 10, "top_n": 10, "force": False}

    def test_sync_custom(self, monkeypatch) -> None:
        seen = {}

        def fake(days, top_n, force):  # noqa: ANN001
            seen.update(days=days, top_n=top_n, force=force)
            return {"ok": True}

        monkeypatch.setattr(ifr, "sync_cn_industry_fund_flow", fake)
        ifr.market_cn_industry_fund_flow_sync({"days": 5, "topN": 20, "force": True})
        assert seen == {"days": 5, "top_n": 20, "force": True}

    def test_sync_error(self, monkeypatch) -> None:
        monkeypatch.setattr(ifr, "sync_cn_industry_fund_flow", lambda days, top_n, force: (_ for _ in ()).throw(RuntimeError("boom")))
        with pytest.raises(HTTPException) as exc:
            ifr.market_cn_industry_fund_flow_sync({"force": True})
        assert exc.value.status_code == 500


class TestMainline:
    def test_get_ok(self, monkeypatch) -> None:
        monkeypatch.setattr(ifr, "get_cn_industry_mainline", lambda as_of_date: {"items": []})
        assert ifr.market_cn_industry_mainline(asOfDate="2026-08-07") == {"items": []}

    def test_get_error(self, monkeypatch) -> None:
        monkeypatch.setattr(ifr, "get_cn_industry_mainline", lambda as_of_date: (_ for _ in ()).throw(ValueError("boom")))
        with pytest.raises(HTTPException) as exc:
            ifr.market_cn_industry_mainline(asOfDate=None)
        assert exc.value.status_code == 500

    def test_sync_ok(self, monkeypatch) -> None:
        seen = {}

        def fake(as_of_date, force):  # noqa: ANN001
            seen.update(as_of_date=as_of_date, force=force)
            return {"ok": True}

        monkeypatch.setattr(ifr, "sync_cn_industry_mainline", fake)
        ifr.market_cn_industry_mainline_sync({"asOfDate": "2026-08-07", "force": True})
        assert seen == {"as_of_date": "2026-08-07", "force": True}

    def test_sync_empty_payload(self, monkeypatch) -> None:
        seen = {}

        def fake(as_of_date, force):  # noqa: ANN001
            seen.update(as_of_date=as_of_date, force=force)
            return {"ok": True}

        monkeypatch.setattr(ifr, "sync_cn_industry_mainline", fake)
        ifr.market_cn_industry_mainline_sync({})
        assert seen == {"as_of_date": None, "force": False}

    def test_sync_error(self, monkeypatch) -> None:
        monkeypatch.setattr(ifr, "sync_cn_industry_mainline", lambda as_of_date, force: (_ for _ in ()).throw(RuntimeError("boom")))
        with pytest.raises(HTTPException) as exc:
            ifr.market_cn_industry_mainline_sync({})
        assert exc.value.status_code == 500
