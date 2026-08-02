"""Macro snapshot and post-close wiring."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient  # type: ignore[import-not-found]

from data_sync_service.main import app  # type: ignore[import-not-found]

pytestmark = pytest.mark.requires_postgres

def test_get_macro_snapshot_endpoint(monkeypatch) -> None:
    import data_sync_service.api.query_routes as query_routes  # type: ignore[import-not-found]

    def _snap() -> dict:
        return {"cnIndexSignals": [], "macro": []}

    monkeypatch.setattr(query_routes, "build_macro_snapshot", _snap)

    client = TestClient(app)
    resp = client.get("/macro/snapshot")
    assert resp.status_code == 200
    assert resp.json() == {"cnIndexSignals": [], "macro": []}


def test_build_macro_snapshot_includes_etf_fund_flow(monkeypatch) -> None:
    import data_sync_service.service.macro_snapshot as mod

    captured: dict[str, object] = {}

    def _etf(**kwargs) -> dict:
        captured.update(kwargs)
        return {"asOfDate": "2026-06-18", "items": [], "shareLag": False, "intradaySafe": True}

    monkeypatch.setattr(mod, "ensure_table", lambda: None)
    monkeypatch.setattr(mod, "get_index_signals", lambda **kw: [])
    monkeypatch.setattr(mod, "fetch_last_closes_batch", lambda *_a, **_k: {})
    monkeypatch.setattr(mod, "get_latest_rows_batch", lambda *_a, **_k: {})
    monkeypatch.setattr(mod, "resolve_put_iv_for_snapshot", lambda **_k: {})
    monkeypatch.setattr(mod, "enrich_macro_items_on_demand", lambda items: items)
    monkeypatch.setattr(mod, "macro_snapshot_warning", lambda: None)
    monkeypatch.setattr(mod, "build_etf_fund_flow_bundle", _etf)
    monkeypatch.setattr(mod, "_is_shanghai_sync_window", lambda: False)
    monkeypatch.setattr(mod, "get_latest_etf_flow_date", lambda: "2026-06-18")

    out = mod.build_macro_snapshot()
    assert out["etfFundFlow"] == {
        "asOfDate": "2026-06-18",
        "items": [],
        "shareLag": False,
        "intradaySafe": True,
    }
    # Outside the sync window we fall back to the latest stored ETF date.
    assert captured.get("as_of_date") == "2026-06-18"


def test_run_post_close_sync(monkeypatch) -> None:
    import data_sync_service.service.post_close_sync as pcs  # type: ignore[import-not-found]

    called: list[str] = []

    def _index() -> dict:
        called.append("index")
        return {"ok": True, "updated": 1}

    def _macro() -> dict:
        called.append("macro")
        return {"ok": True, "updated": 2}

    def _em(**kwargs) -> dict:  # noqa: ANN003
        called.append("em")
        return {"ok": True, "updated": 3}

    def _etf() -> dict:
        called.append("etf")
        return {"ok": True, "updated": 4}

    monkeypatch.setattr(pcs, "sync_index_daily_full", _index)
    monkeypatch.setattr(pcs, "sync_macro_daily_full", _macro)
    monkeypatch.setattr(pcs, "sync_eastmoney_industry_incremental", _em)
    monkeypatch.setattr(pcs, "sync_etf_fund_flow_watchlist", _etf)

    out = pcs.run_post_close_sync()
    assert out["indexDaily"]["ok"] is True
    assert out["macroDaily"]["updated"] == 2
    assert out["eastmoneyIndustry"]["updated"] == 3
    assert out["etfFundFlow"]["updated"] == 4
    assert set(called) == {"index", "macro", "em", "etf"}


def test_enrich_macro_items_on_demand_without_tushare(monkeypatch) -> None:
    from data_sync_service.service import macro_snapshot_on_demand as mod

    monkeypatch.setattr(mod, "try_tushare_pro", lambda: None)
    monkeypatch.setattr(
        mod,
        "_fetch_ixic_via_yfinance",
        lambda: {
            "close": 17000.0,
            "pctChg": 1.2,
            "asOfDate": "2026-06-20",
            "ma5": 16900.0,
            "ma20": 16800.0,
        },
    )

    items = [
        {
            "seriesId": mod.SID_IXIC,
            "name": "Nasdaq",
            "close": 16000.0,
            "asOfDate": "2026-06-10",
            "pctChg": 0.5,
        }
    ]
    out = mod.enrich_macro_items_on_demand(items)
    assert out[0]["close"] == 17000.0
    assert out[0]["asOfDate"] == "2026-06-20"
    assert out[0]["source"] == "yfinance.on_demand"


def test_fetch_hsi_on_demand(monkeypatch) -> None:
    from data_sync_service.service import macro_snapshot_on_demand as mod

    monkeypatch.setattr(
        mod,
        "_fetch_hsi_via_yfinance",
        lambda: {
            "close": 24000.0,
            "pctChg": -0.5,
            "asOfDate": "2026-06-22",
            "ma5": 24100.0,
            "ma20": 24500.0,
        },
    )
    metrics, src = mod.fetch_hk_index_on_demand("HSI")
    assert metrics["close"] == 24000.0
    assert src == "yfinance.on_demand"


def test_fetch_hstech_bars_via_yf_normalizes(monkeypatch) -> None:
    import sys

    import pandas as pd  # type: ignore[import-not-found]

    from data_sync_service.service import macro_daily as md  # type: ignore[import-not-found]

    class _Ticker:
        def __init__(self, ticker: str) -> None:
            assert ticker == "^HSTECH"

        def history(self, **kwargs):  # noqa: ANN003
            return pd.DataFrame(
                {
                    "Open": [3000.0, 3050.0],
                    "High": [3100.0, 3080.0],
                    "Low": [2990.0, 3040.0],
                    "Close": [3050.0, 3070.0],
                    "Volume": [100, 120],
                },
                index=pd.DatetimeIndex(pd.to_datetime(["2026-07-30", "2026-07-31"]), name="Date"),
            )

    class _Yf:
        def Ticker(self, ticker: str):  # noqa: N802
            return _Ticker(ticker)

    monkeypatch.setitem(sys.modules, "yfinance", _Yf())
    df = md._fetch_hstech_bars_via_yf("20260701", "20260802")
    assert df is not None
    assert df["trade_date"].tolist() == ["2026-07-30", "2026-07-31"]
    assert df["close"].tolist() == [3050.0, 3070.0]
    assert abs(df["pct_chg"].iloc[-1] - ((3070.0 / 3050.0 - 1) * 100.0)) < 1e-9
    assert df["vol"].tolist() == [100, 120]


def test_fetch_hstech_bars_via_ak_normalizes(monkeypatch) -> None:
    import sys

    import pandas as pd  # type: ignore[import-not-found]

    from data_sync_service.service import macro_daily as md  # type: ignore[import-not-found]

    class _Ak:
        def stock_hk_index_daily_sina(self, symbol: str) -> pd.DataFrame:
            assert symbol == "HSTECH"
            return pd.DataFrame(
                {
                    "date": pd.to_datetime(["2026-07-27", "2026-07-30", "2026-07-31"]).date,
                    "open": [4500.0, 4600.0, 4818.0],
                    "high": [4600.0, 4700.0, 4871.0],
                    "low": [4400.0, 4500.0, 4763.0],
                    "close": [4550.0, 4803.0, 4829.0],
                    "volume": [100, 120, 130],
                    "amount": [1e9, 1.2e9, 1.3e9],
                }
            )

    monkeypatch.setitem(sys.modules, "akshare", _Ak())
    df = md._fetch_hstech_bars_via_ak("20260727", "20260802")
    assert df is not None
    assert df["trade_date"].tolist() == ["2026-07-27", "2026-07-30", "2026-07-31"]
    assert df["close"].tolist() == [4550.0, 4803.0, 4829.0]
    assert abs(df["pct_chg"].iloc[-1] - ((4829.0 / 4803.0 - 1) * 100.0)) < 1e-9
    assert df["vol"].tolist() == [100, 120, 130]
    assert df["amount"].tolist() == [1e9, 1.2e9, 1.3e9]


def test_fetch_hstech_bars_via_ak_filters_window(monkeypatch) -> None:
    import sys

    import pandas as pd  # type: ignore[import-not-found]

    from data_sync_service.service import macro_daily as md  # type: ignore[import-not-found]

    class _Ak:
        def stock_hk_index_daily_sina(self, symbol: str) -> pd.DataFrame:
            return pd.DataFrame(
                {
                    "date": pd.to_datetime(["2026-01-02", "2026-07-30", "2026-07-31"]).date,
                    "open": [4000.0, 4600.0, 4818.0],
                    "high": [4100.0, 4700.0, 4871.0],
                    "low": [3900.0, 4500.0, 4763.0],
                    "close": [4050.0, 4803.0, 4829.0],
                    "volume": [100, 120, 130],
                    "amount": [1e9, 1.2e9, 1.3e9],
                }
            )

    monkeypatch.setitem(sys.modules, "akshare", _Ak())
    df = md._fetch_hstech_bars_via_ak("20260701", "20260802")
    assert df is not None
    assert df["trade_date"].tolist() == ["2026-07-30", "2026-07-31"]


def test_fetch_hstech_on_demand_prefers_sina(monkeypatch) -> None:
    from data_sync_service.service import macro_snapshot_on_demand as mod

    monkeypatch.setattr(
        mod,
        "_fetch_hstech_via_sina",
        lambda: {
            "close": 4829.22,
            "pctChg": 0.53,
            "asOfDate": "2026-07-31",
            "ma5": 4800.0,
            "ma20": 4700.0,
        },
    )
    metrics, src = mod.fetch_hk_index_on_demand("HSTECH")
    assert metrics["close"] == 4829.22
    assert src == "akshare.on_demand"


def test_fetch_hstech_on_demand_falls_back_to_yfinance(monkeypatch) -> None:
    from data_sync_service.service import macro_snapshot_on_demand as mod

    monkeypatch.setattr(mod, "_fetch_hstech_via_sina", lambda: None)
    monkeypatch.setattr(
        mod,
        "_fetch_hstech_via_yfinance",
        lambda: {
            "close": 4800.0,
            "pctChg": 0.1,
            "asOfDate": "2026-07-31",
            "ma5": 4790.0,
            "ma20": 4700.0,
        },
    )
    metrics, src = mod.fetch_hk_index_on_demand("HSTECH")
    assert metrics["close"] == 4800.0
    assert src == "yfinance.on_demand"


def test_resolve_sgx_a50_main_empty() -> None:
    from data_sync_service.service.macro_daily import (
        resolve_sgx_a50_main,  # type: ignore[import-not-found]
    )

    class _Pro:
        def fut_basic(self, **kwargs):  # noqa: ANN003
            return None

    assert resolve_sgx_a50_main(_Pro()) is None


def test_df_to_metrics_uses_settle_when_close_na() -> None:
    import pandas as pd  # type: ignore[import-not-found]

    from data_sync_service.service.macro_snapshot_on_demand import (
        _df_to_metrics,  # type: ignore[import-not-found]
    )

    df = pd.DataFrame(
        {
            "trade_date": ["20260101", "20260102"],
            "close": [float("nan"), float("nan")],
            "settle": [500.0, 510.0],
            "pct_chg": [0.0, 2.0],
        }
    )
    m = _df_to_metrics(df)
    assert m["close"] == 510.0


def test_df_to_metrics_parses_tushare_dates() -> None:
    import pandas as pd  # type: ignore[import-not-found]

    from data_sync_service.service.macro_snapshot_on_demand import (
        _df_to_metrics,  # type: ignore[import-not-found]
    )

    df = pd.DataFrame(
        {
            "trade_date": ["20260101", "20260102", "20260103"],
            "close": [100.0, 101.0, 102.5],
            "pct_chg": [0.0, 1.0, 1.49],
        }
    )
    m = _df_to_metrics(df)
    assert m["close"] == 102.5
    assert m["pctChg"] == 1.49
    assert m["asOfDate"] == "2026-01-03"


def test_df_to_metrics_falls_back_to_prior_close_diff() -> None:
    import pandas as pd  # type: ignore[import-not-found]

    from data_sync_service.service.macro_snapshot_on_demand import (
        _df_to_metrics,  # type: ignore[import-not-found]
    )

    df = pd.DataFrame(
        {
            "trade_date": ["20260101", "20260102"],
            "close": [100.0, 103.0],
        }
    )
    m = _df_to_metrics(df)
    assert m["close"] == 103.0
    assert m["pctChg"] == 3.0


def test_resolve_main_fut_by_prefix_filters() -> None:
    import pandas as pd  # type: ignore[import-not-found]

    from data_sync_service.service.macro_daily import (
        resolve_main_fut_by_prefix,  # type: ignore[import-not-found]
    )

    class _Pro:
        def fut_basic(self, **kwargs):  # noqa: ANN003
            return pd.DataFrame(
                {
                    "ts_code": ["CU2501.SHF", "AL2501.SHF"],
                    "name": ["铜", "铝"],
                    "list_date": ["20240101", "20240101"],
                }
            )

    assert resolve_main_fut_by_prefix(_Pro(), "SHFE", "CU") == "CU2501.SHF"
