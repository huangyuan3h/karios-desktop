"""Macro snapshot and post-close wiring."""

from __future__ import annotations

from fastapi.testclient import TestClient  # type: ignore[import-not-found]

from data_sync_service.main import app  # type: ignore[import-not-found]


def test_get_macro_snapshot_endpoint(monkeypatch) -> None:
    import data_sync_service.api.query_routes as query_routes  # type: ignore[import-not-found]

    def _snap() -> dict:
        return {"cnIndexSignals": [], "macro": []}

    monkeypatch.setattr(query_routes, "build_macro_snapshot", _snap)

    client = TestClient(app)
    resp = client.get("/macro/snapshot")
    assert resp.status_code == 200
    assert resp.json() == {"cnIndexSignals": [], "macro": []}


def test_run_post_close_sync(monkeypatch) -> None:
    import data_sync_service.service.post_close_sync as pcs  # type: ignore[import-not-found]

    monkeypatch.setattr(pcs, "sync_index_daily_full", lambda: {"ok": True, "updated": 1})
    monkeypatch.setattr(pcs, "sync_macro_daily_full", lambda: {"ok": True, "updated": 2})

    out = pcs.run_post_close_sync()
    assert out["indexDaily"]["ok"] is True
    assert out["macroDaily"]["updated"] == 2


def test_resolve_sgx_a50_main_empty() -> None:
    from data_sync_service.service.macro_daily import resolve_sgx_a50_main  # type: ignore[import-not-found]

    class _Pro:
        def fut_basic(self, **kwargs):  # noqa: ANN003
            return None

    assert resolve_sgx_a50_main(_Pro()) is None


def test_df_to_metrics_parses_tushare_dates() -> None:
    import pandas as pd  # type: ignore[import-not-found]

    from data_sync_service.service.macro_snapshot_on_demand import _df_to_metrics  # type: ignore[import-not-found]

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


def test_resolve_main_fut_by_prefix_filters() -> None:
    import pandas as pd  # type: ignore[import-not-found]

    from data_sync_service.service.macro_daily import resolve_main_fut_by_prefix  # type: ignore[import-not-found]

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
