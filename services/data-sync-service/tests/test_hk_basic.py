import pandas as pd  # type: ignore[import-not-found, import-untyped]
from fastapi.testclient import TestClient  # type: ignore[import-not-found]

from data_sync_service.main import app  # type: ignore[import-not-found]
from data_sync_service.service.hk_basic import map_hk_basic_to_stock_basic_df  # type: ignore[import-not-found]


def test_map_hk_basic_to_stock_basic_df() -> None:
    hk_df = pd.DataFrame(
        [
            {
                "ts_code": "00001.HK",
                "name": "CKH HOLDINGS",
                "list_date": "19721101",
                "delist_date": None,
            },
            {
                "ts_code": "00005.HK",
                "name": "HSBC HOLDINGS",
                "list_date": "19721101",
                "delist_date": "",
            },
        ]
    )
    out = map_hk_basic_to_stock_basic_df(hk_df)
    assert list(out.columns) == [
        "ts_code",
        "symbol",
        "name",
        "industry",
        "market",
        "list_date",
        "delist_date",
    ]
    assert out.loc[0, "ts_code"] == "00001.HK"
    assert out.loc[0, "symbol"] == "00001"
    assert out.loc[0, "market"] == "HK"
    assert out.loc[1, "symbol"] == "00005"


def test_sync_hk_basic_endpoint_shape(monkeypatch) -> None:
    import data_sync_service.api.sync_routes as sync_routes  # type: ignore[import-not-found]

    monkeypatch.setattr(
        sync_routes,
        "sync_hk_basic",
        lambda ts_code=None, list_status="L", force=False: {
            "ok": True,
            "updated": 2,
            "list_status": str(list_status),
            "ts_code": ts_code,
            "force": bool(force),
        },
    )

    client = TestClient(app)
    resp = client.post("/sync/hk-basic?list_status=L&force=true")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["ok"] is True
    assert payload["updated"] == 2

