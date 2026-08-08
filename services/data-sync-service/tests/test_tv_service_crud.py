"""tv.py service CRUD + screener_history coverage."""

from __future__ import annotations

from unittest.mock import patch

from fastapi import HTTPException

from data_sync_service.service import tv as tv_svc


def test_ensure_seeded_seeds_when_empty() -> None:
    with (
        patch.object(tv_svc.tvdb, "count_screeners", return_value=0),
        patch.object(tv_svc.tvdb, "upsert_screener") as upsert,
    ):
        tv_svc.ensure_seeded()
    assert upsert.call_count == 2
    ids = [c.kwargs["screener_id"] for c in upsert.call_args_list]
    assert ids == ["falcon", "blackhorse"]


def test_ensure_seeded_skips_when_non_empty() -> None:
    with (
        patch.object(tv_svc.tvdb, "count_screeners", return_value=3),
        patch.object(tv_svc.tvdb, "upsert_screener") as upsert,
    ):
        tv_svc.ensure_seeded()
    upsert.assert_not_called()


def test_list_screeners() -> None:
    with (
        patch.object(tv_svc.tvdb, "count_screeners", return_value=1),
        patch.object(tv_svc.tvdb, "fetch_screeners", return_value=[{"id": "x"}]) as fetch,
    ):
        out = tv_svc.list_screeners()
    assert out == {"items": [{"id": "x"}]}
    fetch.assert_called_once()


def test_create_screener_defaults() -> None:
    with (
        patch.object(tv_svc.tvdb, "count_screeners", return_value=1),
        patch.object(tv_svc.tvdb, "upsert_screener") as upsert,
    ):
        out = tv_svc.create_screener(name="  My Pick  ", url="  ")
    assert out["id"]
    kw = upsert.call_args.kwargs
    assert kw["name"] == "My Pick"
    assert kw["url"] == ""
    assert kw["enabled"] is True
    assert kw["mode"] == "chrome"


def test_create_screener_from_template_unknown_raises() -> None:
    with (
        patch.object(tv_svc.tvdb, "count_screeners", return_value=1),
        patch(
            "data_sync_service.tv.templates.get_template", return_value=None
        ),
    ):
        try:
            tv_svc.create_screener_from_template(template_id="nope")
            raise AssertionError("expected HTTPException")
        except HTTPException as e:
            assert e.status_code == 400


def test_create_screener_from_template_known() -> None:
    class _T:
        display_name = "T"
        market = "CN"
        filter_json = {"a": 1}
        api_columns = ["c1"]

    with (
        patch.object(tv_svc.tvdb, "count_screeners", return_value=1),
        patch("data_sync_service.tv.templates.get_template", return_value=_T()),
        patch.object(tv_svc.tvdb, "upsert_screener") as upsert,
    ):
        out = tv_svc.create_screener_from_template(template_id="t1")
    assert out["id"]
    assert upsert.call_args.kwargs["mode"] == "api"
    assert upsert.call_args.kwargs["market"] == "CN"


def test_list_screener_templates_delegates() -> None:
    class _T:
        template_id = "t1"
        display_name = "T1"
        market = "CN"
        description = "d"
        nested_filter_validated = True
        screen_title_substr = "s"

    with patch(
        "data_sync_service.tv.templates.list_templates", return_value=[_T()]
    ):
        out = tv_svc.list_screener_templates()
    assert out["items"][0]["templateId"] == "t1"


def test_update_screener_ok_and_404() -> None:
    with (
        patch.object(tv_svc.tvdb, "count_screeners", return_value=1),
        patch.object(tv_svc.tvdb, "update_screener", return_value=True) as upd,
    ):
        out = tv_svc.update_screener(screener_id="s1", name="New")
    assert out == {"ok": True}
    assert upd.call_args.kwargs["name"] == "New"

    with (
        patch.object(tv_svc.tvdb, "count_screeners", return_value=1),
        patch.object(tv_svc.tvdb, "update_screener", return_value=False),
    ):
        try:
            tv_svc.update_screener(screener_id="s1")
            raise AssertionError("expected 404")
        except HTTPException as e:
            assert e.status_code == 404


def test_delete_screener_ok_and_404() -> None:
    with (
        patch.object(tv_svc.tvdb, "count_screeners", return_value=1),
        patch.object(tv_svc.tvdb, "delete_screener", return_value=True) as deleter,
    ):
        out = tv_svc.delete_screener(screener_id="s1")
    assert out == {"ok": True}
    deleter.assert_called_once_with("s1")

    with (
        patch.object(tv_svc.tvdb, "count_screeners", return_value=1),
        patch.object(tv_svc.tvdb, "delete_screener", return_value=False),
    ):
        try:
            tv_svc.delete_screener(screener_id="s1")
            raise AssertionError("expected 404")
        except HTTPException as e:
            assert e.status_code == 404


def test_list_snapshots_requires_id_and_404() -> None:
    with (
        patch.object(tv_svc.tvdb, "count_screeners", return_value=1),
        patch.object(tv_svc.tvdb, "fetch_screener_by_id", return_value=None),
    ):
        try:
            tv_svc.list_snapshots(screener_id="s1")
            raise AssertionError("expected 404")
        except HTTPException as e:
            assert e.status_code == 404

    try:
        tv_svc.list_snapshots(screener_id="")
        raise AssertionError("expected 400")
    except HTTPException as e:
        assert e.status_code == 400


def test_list_snapshots_and_get_snapshot() -> None:
    with (
        patch.object(tv_svc.tvdb, "count_screeners", return_value=1),
        patch.object(tv_svc.tvdb, "fetch_screener_by_id", return_value={"id": "s1"}),
        patch.object(
            tv_svc.tvdb, "list_snapshots_for_screener", return_value=[{"id": "sn"}]
        ) as fs,
    ):
        out = tv_svc.list_snapshots(screener_id="s1", limit=5)
    assert out == {"items": [{"id": "sn"}]}
    assert fs.call_args.kwargs["limit"] == 5

    with (
        patch.object(tv_svc.tvdb, "count_screeners", return_value=1),
        patch.object(tv_svc.tvdb, "fetch_snapshot_detail", return_value={"id": "sn"}) as fsn,
    ):
        assert tv_svc.get_snapshot(snapshot_id="sn") == {"id": "sn"}
    fsn.assert_called_once_with("sn")

    with (
        patch.object(tv_svc.tvdb, "count_screeners", return_value=1),
        patch.object(tv_svc.tvdb, "fetch_snapshot_detail", return_value=None),
    ):
        try:
            tv_svc.get_snapshot(snapshot_id="sn")
            raise AssertionError("expected 404")
        except HTTPException as e:
            assert e.status_code == 404


def test_screener_history_groups_by_local_date() -> None:
    items = [
        {
            "snapshotId": "sn-1",
            "capturedAt": "2026-08-07T09:30:00+00:00",  # 17:30 Shanghai → pm
            "rowCount": 100,
            "screenTitle": "T",
            "filters": [],
        },
        {
            "snapshotId": "sn-2",
            "capturedAt": "2026-08-07T01:30:00+00:00",  # 09:30 Shanghai → am
            "rowCount": 90,
            "screenTitle": "T",
            "filters": [],
        },
    ]
    with (
        patch.object(tv_svc.tvdb, "count_screeners", return_value=1),
        patch.object(tv_svc.tvdb, "fetch_screener_by_id", return_value={"id": "s1", "name": "N"}),
        patch.object(tv_svc.tvdb, "list_snapshots_for_screener_full", return_value=items),
    ):
        out = tv_svc.screener_history(screener_id="s1", days=7)
    assert out["screenerName"] == "N"
    assert out["rows"][0]["date"] == "2026-08-07"
    assert out["rows"][0]["am"]["rowCount"] == 90
    assert out["rows"][0]["pm"]["rowCount"] == 100


def test_screener_history_no_snapshots_uses_today() -> None:
    with (
        patch.object(tv_svc.tvdb, "count_screeners", return_value=1),
        patch.object(tv_svc.tvdb, "fetch_screener_by_id", return_value={"id": "s1", "name": "N"}),
        patch.object(tv_svc.tvdb, "list_snapshots_for_screener_full", return_value=[]),
    ):
        out = tv_svc.screener_history(screener_id="s1", days=7)
    assert len(out["rows"]) == 1
    assert out["rows"][0]["am"] is None
"""tv.py: history, validation, dispatch, migration."""

import sqlite3  # noqa: E402

import pytest  # noqa: E402

from data_sync_service.service import tv as tvmod  # noqa: E402


def test_parse_iso_datetime() -> None:
    assert tvmod._parse_iso_datetime("2026-08-07T10:30:00Z") is not None
    assert tvmod._parse_iso_datetime("2026-08-07T10:30:00+08:00") is not None
    assert tvmod._parse_iso_datetime("  ") is None
    assert tvmod._parse_iso_datetime("garbage") is None


def test_tv_local_date_and_slot() -> None:
    d, slot = tvmod._tv_local_date_and_slot("2026-08-07T03:30:00Z")  # 11:30 +08
    assert slot == "am"
    d2, slot2 = tvmod._tv_local_date_and_slot("2026-08-07T04:00:00Z")  # 12:00 +08
    assert slot2 == "pm"
    d3, slot3 = tvmod._tv_local_date_and_slot("")
    assert slot3 == "unknown"


def test_screener_history_basic(monkeypatch) -> None:
    monkeypatch.setattr(tvmod, "ensure_seeded", lambda: None)
    monkeypatch.setattr(tvmod.tvdb, "fetch_screener_by_id", lambda sid: {"id": "s1", "name": "N"})
    monkeypatch.setattr(tvmod.tvdb, "list_snapshots_for_screener_full", lambda sid, limit=200: [
        {"snapshotId": "a", "capturedAt": "2026-08-07T02:00:00Z", "rowCount": 5, "screenTitle": "T", "filters": ["x > 1"]},
        {"snapshotId": "b", "capturedAt": "2026-08-07T06:00:00Z", "rowCount": 3, "screenTitle": "T", "filters": []},
        {"snapshotId": "c", "capturedAt": "2026-08-06T04:00:00Z", "rowCount": 9, "screenTitle": "T", "filters": []},
    ])
    out = tvmod.screener_history(screener_id="s1", days=10)
    assert out["screenerId"] == "s1"
    assert out["days"] == 10
    assert len(out["rows"]) == 2
    am_row = [r for r in out["rows"] if r["date"] == "2026-08-07"][0]
    assert am_row["am"]["snapshotId"] == "a"
    assert am_row["pm"]["snapshotId"] == "b"
    assert am_row["pm"]["rowCount"] == 3


def test_screener_history_validation(monkeypatch) -> None:
    monkeypatch.setattr(tvmod, "ensure_seeded", lambda: None)
    with pytest.raises(HTTPException) as e1:
        tvmod.screener_history(screener_id="  ")
    assert e1.value.status_code == 400
    monkeypatch.setattr(tvmod.tvdb, "fetch_screener_by_id", lambda sid: None)
    with pytest.raises(HTTPException) as e2:
        tvmod.screener_history(screener_id="s1")
    assert e2.value.status_code == 404


def test_screener_history_no_data_uses_today(monkeypatch) -> None:
    monkeypatch.setattr(tvmod, "ensure_seeded", lambda: None)
    monkeypatch.setattr(tvmod.tvdb, "fetch_screener_by_id", lambda sid: {"id": "s1", "name": "N"})
    monkeypatch.setattr(tvmod.tvdb, "list_snapshots_for_screener_full", lambda sid, limit=200: [])
    out = tvmod.screener_history(screener_id="s1", days=999)
    assert out["days"] == 30
    assert len(out["rows"]) == 1


def test_validate_screener_for_capture(monkeypatch) -> None:
    monkeypatch.setattr(tvmod, "ensure_seeded", lambda: None)
    with pytest.raises(HTTPException) as e0:
        tvmod._validate_screener_for_capture("")
    assert e0.value.status_code == 400
    monkeypatch.setattr(tvmod.tvdb, "fetch_screener_by_id", lambda sid: None)
    with pytest.raises(HTTPException) as e1:
        tvmod._validate_screener_for_capture("s1")
    assert e1.value.status_code == 404
    monkeypatch.setattr(tvmod.tvdb, "fetch_screener_by_id", lambda sid: {"enabled": False})
    with pytest.raises(HTTPException) as e2:
        tvmod._validate_screener_for_capture("s1")
    assert e2.value.status_code == 409
    monkeypatch.setattr(tvmod.tvdb, "fetch_screener_by_id", lambda sid: {"enabled": True, "mode": "chrome", "url": ""})
    with pytest.raises(HTTPException) as e3:
        tvmod._validate_screener_for_capture("s1")
    assert e3.value.status_code == 400
    monkeypatch.setattr(tvmod.tvdb, "fetch_screener_by_id", lambda sid: {"enabled": True, "mode": "chrome", "url": "ftp://x"})
    with pytest.raises(HTTPException) as e4:
        tvmod._validate_screener_for_capture("s1")
    assert e4.value.status_code == 400
    monkeypatch.setattr(tvmod.tvdb, "fetch_screener_by_id", lambda sid: {"enabled": True, "mode": "api", "url": ""})
    assert tvmod._validate_screener_for_capture("s1") is not None


def test_migrate_from_sqlite(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "karios.sqlite3"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE tv_screeners (id TEXT, name TEXT, url TEXT, enabled INTEGER, created_at TEXT, updated_at TEXT)")
    conn.execute("CREATE TABLE tv_screener_snapshots (id TEXT, screener_id TEXT, captured_at TEXT, row_count INTEGER, rows_json TEXT)")
    conn.execute("INSERT INTO tv_screeners VALUES ('s1', 'N1', 'http://a', 1, 't0', 't1')")
    conn.execute("INSERT INTO tv_screener_snapshots VALUES ('p1', 's1', '2026-08-07T00:00:00Z', 4, '{\"rows\": [1]}')")
    conn.execute("INSERT INTO tv_screener_snapshots VALUES ('p2', 's1', '2026-08-07T01:00:00Z', 2, 'not-json')")
    conn.commit()
    conn.close()

    monkeypatch.setattr(tvmod, "ensure_seeded", lambda: None)
    upserted = []
    monkeypatch.setattr(tvmod.tvdb, "upsert_screener", lambda **kw: upserted.append(("s", kw["screener_id"], kw["enabled"])))
    monkeypatch.setattr(tvmod.tvdb, "upsert_snapshot", lambda **kw: upserted.append(("p", kw["snapshot_id"], kw["payload"])))

    out = tvmod.migrate_from_sqlite(sqlite_path=str(db_path))
    assert out["screenersUpserted"] == 1
    assert out["snapshotsUpserted"] == 2
    assert upserted[0][2] is True
    payload = [u for u in upserted if u[0] == "p" and u[1] == "p2"][0][2]
    assert payload == {}

    with pytest.raises(HTTPException) as e:
        tvmod.migrate_from_sqlite(sqlite_path=str(tmp_path / "missing.sqlite3"))
    assert e.value.status_code == 404


def test_dispatch_capture_api_ok(monkeypatch) -> None:
    cap = type("C", (), {"url": "scanner_api://x", "captured_at": "t", "screen_title": "T", "filters": ["a"], "headers": [], "rows": []})()
    monkeypatch.setattr(tvmod, "_capture_via_api", lambda **kw: (cap, "api"))
    out = tvmod._dispatch_capture(mode="api", url="scanner_api://api-screener", filter_json=[{"left": "a"}], api_columns=["col"])
    assert out[1] == "api"


def test_dispatch_capture_api_backstops(monkeypatch) -> None:
    with pytest.raises(HTTPException) as e:
        tvmod._dispatch_capture(mode="api", url="", filter_json=None, api_columns=None)
    assert e.value.status_code == 409
    with pytest.raises(HTTPException) as e2:
        tvmod._dispatch_capture(mode="api", url="", filter_json=[{"left": "x"}], api_columns=None)
    assert e2.value.status_code in (409, 422)


def test_dispatch_capture_api_fallback_chain(monkeypatch) -> None:
    class Transient(tvmod.scanner_api.TransientApiError):
        pass

    cap = object()
    monkeypatch.setattr(tvmod, "_capture_via_api", lambda **kw: (_ for _ in ()).throw(Transient("t")))
    monkeypatch.setattr(tvmod, "_capture_via_ego_lite", lambda url: (cap, "ego_lite"))
    out = tvmod._dispatch_capture(mode="api", url="http://x", filter_json=[{"left": "a"}], api_columns=[])
    assert out[1] == "ego_lite"

    def ego_fail(url):
        raise Transient("no playwright")

    monkeypatch.setattr(tvmod, "_capture_via_ego_lite", ego_fail)
    with pytest.raises(HTTPException) as e:
        tvmod._dispatch_capture(mode="api", url="http://x", filter_json=[{"left": "a"}], api_columns=[])
    assert e.value.status_code == 502

    monkeypatch.setattr(tvmod, "_capture_via_api", lambda **kw: (_ for _ in ()).throw(tvmod.scanner_api.PermanentApiError("p")))
    with pytest.raises(HTTPException) as e2:
        tvmod._dispatch_capture(mode="api", url="http://x", filter_json=[{"left": "a"}], api_columns=[])
    assert e2.value.status_code == 422


def test_dispatch_capture_chrome(monkeypatch) -> None:
    cap = object()
    monkeypatch.setattr(tvmod, "_capture_via_chrome", lambda url: (cap, "chrome"))
    assert tvmod._dispatch_capture(mode="chrome", url="http://x", filter_json=None, api_columns=None)[1] == "chrome"

    with pytest.raises(HTTPException) as e:
        tvmod._dispatch_capture(mode="chrome", url="", filter_json=None, api_columns=None)
    assert e.value.status_code == 409

    monkeypatch.setattr(tvmod, "_capture_via_chrome", lambda url: (_ for _ in ()).throw(RuntimeError("Cannot locate screener grid/table")))
    with pytest.raises(HTTPException) as e2:
        tvmod._dispatch_capture(mode="chrome", url="http://x", filter_json=None, api_columns=None)
    assert e2.value.status_code == 409

    monkeypatch.setattr(tvmod, "_capture_via_chrome", lambda url: (_ for _ in ()).throw(RuntimeError("boom")))
    with pytest.raises(HTTPException) as e3:
        tvmod._dispatch_capture(mode="chrome", url="http://x", filter_json=None, api_columns=None)
    assert e3.value.status_code == 500


def test_filters_from_filter_json() -> None:
    out = tvmod._filters_from_filter_json([
        {"left": "market_cap_basic", "operation": "egreater", "right": 100},
        {"left": "price", "operation": "greater", "right": {"operation": "subtract", "left": 10, "right": 2}},
        {"left": "pe", "operation": "not_equal", "right": 0},
    ])
    assert len(out) == 2
    assert out[0] == "market_cap_basic egreater 100"
    assert out[1] == "price greater 10-2"

    legacy = tvmod._filters_from_filter_json({"and": [{"left": "a", "operation": "equal", "right": 1}]})
    assert legacy == ["a equal 1"]
    assert tvmod._filters_from_filter_json([]) == []
    assert tvmod._filters_from_filter_json("not-a-dict") == []


def test_capture_via_ego_lite_paths(monkeypatch) -> None:
    import data_sync_service.tv.ego_lite as ego_mod

    cap = object()
    monkeypatch.setattr(ego_mod, "capture_screener_ego_lite_sync", lambda url: cap)
    assert tvmod._capture_via_ego_lite(url="http://x") == (cap, "ego_lite")

    def fail(url):
        raise ego_mod.EgoLiteUnavailable("no")

    monkeypatch.setattr(ego_mod, "capture_screener_ego_lite_sync", fail)
    with pytest.raises(tvmod.scanner_api.TransientApiError):
        tvmod._capture_via_ego_lite(url="http://x")


def test_capture_via_chrome(monkeypatch) -> None:
    cap = object()
    monkeypatch.setattr(tvmod, "_ensure_cdp_ready", lambda: "http://127.0.0.1:9222")
    monkeypatch.setattr(tvmod, "capture_screener_over_cdp_sync", lambda cdp_url, url: cap)
    assert tvmod._capture_via_chrome(url="http://x") == (cap, "chrome")
