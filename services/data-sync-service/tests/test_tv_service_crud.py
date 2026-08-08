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
