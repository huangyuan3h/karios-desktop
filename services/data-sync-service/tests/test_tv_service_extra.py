"""service/tv.py coverage: screener CRUD, history, migration, capture dispatch, jobs."""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from data_sync_service.service import tv as svc
from data_sync_service.tv import scanner_api
from data_sync_service.tv.capture import CaptureResult


class _FakeTvdb:
    def __init__(self):
        self.calls = []

    def count_screeners(self):
        return 0

    def upsert_screener(self, **kw):
        self.calls.append(("upsert_screener", kw))

    def fetch_screeners(self):
        return [{"id": "falcon"}]

    def update_screener(self, **kw):
        return True

    def delete_screener(self, sid):
        return True

    def fetch_screener_by_id(self, sid):
        if sid == "missing":
            return None
        return {"id": sid, "name": f"n-{sid}", "enabled": True, "mode": "api",
                "url": "", "filterJson": [{"left": "a", "operation": "greater", "right": 1}],
                "apiColumns": ["c1"]}

    def list_snapshots_for_screener(self, sid, limit=10):
        return [{"snapshotId": "s1"}]

    def fetch_snapshot_detail(self, sid):
        return {"snapshotId": sid} if sid != "missing-snap" else None

    def list_latest_snapshot_details_for_screeners(self, ids):
        return [{"screenerId": i} for i in ids]

    def list_snapshots_for_screener_full(self, sid, limit=200):
        return [
            {"snapshotId": "s1", "capturedAt": "2026-08-07T02:00:00Z", "rowCount": 3, "screenTitle": "T1", "filters": ["f"]},
            {"snapshotId": "s2", "capturedAt": "2026-08-07T08:00:00Z", "rowCount": 4, "screenTitle": "T2", "filters": []},
            {"snapshotId": "s3", "capturedAt": "2026-08-06T01:00:00Z", "rowCount": 5, "screenTitle": "T3", "filters": []},
        ]

    def upsert_snapshot(self, **kw):
        self.calls.append(("upsert_snapshot", kw))


@pytest.fixture(autouse=True)
def _patch_tvdb(monkeypatch):
    fake = _FakeTvdb()
    monkeypatch.setattr(svc, "tvdb", fake)
    return fake


class TestScreenerCrud:
    def test_ensure_seeded(self, monkeypatch) -> None:
        fake = _FakeTvdb()
        fake.count_screeners = lambda: 0
        monkeypatch.setattr(svc, "tvdb", fake)
        svc.ensure_seeded()
        assert [c[0] for c in fake.calls] == ["upsert_screener", "upsert_screener"]

    def test_ensure_seeded_skips(self, monkeypatch) -> None:
        fake = _FakeTvdb()
        fake.count_screeners = lambda: 5
        monkeypatch.setattr(svc, "tvdb", fake)
        svc.ensure_seeded()
        assert fake.calls == []

    def test_list_screeners(self) -> None:
        out = svc.list_screeners()
        assert out["items"] == [{"id": "falcon"}]

    def test_create_screener(self) -> None:
        out = svc.create_screener(name="  X  ", mode="api", market="cn", filter_json={"and": []}, api_columns=["a"])
        assert out["id"]

    def test_create_screener_template_ok(self, monkeypatch) -> None:
        from data_sync_service.tv import templates

        class T:
            display_name = "Tpl"
            market = "cn"
            filter_json = [{"left": "price", "operation": "greater", "right": 5}]
            api_columns = ["price", "name"]

        monkeypatch.setattr(templates, "get_template", lambda tid: T())
        out = svc.create_screener_from_template(template_id="t1")
        assert out["id"]

    def test_create_screener_template_unknown(self, monkeypatch) -> None:
        from data_sync_service.tv import templates

        monkeypatch.setattr(templates, "get_template", lambda tid: None)
        with pytest.raises(HTTPException) as ei:
            svc.create_screener_from_template(template_id="nope")
        assert ei.value.status_code == 400

    def test_list_screener_templates(self, monkeypatch) -> None:
        from data_sync_service.tv import templates

        class T:
            template_id = "t1"
            display_name = "D"
            market = "cn"
            description = "desc"
            nested_filter_validated = True
            screen_title_substr = None

        monkeypatch.setattr(templates, "list_templates", lambda: [T()])
        out = svc.list_screener_templates()
        assert out["items"][0]["templateId"] == "t1"

    def test_update_screener_ok(self) -> None:
        assert svc.update_screener(screener_id="x", name="n") == {"ok": True}

    def test_update_screener_404(self, monkeypatch) -> None:
        monkeypatch.setattr(svc.tvdb, "update_screener", lambda **kw: False)
        with pytest.raises(HTTPException) as ei:
            svc.update_screener(screener_id="x")
        assert ei.value.status_code == 404

    def test_delete_screener_ok(self) -> None:
        assert svc.delete_screener(screener_id="x") == {"ok": True}

    def test_delete_screener_404(self, monkeypatch) -> None:
        monkeypatch.setattr(svc.tvdb, "delete_screener", lambda sid: False)
        with pytest.raises(HTTPException) as ei:
            svc.delete_screener(screener_id="x")
        assert ei.value.status_code == 404

    def test_list_snapshots(self) -> None:
        out = svc.list_snapshots(screener_id="x")
        assert out["items"] == [{"snapshotId": "s1"}]

    def test_list_snapshots_400(self) -> None:
        with pytest.raises(HTTPException) as ei:
            svc.list_snapshots(screener_id="  ")
        assert ei.value.status_code == 400

    def test_list_snapshots_404(self) -> None:
        with pytest.raises(HTTPException) as ei:
            svc.list_snapshots(screener_id="missing")
        assert ei.value.status_code == 404

    def test_get_snapshot(self) -> None:
        assert svc.get_snapshot(snapshot_id="s1") == {"snapshotId": "s1"}

    def test_get_snapshot_404(self) -> None:
        with pytest.raises(HTTPException) as ei:
            svc.get_snapshot(snapshot_id="missing-snap")
        assert ei.value.status_code == 404

    def test_list_latest_snapshots_batch(self) -> None:
        out = svc.list_latest_snapshots_batch(screener_ids=["a", "b"])
        assert out["items"] == [{"screenerId": "a"}, {"screenerId": "b"}]


class TestHistory:
    def test_parse_iso_datetime(self) -> None:
        assert svc._parse_iso_datetime("") is None
        assert svc._parse_iso_datetime("2026-08-07T02:00:00Z") is not None
        assert svc._parse_iso_datetime("2026-08-07T02:00:00+00:00") is not None
        assert svc._parse_iso_datetime("garbage") is None

    def test_tv_local_date_and_slot(self) -> None:
        d, slot = svc._tv_local_date_and_slot("2026-08-07T02:00:00Z")
        assert slot == "am"
        d2, slot2 = svc._tv_local_date_and_slot("2026-08-07T08:00:00Z")
        assert slot2 == "pm"
        d3, slot3 = svc._tv_local_date_and_slot("bad")
        assert slot3 == "unknown" and len(d3) == 10

    def test_screener_history(self) -> None:
        out = svc.screener_history(screener_id="x", days=10)
        assert out["screenerId"] == "x"
        assert out["screenerName"] == "n-x"
        assert any(r["date"] == "2026-08-07" and r["am"] and r["pm"] for r in out["rows"])
        assert any(r["date"] == "2026-08-06" for r in out["rows"])

    def test_screener_history_400(self) -> None:
        with pytest.raises(HTTPException) as ei:
            svc.screener_history(screener_id="")
        assert ei.value.status_code == 400

    def test_screener_history_404(self) -> None:
        with pytest.raises(HTTPException) as ei:
            svc.screener_history(screener_id="missing")
        assert ei.value.status_code == 404

    def test_screener_history_days_clamped(self, monkeypatch) -> None:
        out = svc.screener_history(screener_id="x", days=999)
        assert out["days"] == 30

    def test_screener_history_empty_dates(self, monkeypatch) -> None:
        monkeypatch.setattr(svc.tvdb, "list_snapshots_for_screener_full", lambda sid, limit=200: [])
        out = svc.screener_history(screener_id="x")
        assert out["rows"] and out["rows"][0]["date"] == out["rows"][0]["date"]


class TestMigration:
    def test_migrate(self, monkeypatch, tmp_path) -> None:
        db = tmp_path / "k.sqlite3"
        import sqlite3

        with sqlite3.connect(str(db)) as conn:
            conn.execute("CREATE TABLE tv_screeners (id TEXT, name TEXT, url TEXT, enabled INT, created_at TEXT, updated_at TEXT)")
            conn.execute("CREATE TABLE tv_screener_snapshots (id TEXT, screener_id TEXT, captured_at TEXT, row_count INT, rows_json TEXT)")
            conn.execute("INSERT INTO tv_screeners VALUES ('f1', 'Name', 'url', 1, 't', 't')")
            conn.execute("INSERT INTO tv_screener_snapshots VALUES ('s1', 'f1', '2026-08-07T00:00:00Z', 2, '{\"rows\": [1]}')")
        out = svc.migrate_from_sqlite(sqlite_path=str(db))
        assert out["ok"] is True
        assert out["screenersUpserted"] == 1 and out["snapshotsUpserted"] == 1

    def test_migrate_missing_file(self) -> None:
        with pytest.raises(HTTPException) as ei:
            svc.migrate_from_sqlite(sqlite_path="/nonexistent/x.sqlite3")
        assert ei.value.status_code == 404

    def test_migrate_missing_tables(self, monkeypatch, tmp_path) -> None:
        db = tmp_path / "empty.sqlite3"
        import sqlite3

        with sqlite3.connect(str(db)):
            pass
        out = svc.migrate_from_sqlite(sqlite_path=str(db))
        assert out["screenersUpserted"] == 0 and out["snapshotsUpserted"] == 0

    def test_migrate_bad_payload(self, monkeypatch, tmp_path) -> None:
        db = tmp_path / "bad.sqlite3"
        import sqlite3

        with sqlite3.connect(str(db)) as conn:
            conn.execute("CREATE TABLE tv_screeners (id TEXT, name TEXT, url TEXT, enabled INT, created_at TEXT, updated_at TEXT)")
            conn.execute("CREATE TABLE tv_screener_snapshots (id TEXT, screener_id TEXT, captured_at TEXT, row_count INT, rows_json TEXT)")
            conn.execute("INSERT INTO tv_screeners VALUES ('f1', 'Name', 'url', NULL, NULL, NULL)")
            conn.execute("INSERT INTO tv_screener_snapshots VALUES ('s1', 'f1', NULL, NULL, 'not-json')")
        out = svc.migrate_from_sqlite(sqlite_path=str(db))
        assert out["screenersUpserted"] == 1 and out["snapshotsUpserted"] == 1


class TestValidateAndDispatch:
    def test_validate_ok(self) -> None:
        s = svc._validate_screener_for_capture("x")
        assert s["id"] == "x"

    def test_validate_400(self) -> None:
        with pytest.raises(HTTPException) as ei:
            svc._validate_screener_for_capture("  ")
        assert ei.value.status_code == 400

    def test_validate_404(self) -> None:
        with pytest.raises(HTTPException) as ei:
            svc._validate_screener_for_capture("missing")
        assert ei.value.status_code == 404

    def test_validate_disabled(self, monkeypatch) -> None:
        monkeypatch.setattr(svc.tvdb, "fetch_screener_by_id", lambda sid: {"id": "d", "enabled": False, "mode": "chrome", "url": "u"})
        with pytest.raises(HTTPException) as ei:
            svc._validate_screener_for_capture("d")
        assert ei.value.status_code == 409

    def test_validate_url_empty_chrome(self, monkeypatch) -> None:
        monkeypatch.setattr(svc.tvdb, "fetch_screener_by_id", lambda sid: {"id": "d", "enabled": True, "mode": "chrome", "url": ""})
        with pytest.raises(HTTPException) as ei:
            svc._validate_screener_for_capture("d")
        assert ei.value.status_code == 400

    def test_validate_url_scheme(self, monkeypatch) -> None:
        monkeypatch.setattr(svc.tvdb, "fetch_screener_by_id", lambda sid: {"id": "d", "enabled": True, "mode": "chrome", "url": "ftp://x"})
        with pytest.raises(HTTPException) as ei:
            svc._validate_screener_for_capture("d")
        assert ei.value.status_code == 400

    def test_validate_api_skips_url(self, monkeypatch) -> None:
        monkeypatch.setattr(svc.tvdb, "fetch_screener_by_id", lambda sid: {"id": "d", "enabled": True, "mode": "api", "url": ""})
        assert svc._validate_screener_for_capture("d")["id"] == "d"

    def test_filters_from_filter_json(self) -> None:
        fj = [
            {"left": "price", "operation": "greater", "right": 5},
            {"and": [{"left": "vol", "operation": "less", "right": {"operation": "mult", "left": 2, "right": 3}}]},
            {"or": [{"left": "x", "operation": "equal", "right": {"operation": "subtract", "left": 10, "right": 1}}]},
            {"left": "other", "operation": "not_supported", "right": 1},
            [{"left": "nested", "operation": "in_range", "right": [1, 2]}],
        ]
        out = svc._filters_from_filter_json(fj)
        assert any("price greater 5" in f for f in out)
        assert any("vol less 2*3" in f for f in out)
        assert any("x equal 10-1" in f for f in out)

    def test_op_str(self) -> None:
        assert svc._op_str({"operation": "mult", "left": 2, "right": 3}) == "2*3"
        assert svc._op_str({"operation": "subtract", "left": 2, "right": 3}) == "2-3"
        assert svc._op_str({"right": "x"}) == "x"

    def test_infer_screen_title(self) -> None:
        assert svc._infer_screen_title_from_url("") is None
        assert svc._infer_screen_title_from_url("scanner_api://x") is None
        assert svc._infer_screen_title_from_url("https://tradingview.com/x") is None


class TestCaptureDispatch:
    def _cap(self, rows=None, filters=None, **kw):
        return CaptureResult(url="u", captured_at="2026-08-07T00:00:00Z", screen_title="T", filters=filters or [], headers=[], rows=rows or [], **kw)

    def test_capture_via_api_ok(self, monkeypatch) -> None:
        monkeypatch.setattr(svc.scanner_api, "fetch_screener_via_api", lambda **kw: type("R", (), {"headers": ["h"], "raw_rows": [["a"]], "captured_at": "t"})())
        monkeypatch.setattr(svc.scanner_api, "internal_to_friendly_rows", lambda h, r: (h, r))
        cap, via = svc._capture_via_api(filter_json=[{"left": "a", "operation": "greater", "right": 1}], api_columns=[], url="u", screen_title=None)
        assert via == "api" and cap.screen_title == "TradingView Scanner"

    def test_capture_via_ego_lite_ok(self, monkeypatch) -> None:
        from data_sync_service.tv import ego_lite

        monkeypatch.setattr(ego_lite, "capture_screener_ego_lite_sync", lambda url: self._cap())
        cap, via = svc._capture_via_ego_lite(url="u")
        assert via == "ego_lite"

    def test_capture_via_ego_lite_unavailable(self, monkeypatch) -> None:
        from data_sync_service.tv import ego_lite

        class Unavail(Exception):
            pass

        monkeypatch.setattr(ego_lite, "EgoLiteUnavailable", Unavail)
        monkeypatch.setattr(ego_lite, "capture_screener_ego_lite_sync", lambda url: (_ for _ in ()).throw(Unavail("no playwright")))
        with pytest.raises(scanner_api.TransientApiError):
            svc._capture_via_ego_lite(url="u")

    def test_capture_via_chrome(self, monkeypatch) -> None:
        monkeypatch.setattr(svc, "_ensure_cdp_ready", lambda: "http://127.0.0.1:9222")
        monkeypatch.setattr(svc, "capture_screener_over_cdp_sync", lambda cdp_url, url: self._cap())
        cap, via = svc._capture_via_chrome(url="u")
        assert via == "chrome"

    def test_ensure_cdp_ready_ok(self, monkeypatch) -> None:
        class St:
            cdpOk = True
            host = "127.0.0.1"
            port = 9222

        monkeypatch.setattr(svc.tv_chrome, "status", lambda: St())
        assert svc._ensure_cdp_ready() == "http://127.0.0.1:9222"

    def test_ensure_cdp_ready_autostart(self, monkeypatch) -> None:
        class St:
            def __init__(self, ok):
                self.cdpOk = ok
                self.host = "127.0.0.1"
                self.port = 9222
                self.userDataDir = "/ud"
                self.port = 9222

        st = [St(False), St(True)]
        monkeypatch.setattr(svc.tv_chrome, "status", lambda: st.pop(0))
        monkeypatch.setattr(svc.tv_chrome, "get_setting", lambda k: "")
        monkeypatch.setattr(svc.tv_chrome, "start", lambda **kw: None)
        assert svc._ensure_cdp_ready() == "http://127.0.0.1:9222"

    def test_ensure_cdp_ready_fail(self, monkeypatch) -> None:
        class St:
            cdpOk = False
            host = "127.0.0.1"
            port = 9222
            userDataDir = "/ud"

        monkeypatch.setattr(svc.tv_chrome, "status", lambda: St())
        monkeypatch.setattr(svc.tv_chrome, "get_setting", lambda k: "")
        monkeypatch.setattr(svc.tv_chrome, "start", lambda **kw: None)
        with pytest.raises(HTTPException) as ei:
            svc._ensure_cdp_ready()
        assert ei.value.status_code == 409

    def test_dispatch_api_ok(self, monkeypatch) -> None:
        monkeypatch.setattr(svc, "_capture_via_api", lambda **kw: (self._cap(), "api"))
        cap, via = svc._dispatch_capture(mode="api", url="", filter_json=[{"left": "a", "operation": "greater", "right": 1}], api_columns=None)
        assert via == "api"

    def test_dispatch_api_no_filter(self) -> None:
        with pytest.raises(HTTPException) as ei:
            svc._dispatch_capture(mode="api", url="", filter_json=None, api_columns=None)
        assert ei.value.status_code == 409

    def test_dispatch_api_transient_then_ego(self, monkeypatch) -> None:
        monkeypatch.setattr(svc, "_capture_via_api", lambda **kw: (_ for _ in ()).throw(scanner_api.TransientApiError("t")))
        monkeypatch.setattr(svc, "_capture_via_ego_lite", lambda url: (self._cap(), "ego_lite"))
        cap, via = svc._dispatch_capture(mode="api", url="u", filter_json=[{}], api_columns=None)
        assert via == "ego_lite"

    def test_dispatch_api_transient_fail(self, monkeypatch) -> None:
        monkeypatch.setattr(svc, "_capture_via_api", lambda **kw: (_ for _ in ()).throw(scanner_api.TransientApiError("t")))
        monkeypatch.setattr(svc, "_capture_via_ego_lite", lambda url: (_ for _ in ()).throw(scanner_api.TransientApiError("e")))
        with pytest.raises(HTTPException) as ei:
            svc._dispatch_capture(mode="api", url="u", filter_json=[{}], api_columns=None)
        assert ei.value.status_code == 502

    def test_dispatch_api_permanent(self, monkeypatch) -> None:
        monkeypatch.setattr(svc, "_capture_via_api", lambda **kw: (_ for _ in ()).throw(scanner_api.PermanentApiError("p")))
        with pytest.raises(HTTPException) as ei:
            svc._dispatch_capture(mode="api", url="u", filter_json=[{}], api_columns=None)
        assert ei.value.status_code == 422

    def test_dispatch_chrome_no_url(self) -> None:
        with pytest.raises(HTTPException) as ei:
            svc._dispatch_capture(mode="chrome", url="", filter_json=None, api_columns=None)
        assert ei.value.status_code == 409

    def test_dispatch_chrome_ok(self, monkeypatch) -> None:
        monkeypatch.setattr(svc, "_capture_via_chrome", lambda url: (self._cap(), "chrome"))
        cap, via = svc._dispatch_capture(mode="chrome", url="u", filter_json=None, api_columns=None)
        assert via == "chrome"

    def test_dispatch_chrome_specific_error(self, monkeypatch) -> None:
        monkeypatch.setattr(svc, "_capture_via_chrome", lambda url: (_ for _ in ()).throw(RuntimeError("Cannot locate screener grid/table")))
        with pytest.raises(HTTPException) as ei:
            svc._dispatch_capture(mode="chrome", url="u", filter_json=None, api_columns=None)
        assert ei.value.status_code == 409

    def test_dispatch_chrome_generic_error(self, monkeypatch) -> None:
        monkeypatch.setattr(svc, "_capture_via_chrome", lambda url: (_ for _ in ()).throw(RuntimeError("boom")))
        with pytest.raises(HTTPException) as ei:
            svc._dispatch_capture(mode="chrome", url="u", filter_json=None, api_columns=None)
        assert ei.value.status_code == 500

    def test_capture_and_persist(self, monkeypatch) -> None:
        monkeypatch.setattr(svc, "_validate_screener_for_capture", lambda sid: {"id": "x", "url": "u", "mode": "api",
                                                                                "filterJson": [{"left": "a", "operation": "greater", "right": 1}], "apiColumns": ["c"]})
        monkeypatch.setattr(svc, "_dispatch_capture", lambda **kw: (self._cap(rows=[[1]], filters=["f"]), "api"))
        out = svc._capture_and_persist_screener(screener_id="x")
        assert out["screenerId"] == "x" and out["capturedVia"] == "api"


class TestJobs:
    def test_job_to_api(self) -> None:
        j = {"id": 1, "screener_id": "s", "status": "pending", "trigger_source": "cron", "created_at": "t", "row_count": 2, "error_message": None}
        api = svc.job_to_api(j)
        assert api["jobId"] == "1" and api["status"] == "pending" and api["trigger"] == "cron"

    def test_enqueue(self, monkeypatch) -> None:
        from data_sync_service.service import tv_capture_worker as tcw

        monkeypatch.setattr(svc, "_validate_screener_for_capture", lambda sid: {"id": "x"})
        monkeypatch.setattr(svc.jobdb, "enqueue_or_get_active", lambda screener_id, trigger_source: {"id": 1, "screener_id": "x", "status": "queued"})
        monkeypatch.setattr(tcw, "wake_tv_capture_worker", lambda: None)
        out = svc.enqueue_screener_capture(screener_id="x")
        assert out["status"] == "queued"

    def test_get_capture_job(self, monkeypatch) -> None:
        monkeypatch.setattr(svc.jobdb, "get_job", lambda jid: {"id": 1, "screener_id": "x", "status": "done"})
        assert svc.get_capture_job("1")["jobId"] == "1"

    def test_get_capture_job_404(self, monkeypatch) -> None:
        monkeypatch.setattr(svc.jobdb, "get_job", lambda jid: None)
        with pytest.raises(HTTPException) as ei:
            svc.get_capture_job("1")
        assert ei.value.status_code == 404

    def test_list_capture_jobs(self, monkeypatch) -> None:
        monkeypatch.setattr(svc.jobdb, "list_jobs", lambda screener_id=None, limit=20: [{"id": 1}])
        out = svc.list_capture_jobs(screener_id="x", limit=5)
        assert out["items"][0]["jobId"] == "1"

    def test_process_capture_job_ok(self, monkeypatch) -> None:
        monkeypatch.setattr(svc.jobdb, "get_job", lambda jid: {"id": 1, "screener_id": "x"})
        monkeypatch.setattr(svc, "_capture_and_persist_screener", lambda screener_id: {"snapshotId": "sn", "rowCount": 3})
        monkeypatch.setattr(svc.jobdb, "mark_done", lambda **kw: None)
        out = svc.process_capture_job("1")
        assert out["snapshotId"] == "sn"

    def test_process_capture_job_http_error(self, monkeypatch) -> None:
        monkeypatch.setattr(svc.jobdb, "get_job", lambda jid: {"id": 1, "screener_id": "x"})
        monkeypatch.setattr(svc, "_capture_and_persist_screener", lambda screener_id: (_ for _ in ()).throw(HTTPException(status_code=409, detail="disabled")))
        marked = {}
        monkeypatch.setattr(svc.jobdb, "mark_failed", lambda job_id, error_message: marked.update({"id": job_id, "err": error_message}))
        with pytest.raises(HTTPException):
            svc.process_capture_job("1")
        assert marked["err"] == "disabled"

    def test_process_capture_job_404(self, monkeypatch) -> None:
        monkeypatch.setattr(svc.jobdb, "get_job", lambda jid: None)
        with pytest.raises(HTTPException) as ei:
            svc.process_capture_job("1")
        assert ei.value.status_code == 404

    def test_process_capture_job_generic_error(self, monkeypatch) -> None:
        monkeypatch.setattr(svc.jobdb, "get_job", lambda jid: {"id": 1, "screener_id": "x"})
        monkeypatch.setattr(svc, "_capture_and_persist_screener", lambda screener_id: (_ for _ in ()).throw(RuntimeError("boom")))
        marked = {}
        monkeypatch.setattr(svc.jobdb, "mark_failed", lambda job_id, error_message: marked.update({"id": job_id, "err": error_message}))
        with pytest.raises(RuntimeError):
            svc.process_capture_job("1")
        assert "boom" in marked["err"]

    def test_wait_for_capture_jobs_empty(self, monkeypatch) -> None:
        assert svc.wait_for_capture_jobs([]) == []
        assert svc.wait_for_capture_jobs([" ", None]) == []

    def test_wait_for_capture_jobs_done(self, monkeypatch) -> None:
        monkeypatch.setattr(svc.jobdb, "get_job", lambda jid: {"id": jid, "screener_id": "x", "status": "done", "row_count": 2})
        out = svc.wait_for_capture_jobs(["1", "2"])
        assert [j["status"] for j in out] == ["done", "done"]

    def test_wait_for_capture_jobs_on_update(self, monkeypatch) -> None:
        statuses = {"1": ["queued", "running", "done"]}

        def get_job(jid):
            return {"id": jid, "screener_id": "x", "status": statuses[jid].pop(0)}

        monkeypatch.setattr(svc.jobdb, "get_job", get_job)
        seen = []
        svc.wait_for_capture_jobs(["1"], poll_s=0.01, on_update=lambda api: seen.append(api["status"]))
        assert "queued" in seen and "running" in seen

    def test_wait_for_capture_jobs_404(self, monkeypatch) -> None:
        monkeypatch.setattr(svc.jobdb, "get_job", lambda jid: None)
        with pytest.raises(HTTPException) as ei:
            svc.wait_for_capture_jobs(["1"])
        assert ei.value.status_code == 404

    def test_wait_for_capture_jobs_timeout(self, monkeypatch) -> None:
        monkeypatch.setattr(svc.jobdb, "get_job", lambda jid: {"id": jid, "screener_id": "x", "status": "queued"})
        with pytest.raises(HTTPException) as ei:
            svc.wait_for_capture_jobs(["1"], timeout_s=0.01, poll_s=0.01)
        assert ei.value.status_code == 504

    def test_sync_screener_done(self, monkeypatch) -> None:
        monkeypatch.setattr(svc, "enqueue_screener_capture", lambda screener_id, trigger: {"jobId": "1"})
        monkeypatch.setattr(svc, "wait_for_capture_jobs", lambda ids, timeout_s=None: [{"jobId": "1", "status": "done", "snapshotId": "sn", "finishedAt": "t", "rowCount": 2}])
        out = svc.sync_screener(screener_id="x")
        assert out["snapshotId"] == "sn"

    def test_sync_screener_failed(self, monkeypatch) -> None:
        monkeypatch.setattr(svc, "enqueue_screener_capture", lambda screener_id, trigger: {"jobId": "1"})
        monkeypatch.setattr(svc, "wait_for_capture_jobs", lambda ids, timeout_s=None: [{"status": "failed", "error": "bad"}])
        with pytest.raises(HTTPException) as ei:
            svc.sync_screener(screener_id="x")
        assert ei.value.status_code == 500

    def test_sync_screener_not_done(self, monkeypatch) -> None:
        monkeypatch.setattr(svc, "enqueue_screener_capture", lambda screener_id, trigger: {"jobId": "1"})
        monkeypatch.setattr(svc, "wait_for_capture_jobs", lambda ids, timeout_s=None: [{"status": "queued"}])
        with pytest.raises(HTTPException) as ei:
            svc.sync_screener(screener_id="x")
        assert ei.value.status_code == 504
