"""OPT-209: strategy-pool automation (S-3 + 星舰 add / invalidation GC).

Pure-unit coverage: the DB seams (``upsert_registry`` / ``ack_run``) and the
data builders are patched so these run without Postgres.
"""

from __future__ import annotations

from unittest.mock import patch

from data_sync_service.service import watchlist_automation as wa

DAY = "2026-09-15"
PREV = "2026-09-14"


class TestS3PoolAdditions:
    def test_compute_s3_pool_tags_markets_and_source(self) -> None:
        calls: list[dict] = []

        def fake_build(**kwargs):
            calls.append(kwargs)
            market = kwargs.get("market")
            return [{"symbol": f"{market}:600001", "score": 90.0, "rs": 0.8, "ts_code": "600001.SH"}]

        with patch("data_sync_service.service.paper_s3.build_s3_candidates", fake_build):
            out = wa.compute_s3_pool(day=DAY)
        assert {c["symbol"] for c in out} == {"CN:600001", "HK:600001"}
        assert all(c["source"] == wa.SOURCE_S3 for c in out)
        assert {c["market"] for c in out} == {"CN", "HK"}
        assert all(c["gate_mode"] == "pool" for c in calls)

    def test_compute_s3_pool_survives_market_failure(self) -> None:
        def fake_build(**kwargs):
            if kwargs.get("market") == "HK":
                raise RuntimeError("boom")
            return [{"symbol": "CN:600001", "score": 90.0, "rs": 0.8, "ts_code": "600001.SH"}]

        with patch("data_sync_service.service.paper_s3.build_s3_candidates", fake_build):
            out = wa.compute_s3_pool(day=DAY)
        assert [c["symbol"] for c in out] == ["CN:600001"]


class TestSatellitePoolAdditions:
    def test_compute_satellite_pool_maps_legs(self) -> None:
        replay = {
            "decisionAvailable": True,
            "decisionUnavailableDays": [],
            "openPositions": [
                {
                    "ts": "300906.SZ",
                    "entryDate": "2026-09-11",
                    "exitDue": "2026-09-15",
                    "heldDays": 3,
                    "daysLeft": 1,
                },
                {"ts": "", "entryDate": "2026-09-11"},
            ]
        }
        with patch(
            "data_sync_service.service.state_bucket_track.build_state_bucket_timeline",
            lambda **kw: replay,
        ):
            out = wa.compute_satellite_pool(day=DAY)
        assert out == [
            {
                "symbol": "CN:300906",
                "ts_code": "300906.SZ",
                "name": None,
                "source": wa.SOURCE_SATELLITE,
                "entryDate": "2026-09-11",
                "exitDue": "2026-09-15",
                "heldDays": 3,
                "daysLeft": 1,
            }
        ]

    def test_compute_satellite_pool_returns_none_on_incomplete_panel(self) -> None:
        with patch(
            "data_sync_service.service.state_bucket_track.build_state_bucket_timeline",
            lambda **kw: {
                "decisionAvailable": False,
                "decisionUnavailableDays": [DAY],
                "openPositions": [{"ts": "300906.SZ"}],
            },
        ):
            assert wa.compute_satellite_pool(day=DAY) is None

    def test_compute_satellite_pool_returns_none_on_failure(self) -> None:
        """Replay failure is distinguishable from "no legs" — the removal
        layer must fail open instead of wiping every satellite row."""

        def boom(**kw):
            raise RuntimeError("replay down")

        with patch(
            "data_sync_service.service.state_bucket_track.build_state_bucket_timeline", boom
        ):
            assert wa.compute_satellite_pool(day=DAY) is None


class TestPoolRemovals:
    def _registry(self):
        return [
            {"symbol": "CN:600001", "source": wa.SOURCE_S3},
            {"symbol": "CN:600002", "source": wa.SOURCE_S3, "positionPct": 10},
            {"symbol": "CN:600003", "source": wa.SOURCE_S3},
            {"symbol": "CN:300906", "source": wa.SOURCE_SATELLITE},
            {"symbol": "CN:300907", "source": wa.SOURCE_SATELLITE},
            {"symbol": "CN:000001", "source": "manual"},
            {"symbol": "US:AAPL", "source": "alpha_radar"},
        ]

    def test_two_day_s3_fail_removes_and_satellite_exit_removes(self) -> None:
        out = wa.compute_pool_removals(
            self._registry(),
            s3_symbols_today={"CN:600003"},
            s3_symbols_prev={"CN:600002", "CN:600003"},
            satellite_symbols_today={"CN:300907"},
        )
        by_sym = {x["symbol"]: x["reason"] for x in out}
        # 600001 fails today and prev -> removed
        assert by_sym == {"CN:600001": "s3_failed_2d", "CN:300906": "satellite_exited"}

    def test_fail_once_keeps_the_row(self) -> None:
        out = wa.compute_pool_removals(
            self._registry(),
            s3_symbols_today=set(),
            s3_symbols_prev={"CN:600001", "CN:600002", "CN:600003"},
            satellite_symbols_today={"CN:300906", "CN:300907"},
        )
        assert out == []

    def test_unknown_prev_session_fails_open(self) -> None:
        out = wa.compute_pool_removals(
            self._registry(),
            s3_symbols_today=set(),
            s3_symbols_prev=None,
            satellite_symbols_today={"CN:300906", "CN:300907"},
        )
        assert [x["symbol"] for x in out] == []

    def test_unknown_satellite_legs_fail_open(self) -> None:
        """Replay failure (None) must not remove any satellite row."""
        out = wa.compute_pool_removals(
            self._registry(),
            s3_symbols_today={"CN:600001", "CN:600002", "CN:600003"},
            s3_symbols_prev={"CN:600001", "CN:600002", "CN:600003"},
            satellite_symbols_today=None,
        )
        assert out == []

    def test_manual_and_legacy_sources_are_never_gc_ed(self) -> None:
        out = wa.compute_pool_removals(
            self._registry(),
            s3_symbols_today=set(),
            s3_symbols_prev=set(),
            satellite_symbols_today=set(),
        )
        assert {x["symbol"] for x in out} == {
            "CN:600001",
            "CN:600003",
            "CN:300906",
            "CN:300907",
        }


class TestApplyPoolRun:
    def test_merge_keeps_manual_rows_and_refreshes_pool_payload(self) -> None:
        registry = [
            {"symbol": "CN:600001", "source": wa.SOURCE_S3, "addedAt": "2026-09-10", "score": 70},
            {"symbol": "CN:000001", "source": "manual", "addedAt": "2026-09-01"},
            {"symbol": "CN:600099", "source": wa.SOURCE_S3, "addedAt": "2026-09-10"},
        ]
        saved: dict = {}

        def fake_upsert(items):
            saved["items"] = items
            return len(items)

        with (
            patch.object(wa, "upsert_registry", fake_upsert),
            patch.object(wa, "ack_run", lambda run_id: saved.update(acked=run_id)),
            # addedAt comes from the (real) Shanghai clock — pin it to DAY so
            # the test does not turn red on every later calendar date.
            patch.object(wa, "_shanghai_today_iso", lambda: DAY),
        ):
            out = wa.apply_pool_run(
                run_id="run-1",
                registry=registry,
                remove_items=[{"symbol": "CN:600099", "reason": "s3_failed_2d"}],
                pool_add=[
                    {"symbol": "CN:600001", "source": wa.SOURCE_S3, "score": 95, "rs": 0.9},
                    {"symbol": "CN:300906", "source": wa.SOURCE_SATELLITE, "entryDate": DAY},
                ],
                satellite_symbols_today={"CN:300906"},
            )
        items = {x["symbol"]: x for x in saved["items"]}
        assert "CN:600099" not in items  # removal applied
        assert items["CN:600001"]["score"] == 95  # payload refreshed
        assert items["CN:600001"]["addedAt"] == "2026-09-10"  # addedAt preserved
        assert items["CN:000001"]["source"] == "manual"  # manual row untouched
        assert items["CN:300906"] == {
            "symbol": "CN:300906",
            "source": wa.SOURCE_SATELLITE,
            "entryDate": DAY,
            "addedAt": DAY,
        }
        assert out == {"added": 1, "removed": 1, "registry": 3}
        assert saved["acked"] == "run-1"

    def test_dual_leg_name_keeps_satellite_tag(self) -> None:
        saved: dict = {}
        with (
            patch.object(wa, "upsert_registry", lambda items: saved.update(items=items)),
            patch.object(wa, "ack_run", lambda run_id: None),
        ):
            wa.apply_pool_run(
                run_id="run-2",
                registry=[{"symbol": "CN:300906", "source": wa.SOURCE_S3, "addedAt": DAY}],
                remove_items=[],
                pool_add=[
                    {"symbol": "CN:300906", "source": wa.SOURCE_S3, "score": 80},
                    {"symbol": "CN:300906", "source": wa.SOURCE_SATELLITE, "entryDate": DAY},
                ],
                satellite_symbols_today={"CN:300906"},
            )
        assert saved["items"][0]["source"] == wa.SOURCE_SATELLITE


class TestSatellitePoolRefresh:
    """OPT-219: post-5min refresh (17:30 run predates the 14:30 fill panel)."""

    def _seams(self, monkeypatch, *, legs, registry, prev_meta=None):
        state: dict = {"run": None, "applied": None}
        monkeypatch.setattr(wa, "compute_satellite_pool", lambda day: legs)
        monkeypatch.setattr(wa, "list_registry", lambda: list(registry))
        monkeypatch.setattr(
            "data_sync_service.db.watchlist_automation.get_latest_run",
            lambda: ({"tradeDate": DAY, "meta": prev_meta} if prev_meta is not None else None),
        )

        def fake_insert(**kw):
            state["run"] = kw
            return "run-refresh"

        def fake_apply(**kw):
            state["applied"] = kw
            return {"added": 1, "removed": 1, "registry": 2}

        monkeypatch.setattr(wa, "insert_automation_run", fake_insert)
        monkeypatch.setattr(wa, "apply_pool_run", fake_apply)
        return state

    def test_adds_new_legs_removes_exited_and_accumulates_meta(self, monkeypatch) -> None:
        legs = [
            {
                "symbol": "CN:300908",
                "source": wa.SOURCE_SATELLITE,
                "ts_code": "300908.SZ",
                "entryDate": DAY,
                "exitDue": "2026-09-18",
                "heldDays": 1,
                "daysLeft": 2,
            }
        ]
        registry = [
            {"symbol": "CN:300906", "source": wa.SOURCE_SATELLITE},
            {"symbol": "CN:600001", "source": wa.SOURCE_S3},
        ]
        state = self._seams(
            monkeypatch,
            legs=legs,
            registry=registry,
            prev_meta={"s3PoolSize": 10, "poolAdded": {"s3": 3, "satellite": 1},
                       "poolRemoved": {"s3": 1, "satellite": 0}},
        )
        out = wa.refresh_satellite_pool(day=DAY)
        assert out["ok"] is True
        assert out["satellitePoolSize"] == 1
        assert state["run"]["trigger_type"] == "post_5min"
        assert state["run"]["trade_date"] == DAY
        # exited leg removed; new leg added; S-3 row untouched
        assert [r["symbol"] for r in state["run"]["remove_items"]] == ["CN:300906"]
        assert [r["symbol"] for r in state["run"]["alpha_add"]] == ["CN:300908"]
        assert state["run"]["meta"]["s3PoolSize"] == 10  # carried forward
        assert state["run"]["meta"]["poolAdded"] == {"s3": 3, "satellite": 2}
        assert state["run"]["meta"]["poolRemoved"] == {"s3": 1, "satellite": 1}
        assert state["applied"]["run_id"] == "run-refresh"

    def test_replay_failure_is_a_noop(self, monkeypatch) -> None:
        called: list = []
        monkeypatch.setattr(wa, "compute_satellite_pool", lambda day: None)
        monkeypatch.setattr(wa, "insert_automation_run", lambda **kw: called.append(kw) or "x")
        out = wa.refresh_satellite_pool(day=DAY)
        assert out["ok"] is False
        assert called == []  # no run row, no registry write
