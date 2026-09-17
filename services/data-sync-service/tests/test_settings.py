"""OPT-223: cross-layer settings (strategy mode) — db + route contracts."""

from __future__ import annotations

from typing import Any


class TestSettingsDb:
    def test_get_setting_roundtrip_with_fake_connection(self, monkeypatch) -> None:
        from data_sync_service.db import app_settings as st

        state: dict[str, Any] = {"value": None}
        executed: list[tuple] = []

        class _Cur:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return None

            def execute(self, sql, params=None) -> None:
                executed.append((sql, params))

            def fetchone(self):
                return (state["value"],) if state["value"] is not None else None

        class _Conn:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return None

            def cursor(self):
                return _Cur()

            def commit(self) -> None:
                pass

        monkeypatch.setattr(st, "ensure_table", lambda: None)
        monkeypatch.setattr(st, "get_connection", lambda: _Conn())

        assert st.get_setting("strategy_mode", "starport") == "starport"  # default
        state["value"] = "starship"
        assert st.get_setting("strategy_mode") == "starship"

        st.set_setting("strategy_mode", "starship")
        assert "INSERT INTO app_settings" in executed[-1][0]
        assert executed[-1][1][0] == "strategy_mode"


class TestSettingsRoute:
    def test_get_returns_stored_mode(self, monkeypatch) -> None:
        from data_sync_service.api import settings_routes as sr

        monkeypatch.setattr(sr.app_settings, "get_setting", lambda key, default=None: "twin_star")
        assert sr.get_strategy_mode() == {"ok": True, "mode": "twin_star"}

    def test_put_validates_and_stores(self, monkeypatch) -> None:
        from fastapi import HTTPException

        from data_sync_service.api import settings_routes as sr

        saved: list[tuple] = []
        monkeypatch.setattr(sr.app_settings, "set_setting", lambda k, v: saved.append((k, v)))

        out = sr.put_strategy_mode(sr.StrategyModeBody(mode="starship"))
        assert out == {"ok": True, "mode": "starship"}
        assert saved == [("strategy_mode", "starship")]

        try:
            sr.put_strategy_mode(sr.StrategyModeBody(mode="nope"))
        except HTTPException as exc:
            assert exc.status_code == 422
        else:  # pragma: no cover - explicit failure path
            raise AssertionError("invalid mode must be rejected")

    def test_selected_mode_falls_back_on_bad_value(self, monkeypatch) -> None:
        from data_sync_service.api import settings_routes as sr

        monkeypatch.setattr(sr.app_settings, "get_setting", lambda key, default=None: "garbage")
        assert sr.selected_strategy_mode() == sr.DEFAULT_STRATEGY_MODE
        monkeypatch.setattr(sr.app_settings, "get_setting", lambda key, default=None: "harbor")
        assert sr.selected_strategy_mode() == "harbor"
