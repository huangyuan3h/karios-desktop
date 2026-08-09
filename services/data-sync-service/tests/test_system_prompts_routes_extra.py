"""api/system_prompts_routes.py coverage."""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from data_sync_service.api import system_prompts_routes as spr
from data_sync_service.api.system_prompts_routes import (
    CreateSystemPromptPresetRequest,
    SetActiveSystemPromptRequest,
    SystemPromptRequest,
    UpdateSystemPromptPresetRequest,
    create_system_prompt,
    delete_system_prompt_preset,
    get_active_system_prompt,
    get_system_prompt,
    get_system_prompt_preset,
    list_system_prompts,
    put_active_system_prompt,
    put_system_prompt,
    put_system_prompt_preset,
)


class TestEndpoints:
    def test_get_prompt(self, monkeypatch) -> None:
        monkeypatch.setattr(spr.spsvc, "get_system_prompt_value", lambda: {"value": "v"})
        assert get_system_prompt() == {"value": "v"}
        monkeypatch.setattr(spr.spsvc, "get_system_prompt_value", lambda: (_ for _ in ()).throw(RuntimeError("boom")))
        with pytest.raises(HTTPException) as exc:
            get_system_prompt()
        assert exc.value.status_code == 500 and exc.value.detail == "boom"

    def test_put_prompt(self, monkeypatch) -> None:
        monkeypatch.setattr(spr.spsvc, "put_system_prompt_value", lambda value: {"ok": True})
        assert put_system_prompt(SystemPromptRequest(value="v")) == {"ok": True}
        monkeypatch.setattr(spr.spsvc, "put_system_prompt_value", lambda value: (_ for _ in ()).throw(HTTPException(status_code=400, detail="bad")))
        with pytest.raises(HTTPException) as exc:
            put_system_prompt(SystemPromptRequest(value="v"))
        assert exc.value.status_code == 400
        monkeypatch.setattr(spr.spsvc, "put_system_prompt_value", lambda value: (_ for _ in ()).throw(RuntimeError("boom")))
        with pytest.raises(HTTPException) as exc:
            put_system_prompt(SystemPromptRequest(value="v"))
        assert exc.value.status_code == 500

    def test_list_prompts(self, monkeypatch) -> None:
        monkeypatch.setattr(spr.spsvc, "list_presets", lambda: {"items": []})
        assert list_system_prompts() == {"items": []}
        monkeypatch.setattr(spr.spsvc, "list_presets", lambda: (_ for _ in ()).throw(RuntimeError("boom")))
        with pytest.raises(HTTPException):
            list_system_prompts()

    def test_create_prompt(self, monkeypatch) -> None:
        monkeypatch.setattr(spr.spsvc, "create_preset", lambda title, content: {"id": "p1"})
        assert create_system_prompt(CreateSystemPromptPresetRequest(title="t", content="c")) == {"id": "p1"}
        monkeypatch.setattr(spr.spsvc, "create_preset", lambda title, content: (_ for _ in ()).throw(RuntimeError("boom")))
        with pytest.raises(HTTPException):
            create_system_prompt(CreateSystemPromptPresetRequest(title="t", content="c"))

    def test_active(self, monkeypatch) -> None:
        monkeypatch.setattr(spr.spsvc, "get_active_prompt", lambda: {"id": "p1"})
        assert get_active_system_prompt() == {"id": "p1"}
        monkeypatch.setattr(spr.spsvc, "get_active_prompt", lambda: (_ for _ in ()).throw(RuntimeError("boom")))
        with pytest.raises(HTTPException):
            get_active_system_prompt()
        monkeypatch.setattr(spr.spsvc, "set_active_prompt", lambda preset_id: {"ok": True})
        assert put_active_system_prompt(SetActiveSystemPromptRequest(id="p1")) == {"ok": True}
        monkeypatch.setattr(spr.spsvc, "set_active_prompt", lambda preset_id: (_ for _ in ()).throw(RuntimeError("boom")))
        with pytest.raises(HTTPException):
            put_active_system_prompt(SetActiveSystemPromptRequest(id="p1"))

    def test_preset_crud(self, monkeypatch) -> None:
        monkeypatch.setattr(spr.spsvc, "get_preset", lambda preset_id: {"id": preset_id, "value": "v"})
        assert get_system_prompt_preset("p1")["id"] == "p1"
        monkeypatch.setattr(spr.spsvc, "get_preset", lambda preset_id: (_ for _ in ()).throw(RuntimeError("boom")))
        with pytest.raises(HTTPException):
            get_system_prompt_preset("p1")
        monkeypatch.setattr(spr.spsvc, "update_preset", lambda preset_id, title, content: {"ok": True})
        assert put_system_prompt_preset("p1", UpdateSystemPromptPresetRequest(title="t", content="c")) == {"ok": True}
        monkeypatch.setattr(spr.spsvc, "update_preset", lambda preset_id, title, content: (_ for _ in ()).throw(RuntimeError("boom")))
        with pytest.raises(HTTPException):
            put_system_prompt_preset("p1", UpdateSystemPromptPresetRequest(title="t", content="c"))
        monkeypatch.setattr(spr.spsvc, "delete_preset", lambda preset_id: {"ok": True})
        assert delete_system_prompt_preset("p1") == {"ok": True}
        monkeypatch.setattr(spr.spsvc, "delete_preset", lambda preset_id: (_ for _ in ()).throw(RuntimeError("boom")))
        with pytest.raises(HTTPException):
            delete_system_prompt_preset("p1")


class TestHttpExceptionPassthrough:
    def test_all_endpoints_rethrow(self, monkeypatch) -> None:
        def thrower(*a, **k):
            raise HTTPException(status_code=409, detail="conflict")

        monkeypatch.setattr(spr.spsvc, "get_system_prompt_value", thrower)
        monkeypatch.setattr(spr.spsvc, "put_system_prompt_value", thrower)
        monkeypatch.setattr(spr.spsvc, "list_presets", thrower)
        monkeypatch.setattr(spr.spsvc, "create_preset", thrower)
        monkeypatch.setattr(spr.spsvc, "get_active_prompt", thrower)
        monkeypatch.setattr(spr.spsvc, "set_active_prompt", thrower)
        monkeypatch.setattr(spr.spsvc, "get_preset", thrower)
        monkeypatch.setattr(spr.spsvc, "update_preset", thrower)
        monkeypatch.setattr(spr.spsvc, "delete_preset", thrower)
        cases = [
            lambda: get_system_prompt(),
            lambda: put_system_prompt(SystemPromptRequest(value="v")),
            lambda: list_system_prompts(),
            lambda: create_system_prompt(CreateSystemPromptPresetRequest(title="t", content="c")),
            lambda: get_active_system_prompt(),
            lambda: put_active_system_prompt(SetActiveSystemPromptRequest(id="p1")),
            lambda: get_system_prompt_preset("p1"),
            lambda: put_system_prompt_preset("p1", UpdateSystemPromptPresetRequest(title="t", content="c")),
            lambda: delete_system_prompt_preset("p1"),
        ]
        for case in cases:
            with pytest.raises(HTTPException) as exc:
                case()
            assert exc.value.status_code == 409


def test_router_paths() -> None:
    paths = {r.path for r in spr.router.routes}
    assert "/settings/system-prompt" in paths
    assert "/system-prompts/{preset_id}" in paths
