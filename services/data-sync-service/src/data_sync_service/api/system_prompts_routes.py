from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException  # type: ignore[import-not-found]
from pydantic import BaseModel  # type: ignore[import-not-found]

from data_sync_service.service import system_prompts as spsvc

router = APIRouter()


class SystemPromptRequest(BaseModel):
    value: str


class CreateSystemPromptPresetRequest(BaseModel):
    title: str
    content: str


class UpdateSystemPromptPresetRequest(BaseModel):
    title: str
    content: str


class SetActiveSystemPromptRequest(BaseModel):
    id: str | None


@router.get("/settings/system-prompt")
def get_system_prompt() -> dict[str, str]:
    try:
        return spsvc.get_system_prompt_value()
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e) or e.__class__.__name__) from e


@router.put("/settings/system-prompt")
def put_system_prompt(req: SystemPromptRequest) -> dict[str, bool]:
    try:
        return spsvc.put_system_prompt_value(value=req.value)
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e) or e.__class__.__name__) from e


@router.get("/system-prompts")
def list_system_prompts() -> dict[str, Any]:
    try:
        return spsvc.list_presets()
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e) or e.__class__.__name__) from e


@router.post("/system-prompts")
def create_system_prompt(req: CreateSystemPromptPresetRequest) -> dict[str, str]:
    try:
        return spsvc.create_preset(title=req.title, content=req.content)
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e) or e.__class__.__name__) from e


@router.get("/system-prompts/active")
def get_active_system_prompt() -> dict[str, Any]:
    try:
        return spsvc.get_active_prompt()
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e) or e.__class__.__name__) from e


@router.put("/system-prompts/active")
def put_active_system_prompt(req: SetActiveSystemPromptRequest) -> dict[str, bool]:
    try:
        return spsvc.set_active_prompt(preset_id=req.id)
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e) or e.__class__.__name__) from e


@router.get("/system-prompts/{preset_id}")
def get_system_prompt_preset(preset_id: str) -> dict[str, str]:
    try:
        return spsvc.get_preset(preset_id=preset_id)
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e) or e.__class__.__name__) from e


@router.put("/system-prompts/{preset_id}")
def put_system_prompt_preset(preset_id: str, req: UpdateSystemPromptPresetRequest) -> dict[str, bool]:
    try:
        return spsvc.update_preset(preset_id=preset_id, title=req.title, content=req.content)
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e) or e.__class__.__name__) from e


@router.delete("/system-prompts/{preset_id}")
def delete_system_prompt_preset(preset_id: str) -> dict[str, bool]:
    try:
        return spsvc.delete_preset(preset_id=preset_id)
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=str(e) or e.__class__.__name__) from e

