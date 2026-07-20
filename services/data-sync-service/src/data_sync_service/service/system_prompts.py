from __future__ import annotations

import uuid
from typing import Any

from fastapi import HTTPException  # type: ignore[import-not-found]

from data_sync_service.db import system_prompts as spdb


def get_system_prompt_value() -> dict[str, str]:
    """
    Legacy endpoint shape: { value }.
    Prefer active preset content; fall back to legacy content.
    """
    active = get_active_prompt()
    return {"value": str(active.get("content") or "")}


def put_system_prompt_value(*, value: str) -> dict[str, bool]:
    """
    Backward compatible behavior:
    - If there's an active preset, update that preset's content.
    - Otherwise update the legacy value.
    """
    st = spdb.get_state()
    active_id = st.get("activePresetId")
    if active_id:
        ok = spdb.update_preset(preset_id=str(active_id), title=None, content=value)
        if ok:
            return {"ok": True}
    spdb.set_legacy_content(value)
    return {"ok": True}


def list_presets() -> dict[str, Any]:
    return {"items": spdb.list_presets()}


def create_preset(*, title: str, content: str) -> dict[str, str]:
    pid = str(uuid.uuid4())
    spdb.create_preset(preset_id=pid, title=(title or "").strip() or "Untitled", content=str(content or ""))
    # Newly created preset becomes active by default.
    spdb.set_active_preset_id(pid)
    return {"id": pid}


def get_preset(*, preset_id: str) -> dict[str, str]:
    p = spdb.get_preset(preset_id)
    if p is None:
        raise HTTPException(status_code=404, detail="Not found")
    return p


def update_preset(*, preset_id: str, title: str, content: str) -> dict[str, bool]:
    ok = spdb.update_preset(
        preset_id=preset_id,
        title=(title or "").strip() or "Untitled",
        content=str(content or ""),
    )
    if not ok:
        raise HTTPException(status_code=404, detail="Not found")
    return {"ok": True}


def delete_preset(*, preset_id: str) -> dict[str, bool]:
    deleted = spdb.delete_preset(preset_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Not found")
    st = spdb.get_state()
    if st.get("activePresetId") == preset_id:
        spdb.set_active_preset_id(None)
    return {"ok": True}


def get_active_prompt() -> dict[str, Any]:
    st = spdb.get_state()
    active_id = st.get("activePresetId")
    if active_id:
        p = spdb.get_preset(str(active_id))
        if p:
            return {"id": p["id"], "title": p["title"], "content": p["content"]}
    legacy = str(st.get("legacyContent") or "")
    return {"id": None, "title": "Legacy", "content": legacy}


def set_active_prompt(*, preset_id: str | None) -> dict[str, bool]:
    pid = (preset_id or "").strip() if preset_id is not None else ""
    if not pid:
        spdb.set_active_preset_id(None)
        return {"ok": True}
    if spdb.get_preset(pid) is None:
        raise HTTPException(status_code=404, detail="Not found")
    spdb.set_active_preset_id(pid)
    return {"ok": True}

