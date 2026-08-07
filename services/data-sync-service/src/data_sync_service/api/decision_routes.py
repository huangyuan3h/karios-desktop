"""Decision agent loop endpoints (TIP-015).

Stateful side of the decision agent: sessions, messages, snapshots.
The chat itself streams through ai-service (/decision/chat); this router
only persists the conversation and the archive.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, Query

router = APIRouter(prefix="/api/decision", tags=["decision"])


@router.get("/sessions")
def list_sessions_endpoint(limit: int = Query(50, ge=1, le=200)) -> dict[str, Any]:
    """All decision sessions, newest activity first."""
    from data_sync_service.db.decision import list_sessions

    return {"ok": True, "sessions": list_sessions(limit=limit)}


@router.post("/sessions")
def create_session_endpoint(
    title: str | None = Body(None),
    model_profile: str | None = Body(None),
    system_prompt: str | None = Body(None),
) -> dict[str, Any]:
    from data_sync_service.db.decision import create_session

    return {"ok": True, "session": create_session(
        title=title, model_profile=model_profile, system_prompt=system_prompt
    )}


@router.patch("/sessions/{session_id}")
def update_session_endpoint(
    session_id: int,
    title: str | None = Body(None, embed=True),
    system_prompt: str | None = Body(None, embed=True),
) -> dict[str, Any]:
    from data_sync_service.db.decision import update_session_settings

    rec = update_session_settings(
        session_id, title=title, system_prompt=system_prompt
    )
    if not rec:
        return {"ok": False, "error": "session not found"}
    return {"ok": True, "session": rec}


@router.get("/sessions/{session_id}/messages")
def list_messages_endpoint(session_id: int, limit: int = Query(200, ge=1, le=500)) -> dict[str, Any]:
    from data_sync_service.db.decision import list_messages

    return {"ok": True, "messages": list_messages(session_id, limit=limit)}


@router.post("/sessions/{session_id}/messages")
def append_message_endpoint(
    session_id: int,
    role: str = Body(..., embed=True),
    content: str = Body(..., embed=True),
    context_snapshot: dict[str, Any] | None = Body(None, embed=True),
) -> dict[str, Any]:
    from data_sync_service.db.decision import append_message

    return {"ok": True, "message": append_message(
        session_id, role=role, content=content, context_snapshot=context_snapshot
    )}


@router.delete("/sessions/{session_id}/messages/{message_id}")
def delete_message_endpoint(session_id: int, message_id: int) -> dict[str, Any]:
    """Delete a single message from a session."""
    from data_sync_service.db.decision import delete_message

    if not delete_message(session_id, message_id):
        return {"ok": False, "error": "message not found"}
    return {"ok": True}


@router.get("/snapshots")
def list_snapshots_endpoint(limit: int = Query(30, ge=1, le=120)) -> dict[str, Any]:
    """Daily decision snapshots (archive layer), newest first."""
    from data_sync_service.db.decision import list_snapshots

    return {"ok": True, "snapshots": list_snapshots(limit=limit)}


@router.get("/snapshots/{snapshot_date}")
def snapshot_detail_endpoint(snapshot_date: str) -> dict[str, Any]:
    """Full archive snapshot for a date (exchanges + outcome)."""
    from datetime import date as _date

    from data_sync_service.db.decision import list_snapshots
    from data_sync_service.service.decision import _messages_on

    try:
        d = _date.fromisoformat(snapshot_date)
    except ValueError:
        return {"ok": False, "error": f"invalid date: {snapshot_date}"}
    snap = next(
        (s for s in list_snapshots(limit=60) if str(s.get("snapshotDate"))[:10] == snapshot_date),
        None,
    )
    if not snap:
        return {"ok": False, "error": "snapshot not found"}
    exchanges = _messages_on(d)
    snap["exchanges"] = [
        {
            "role": m["role"],
            "content": str(m["content"])[:3000],
            "createdAt": m["created_at"].isoformat()
            if hasattr(m["created_at"], "isoformat")
            else str(m["created_at"]),
        }
        for m in exchanges
    ]
    return {"ok": True, "snapshot": snap}


@router.get("/archive/search")
def archive_search_endpoint(symbol: str = Query(..., min_length=1), limit: int = Query(10, ge=1, le=20)) -> dict[str, Any]:
    """Search archive snapshots whose exchanges mention a symbol (TIP-015 M3)."""
    from data_sync_service.service.decision import search_archive_by_symbol

    return {"ok": True, "hits": search_archive_by_symbol(symbol, limit=limit)}


@router.get("/actions")
def list_actions_endpoint(
    status: str | None = Query(None),
    days: int = Query(30, ge=1, le=365),
    limit: int = Query(200, ge=1, le=500),
) -> dict[str, Any]:
    """Extracted decision-agent actions with execution/outcome status (TIP-015)."""
    from data_sync_service.db.decision import list_actions

    return {
        "ok": True,
        "actions": list_actions(status=status, days=days, limit=limit),
    }


@router.get("/analysis")
def analysis_endpoint(
    fired_days: int = Query(30, ge=1, le=365),
) -> dict[str, Any]:
    """Decision-loop analytics: judgment volume by source, paper outcomes, context audit (TIP-015 M4)."""
    from data_sync_service.service.decision import analysis_stats

    return {"ok": True, **analysis_stats(fired_days=fired_days)}
