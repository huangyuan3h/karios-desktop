"""Trade journal API routes."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from data_sync_service.db import journal as journal_db

router = APIRouter()


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


class TradeJournal(BaseModel):
    id: str
    title: str
    contentMd: str
    createdAt: str
    updatedAt: str


class TradeJournalCreateRequest(BaseModel):
    title: str | None = None
    contentMd: str = ""


class TradeJournalUpdateRequest(BaseModel):
    title: str | None = None
    contentMd: str | None = None


class ListTradeJournalsResponse(BaseModel):
    total: int
    items: list[TradeJournal]


@router.get("/journals", response_model=ListTradeJournalsResponse)
def list_journals(limit: int = Query(20, ge=1, le=200), offset: int = Query(0, ge=0)) -> ListTradeJournalsResponse:
    """List journals with pagination."""
    total, items = journal_db.fetch_all(limit=limit, offset=offset)
    return ListTradeJournalsResponse(total=total, items=[TradeJournal(**it) for it in items])


@router.get("/journals/{journal_id}", response_model=TradeJournal)
def get_journal(journal_id: str) -> TradeJournal:
    """Get a single journal by id."""
    jid = (journal_id or "").strip()
    if not jid:
        raise HTTPException(status_code=400, detail="journal_id is required")
    journal = journal_db.fetch_by_id(jid)
    if not journal:
        raise HTTPException(status_code=404, detail="Journal not found")
    return TradeJournal(**journal)


@router.post("/journals", response_model=TradeJournal)
def create_journal(req: TradeJournalCreateRequest) -> TradeJournal:
    """Create a new journal entry."""
    now = _now_iso()
    jid = str(uuid4())
    title = (req.title or "").strip() or "Trading Journal"
    content = req.contentMd or ""
    journal = journal_db.create_journal(journal_id=jid, title=title, content_md=content, created_at=now, updated_at=now)
    return TradeJournal(**journal)


@router.put("/journals/{journal_id}", response_model=TradeJournal)
def update_journal(journal_id: str, req: TradeJournalUpdateRequest) -> TradeJournal:
    """Update an existing journal entry."""
    jid = (journal_id or "").strip()
    if not jid:
        raise HTTPException(status_code=400, detail="journal_id is required")
    now = _now_iso()
    journal = journal_db.update_journal(journal_id=jid, title=req.title, content_md=req.contentMd, updated_at=now)
    if not journal:
        raise HTTPException(status_code=404, detail="Journal not found")
    return TradeJournal(**journal)


@router.delete("/journals/{journal_id}")
def delete_journal(journal_id: str) -> dict[str, Any]:
    """Delete a journal entry."""
    jid = (journal_id or "").strip()
    if not jid:
        raise HTTPException(status_code=400, detail="journal_id is required")
    ok = journal_db.delete_journal(jid)
    if not ok:
        raise HTTPException(status_code=404, detail="Journal not found")
    return {"ok": True}
