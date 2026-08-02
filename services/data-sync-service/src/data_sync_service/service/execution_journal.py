"""Execution decision journal: content hash, diff, ingest, markdown."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from data_sync_service.db import execution_journal as ej_db

DECISION_CARD_FIELDS = (
    "symbol",
    "action",
    "why",
    "trigger",
    "entryTrigger",
    "exitStop",
    "positionPct",
    "hardStop",
    "trailStop",
)

# Latest Actions table only lists symbols with a meaningful decision delta today.
LATEST_ACTIONS_DELTA_FIELDS = frozenset(
    {"action", "trigger", "entryTrigger", "exitStop", "hardStop", "trailStop"}
)

# hardStop changes smaller than this (relative to old value) are noise (2026-08-01 · wife feedback).
HARDSTOP_NOISE_THRESHOLD_PCT = 0.01


def _is_hardstop_noise(old_value: Any, new_value: Any, threshold: float = HARDSTOP_NOISE_THRESHOLD_PCT) -> bool:
    """True when hardStop drift is smaller than `threshold` (relative)."""
    try:
        o = float(old_value)
        n = float(new_value)
    except (TypeError, ValueError):
        return False
    if o == 0:
        return abs(n) < threshold
    return abs(n - o) / abs(o) < threshold


def filter_journal_changes(changes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Drop hardStop drifts < HARDSTOP_NOISE_THRESHOLD_PCT (2026-08-01)."""
    out: list[dict[str, Any]] = []
    for c in changes:
        if not isinstance(c, dict):
            continue
        if str(c.get("scope")) == "symbol" and str(c.get("field")) == "hardStop":
            if _is_hardstop_noise(c.get("oldValue"), c.get("newValue")):
                continue
        out.append(c)
    return out


def _norm_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float):
        if v != v:  # NaN
            return ""
        return f"{v:.6g}"
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


def decision_payload_for_hash(gate: dict[str, Any] | None, cards: list[dict[str, Any]] | None) -> dict[str, Any]:
    g = gate if isinstance(gate, dict) else {}
    mode = _norm_str(g.get("mode"))
    allow = bool(g.get("allowNewEntries"))
    card_rows: list[dict[str, str]] = []
    for c in cards or []:
        if not isinstance(c, dict):
            continue
        sym = _norm_str(c.get("symbol")).strip()
        if not sym:
            continue
        card_rows.append(
            {
                "symbol": sym,
                "action": _norm_str(c.get("action")),
                "why": _norm_str(c.get("why")),
                "trigger": _norm_str(c.get("trigger")),
                "entryTrigger": _norm_str(c.get("entryTrigger")),
                "exitStop": _norm_str(c.get("exitStop")),
                "positionPct": _norm_str(c.get("positionPct")),
                "hardStop": _norm_str(c.get("hardStop")),
                "trailStop": _norm_str(c.get("trailStop")),
            }
        )
    card_rows.sort(key=lambda r: r["symbol"])
    return {"mode": mode, "allowNewEntries": allow, "cards": card_rows}


def compute_content_hash(gate: dict[str, Any] | None, cards: list[dict[str, Any]] | None) -> str:
    payload = decision_payload_for_hash(gate, cards)
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _cards_by_symbol(cards: list[dict[str, Any]] | None) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for c in cards or []:
        if not isinstance(c, dict):
            continue
        sym = str(c.get("symbol") or "").strip()
        if sym:
            out[sym] = c
    return out


def diff_snapshots(
    prev: dict[str, Any] | None,
    curr_gate: dict[str, Any],
    curr_cards: list[dict[str, Any]],
    *,
    trade_date: str,
    from_snapshot_id: str | None,
    to_snapshot_id: str,
) -> list[dict[str, Any]]:
    """Return change rows (not yet persisted)."""
    changes: list[dict[str, Any]] = []
    prev_gate = (prev or {}).get("gate") if prev else None
    prev_cards = (prev or {}).get("cards") if prev else None
    if not isinstance(prev_gate, dict):
        prev_gate = {}
    if not isinstance(prev_cards, list):
        prev_cards = []

    prev_mode = _norm_str(prev_gate.get("mode"))
    curr_mode = _norm_str(curr_gate.get("mode"))
    if prev and prev_mode != curr_mode:
        changes.append(
            {
                "trade_date": trade_date,
                "from_snapshot_id": from_snapshot_id,
                "to_snapshot_id": to_snapshot_id,
                "scope": "gate",
                "symbol": None,
                "field": "mode",
                "old_value": prev_mode or None,
                "new_value": curr_mode or None,
            }
        )

    prev_allow = bool(prev_gate.get("allowNewEntries")) if prev else None
    curr_allow = bool(curr_gate.get("allowNewEntries"))
    if prev is not None and prev_allow != curr_allow:
        changes.append(
            {
                "trade_date": trade_date,
                "from_snapshot_id": from_snapshot_id,
                "to_snapshot_id": to_snapshot_id,
                "scope": "gate",
                "symbol": None,
                "field": "allowNewEntries",
                "old_value": "true" if prev_allow else "false",
                "new_value": "true" if curr_allow else "false",
            }
        )

    prev_map = _cards_by_symbol(prev_cards if prev else [])
    curr_map = _cards_by_symbol(curr_cards)
    symbols = sorted(set(prev_map) | set(curr_map))
    for sym in symbols:
        p = prev_map.get(sym) or {}
        c = curr_map.get(sym) or {}
        if not prev:
            # First snapshot of the day/session: do not flood with "appeared" events.
            continue
        if sym not in prev_map and sym in curr_map:
            changes.append(
                {
                    "trade_date": trade_date,
                    "from_snapshot_id": from_snapshot_id,
                    "to_snapshot_id": to_snapshot_id,
                    "scope": "symbol",
                    "symbol": sym,
                    "field": "action",
                    "old_value": None,
                    "new_value": _norm_str(c.get("action")) or None,
                }
            )
            why = _norm_str(c.get("why"))
            if why:
                changes.append(
                    {
                        "trade_date": trade_date,
                        "from_snapshot_id": from_snapshot_id,
                        "to_snapshot_id": to_snapshot_id,
                        "scope": "symbol",
                        "symbol": sym,
                        "field": "why",
                        "old_value": None,
                        "new_value": why,
                    }
                )
            continue
        if sym in prev_map and sym not in curr_map:
            changes.append(
                {
                    "trade_date": trade_date,
                    "from_snapshot_id": from_snapshot_id,
                    "to_snapshot_id": to_snapshot_id,
                    "scope": "symbol",
                    "symbol": sym,
                    "field": "action",
                    "old_value": _norm_str(p.get("action")) or None,
                    "new_value": None,
                }
            )
            continue
        for field in (
            "action",
            "why",
            "trigger",
            "entryTrigger",
            "exitStop",
            "positionPct",
            "hardStop",
            "trailStop",
        ):
            ov = _norm_str(p.get(field))
            nv = _norm_str(c.get(field))
            if ov != nv:
                changes.append(
                    {
                        "trade_date": trade_date,
                        "from_snapshot_id": from_snapshot_id,
                        "to_snapshot_id": to_snapshot_id,
                        "scope": "symbol",
                        "symbol": sym,
                        "field": field,
                        "old_value": ov or None,
                        "new_value": nv or None,
                    }
                )
    return changes


def symbols_with_latest_action_deltas(day_changes: list[dict[str, Any]]) -> set[str]:
    """Symbols whose Action / Trigger / HardStop / TrailStop changed today."""
    out: set[str] = set()
    for c in day_changes:
        if not isinstance(c, dict):
            continue
        if str(c.get("scope") or "") != "symbol":
            continue
        if str(c.get("field") or "") not in LATEST_ACTIONS_DELTA_FIELDS:
            continue
        # API rows use camelCase; diff rows use snake_case before persist.
        sym = str(c.get("symbol") or "").strip()
        if sym:
            out.add(sym)
    return out


def ingest_snapshot(
    *,
    trade_date: str,
    source: str,
    gate: dict[str, Any],
    cards: list[dict[str, Any]],
    meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Insert snapshot if decision hash changed; else heartbeat-update latest same-day row.
    Returns API-shaped result.
    """
    content_hash = compute_content_hash(gate, cards)
    latest = ej_db.fetch_latest_snapshot(trade_date=trade_date)

    if latest and latest.get("contentHash") == content_hash:
        touched = ej_db.touch_snapshot_captured_at(latest["id"])
        snap = touched or latest
        return {
            "snapshotId": snap["id"],
            "changed": False,
            "heartbeat": True,
            "snapshot": snap,
            "changes": [],
        }

    # Prefer previous snapshot overall (may be prior day) for meaningful diffs across sessions.
    prev = ej_db.fetch_latest_snapshot()
    snap = ej_db.insert_snapshot(
        trade_date=trade_date,
        source=source,
        gate=gate,
        cards=cards,
        content_hash=content_hash,
        meta=meta,
    )
    change_rows = diff_snapshots(
        prev,
        gate,
        cards,
        trade_date=trade_date,
        from_snapshot_id=(prev or {}).get("id"),
        to_snapshot_id=snap["id"],
    )
    persisted = ej_db.insert_changes(change_rows) if change_rows else []
    return {
        "snapshotId": snap["id"],
        "changed": True,
        "heartbeat": False,
        "snapshot": snap,
        "changes": persisted,
    }


def _md_cell(v: Any) -> str:
    if v is None:
        return "—"
    s = str(v).replace("|", "\\|").replace("\n", " ")
    return s if s else "—"


def build_journal_markdown(
    *,
    trade_date: str,
    days: int = 5,
    changes_limit: int = 80,
) -> str:
    days = max(1, min(int(days), 30))
    latest = ej_db.fetch_latest_snapshot(trade_date=trade_date)
    changes = ej_db.list_changes(trade_date=trade_date, limit=changes_limit)
    if not changes and days > 1:
        changes = ej_db.list_changes(limit=changes_limit)
    # 2026-08-01 noise filter: drop hardStop drifts < HARDSTOP_NOISE_THRESHOLD_PCT
    changes = filter_journal_changes(changes)

    lines: list[str] = []
    lines.append("## Decision Journal")
    lines.append(f"- tradeDate: {trade_date}")
    lines.append(
        f"- latestSnapshotAt: {_md_cell((latest or {}).get('capturedAt'))}"
    )
    lines.append(f"- latestSource: {_md_cell((latest or {}).get('source'))}")
    lines.append(
        "- note: Prefer Action/Why transitions below over re-deriving rules."
    )
    lines.append(
        f"- note: hardStop drifts < {int(HARDSTOP_NOISE_THRESHOLD_PCT * 100)}% suppressed (noise filter)"
    )
    lines.append("")

    lines.append("### Changes (today)")
    lines.append("| Time | Scope | Symbol | Field | From | To |")
    lines.append("| --- | --- | --- | --- | --- | --- |")
    day_changes = [c for c in changes if c.get("tradeDate") == trade_date]
    if not day_changes:
        lines.append("| — | — | — | — | — | — |")
    else:
        for c in day_changes:
            lines.append(
                "| "
                + " | ".join(
                    [
                        _md_cell(c.get("changedAt")),
                        _md_cell(c.get("scope")),
                        _md_cell(c.get("symbol")),
                        _md_cell(c.get("field")),
                        _md_cell(c.get("oldValue")),
                        _md_cell(c.get("newValue")),
                    ]
                )
                + " |"
            )
    lines.append("")

    lines.append("### Latest Actions")
    lines.append(
        "- note: delta-only — Action / Trigger / HardStop / TrailStop changes; silent WATCH omitted"
    )
    lines.append("| Symbol | Action | Why | Trigger | HardStop | TrailStop | Pos% | Mainline |")
    lines.append("| --- | --- | --- | --- | --- | --- | --- | --- |")
    cards = (latest or {}).get("cards") or []
    delta_symbols = symbols_with_latest_action_deltas(day_changes)
    delta_cards: list[dict[str, Any]] = []
    if isinstance(cards, list) and delta_symbols:
        for c in cards:
            if not isinstance(c, dict):
                continue
            sym = str(c.get("symbol") or "").strip()
            if sym in delta_symbols:
                delta_cards.append(c)
    if not delta_cards:
        lines.append("| — | — | — | — | — | — | — | — |")
    else:
        for c in delta_cards:
            ml = "ok" if c.get("mainlineOk") else "no"
            tag = c.get("mainlineTag")
            if tag:
                ml = str(tag)
            lines.append(
                "| "
                + " | ".join(
                    [
                        _md_cell(c.get("symbol")),
                        _md_cell(c.get("action")),
                        _md_cell(c.get("why")),
                        _md_cell(c.get("trigger")),
                        _md_cell(c.get("hardStop")),
                        _md_cell(c.get("trailStop")),
                        _md_cell(c.get("positionPct")),
                        _md_cell(ml),
                    ]
                )
                + " |"
            )
    lines.append("")

    # 2026-08-01: Recent snapshots block removed — only signal changes matter (Action / Why / Gate)
    return "\n".join(lines)
