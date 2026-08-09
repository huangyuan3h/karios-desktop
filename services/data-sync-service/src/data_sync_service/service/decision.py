"""Decision agent archive & feedback (TIP-015 M3).

Daily decision snapshots (exchanges + watchlist context) and T+1 outcome
feedback from execution journal changes + paper trades.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, date, datetime, time, timedelta, timezone
from typing import Any

from data_sync_service.db.decision import (
    ensure_table,
    get_connection,
    list_snapshots,
    upsert_snapshot,
)

MAX_EXCHANGE_MESSAGES = 20
MAX_EXCHANGE_CHARS = 2000


SHANGHAI_TZ = timezone(timedelta(hours=8))


def shanghai_today() -> date:
    """Shanghai calendar date (H6: explicit +08:00, not host-local timezone)."""
    return datetime.now(SHANGHAI_TZ).date()


def _watchlist_ref() -> dict[str, Any]:
    ensure_table()
    try:
        from data_sync_service.db.watchlist_automation import list_registry

        rows = list_registry()
        symbols = [r.get("symbol") for r in rows if r.get("symbol")]
        sources: dict[str, int] = {}
        for r in rows:
            src = str(r.get("source") or "unknown")
            sources[src] = sources.get(src, 0) + 1
        return {"watchlistSymbols": symbols, "bySource": sources, "count": len(symbols)}
    except Exception:  # noqa: BLE001
        return {"watchlistSymbols": [], "bySource": {}, "count": 0}


def _messages_on(snapshot_date: date, limit: int = 100) -> list[dict[str, Any]]:
    """All decision-agent messages for a given SHANGHAI calendar day.

    H6 (2026-08-08): the window must be the Shanghai day boundary, not the
    UTC one — a message posted between Shanghai 00:00-07:59 (UTC of the
    previous day) used to fall outside the UTC-day window and was silently
    dropped from the daily snapshot.
    """
    ensure_table()
    start = datetime.combine(snapshot_date, time.min, tzinfo=SHANGHAI_TZ)
    end = datetime.combine(snapshot_date, time.max, tzinfo=SHANGHAI_TZ)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT session_id, role, content, created_at
                FROM decision_messages
                WHERE created_at >= %s::timestamptz AND created_at <= %s::timestamptz
                ORDER BY created_at ASC
                LIMIT %s
                """,
                (start.isoformat(), end.isoformat(), limit),
            )
            rows = cur.fetchall()
    cols = ("session_id", "role", "content", "created_at")
    return [dict(zip(cols, r, strict=False)) for r in rows]


def build_daily_snapshot(*, snapshot_date: date | None = None) -> dict[str, Any]:
    """Collect today's decision exchanges + watchlist context into the archive."""
    target = snapshot_date or shanghai_today()
    messages = _messages_on(target)
    exchanges = [
        {
            "role": m["role"],
            "content": str(m["content"])[:MAX_EXCHANGE_CHARS],
            "createdAt": m["created_at"].isoformat()
            if hasattr(m["created_at"], "isoformat")
            else str(m["created_at"]),
        }
        for m in messages[-MAX_EXCHANGE_MESSAGES:]
    ]
    active_ref = _watchlist_ref()
    active_ref["snapshotAt"] = datetime.now(UTC).isoformat()
    rec = upsert_snapshot(
        snapshot_date=target,
        active_layer_ref=active_ref,
        agent_exchanges=exchanges,
        status="open",
    )
    rec["exchangeCount"] = len(exchanges)
    return rec


def apply_daily_outcomes(*, days: int = 5) -> dict[str, Any]:
    """Backfill outcome (fired changes + paper results) for recent snapshots."""
    from data_sync_service.db.execution_journal import list_changes
    from data_sync_service.db.paper_trading import list_paper_trades

    snapshots = list_snapshots(limit=days)
    updated: list[str] = []
    for snap in snapshots:
        d = snap["snapshotDate"]
        date_str = d.isoformat() if hasattr(d, "isoformat") else str(d)[:10]
        changes = list_changes(trade_date=date_str, limit=200)
        fired = [
            {
                "symbol": c.get("symbol"),
                "field": c.get("field"),
                "newValue": c.get("newValue"),
                "source": c.get("source"),
            }
            for c in changes
            if c.get("field") == "action"
        ]
        paper = []
        try:
            trades = list_paper_trades(limit=100)
            for t in trades:
                entry = str(t.get("entryDate") or "")[:10]
                if entry == date_str:
                    paper.append(
                        {
                            "symbol": t.get("symbol"),
                            "side": t.get("side"),
                            "status": t.get("status"),
                            "pnlPct": t.get("pnlPct"),
                        }
                    )
        except Exception:  # noqa: BLE001
            pass
        outcome = {"fired": fired, "paper": paper}
        if fired or paper:
            _ = upsert_snapshot(
                snapshot_date=d,
                status="reviewed",
                outcome=outcome,
            )
            updated.append(date_str)
    return {"ok": True, "updated": updated}


def analysis_stats(*, fired_days: int = 30, paper_limit: int = 500) -> dict[str, Any]:
    """Aggregate decision-loop analytics (TIP-015 M4).

    - Judgment volume by provenance (TIP-011 bucket, action changes)
    - Paper-trading outcome distribution (win rate, avg pnl) — NET-of-costs
      since v0.2 (OPT-062); per-market split in ``paperByMarket``
    - Per-session context audit: rounds, average injected tokens
    """
    from datetime import timedelta

    from data_sync_service.db.execution_journal import count_changes_by_source
    from data_sync_service.db.paper_trading import count_by_market_since, list_paper_trades

    since = (datetime.now(UTC) - timedelta(days=fired_days)).isoformat()
    by_source = count_changes_by_source(
        since=since,
        field="action",
        new_value="BUY",
    )

    trades: list[dict[str, Any]] = []
    try:
        trades = list_paper_trades(limit=paper_limit)
    except Exception:  # noqa: BLE001
        pass
    closed = [t for t in trades if str(t.get("status") or "") == "closed"]
    wins = [t for t in closed if (t.get("pnlPct") or 0) > 0]
    losses = [t for t in closed if (t.get("pnlPct") or 0) <= 0]
    pnls = [t.get("pnlPct") or 0 for t in closed]
    avg_pnl = round(sum(pnls) / len(pnls), 2) if pnls else None

    # OPT-062: per-market NET stats for the same window. Best-effort — the
    # analysis must not break when the DB read fails.
    try:
        market_stats = count_by_market_since(since.split("T")[0])
    except Exception:  # noqa: BLE001
        market_stats = {}

    sessions: list[dict[str, Any]] = []
    ensure_table()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT s.id, s.title, s.last_active_at,
                       COUNT(m.id) AS msg_count,
                       COUNT(m.id) FILTER (WHERE m.context_snapshot IS NOT NULL) AS snapshot_count
                FROM decision_sessions s
                LEFT JOIN decision_messages m ON m.session_id = s.id
                GROUP BY s.id, s.title, s.last_active_at
                ORDER BY s.last_active_at DESC
                LIMIT 10
                """
            )
            rows = cur.fetchall()
    for r in rows:
        sessions.append(
            {
                "id": r[0],
                "title": r[1],
                "lastActiveAt": r[2].isoformat() if hasattr(r[2], "isoformat") else str(r[2]),
                "messageCount": int(r[3] or 0),
                "auditRounds": int(r[4] or 0),
            }
        )
    return {
        "firedDays": fired_days,
        "firedBySource": by_source,
        "firedTotal": sum(by_source.values()),
        "paper": {
            "total": len(trades),
            "open": len([t for t in trades if str(t.get("status") or "") == "open"]),
            "closed": len(closed),
            "wins": len(wins),
            "losses": len(losses),
            "winRate": round(len(wins) / len(closed), 3) if closed else None,
            "avgPnlPct": avg_pnl,
            "byMarket": market_stats,
        },
        "sessions": sessions,
    }


def extract_pending_actions(*, hours: int = 48, brief_marker: str = "## 操作建议") -> dict[str, Any]:
    """Extract structured actions from recent assistant brief messages.

    Calls ai-service /decision/extract-actions per brief; stores into
    decision_actions (idempotent per message_id).
    """
    import urllib.request

    from data_sync_service.config import get_settings
    from data_sync_service.db.decision import upsert_actions

    ensure_table()
    cutoff = f"now() - INTERVAL '{int(hours)} hours'"
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, session_id, content, created_at
                FROM decision_messages
                WHERE role = 'assistant'
                  AND content LIKE %s
                  AND created_at >= {cutoff}
                ORDER BY created_at ASC
                LIMIT 50
                """,
                (f"%{brief_marker}%",),
            )
            rows = cur.fetchall()
    cols = ("id", "session_id", "content", "created_at")
    messages = [dict(zip(cols, r, strict=False)) for r in rows]

    settings = get_settings()
    base = settings.ai_service_base_url
    extracted_total = 0
    processed: list[int] = []
    for m in messages:
        try:
            body = json.dumps({"text": str(m["content"])[:6000]}).encode()
            req = urllib.request.Request(
                f"{base}/decision/extract-actions",
                data=body,
                headers={"content-type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())
            actions = data.get("actions") or []
        except Exception:  # noqa: BLE001
            continue
        if not actions:
            continue
        recs = [
            {
                "session_id": m["session_id"],
                "message_id": m["id"],
                "symbol": a["symbol"],
                "action": a["action"],
                "rationale": a.get("rationale"),
                "confidence": a.get("confidence"),
                "source": "decision_agent",
                "snapshot_date": m["created_at"].strftime("%Y-%m-%d")
                if hasattr(m["created_at"], "strftime")
                else str(m["created_at"])[:10],
            }
            for a in actions
        ]
        extracted_total += upsert_actions(recs)
        processed.append(m["id"])
    return {"ok": True, "processed": processed, "extracted": extracted_total}


def match_executions(*, match_window_days: int = 3) -> dict[str, Any]:
    """Match proposed actions against execution journal changes.

    An action is 'executed' when a change with the same symbol+action appears
    within `match_window_days` after the action was created. HOLD is
    non-tradeable and stays proposed.
    """

    from data_sync_service.db.decision import list_actions, update_action_status
    from data_sync_service.db.execution_journal import list_changes

    proposed = [
        a
        for a in list_actions(days=30, limit=500)
        if a["status"] == "proposed" and a["action"] in ("BUY", "ADD", "EXIT")
    ]
    matched = 0
    for a in proposed:
        changes = list_changes(since=a["createdAt"], limit=200)
        hit = next(
            (
                c
                for c in changes
                if c.get("symbol") == a["symbol"]
                and str(c.get("newValue") or "").upper() == a["action"]
                and c.get("field") == "action"
            ),
            None,
        )
        if hit:
            update_action_status(a["id"], status="executed", matched_change_id=str(hit["id"]))
            matched += 1
    return {"ok": True, "matched": matched, "proposed": len(proposed)}


def track_action_outcomes(*, horizon_days: int = 5) -> dict[str, Any]:
    """Fill price outcomes (1/3/5 trading-day returns) for recent actions.

    Executed actions are measured from the matched change; proposed-only
    actions get a benchmark return for reference.
    """
    from data_sync_service.db.daily import fetch_daily_for_codes
    from data_sync_service.db.decision import list_actions, update_action_status

    actions = [
        a
        for a in list_actions(days=14, limit=300)
        if a["outcome"] is None
    ]
    ts_codes = [f"{a['symbol'][3:9]}.{a['symbol'][10:]}" for a in actions if len(a["symbol"]) >= 11]
    if not actions:
        return {"ok": True, "tracked": 0}

    start = (datetime.now(UTC).date() - timedelta(days=horizon_days + 5)).isoformat()
    bars = fetch_daily_for_codes(
        [c for c in ts_codes if len(c) == 10],
        start,
        datetime.now(UTC).date().isoformat(),
    )
    by_code: dict[str, list[dict[str, Any]]] = {}
    for b in bars:
        by_code.setdefault(b["ts_code"], []).append(b)
    tracked = 0
    for a, code in zip(actions, ts_codes, strict=False):
        rows = sorted(by_code.get(code, []), key=lambda r: r["trade_date"])
        created_date = a["createdAt"][:10]
        base_idx = next(
            (i for i, r in enumerate(rows) if str(r["trade_date"]) >= created_date),
            None,
        )
        if base_idx is None or not rows:
            continue
        base = float(rows[base_idx]["close"] or 0)
        if base <= 0:
            continue
        def pct(i: int, base_idx: int = base_idx, base: float = base, rows: list[dict[str, Any]] = rows) -> float | None:
            if base_idx + i >= len(rows):
                return None
            c = float(rows[base_idx + i]["close"] or 0)
            return round((c / base - 1) * 100, 2) if c > 0 else None
        outcome = {"pct1": pct(1), "pct3": pct(3), "pct5": pct(5)}
        if any(v is not None for v in outcome.values()):
            update_action_status(a["id"], status=a["status"], outcome=outcome)
            tracked += 1
    return {"ok": True, "tracked": tracked}


_SYMBOL_RE = re.compile(r"CN:[A-Z0-9]+\.[A-Z]{2}")


def search_archive_by_symbol(symbol: str, *, limit: int = 20) -> list[dict[str, Any]]:
    """Find snapshots whose exchanges mention the symbol (case-insensitive)."""
    needle = symbol.strip().upper()
    if not needle:
        return []
    snapshots = list_snapshots(limit=60)
    hits: list[dict[str, Any]] = []
    for snap in snapshots:
        exchanges = snap.get("agentExchanges") or []
        matched: list[str] = []
        for ex in exchanges:
            content = str(ex.get("content") or "")
            if needle.upper() in content.upper():
                snippet = content[:160].replace("\n", " ")
                matched.append(f"{ex.get('role')}: {snippet}")
        if matched:
            d = snap["snapshotDate"]
            hits.append(
                {
                    "date": d.isoformat() if hasattr(d, "isoformat") else str(d)[:10],
                    "status": snap.get("status"),
                    "matches": matched[:6],
                    "outcome": snap.get("outcome"),
                }
            )
        if len(hits) >= limit:
            break
    return hits
