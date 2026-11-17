"""Event / geo tags — advice-only annotations, never engine rewrites.

Yuan's rule (2026-11-10): a current-affairs / event tag may only *advise*
whether to rebuy and which strategy to follow. It must never veto a stop-loss /
trail exit, delay a fill, change ``sleeve.action`` or the traffic cooldown
(k=0). The authority order stays::

    frozen engine exits > selected mode target + fill clock
    > human-confirmed overlays > event tags > chat narrative

Naming: this module speaks ``eventTags`` / ``geoRegime`` / ``eventTag``. It
never reuses ``regime=Weak/Strong`` from ``market_regime.py`` — that is the S-3
open-position gate, a different concept.

Storage reuses the existing ``app_settings`` JSONB store
(``agent_event_tags_v1``) exactly like ``agent_overlays``: no fifth ledger and
no schema migration. Reads are fail-open; a missing store / low confidence tag
means the contract and brief keep their Step-1 shape (no tag block).

Recording a sourced tag (the only way to add a political / geopolitical
judgment)::

    POST /v1/agent/event-tags
    {
      "tag": "oil_political_calendar",
      "effect": "advise_defer_same_key_rebuy",
      "relatedAssets": ["OIL", "ETF:513350"],
      "confidence": 0.7,
      "sources": [{"url": "https://<real article>", "title": "..."}],
      "validFrom": "2026-10-15",
      "validUntil": "2026-11-10",
      "note": "中东风 + 美国政治日历，油品可能二次重定价"
    }

Sources must be a real URL or a news-brief id; fabricated URLs are rejected by
construction (an empty source list is a 422). ``GET /api/news/brief/latest``
can be ingested as a *source list* only (``news_brief_sources``) — never as a
raw trade signal.
"""

from __future__ import annotations

import logging
import uuid
from datetime import date, timedelta
from typing import Any

from data_sync_service.db import app_settings

logger = logging.getLogger(__name__)

EVENT_TAG_SETTINGS_KEY = "agent_event_tags_v1"

# The only effects an event tag may carry. Every one is advice or a label.
EFFECT_WHITELIST = frozenset(
    {
        # Advice: postpone a same-key rebuy (oil after a political repricing).
        "advise_defer_same_key_rebuy",
        # Advice: maintain / watch another mode — never writes settings.
        "advise_strategy",
        # Market-fact labels derived from sleeve facts already in the contract.
        "trail_exit",
        "below_ma200",
    }
)

# Explicitly forbidden. Kept as a separate set so an old / typo'd effect fails
# loudly instead of silently becoming a no-op that looks like an exit veto.
FORBIDDEN_EFFECTS = frozenset(
    {
        "block_exit",
        "veto_exit",
        "delay_exit",
        "delay_fill",
        "hold_must_act",
        "downgrade_sell",
        "skip_trail_confirmation",
        "block_sell_to_repo",
        "change_settings_mode",
        "switch_strategy_mode",
    }
)

# Below this confidence a stored tag is ignored by the contract / brief
# (fail-open: the payload matches Step 1 exactly).
MIN_CONFIDENCE = 0.5
MAX_TAGS = 80
DEFAULT_VALID_DAYS = 10

TAG_STATUSES = frozenset({"active", "cleared", "rejected"})

# Assets that make a tag part of the geo/event regime view. OIL is the live
# case (513350); US / GEO / POLITICS are bookkeeping labels for a calendar tag.
GEO_ASSETS = frozenset({"OIL", "ETF:513350", "GEO", "US", "POLITICS"})

EFFECT_LABELS: dict[str, str] = {
    "advise_defer_same_key_rebuy": "建议暂缓同键回买（仅建议）",
    "advise_strategy": "策略档位建议（需 Yuan 确认）",
    "trail_exit": "已触发移动止损（引擎事实）",
    "below_ma200": "跌破 MA200（引擎事实）",
}


def _today_iso() -> str:
    from data_sync_service.service.trade_calendar_utils import shanghai_today_iso

    return shanghai_today_iso()


def _now_iso() -> str:
    from datetime import UTC, datetime

    return datetime.now(UTC).isoformat()


def _norm_effect(value: str | None) -> str:
    effect = str(value or "").strip()
    if effect in FORBIDDEN_EFFECTS:
        raise ValueError(
            f"effect {effect!r} is forbidden: event tags may only advise, "
            "never veto/delay a stop-loss or trail exit"
        )
    if effect not in EFFECT_WHITELIST:
        raise ValueError(f"effect must be one of {sorted(EFFECT_WHITELIST)} (got {effect!r})")
    return effect


def _norm_assets(values: Any) -> list[str]:
    if not isinstance(values, (list, tuple)):
        return []
    out: list[str] = []
    for value in values:
        item = str(value or "").strip().upper()
        if item and item not in out:
            out.append(item)
    return out


def _norm_sources(values: Any) -> list[dict[str, Any]]:
    """Normalise ``[{url|briefId|title}]``; keep only attributable sources."""
    if not isinstance(values, (list, tuple)):
        return []
    out: list[dict[str, Any]] = []
    for raw in values:
        if isinstance(raw, str):
            item: dict[str, Any] = {"url": raw.strip()}
        elif isinstance(raw, dict):
            item = {}
            url = str(raw.get("url") or "").strip()
            brief_id = str(raw.get("briefId") or raw.get("brief_id") or "").strip()
            title = str(raw.get("title") or "").strip()
            if url:
                item["url"] = url
            if brief_id:
                item["briefId"] = brief_id
            if title:
                item["title"] = title
        else:
            continue
        if item.get("url") or item.get("briefId"):
            out.append(item)
    return out


def _has_attribution(sources: list[dict[str, Any]]) -> bool:
    return any(s.get("url") or s.get("briefId") for s in sources)


def _load_raw() -> list[dict[str, Any]]:
    try:
        raw = app_settings.get_setting(EVENT_TAG_SETTINGS_KEY, [])
    except Exception as exc:  # noqa: BLE001 — read path is fail-open
        logger.warning("agent event tags read failed: %s", exc)
        return []
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for item in raw:
        if isinstance(item, dict) and item.get("id"):
            out.append(dict(item))
    return out


def _save_raw(items: list[dict[str, Any]]) -> None:
    try:
        app_settings.set_setting(EVENT_TAG_SETTINGS_KEY, items[-MAX_TAGS:])
    except Exception as exc:  # noqa: BLE001 — a failed write must not break a read
        logger.warning("agent event tags write failed: %s", exc)


def default_valid_until(day: str, valid_days: int = DEFAULT_VALID_DAYS) -> str:
    """``valid_days`` open sessions after ``day`` (calendar-aware, fail-open)."""
    days = max(1, int(valid_days))
    try:
        from data_sync_service.service.agent_overlays import default_valid_until as _overlay_until

        return _overlay_until(day, days)
    except Exception:  # noqa: BLE001 — calendar unseeded → calendar-day fallback
        try:
            start = date.fromisoformat(str(day)[:10])
        except ValueError:
            start = date.today()
        return (start + timedelta(days=days * 2 + 2)).isoformat()


def _effective_status(item: dict[str, Any], day: str) -> str:
    status = str(item.get("status") or "active")
    if status in {"cleared", "rejected"}:
        return status
    valid_from = str(item.get("validFrom") or "")[:10]
    valid_until = str(item.get("validUntil") or "")[:10]
    if valid_from and valid_from > day:
        return "pending"
    if valid_until and valid_until < day:
        return "expired"
    return "active"


def _decorate(item: dict[str, Any], day: str) -> dict[str, Any]:
    effect = str(item.get("effect") or "")
    confidence = item.get("confidence")
    try:
        confidence = round(float(confidence), 3)
    except (TypeError, ValueError):
        confidence = None
    return {
        "id": item.get("id"),
        "tag": item.get("tag"),
        "effect": effect,
        "effectLabel": EFFECT_LABELS.get(effect, effect),
        "relatedAssets": _norm_assets(item.get("relatedAssets")),
        "confidence": confidence,
        "sources": _norm_sources(item.get("sources")),
        "validFrom": item.get("validFrom"),
        "validUntil": item.get("validUntil"),
        "effectiveStatus": _effective_status(item, day),
        "note": item.get("note"),
        "source": item.get("source"),
        "createdAt": item.get("createdAt"),
        "updatedAt": item.get("updatedAt"),
        "adviceOnly": True,
        "changesAction": False,
        "changesFillClock": False,
    }


def list_event_tags(
    *,
    day: str | None = None,
    effect: str | None = None,
    include_expired: bool = True,
) -> list[dict[str, Any]]:
    """All stored tags, newest first, with ``effectiveStatus`` for ``day``."""
    as_of = str(day or _today_iso())[:10]
    want_effect = str(effect or "").strip() or None
    out: list[dict[str, Any]] = []
    for item in reversed(_load_raw()):
        rec = _decorate(item, as_of)
        if want_effect and rec.get("effect") != want_effect:
            continue
        if not include_expired and rec.get("effectiveStatus") != "active":
            continue
        out.append(rec)
    return out


def active_event_tags(*, day: str | None = None) -> list[dict[str, Any]]:
    """Effective tags at/above ``MIN_CONFIDENCE`` (the contract read path)."""
    out: list[dict[str, Any]] = []
    for rec in list_event_tags(day=day):
        if rec.get("effectiveStatus") != "active":
            continue
        confidence = rec.get("confidence")
        if confidence is None or float(confidence) < MIN_CONFIDENCE:
            continue
        out.append(rec)
    return out


def record_event_tag(
    *,
    tag: str,
    effect: str,
    confidence: float,
    sources: Any,
    related_assets: Any = None,
    valid_from: str | None = None,
    valid_until: str | None = None,
    valid_days: int | None = None,
    note: str | None = None,
    source: str = "agent",
    status: str = "active",
    day: str | None = None,
) -> dict[str, Any]:
    """Create or update one sourced event tag (advice only).

    The whitelist effect + at least one attributable source + a confidence in
    ``[0, 1]`` are mandatory. Re-posting ``(effect, tag)`` updates the existing
    row instead of stacking duplicates.
    """
    label = str(tag or "").strip()
    if not label:
        raise ValueError("tag is required")
    if len(label) > 80:
        raise ValueError("tag must be <= 80 characters")
    kind = _norm_effect(effect)
    state = str(status or "active").strip()
    if state not in TAG_STATUSES:
        raise ValueError(f"status must be one of {sorted(TAG_STATUSES)} (got {state!r})")
    try:
        conf = float(confidence)
    except (TypeError, ValueError) as exc:
        raise ValueError("confidence must be a number in [0, 1]") from exc
    if conf < 0.0 or conf > 1.0:
        raise ValueError("confidence must be in [0, 1]")
    src = _norm_sources(sources)
    if state == "active" and not _has_attribution(src):
        raise ValueError("at least one source with url or briefId is required")

    as_of = str(day or _today_iso())[:10]
    start = str(valid_from or as_of)[:10] or as_of
    until = str(valid_until or "")[:10] or None
    now_iso = _now_iso()

    items = _load_raw()
    existing: dict[str, Any] | None = None
    for item in items:
        if str(item.get("tag") or "") == label and str(item.get("effect") or "") == kind:
            existing = item
            break

    if existing is None:
        until = until or default_valid_until(start, valid_days or DEFAULT_VALID_DAYS)
        rec: dict[str, Any] = {
            "id": f"et-{uuid.uuid4().hex[:10]}",
            "tag": label,
            "effect": kind,
            "relatedAssets": _norm_assets(related_assets),
            "confidence": round(conf, 3),
            "sources": src,
            "validFrom": start,
            "validUntil": until,
            "note": note,
            "source": source,
            "status": state,
            "createdAt": now_iso,
            "updatedAt": now_iso,
        }
        items.append(rec)
    else:
        rec = existing
        rec.update(
            {
                "status": state,
                "relatedAssets": _norm_assets(related_assets) or rec.get("relatedAssets") or [],
                "confidence": round(conf, 3),
                "sources": src or rec.get("sources") or [],
                "note": note if note is not None else rec.get("note"),
                "source": source or rec.get("source"),
                "updatedAt": now_iso,
            }
        )
        if valid_from:
            rec["validFrom"] = start
        if until:
            rec["validUntil"] = until
        elif state == "active" and not rec.get("validUntil"):
            rec["validUntil"] = default_valid_until(as_of, valid_days or DEFAULT_VALID_DAYS)
    _save_raw(items)
    return _decorate(rec, as_of)


def update_event_tag_status(
    tag_id: str,
    *,
    status: str,
    day: str | None = None,
    note: str | None = None,
) -> dict[str, Any] | None:
    state = str(status or "").strip()
    if state not in TAG_STATUSES:
        raise ValueError(f"status must be one of {sorted(TAG_STATUSES)} (got {state!r})")
    as_of = str(day or _today_iso())[:10]
    items = _load_raw()
    for item in items:
        if item.get("id") != tag_id:
            continue
        item["status"] = state
        item["updatedAt"] = _now_iso()
        if note is not None:
            item["note"] = note
        _save_raw(items)
        return _decorate(item, as_of)
    return None


# ---------------------------------------------------------------------------
# news brief ingest — source list only, never a trade signal
# ---------------------------------------------------------------------------


def news_brief_sources(*, brief_type: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
    """Attribute rows from ``GET /api/news/brief/latest`` (source list only).

    Fail-open: no DB / no brief → ``[]``. The caller must still supply a
    whitelist effect + confidence; this helper never creates a tag by itself.
    """
    try:
        from data_sync_service.db.morning_brief import fetch_latest_brief

        brief = fetch_latest_brief(brief_type=brief_type)
    except Exception as exc:  # noqa: BLE001 — source enrichment only
        logger.warning("agent event tags news brief read failed: %s", exc)
        return []
    if not isinstance(brief, dict):
        return []
    brief_id = str(brief.get("id") or "").strip()
    out: list[dict[str, Any]] = []
    for item in (brief.get("items") or [])[:limit]:
        if not isinstance(item, dict):
            continue
        url = str(item.get("link") or item.get("url") or "").strip()
        title = str(item.get("title") or "").strip()
        source_id = str(item.get("sourceItemId") or item.get("source_item_id") or "").strip()
        row: dict[str, Any] = {}
        if brief_id:
            row["briefId"] = brief_id
        if url:
            row["url"] = url
        if title:
            row["title"] = title
        if source_id:
            row["sourceItemId"] = source_id
        if row.get("briefId") or row.get("url"):
            out.append(row)
    return out


# ---------------------------------------------------------------------------
# contract assembly
# ---------------------------------------------------------------------------


def _engine_tag(
    *,
    tag: str,
    effect: str,
    confidence: float,
    related_assets: list[str],
    day: str,
    contract_id: str | None,
    note: str | None = None,
) -> dict[str, Any]:
    """A market-fact label derived from the contract (not a persisted tag)."""
    kind = _norm_effect(effect)
    return {
        "id": f"auto:{tag}",
        "tag": tag,
        "effect": kind,
        "effectLabel": EFFECT_LABELS.get(kind, kind),
        "relatedAssets": _norm_assets(related_assets),
        "confidence": confidence,
        "sources": [{"type": "engine_fact", "contractId": contract_id}],
        "validFrom": day,
        "validUntil": day,
        "effectiveStatus": "active",
        "note": note,
        "source": "engine_fact",
        "createdAt": None,
        "updatedAt": None,
        "adviceOnly": True,
        "changesAction": False,
        "changesFillClock": False,
    }


def _catalog_row(timeline: str | None) -> dict[str, Any] | None:
    if not timeline:
        return None
    try:
        from data_sync_service.service.strategy_catalog import strategy_catalog

        for row in strategy_catalog():
            if str(row.get("timelineStrategy") or "") == str(timeline):
                return {
                    "key": row.get("key"),
                    "name": row.get("name"),
                    "timelineStrategy": row.get("timelineStrategy"),
                    "status": row.get("status"),
                    "statusLabel": row.get("statusLabel"),
                }
    except Exception as exc:  # noqa: BLE001 — display only
        logger.warning("agent event tags strategy catalog failed: %s", exc)
    return None


def strategy_advice(
    *,
    mode: str,
    timeline: str | None,
    followability: dict[str, Any],
    day: str,
) -> dict[str, Any] | None:
    """Advice-only comparison of the current settings mode vs the baseline.

    Compares against the frozen strategy catalog (in-memory, no network) and
    the Step-1 ``followability`` verdict. It never writes settings and always
    states that Yuan's confirmation is required before any mode switch.
    """
    if not isinstance(followability, dict):
        followability = {}
    if followability.get("followable") is False:
        recommended_mode = str(followability.get("recommendedMode") or "homeport")
        recommended_timeline = str(followability.get("recommendedTimeline") or "homeport_m30")
        return {
            "advice": "switch_after_confirmation",
            "currentMode": mode,
            "currentTimeline": timeline,
            "currentCatalog": _catalog_row(timeline),
            "recommendedMode": recommended_mode,
            "recommendedTimeline": recommended_timeline,
            "recommendedCatalog": _catalog_row(recommended_timeline),
            "reason": followability.get("reason"),
            "requiresConfirmation": True,
            "settingsUnchanged": True,
            "confirmationNote": (
                "改档必须由 Yuan 在 /settings/strategy-mode 明确确认；"
                "本建议不改 settings、不切停车腿、不下单。"
            ),
            "adviceOnly": True,
        }
    if mode and mode != "homeport":
        return {
            "advice": "maintain",
            "currentMode": mode,
            "currentTimeline": timeline,
            "currentCatalog": _catalog_row(timeline),
            "reason": (f"settings 档 `{mode}` 没有被冷因子门拦下；维持当前档，不因事件标签切档。"),
            "requiresConfirmation": True,
            "settingsUnchanged": True,
            "confirmationNote": (
                "任何切档都必须由 Yuan 在 /settings/strategy-mode 明确确认；本建议不改 settings。"
            ),
            "adviceOnly": True,
        }
    return None


def _advice_tag(advice: dict[str, Any], *, day: str, contract_id: str | None) -> dict[str, Any]:
    mode = str(advice.get("currentMode") or "")
    target = str(advice.get("recommendedMode") or "") or mode
    assets = [f"STRATEGY:{mode}"]
    if target and target != mode:
        assets.append(f"STRATEGY:{target}")
    return _engine_tag(
        tag=f"strategy_{advice.get('advice')}_{mode}",
        effect="advise_strategy",
        confidence=1.0,
        related_assets=assets,
        day=day,
        contract_id=contract_id,
        note=advice.get("confirmationNote"),
    )


def _is_geo(tag: dict[str, Any]) -> bool:
    assets = {str(a).upper() for a in (tag.get("relatedAssets") or [])}
    return bool(assets & GEO_ASSETS)


def geo_regime(tags: list[dict[str, Any]], *, day: str) -> dict[str, Any]:
    """The event/geo view — deliberately NOT ``market_regime`` Weak/Strong."""
    geo_tags = [t for t in tags if _is_geo(t)]
    assets: list[str] = []
    for t in geo_tags:
        for asset in t.get("relatedAssets") or []:
            if asset not in assets:
                assets.append(asset)
    confidences = [float(t.get("confidence") or 0.0) for t in geo_tags]
    return {
        "active": bool(geo_tags),
        "label": "oil_political" if geo_tags else None,
        "asOfDate": day,
        "assets": assets,
        "tagIds": [t.get("id") for t in geo_tags],
        "confidence": max(confidences) if confidences else None,
        "adviceOnly": True,
        "notMarketRegime": True,
        "neverVetoesExits": True,
        "note": (
            "geo/event regime is advisory only: it is not market_regime.py "
            "(Weak/Strong) and never gates S-3 entries, exits or the fill clock"
        ),
    }


def assert_event_tags_never_override_exit(
    *,
    sleeve: dict[str, Any],
    fill_clock: dict[str, Any],
    tags: list[dict[str, Any]],
) -> None:
    """Invariant: a triggered exit keeps its action and fill clock.

    Called by ``build_decision_contract`` before the tag block is attached.
    Raises (rather than silently degrading) if a tag carries a forbidden
    effect or a suggested action override while a trail / must-act sell is
    active.
    """
    action = str(sleeve.get("action") or "NONE").upper()
    protected = bool(
        action == "SELL_TO_REPO" or sleeve.get("trailExit") or sleeve.get("pendingOpenSell")
    )
    for tag in tags:
        effect = str(tag.get("effect") or "")
        if effect in FORBIDDEN_EFFECTS:
            raise ValueError(f"event tag effect {effect!r} would override an engine exit")
    if not protected:
        return
    for tag in tags:
        effect = str(tag.get("effect") or "")
        if effect not in EFFECT_WHITELIST:
            raise ValueError(f"event tag effect {effect!r} is not advice-only")
        suggested = str(tag.get("suggestedAction") or "").upper()
        if suggested in {"HOLD", "SKIP", "DEFER", "CANCEL"}:
            raise ValueError(
                f"event tag {tag.get('id')!r} suggests {suggested} while a sell is active"
            )
    if not fill_clock.get("closeBasis") or fill_clock.get("tradableNow") is not False:
        raise ValueError("fill clock must stay close-basis / not tradable")


def build_event_tag_block(
    *,
    contract: dict[str, Any],
    day: str,
    timeline: str | None = None,
) -> dict[str, Any] | None:
    """Assemble ``eventTags`` + ``geoRegime`` for a decision contract.

    Returns ``None`` when nothing applies (no stored tag at/above the
    confidence threshold, no engine fact, no strategy advice). Callers must
    then keep the exact Step-1 payload shape.
    """
    sleeve = contract.get("sleeve") or {}
    strat = contract.get("strategy") or {}
    mode = str(strat.get("mode") or "")
    contract_id = contract.get("contractId")
    action = str(sleeve.get("action") or "NONE").upper()
    symbol = str(sleeve.get("symbol") or "") or None
    pick_key = str(sleeve.get("pickKey") or "") or None

    tags: list[dict[str, Any]] = []
    if sleeve.get("trailExit") or sleeve.get("pendingOpenSell") or action == "SELL_TO_REPO":
        assets = [a for a in (pick_key, symbol) if a]
        tags.append(
            _engine_tag(
                tag="trail_exit",
                effect="trail_exit",
                confidence=1.0,
                related_assets=assets,
                day=day,
                contract_id=contract_id,
                note="引擎已确认的移动止损/卖出事实；事件标签不能改动作或时钟。",
            )
        )
    next_leg = contract.get("nextLeg") or {}
    if next_leg.get("aboveMa200") is False:
        assets = [a for a in (next_leg.get("key"), next_leg.get("symbol")) if a]
        tags.append(
            _engine_tag(
                tag="below_ma200",
                effect="below_ma200",
                confidence=1.0,
                related_assets=assets,
                day=day,
                contract_id=contract_id,
                note="下一腿跌破 MA200，仅作标签（闸门仍由引擎判定）。",
            )
        )
    whip_ctx = ((contract.get("whip") or {}).get("context") or {}) if contract.get("whip") else {}
    # Any parking key trailed out inside the whip memory window (not only OIL).
    rebuy_ctx = (whip_ctx or {}).get("rebuy") or {}
    rebuy_by_key = rebuy_ctx.get("byKey") or {}
    if not rebuy_by_key:
        # Back-compat for hand-built contexts that only carry the OIL alias.
        oil_alias = (whip_ctx or {}).get("oil") or {}
        if oil_alias.get("active"):
            rebuy_by_key = {"OIL": oil_alias}
    for key, info in rebuy_by_key.items():
        if not isinstance(info, dict) or not info.get("active"):
            continue
        key_norm = str(key or "").upper()
        assets = [key_norm]
        if info.get("symbol"):
            assets.append(str(info["symbol"]))
        tags.append(
            _engine_tag(
                tag="same_key_rebuy_oil"
                if key_norm == "OIL"
                else f"same_key_rebuy_{key_norm.lower()}",
                effect="advise_defer_same_key_rebuy",
                confidence=1.0,
                related_assets=assets,
                day=day,
                contract_id=contract_id,
                note=info.get("message"),
            )
        )

    for rec in active_event_tags(day=day):
        tags.append(rec)

    advice = strategy_advice(
        mode=mode,
        timeline=timeline,
        followability=strat.get("followability") or {},
        day=day,
    )
    if advice:
        tags.append(_advice_tag(advice, day=day, contract_id=contract_id))

    if not tags:
        return None

    # Dedupe by id while keeping insertion order (engine facts first).
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for tag in tags:
        tag_id = str(tag.get("id") or "")
        if tag_id in seen:
            continue
        seen.add(tag_id)
        deduped.append(tag)

    return {
        "eventTags": {
            "available": True,
            "asOfDate": day,
            "threshold": MIN_CONFIDENCE,
            "count": len(deduped),
            "tags": deduped,
            "strategyAdvice": advice,
            "adviceOnly": True,
            "neverVetoesExits": True,
            "authority": "frozen_engine_exits > event_tags",
            "note": (
                "事件/地缘标签只建议（是否回买、是否维持当前档）；"
                "不能否决已触发的止损/移动止损/Must-act 卖出，也不改 settings、"
                "不改 TRAIL_PCT、不改 cooldown（k=0）。"
            ),
        },
        "geoRegime": geo_regime(deduped, day=day),
    }
