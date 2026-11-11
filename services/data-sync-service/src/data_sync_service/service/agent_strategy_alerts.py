"""After-close strategy alerts — a delta pack 乐乐 can broadcast in chat.

Yuan's maturity plan item ⑥: after each A-share close is ingested, tell 乐乐
**in the discussion chat** when a strategy shows excess vs the current
baseline, or when a factor light newly decays / revives. He often will not open
Karios, so the alerts must be pollable with Chinese copy and must stay quiet
when nothing new happened.

This module reuses the existing factor system state machine (``_decays`` /
``factor_vault_history.json`` / ``sgap_decay.json`` / ``factor_live_trades``)
instead of inventing a second one:

- **decay** — a followed factor goes revived/watch -> cold, its suggested
  position steps down, or S-gap K2 ``revival_pos`` steps down.
- **revival** — a factor newly enters ``revived`` or S-gap ``revival_pos``
  steps up. Sibling of decay so a light turning on is not missed.
- **excess_change** — a published, same-unit recent net crosses the baseline
  reference (``homeport_m30`` / 母港 M30). Incomparable units are recorded in
  ``skippedIncomparable`` and never compared.

Only events provably dated on ``asOf`` are emitted; standing facts such as
「starship_b 仍在冷库」 live in ``currentState`` and are never re-broadcast.
The persisted snapshot is idempotent on ``(asOf)``: a re-run overwrites that
day's file, and yesterday's snapshot is the previous classification baseline.

Advice / notification only. This module never touches ``sleeve.action``,
``TRAIL_PCT`` (8%), the parking cooldown (k=0), settings, a watchlist row, a
target weight, or a broker order. It is also write-free on the GET path:
``read_strategy_alerts`` only loads the persisted snapshot; only the weekday
19:20 job / POST one-shot calls ``run_strategy_alerts``.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from data_sync_service.service.factor_system import (
    BASELINE,
    BASELINE_LABEL,
    SATELLITE_IDS,
    build_factor_system,
)

logger = logging.getLogger(__name__)

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")

ALERTS_DIRNAME = "strategy_alerts"
SNAPSHOT_VERSION = 1
SNAPSHOT_SUFFIX = ".json"
BASELINE_IDS: tuple[str, ...] = ("homeport_m30", "m30")
REFERENCE_LIGHT_IDS: tuple[str, ...] = ("harbor",)

STATUS_ZH = {"cold": "冷库", "watch": "观察", "revived": "复活"}
CLASS_ZH = {"ahead": "跑赢", "behind": "落后", "flat": "持平"}
NOTICE_PREFIX = "（仅告知，不自动改设置、不下单）"


# ---------------------------------------------------------------------------
# paths / io
# ---------------------------------------------------------------------------


def _reports_dir() -> Path:
    from data_sync_service.api import backtest_routes as br

    return Path(br.REPORTS_DIR)


def alerts_dir(reports_dir: Path | str | None = None) -> Path:
    base = Path(reports_dir) if reports_dir is not None else _reports_dir()
    return base / ALERTS_DIRNAME


def snapshot_path(as_of: str, reports_dir: Path | str | None = None) -> Path:
    return alerts_dir(reports_dir) / f"{str(as_of)[:10]}{SNAPSHOT_SUFFIX}"


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _as_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_float(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out == out else None


def _now_shanghai_iso() -> str:
    return datetime.now(SHANGHAI_TZ).isoformat(timespec="seconds")


def _now_utc_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def resolve_as_of(as_of: str | None = None) -> str:
    """``asOf`` or the latest complete A-share session (fail-open to today)."""
    text = str(as_of or "").strip()[:10]
    if text:
        return text
    try:
        from data_sync_service.service.factor_live import latest_complete_session

        latest = latest_complete_session()
        if latest:
            return str(latest)[:10]
    except Exception as exc:  # noqa: BLE001 — display default, never a gate
        logger.warning("strategy alerts latest session lookup failed: %s", exc)
    from data_sync_service.service.trade_calendar_utils import shanghai_today_iso

    return shanghai_today_iso()


# ---------------------------------------------------------------------------
# previous snapshot (the classification baseline)
# ---------------------------------------------------------------------------


def _snapshot_dates(reports_dir: Path | str | None = None) -> list[str]:
    directory = alerts_dir(reports_dir)
    out: list[str] = []
    try:
        paths = list(directory.glob(f"*{SNAPSHOT_SUFFIX}"))
    except OSError:
        return out
    for path in paths:
        day = path.stem
        try:
            date.fromisoformat(day)
        except ValueError:
            continue
        out.append(day)
    return sorted(out)


def _previous_snapshot(
    as_of: str, reports_dir: Path | str | None = None
) -> tuple[str | None, dict[str, Any]]:
    """Latest persisted snapshot strictly before ``as_of`` (never today's own)."""
    for day in reversed(_snapshot_dates(reports_dir)):
        if day >= as_of:
            continue
        raw = _read_json(snapshot_path(day, reports_dir))
        if isinstance(raw, dict):
            return day, raw
    return None, {}


def _positions_from_snapshot(snapshot: dict[str, Any]) -> dict[str, int]:
    out: dict[str, int] = {}
    for fid, value in (snapshot.get("positions") or {}).items():
        pos = _as_int(value)
        if fid and pos is not None:
            out[str(fid)] = pos
    return out


def _states_from_snapshot(snapshot: dict[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for fid, value in (snapshot.get("states") or {}).items():
        if fid and value:
            out[str(fid)] = str(value)
    return out


def _sgap_pos(snapshot: dict[str, Any]) -> int | None:
    sgap = snapshot.get("sgap") or {}
    if isinstance(sgap, dict):
        return _as_int(sgap.get("revivalPos"))
    return _as_int(sgap)


# ---------------------------------------------------------------------------
# dated "new today" evidence
# ---------------------------------------------------------------------------


def _history_rows(reports_dir: Path | str | None = None) -> list[dict[str, Any]]:
    base = Path(reports_dir) if reports_dir is not None else _reports_dir()
    raw = _read_json(base / "factor_vault_history.json")
    rows = raw.get("history") if isinstance(raw, dict) else raw
    if not isinstance(rows, list):
        return []
    return [r for r in rows if isinstance(r, dict) and str(r.get("date") or "")]


def _state_rows(
    history: list[dict[str, Any]], as_of: str
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    today_row: dict[str, Any] | None = None
    prev_row: dict[str, Any] | None = None
    for row in history:
        day = str(row.get("date") or "")[:10]
        if day == as_of:
            today_row = row
    for row in reversed(history):
        day = str(row.get("date") or "")[:10]
        if day < as_of:
            prev_row = row
            break
    return today_row, prev_row


def _transitions(as_of: str, reports_dir: Path | str | None = None) -> list[dict[str, Any]]:
    base = Path(reports_dir) if reports_dir is not None else _reports_dir()
    raw = _read_json(base / "factor_vault_transitions.json")
    rows = raw.get("transitions") if isinstance(raw, dict) else raw
    if not isinstance(rows, list):
        return []
    return [r for r in rows if isinstance(r, dict) and str(r.get("date") or "")[:10] == as_of]


def _new_status_change(
    *,
    fid: str,
    current: str,
    today_row: dict[str, Any] | None,
    prev_row: dict[str, Any] | None,
    prev_snapshot: dict[str, Any],
    transitions: list[dict[str, Any]],
) -> tuple[bool, str | None]:
    """True when ``fid`` changed to ``current`` and the change is dated today.

    Proof order: today's history row vs the previous row, then a transition
    row written today, then the previous snapshot's state. A change that
    happened days ago is not re-broadcast.
    """
    if today_row and prev_row:
        today_states = today_row.get("states") or {}
        prev_states = prev_row.get("states") or {}
        before = prev_states.get(fid)
        if str(today_states.get(fid) or "") == current:
            if before not in (None, current):
                return True, str(before)
            if before == current:
                # Yesterday's history row is authoritative: a previously seen
                # revived/cold state must not be re-broadcast from an older
                # snapshot (e.g. the job missed a day).
                return False, None
    for trans in transitions:
        if str(trans.get("factor_id") or "") != fid:
            continue
        if str(trans.get("to") or "") == current:
            before = trans.get("from")
            return True, str(before) if before is not None else None
    before = _states_from_snapshot(prev_snapshot).get(fid)
    if before not in (None, current):
        return True, before
    return False, None


def _sgap_generated_date(decay: dict[str, Any]) -> str | None:
    meta = decay.get("meta") if isinstance(decay, dict) else None
    text = str((meta or {}).get("generated_at") or "")[:10]
    return text or None


def _sgap_change_dated(
    *,
    as_of: str,
    decay: dict[str, Any],
    transitions: list[dict[str, Any]],
) -> bool:
    if _sgap_generated_date(decay) == as_of:
        return True
    return any(str(t.get("event_type") or "") == "sgap_revival" for t in transitions)


def _load_sgap_decay(reports_dir: Path | str | None = None) -> dict[str, Any]:
    base = Path(reports_dir) if reports_dir is not None else _reports_dir()
    raw = _read_json(base / "sgap_decay.json")
    return raw if isinstance(raw, dict) else {}


# ---------------------------------------------------------------------------
# copy helpers
# ---------------------------------------------------------------------------


def _status_zh(status: Any) -> str:
    return STATUS_ZH.get(str(status or ""), str(status or "未知"))


def _fmt_num(value: Any, unit: str | None = None) -> str:
    num = _as_float(value)
    if num is None:
        return "—"
    suffix = unit or ""
    return f"{num:+.2f}{suffix}"


def _policy_note(action: Any) -> str:
    act = str(action or "")
    if act == "advise_exit":
        return "当前政策 advise_exit：仅建议退出该卫星袖套、回母港 M30 等待。"
    if act == "advise_follow":
        return "当前政策 advise_follow：仅建议按规则权重跟随；实盘前置未满时建议实盘权重仍为 0%。"
    return "当前政策 wait_in_baseline：继续坐母港 M30 基线。"


def _event_key(kind: str, fid: str, as_of: str) -> str:
    return f"strategy_alert:{kind}:{fid}:{as_of}"


def _decay_event(
    *,
    fid: str,
    name: str,
    as_of: str,
    from_status: str | None,
    to_status: str | None,
    from_position: int | None,
    to_position: int | None,
    policy_action: str,
    percentile: Any = None,
    net: Any = None,
    unit: Any = None,
) -> dict[str, Any]:
    if from_status and to_status and from_status != to_status:
        detail = f"{name} 从 {_status_zh(from_status)} 衰减到 {_status_zh(to_status)}"
    else:
        detail = f"{name} 建议权重下台阶（状态仍 {_status_zh(to_status or from_status)}）"
    if from_position is not None or to_position is not None:
        detail += f"（建议权重 {from_position if from_position is not None else '—'}% -> "
        detail += f"{to_position if to_position is not None else '—'}%）"
    copy = f"衰减提醒{NOTICE_PREFIX}：{detail}；{_policy_note(policy_action)}"
    return {
        "eventKey": _event_key("decay", fid, as_of),
        "kind": "decay",
        "factorId": fid,
        "name": name,
        "asOf": as_of,
        "newToday": True,
        "from": from_status,
        "to": to_status,
        "fromPosition": from_position,
        "toPosition": to_position,
        "evidence": {
            "statusFrom": from_status,
            "statusTo": to_status,
            "positionFrom": from_position,
            "positionTo": to_position,
            "percentile": _as_float(percentile),
            "net": _as_float(net),
            "unit": unit,
        },
        "policyAction": policy_action,
        "copy": copy,
    }


def _revival_event(
    *,
    fid: str,
    name: str,
    as_of: str,
    from_status: str | None,
    to_status: str | None,
    from_position: int | None,
    to_position: int | None,
    policy_action: str,
    percentile: Any = None,
    net: Any = None,
    unit: Any = None,
) -> dict[str, Any]:
    if from_status and to_status and from_status != to_status:
        detail = f"{name} 从 {_status_zh(from_status)} 转入 {_status_zh(to_status)}"
    else:
        detail = f"{name} 复活位 / 建议权重上台阶"
    if from_position is not None or to_position is not None:
        detail += f"（建议权重 {from_position if from_position is not None else '—'}% -> "
        detail += f"{to_position if to_position is not None else '—'}%）"
    copy = f"复活提醒{NOTICE_PREFIX}：{detail}；{_policy_note(policy_action)}"
    return {
        "eventKey": _event_key("revival", fid, as_of),
        "kind": "revival",
        "factorId": fid,
        "name": name,
        "asOf": as_of,
        "newToday": True,
        "from": from_status,
        "to": to_status,
        "fromPosition": from_position,
        "toPosition": to_position,
        "evidence": {
            "statusFrom": from_status,
            "statusTo": to_status,
            "positionFrom": from_position,
            "positionTo": to_position,
            "percentile": _as_float(percentile),
            "net": _as_float(net),
            "unit": unit,
        },
        "policyAction": policy_action,
        "copy": copy,
    }


def _classify(net: float, baseline_net: float) -> str:
    if net > baseline_net:
        return "ahead"
    if net < baseline_net:
        return "behind"
    return "flat"


def _excess_event(
    *,
    brick: dict[str, Any],
    as_of: str,
    baseline_id: str,
    baseline_label: str,
    baseline_net: float,
    unit: str,
    from_class: str | None,
    to_class: str,
    policy_action: str,
) -> dict[str, Any]:
    recent = brick.get("recentPerformance") or {}
    fid = str(brick.get("id") or "")
    name = str(brick.get("name") or fid)
    net = _as_float(recent.get("net"))
    gap = (net - baseline_net) if net is not None else None
    if from_class is None:
        change = f"首次可同口径比较，相对基线 {baseline_label} 为{CLASS_ZH.get(to_class, to_class)}"
    else:
        change = (
            f"由 {CLASS_ZH.get(from_class, from_class)} 变为 {CLASS_ZH.get(to_class, to_class)}"
        )
    copy = (
        f"超额变化提醒{NOTICE_PREFIX}：{name} 近期净 {_fmt_num(net, unit)}，"
        f"基线 {baseline_label}（{baseline_id}）为 {_fmt_num(baseline_net, unit)}"
        f"（同口径 {unit}，差 {_fmt_num(gap, unit)}），{change}；{_policy_note(policy_action)}"
    )
    return {
        "eventKey": _event_key("excess_change", fid, as_of),
        "kind": "excess_change",
        "factorId": fid,
        "name": name,
        "asOf": as_of,
        "newToday": True,
        "fromClass": from_class,
        "toClass": to_class,
        "evidence": {
            "net": net,
            "baselineNet": baseline_net,
            "baselineId": baseline_id,
            "unit": unit,
            "gap": gap,
            "window": recent.get("window"),
            "recentAsOf": recent.get("asOf"),
            "percentile": _as_float(recent.get("percentile")),
            "source": recent.get("source"),
        },
        "policyAction": policy_action,
        "copy": copy,
    }


# ---------------------------------------------------------------------------
# snapshot build
# ---------------------------------------------------------------------------


def _baseline_brick(bricks: list[dict[str, Any]]) -> dict[str, Any] | None:
    by_id = {str(row.get("id") or ""): row for row in bricks if isinstance(row, dict)}
    for fid in BASELINE_IDS:
        if fid in by_id:
            return by_id[fid]
    return None


def _standing_lights(factors: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for fid in (*SATELLITE_IDS, *REFERENCE_LIGHT_IDS):
        row = factors.get(fid) if isinstance(factors, dict) else None
        if not isinstance(row, dict):
            continue
        status = str(row.get("status") or "")
        if status == "revived":
            note = (
                "参考复活灯：只做对照，不是下单指令。"
                if fid in REFERENCE_LIGHT_IDS
                else "复活灯：实盘三前置未满足时权重仍为 0%。"
            )
        elif status == "cold":
            note = "仍在冷库（当前状态，不是今日新提醒）。"
        else:
            note = "观察中（当前状态，不是今日新提醒）。"
        out.append(
            {
                "id": fid,
                "name": row.get("name") or fid,
                "status": status,
                "position": _as_int(row.get("position")) or 0,
                "note": note,
            }
        )
    return out


def _empty_snapshot(
    *,
    as_of: str,
    now_shanghai: str,
    error: str,
    freshness: dict[str, Any] | None = None,
) -> dict[str, Any]:
    fresh = freshness or {
        "stale": True,
        "reason": error,
        "checkedAt": now_shanghai,
    }
    return {
        "version": SNAPSHOT_VERSION,
        "available": False,
        "degraded": True,
        "error": error,
        "asOf": as_of,
        "generatedAt": _now_utc_iso(),
        "generatedAtShanghai": now_shanghai,
        "freshness": fresh,
        "baseline": BASELINE,
        "baselineLabel": BASELINE_LABEL,
        "baselineFactorId": None,
        "currentState": {
            "policyAction": "wait_in_baseline",
            "policyReason": "因子库不可用；按母港 M30 基线等待，不自动跟卫星。",
            "nCold": 0,
            "nWatch": 0,
            "nRevived": 0,
            "standingLights": [],
        },
        "events": [],
        "newToday": [],
        "copyLines": [],
        "quiet": True,
        "skippedIncomparable": [],
        "classifications": {},
        "states": {},
        "positions": {},
        "sgap": {"revivalPos": None, "generatedAt": None},
        "previousAsOf": None,
        "adviceOnly": True,
    }


def build_strategy_alerts(
    *,
    as_of: str | None = None,
    reports_dir: Path | str | None = None,
) -> dict[str, Any]:
    """Build (but do not persist) the after-close alert snapshot."""
    day = resolve_as_of(as_of)
    now_shanghai = _now_shanghai_iso()
    base = Path(reports_dir) if reports_dir is not None else _reports_dir()

    prev_day, prev_snapshot = _previous_snapshot(day, base)
    prev_positions = _positions_from_snapshot(prev_snapshot)
    prev_sgap = _sgap_pos(prev_snapshot)
    prev_classes = prev_snapshot.get("classifications") or {}
    if not isinstance(prev_classes, dict):
        prev_classes = {}

    prev_factors = (
        {fid: {"position": pos} for fid, pos in prev_positions.items()}
        if prev_positions
        else None
    )
    payload = build_factor_system(
        reports_dir=base,
        now=day,
        latest_session=day,
        prev_factors=prev_factors,
        prev_sgap_revival_pos=prev_sgap,
    )
    if not payload.get("available"):
        return _empty_snapshot(
            as_of=day,
            now_shanghai=now_shanghai,
            error=str(payload.get("error") or "factor_vault_unavailable"),
            freshness={
                "stale": True,
                "reason": str((payload.get("freshness") or {}).get("reason") or "因子库不可读"),
                "checkedAt": now_shanghai,
            },
        )

    policy = payload.get("policy") or {}
    action = str(policy.get("action") or "wait_in_baseline")
    summary = payload.get("summary") or {}
    factors = {str(row.get("id")): row for row in payload.get("bricks") or [] if row.get("id")}

    vault_generated = str(
        (payload.get("freshness") or {}).get("sourceGeneratedAt") or ""
    )[:10]
    vault_dated_today = vault_generated == day

    history = _history_rows(base)
    today_row, prev_row = _state_rows(history, day)
    transitions = _transitions(day, base)
    decay_doc = _load_sgap_decay(base)
    sgap_generated = _sgap_generated_date(decay_doc)
    sgap_summary = decay_doc.get("summary") or {}
    sgap_pos = _as_int(sgap_summary.get("revival_pos"))
    sgap_dated_today = _sgap_change_dated(as_of=day, decay=decay_doc, transitions=transitions)

    events: list[dict[str, Any]] = []
    seen_keys: set[str] = set()

    def _add(event: dict[str, Any]) -> None:
        key = str(event.get("eventKey") or "")
        if not key or key in seen_keys:
            return
        seen_keys.add(key)
        events.append(event)

    # -- decays (reuse the factor system's own detector) -------------------
    for candidate in policy.get("decayedFactors") or []:
        if not isinstance(candidate, dict):
            continue
        fid = str(candidate.get("id") or "")
        if not fid:
            continue
        if fid == "sgap":
            if (
                sgap_pos is None
                or prev_sgap is None
                or sgap_pos >= prev_sgap
                or not sgap_dated_today
            ):
                continue
            _add(
                _decay_event(
                    fid="sgap",
                    name=str(candidate.get("name") or "S-gap K2 复活位"),
                    as_of=day,
                    from_status=None,
                    to_status=None,
                    from_position=prev_sgap,
                    to_position=sgap_pos,
                    policy_action=action,
                    percentile=sgap_summary.get("latest_r40_percentile"),
                    net=sgap_summary.get("latest_r40_mean"),
                    unit="%",
                )
            )
            continue

        row = factors.get(fid) or {}
        from_status = (
            str(candidate.get("from")) if candidate.get("from") is not None else None
        )
        to_status = str(candidate.get("to")) if candidate.get("to") is not None else None
        from_position = _as_int(candidate.get("fromPosition"))
        to_position = _as_int(candidate.get("toPosition"))

        if from_status and to_status and from_status != to_status:
            new_today, before = _new_status_change(
                fid=fid,
                current=to_status,
                today_row=today_row,
                prev_row=prev_row,
                prev_snapshot=prev_snapshot,
                transitions=transitions,
            )
            if not new_today:
                continue
            from_status = before or from_status
            matching_transition = any(
                str(t.get("factor_id") or "") == fid
                and str(t.get("to") or "") == to_status
                for t in transitions
            )
            if not vault_dated_today and not matching_transition:
                continue
        else:
            # Position step-down: first appears in today's vault vs the
            # previous snapshot (history only records statuses).
            if (
                not vault_dated_today
                or not prev_snapshot
                or from_position is None
                or to_position is None
                or to_position >= from_position
                or prev_positions.get(fid) != from_position
            ):
                continue
            if today_row and prev_row:
                today_states = today_row.get("states") or {}
                prev_states_row = prev_row.get("states") or {}
                if today_states.get(fid) != prev_states_row.get(fid):
                    # A status change would be its own event; skip this form.
                    continue

        _add(
            _decay_event(
                fid=fid,
                name=str(candidate.get("name") or row.get("name") or fid),
                as_of=day,
                from_status=from_status,
                to_status=to_status,
                from_position=from_position,
                to_position=to_position,
                policy_action=action,
                percentile=row.get("percentile"),
                net=(row.get("recentPerformance") or {}).get("net"),
                unit=(row.get("recentPerformance") or {}).get("unit") or row.get("unit"),
            )
        )

    # -- revivals ----------------------------------------------------------
    for fid in SATELLITE_IDS:
        row = factors.get(fid)
        if not isinstance(row, dict):
            continue
        if str(row.get("status") or "") != "revived":
            continue
        new_today, before = _new_status_change(
            fid=fid,
            current="revived",
            today_row=today_row,
            prev_row=prev_row,
            prev_snapshot=prev_snapshot,
            transitions=transitions,
        )
        if not new_today:
            continue
        matching_transition = any(
            str(t.get("factor_id") or "") == fid and str(t.get("to") or "") == "revived"
            for t in transitions
        )
        if not vault_dated_today and not matching_transition:
            continue
        prev_pos = prev_positions.get(fid)
        _add(
            _revival_event(
                fid=fid,
                name=str(row.get("name") or fid),
                as_of=day,
                from_status=before,
                to_status="revived",
                from_position=prev_pos,
                to_position=_as_int(row.get("position")) or 0,
                policy_action=action,
                percentile=row.get("percentile"),
                net=(row.get("recentPerformance") or {}).get("net"),
                unit=(row.get("recentPerformance") or {}).get("unit") or row.get("unit"),
            )
        )

    if (
        sgap_pos is not None
        and prev_sgap is not None
        and sgap_pos > prev_sgap
        and sgap_dated_today
    ):
        _add(
            _revival_event(
                fid="sgap",
                name="S-gap K2 复活位",
                as_of=day,
                from_status=None,
                to_status=None,
                from_position=prev_sgap,
                to_position=sgap_pos,
                policy_action=action,
                percentile=sgap_summary.get("latest_r40_percentile"),
                net=sgap_summary.get("latest_r40_mean"),
                unit="%",
            )
        )

    # -- excess vs baseline (same unit only) -------------------------------
    bricks = [row for row in payload.get("bricks") or [] if isinstance(row, dict)]
    baseline_brick = _baseline_brick(bricks)
    baseline_id = str(baseline_brick.get("id") or "") if baseline_brick else None
    baseline_recent = (baseline_brick or {}).get("recentPerformance") or {}
    baseline_net = _as_float(baseline_recent.get("net"))
    baseline_unit = baseline_recent.get("unit")
    if not isinstance(baseline_unit, str):
        baseline_unit = (baseline_brick or {}).get("unit")

    skipped: list[dict[str, Any]] = []
    classifications: dict[str, dict[str, Any]] = {}
    for brick in bricks:
        fid = str(brick.get("id") or "")
        if not fid or fid == baseline_id:
            continue
        recent = brick.get("recentPerformance") or {}
        unit = recent.get("unit") or brick.get("unit")
        net = _as_float(recent.get("net"))
        if baseline_id is None or baseline_net is None:
            skipped.append(
                {
                    "factorId": fid,
                    "name": brick.get("name") or fid,
                    "unit": unit,
                    "baselineUnit": baseline_unit,
                    "reason": "baseline_recent_net_missing",
                }
            )
            continue
        if not isinstance(unit, str) or unit != baseline_unit:
            skipped.append(
                {
                    "factorId": fid,
                    "name": brick.get("name") or fid,
                    "unit": unit,
                    "baselineUnit": baseline_unit,
                    "reason": "unit_mismatch",
                }
            )
            continue
        if net is None or recent.get("missing"):
            continue
        cls = _classify(net, baseline_net)
        classifications[fid] = {
            "class": cls,
            "unit": unit,
            "net": net,
            "baselineNet": baseline_net,
            "baselineId": baseline_id,
            "recentAsOf": recent.get("asOf"),
            "window": recent.get("window"),
        }
        prev_class = None
        if prev_day:
            prev_row_cls = prev_classes.get(fid) or {}
            if isinstance(prev_row_cls, dict):
                prev_class = prev_row_cls.get("class")
        first_comparable = prev_day is None or fid not in prev_classes
        if not first_comparable and prev_class == cls:
            continue
        _add(
            _excess_event(
                brick=brick,
                as_of=day,
                baseline_id=str(baseline_id),
                baseline_label=BASELINE_LABEL,
                baseline_net=baseline_net,
                unit=unit,
                from_class=str(prev_class) if prev_class else None,
                to_class=cls,
                policy_action=action,
            )
        )

    fresh = payload.get("freshness") or {}
    stale = bool(fresh.get("stale"))
    freshness = {
        "stale": stale,
        "reason": fresh.get("reason"),
        "vaultGeneratedAt": fresh.get("sourceGeneratedAt"),
        "liveAsOf": fresh.get("liveAsOf"),
        "expectedSession": fresh.get("expectedSession"),
        "checkedAt": now_shanghai,
    }

    states = {str(row.get("id")): str(row.get("status") or "") for row in factors.values()}
    positions = {
        str(row.get("id")): _as_int(row.get("position")) or 0 for row in factors.values()
    }

    new_today_summaries = [
        {
            "eventKey": event.get("eventKey"),
            "kind": event.get("kind"),
            "factorId": event.get("factorId"),
            "name": event.get("name"),
            "copy": event.get("copy"),
        }
        for event in events
    ]

    return {
        "version": SNAPSHOT_VERSION,
        "available": True,
        "degraded": stale,
        "error": None,
        "asOf": day,
        "generatedAt": _now_utc_iso(),
        "generatedAtShanghai": now_shanghai,
        "freshness": freshness,
        "baseline": BASELINE,
        "baselineLabel": BASELINE_LABEL,
        "baselineFactorId": baseline_id,
        "currentState": {
            "policyAction": action,
            "policyReason": policy.get("reason"),
            "nCold": _as_int(summary.get("nCold")) or 0,
            "nWatch": _as_int(summary.get("nWatch")) or 0,
            "nRevived": _as_int(summary.get("nRevived")) or 0,
            "standingLights": _standing_lights(factors),
        },
        "events": events,
        "newToday": new_today_summaries,
        "copyLines": [str(event.get("copy")) for event in events if event.get("copy")],
        "quiet": not events,
        "skippedIncomparable": skipped,
        "classifications": classifications,
        "states": states,
        "positions": positions,
        "sgap": {
            "revivalPos": sgap_pos,
            "generatedAt": sgap_generated,
        },
        "previousAsOf": prev_day,
        "adviceOnly": True,
    }


# ---------------------------------------------------------------------------
# persist / read
# ---------------------------------------------------------------------------


def _write_snapshot(
    payload: dict[str, Any], reports_dir: Path | str | None = None
) -> tuple[bool, str | None]:
    path = snapshot_path(str(payload.get("asOf") or ""), reports_dir)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, path)
        return True, None
    except OSError as exc:
        logger.warning("strategy alerts snapshot write failed: %s", exc)
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        return False, str(exc)


def run_strategy_alerts(
    *,
    as_of: str | None = None,
    reports_dir: Path | str | None = None,
) -> dict[str, Any]:
    """Build + persist today's snapshot (idempotent per ``asOf``)."""
    payload = build_strategy_alerts(as_of=as_of, reports_dir=reports_dir)
    ok, error = _write_snapshot(payload, reports_dir)
    payload["persisted"] = bool(ok)
    if not ok:
        payload["writeError"] = error
    return payload


def read_strategy_alerts(
    *,
    as_of: str | None = None,
    reports_dir: Path | str | None = None,
) -> dict[str, Any]:
    """Read the persisted snapshot only — never builds or writes on GET."""
    day = resolve_as_of(as_of)
    base = Path(reports_dir) if reports_dir is not None else _reports_dir()
    raw = _read_json(snapshot_path(day, base))
    if isinstance(raw, dict):
        out = dict(raw)
        out["snapshotMissing"] = False
        return out

    dates = _snapshot_dates(base)
    previous = dates[-1] if dates else None
    if previous:
        reason = f"今日盘后策略提醒快照缺失（最近一份 {previous}）；不自动补写，请跑 19:20 任务。"
        degraded = True
    else:
        reason = "盘后策略提醒尚未生成（工作日 19:20 任务未跑过）。"
        degraded = False
    out = _empty_snapshot(
        as_of=day,
        now_shanghai=_now_shanghai_iso(),
        error="snapshot_missing",
        freshness={"stale": bool(previous), "reason": reason, "checkedAt": _now_shanghai_iso()},
    )
    out["degraded"] = degraded
    out["snapshotMissing"] = True
    out["reason"] = reason
    out["latestSnapshotAsOf"] = previous
    return out


def compact_strategy_alerts(payload: dict[str, Any]) -> dict[str, Any]:
    """Compact brief block: quiet / new-today copies / asOf / stale."""
    current = payload.get("currentState") or {}
    fresh = payload.get("freshness") or {}
    skipped = payload.get("skippedIncomparable") or []
    return {
        "available": bool(payload.get("available")),
        "degraded": bool(payload.get("degraded")),
        "snapshotMissing": bool(payload.get("snapshotMissing")),
        "asOf": payload.get("asOf"),
        "generatedAtShanghai": payload.get("generatedAtShanghai"),
        "baseline": payload.get("baseline") or BASELINE,
        "baselineLabel": payload.get("baselineLabel") or BASELINE_LABEL,
        "quiet": bool(payload.get("quiet", True)),
        "stale": bool(fresh.get("stale")),
        "freshness": {"stale": bool(fresh.get("stale")), "reason": fresh.get("reason")},
        "currentState": {
            "policyAction": current.get("policyAction"),
            "nCold": current.get("nCold"),
            "nWatch": current.get("nWatch"),
            "nRevived": current.get("nRevived"),
        },
        "newToday": [
            {
                "eventKey": row.get("eventKey"),
                "kind": row.get("kind"),
                "factorId": row.get("factorId"),
                "name": row.get("name"),
                "copy": row.get("copy"),
            }
            for row in payload.get("newToday") or []
            if isinstance(row, dict)
        ],
        "copyLines": [str(line) for line in payload.get("copyLines") or []],
        "skippedIncomparable": [
            {
                "factorId": row.get("factorId"),
                "unit": row.get("unit"),
                "baselineUnit": row.get("baselineUnit"),
                "reason": row.get("reason"),
            }
            for row in skipped
            if isinstance(row, dict)
        ],
        "reason": payload.get("reason"),
        "adviceOnly": True,
    }
