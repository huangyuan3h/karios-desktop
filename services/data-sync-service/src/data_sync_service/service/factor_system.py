"""Factor system contract — vault state to advice-only policy (2026-11-11.4).

This turns the read-only factor vault (27 factors, H2k cold/watch/revived)
into one stable, agent-readable policy. It is **advice only**: it never
rewrites ``sleeve.action``, ``TRAIL_PCT``, the parking cooldown (k=0), the
settings mode, a strategy weight, or a broker order.

Policy priority (frozen):
1. A followed satellite factor decays (revived/watch -> cold, or its
   suggested 0/10/20 position falls, or K2 ``revival_pos`` steps down)
   => ``advise_exit``: leave the satellite sleeve, wait in baseline.
2. Relevant satellite factors are revived with a confirmed position
   => ``advise_follow`` at the rule's 0/10/20 weight (never above 20).
3. Cold, or watch without confirmation => ``wait_in_baseline``.
   The baseline is ``homeport_m30`` / 母港 M30, never a new engine.

Live preconditions stay honest and block *live* follow only; they never hide
the light. The three documented H2k gates are:
- paper-20 same recipe,
- holdout recovered to within -10%,
- filtered valid turned positive (> 0).
If any is unmet, a revived factor can still show its light but
``suggestedLiveWeight`` stays 0 and ``liveBlocked`` explains why.

Freshness is surfaced, not hidden: the vault and trade blotter are frozen
(no live append path in this slice), so the lights do not move on their own.

Fail-open: missing/corrupt vault files return ``available: false`` with an
``error`` and a conservative wait-in-baseline policy; the judgment brief
still returns. No DB, no network, no clock side effects (``now`` is injected;
only the API wrapper defaults it to today).
"""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any

from data_sync_service.service import combo_tiers_config as cfg

logger = logging.getLogger(__name__)

ADVICE_ONLY = True
BASELINE = "homeport_m30"
BASELINE_LABEL = "母港 M30"
MAX_LIVE_WEIGHT = 20
STALE_AFTER_DAYS = 3

# The S-gap / starship family is the satellite sleeve this policy can follow.
# h11/h08/h13 are S-gap kin / satellite-observation factors.
SATELLITE_IDS: tuple[str, ...] = (
    "starship_b",
    "starship_a25",
    "starship_b3",
    "starship_h2",
    "starship_cap50",
    "h11",
    "h08",
    "h13",
)
STARSHIP_IDS: tuple[str, ...] = (
    "starship_b",
    "starship_a25",
    "starship_b3",
    "starship_h2",
    "starship_cap50",
)
# Reference lights are displayed but are never an order or a follow signal.
REFERENCE_IDS: frozenset[str] = frozenset({"harbor", "m30", "bleg"})

# Documented current paper-20 count (H2k Sec 4 / STEP1 2026-10-03: ~4/20,
# all losing). There is no live blotter append path in this slice, so this
# stays a dated documented value and is surfaced as stale, not silently live.
PAPER_FILLS_AS_OF = 4
PAPER_FILLS_AS_OF_DATE = "2026-10-03"

FOCUS_LIMIT = 8
TRANSITIONS_LIMIT = 5


def _reports_dir() -> Path:
    from data_sync_service.api import backtest_routes as br

    return Path(br.REPORTS_DIR)


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_history(path: Path) -> list[dict[str, Any]]:
    try:
        raw = _read_json(path)
    except (OSError, ValueError):
        return []
    if isinstance(raw, dict):
        hist = raw.get("history", [])
        return hist if isinstance(hist, list) else []
    return raw if isinstance(raw, list) else []


def _load_transitions(path: Path) -> list[dict[str, Any]]:
    try:
        raw = _read_json(path)
    except (OSError, ValueError):
        return []
    if isinstance(raw, dict):
        rows = raw.get("transitions", [])
        return rows if isinstance(rows, list) else []
    return raw if isinstance(raw, list) else []


def _as_float(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out == out else None


def _as_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _factor_map(vault: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in vault.get("factors") or []:
        if isinstance(row, dict) and row.get("id"):
            out[str(row["id"])] = row
    return out


def _age_days(generated_at: Any, today: date) -> int | None:
    text = str(generated_at or "")[:10]
    if not text:
        return None
    try:
        return (today - date.fromisoformat(text)).days
    except ValueError:
        return None


def _history_prev_states(
    history: list[dict[str, Any]], current_states: dict[str, str]
) -> dict[str, str]:
    """Last history snapshot that differs from today (today's own row skipped)."""
    for entry in reversed(history):
        states = (entry or {}).get("states") or {}
        if not isinstance(states, dict) or not states:
            continue
        norm = {str(k): str(v) for k, v in states.items() if k and v}
        if norm == current_states:
            continue
        return norm
    return {}


def _previous_positions(prev_factors: dict[str, Any] | None) -> dict[str, int]:
    out: dict[str, int] = {}
    for fid, row in (prev_factors or {}).items():
        if isinstance(row, dict):
            pos = _as_int(row.get("position"))
            if pos is not None:
                out[str(fid)] = pos
        else:
            pos = _as_int(row)
            if pos is not None:
                out[str(fid)] = pos
    return out


def _decays(
    *,
    factors: dict[str, dict[str, Any]],
    prev_states: dict[str, str],
    prev_positions: dict[str, int],
    prev_sgap_pos: int | None,
    decay: dict[str, Any],
) -> list[dict[str, Any]]:
    """Followed-factor decays from status, position, and K2 revival_pos steps."""
    out: list[dict[str, Any]] = []
    for fid in SATELLITE_IDS:
        row = factors.get(fid)
        if not row:
            continue
        status = str(row.get("status") or "")
        prev = prev_states.get(fid)
        pos = _as_int(row.get("position")) or 0
        prev_pos = prev_positions.get(fid)
        if prev in ("revived", "watch") and status == "cold":
            out.append(
                {
                    "id": fid,
                    "name": row.get("name") or fid,
                    "from": prev,
                    "to": status,
                    "fromPosition": prev_pos,
                    "toPosition": pos,
                    "reason": f"{row.get('name') or fid} 从 {prev} 衰减到冷库",
                }
            )
        elif prev_pos is not None and pos < prev_pos:
            out.append(
                {
                    "id": fid,
                    "name": row.get("name") or fid,
                    "from": prev or status,
                    "to": status,
                    "fromPosition": prev_pos,
                    "toPosition": pos,
                    "reason": f"{row.get('name') or fid} 建议权重 {prev_pos}% -> {pos}%",
                }
            )
    summary = (decay or {}).get("summary") or {}
    cur_sgap = _as_int(summary.get("revival_pos"))
    if prev_sgap_pos is None:
        prev_sgap_pos = _as_int((decay or {}).get("prev_revival_pos"))
    if prev_sgap_pos is None:
        # Same-series step-down: compare the last two non-null K2 states in
        # the frozen rolling array (no separate history file in this slice).
        rolling = (decay or {}).get("rolling") or []
        states: list[int] = []
        for row in reversed(rolling if isinstance(rolling, list) else []):
            if not isinstance(row, dict):
                continue
            state = _as_int(row.get("revival_pos"))
            if state is not None:
                states.append(state)
            if len(states) >= 2:
                break
        if len(states) >= 2:
            cur_sgap, prev_sgap_pos = states[0], states[1]
    if prev_sgap_pos is not None and cur_sgap is not None and cur_sgap < prev_sgap_pos:
        out.append(
            {
                "id": "sgap",
                "name": "S-gap K2 复活位",
                "from": prev_sgap_pos,
                "to": cur_sgap,
                "fromPosition": prev_sgap_pos,
                "toPosition": cur_sgap,
                "reason": f"S-gap K2 revival_pos {prev_sgap_pos}% -> {cur_sgap}%",
            }
        )
    return out


def _holdout_pct(factors: dict[str, dict[str, Any]], fid: str) -> float | None:
    row = factors.get(fid) or {}
    for entry in row.get("history") or []:
        if not isinstance(entry, dict) or entry.get("window") != "holdout":
            continue
        return _as_float(entry.get("net"))
    return None


def _valid_pct(factors: dict[str, dict[str, Any]], fid: str) -> float | None:
    row = factors.get(fid) or {}
    for entry in row.get("history") or []:
        if not isinstance(entry, dict) or entry.get("window") != "valid":
            continue
        return _as_float(entry.get("net"))
    return None


def _live_preconditions(
    factors: dict[str, dict[str, Any]],
    override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """The three H2k live gates, honest about what is missing.

    ``paper20`` is a dated documented count (no live append path yet).
    ``holdoutRecovery`` and ``filteredValidPositive`` come from the vault's
    starship_b / starship_cap50 holdout+valid net (same frozen units).
    """
    override = override or {}
    paper_req = cfg.PAPER_REQUIRED_N
    paper_done = int(override.get("paperFills", PAPER_FILLS_AS_OF))
    paper_pass = bool(override.get("paper20Pass", paper_done >= paper_req))

    holdout_now = _as_float(override.get("holdoutPct"))
    if holdout_now is None:
        holdout_now = _holdout_pct(factors, "starship_b")
    holdout_threshold = float(cfg.HOLDOUT_RECOVER_TO) * 100.0
    holdout_pass = bool(
        override.get(
            "holdoutRecovered",
            holdout_now is not None and holdout_now >= holdout_threshold,
        )
    )

    filtered_now = _as_float(override.get("filteredValidPct"))
    if filtered_now is None:
        filtered_now = _valid_pct(factors, "starship_cap50")
    filtered_threshold = float(cfg.FILTERED_VALID_POSITIVE) * 100.0
    filtered_pass = bool(
        override.get(
            "filteredValidPositive",
            filtered_now is not None and filtered_now > filtered_threshold,
        )
    )

    all_pass = bool(paper_pass and holdout_pass and filtered_pass)
    return {
        "paper20": {
            "required": paper_req,
            "completed": paper_done,
            "pass": paper_pass,
            "asOfDate": PAPER_FILLS_AS_OF_DATE,
            "source": "documented 2026-10-03 audit; no live blotter append in this slice",
            "stale": True,
        },
        "holdoutRecovery": {
            "currentPct": holdout_now,
            "recoverToPct": holdout_threshold,
            "pass": holdout_pass,
            "source": "factor vault starship_b holdout net (frozen)",
        },
        "filteredValidPositive": {
            "currentPct": filtered_now,
            "thresholdPct": filtered_threshold,
            "pass": filtered_pass,
            "source": "factor vault starship_cap50 valid net (frozen)",
        },
        "allPass": all_pass,
    }


def _live_blocked_reason(pre: dict[str, Any]) -> str:
    bits: list[str] = []
    paper = pre.get("paper20") or {}
    if not paper.get("pass"):
        bits.append(f"paper {paper.get('completed')}/{paper.get('required')} 未满")
    hold = pre.get("holdoutRecovery") or {}
    if not hold.get("pass"):
        cur = hold.get("currentPct")
        cur_txt = f"{cur:.1f}%" if isinstance(cur, (int, float)) else "未知"
        bits.append(f"holdout {cur_txt} 未收复至 {hold.get('recoverToPct')}%")
    filt = pre.get("filteredValidPositive") or {}
    if not filt.get("pass"):
        cur = filt.get("currentPct")
        cur_txt = f"{cur:.1f}%" if isinstance(cur, (int, float)) else "未知"
        bits.append(f"过滤版 valid {cur_txt} 未转正")
    if not bits:
        return ""
    return "、".join(bits) + "；卫星实盘权重保持 0%，复活灯只作观察。"


def _role(fid: str) -> str:
    if fid in REFERENCE_IDS:
        return "reference"
    if fid in SATELLITE_IDS:
        return "satellite"
    return "research"


def _factor_focus_item(row: dict[str, Any], note: str | None = None) -> dict[str, Any]:
    fid = str(row.get("id") or "")
    role = _role(fid)
    status = str(row.get("status") or "")
    if note is None:
        if role == "reference" and status == "revived":
            note = "参考灯：复活只代表参考，不是下单指令；实盘仍 0%。"
        elif fid == "starship_b" and status == "cold":
            note = "S-gap 冷库：坐在母港 M30，不自动跟星舰。"
        elif status == "revived":
            note = "复活灯：需同时满足三条实盘前置，否则仍 0%。"
        else:
            note = "观察/冷库：不作为跟随指令。"
    return {
        "id": fid,
        "name": row.get("name") or fid,
        "family": row.get("family"),
        "status": status,
        "badge": row.get("badge"),
        "role": role,
        "position": _as_int(row.get("position")) or 0,
        "percentile": _as_float(row.get("percentile")),
        "actionable": role == "satellite" and status == "revived",
        "note": note,
    }


def _focus(
    factors: dict[str, dict[str, Any]],
    decayed: list[dict[str, Any]],
    transitions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Short focus list: revived, recent transitions, starship family."""
    out: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add(row: dict[str, Any], note: str | None = None) -> None:
        fid = str(row.get("id") or "")
        if not fid or fid in seen or len(out) >= FOCUS_LIMIT:
            return
        seen.add(fid)
        out.append(_factor_focus_item(row, note))

    for item in decayed:
        fid = str(item.get("id") or "")
        row = factors.get(fid)
        if row:
            add(row, str(item.get("reason") or "衰减：退出该因子/袖套，回母港 M30。"))

    for row in factors.values():
        if str(row.get("status") or "") == "revived":
            add(row)

    for fid in STARSHIP_IDS:
        row = factors.get(fid)
        if row:
            add(row)

    for trans in transitions[-TRANSITIONS_LIMIT:]:
        fid = str((trans or {}).get("factor_id") or "")
        row = factors.get(fid)
        if row:
            note = f"最近转换 {trans.get('date')}: {trans.get('from')} -> {trans.get('to')}"
            add(row, note)
    return out


def _summary(factors: dict[str, dict[str, Any]], vault: dict[str, Any]) -> dict[str, int]:
    rows = list(factors.values())
    n_cold = sum(1 for r in rows if r.get("status") == "cold")
    n_watch = sum(1 for r in rows if r.get("status") == "watch")
    n_rev = sum(1 for r in rows if r.get("status") == "revived")
    return {
        "nFactors": len(rows),
        "nCold": n_cold,
        "nWatch": n_watch,
        "nRevived": n_rev,
    }


def compact_factor_system(payload: dict[str, Any]) -> dict[str, Any]:
    """Compact status for ``mode=compact`` (counts, action, freshness, block)."""
    policy = payload.get("policy") or {}
    return {
        "available": bool(payload.get("available")),
        "error": payload.get("error"),
        "generatedAt": payload.get("generatedAt"),
        "freshness": {
            "sourceGeneratedAt": (payload.get("freshness") or {}).get("sourceGeneratedAt"),
            "stale": (payload.get("freshness") or {}).get("stale"),
            "reason": (payload.get("freshness") or {}).get("reason"),
        },
        "summary": payload.get("summary") or {},
        "policy": {
            "action": policy.get("action"),
            "baseline": policy.get("baseline"),
            "suggestedWeight": policy.get("suggestedWeight"),
            "suggestedLiveWeight": policy.get("suggestedLiveWeight"),
            "liveBlocked": policy.get("liveBlocked"),
            "reason": policy.get("reason"),
        },
        "liveBlocked": policy.get("liveBlocked"),
        "liveBlockedReason": policy.get("liveBlockedReason"),
        "adviceOnly": True,
    }


def _unavailable(
    *,
    error: str,
    generated_at: str,
    freshness: dict[str, Any] | None = None,
) -> dict[str, Any]:
    reason = f"因子库不可用（{error}）；按母港 M30 基线等待，不自动跟任何卫星。"
    return {
        "available": False,
        "error": error,
        "generatedAt": generated_at,
        "freshness": freshness
        or {
            "sourceGeneratedAt": None,
            "stale": True,
            "reason": "因子库文件缺失或不可读，无法判断新鲜度。",
        },
        "summary": {"nFactors": 0, "nCold": 0, "nWatch": 0, "nRevived": 0},
        "policy": {
            "action": "wait_in_baseline",
            "baseline": BASELINE,
            "baselineLabel": BASELINE_LABEL,
            "suggestedWeight": 0,
            "suggestedLiveWeight": 0,
            "maxLiveWeight": MAX_LIVE_WEIGHT,
            "liveBlocked": True,
            "liveBlockedReason": reason,
            "reason": reason,
            "decayedFactors": [],
            "revivedFactors": [],
            "preconditions": {},
        },
        "focus": [],
        "recentTransitions": [],
        "adviceOnly": True,
    }


def build_factor_system(
    *,
    reports_dir: Path | str | None = None,
    now: str | date | None = None,
    compact: bool = False,
    preconditions_override: dict[str, Any] | None = None,
    prev_factors: dict[str, Any] | None = None,
    prev_sgap_revival_pos: int | None = None,
) -> dict[str, Any]:
    """Build the advice-only factor-system contract (fail-open, no DB/network)."""
    today = _coerce_date(now)
    generated_at = today.isoformat()
    base = Path(reports_dir) if reports_dir is not None else _reports_dir()

    try:
        vault = _read_json(base / "factor_vault.json")
    except (OSError, ValueError) as exc:
        return _unavailable(error=f"factor_vault.json: {exc}", generated_at=generated_at)
    if not isinstance(vault, dict):
        return _unavailable(error="factor_vault.json is not an object", generated_at=generated_at)
    factors = _factor_map(vault)
    if not factors:
        return _unavailable(error="factor_vault.json has no factors", generated_at=generated_at)

    history = _load_history(base / "factor_vault_history.json")
    transitions = _load_transitions(base / "factor_vault_transitions.json")
    try:
        decay = _read_json(base / "sgap_decay.json")
        decay = decay if isinstance(decay, dict) else {}
    except (OSError, ValueError):
        decay = {}

    meta = vault.get("meta") or {}
    summary_raw = vault.get("summary") or {}
    summary = {
        "nFactors": _as_int(summary_raw.get("n_factors")) or len(factors),
        "nCold": _as_int(summary_raw.get("n_cold")) or 0,
        "nWatch": _as_int(summary_raw.get("n_watch")) or 0,
        "nRevived": _as_int(summary_raw.get("n_revived")) or 0,
    }
    if summary["nCold"] + summary["nWatch"] + summary["nRevived"] != summary["nFactors"]:
        summary = _summary(factors, vault)

    current_states = {fid: str(row.get("status") or "") for fid, row in factors.items()}
    prev_states = _history_prev_states(history, current_states)
    prev_positions = _previous_positions(prev_factors)
    decayed = _decays(
        factors=factors,
        prev_states=prev_states,
        prev_positions=prev_positions,
        prev_sgap_pos=prev_sgap_revival_pos,
        decay=decay,
    )

    # Freshness: the vault and the frozen trade blotter are both dated.
    vault_at = meta.get("generated_at")
    summary_decay = decay.get("summary") or {}
    blotter_end = summary_decay.get("date_end")
    age = _age_days(vault_at, today)
    blotter_age = _age_days(blotter_end, today)
    stale = bool(
        age is None
        or age > STALE_AFTER_DAYS
        or (blotter_age is not None and blotter_age > STALE_AFTER_DAYS)
    )
    fresh_reason = []
    if age is None:
        fresh_reason.append("因子库缺少 generated_at")
    elif age > STALE_AFTER_DAYS:
        fresh_reason.append(f"因子库生成于 {vault_at}，距今 {age} 天")
    if blotter_age is None:
        fresh_reason.append("S-gap 流水日期缺失")
    elif blotter_age > STALE_AFTER_DAYS:
        fresh_reason.append(f"交易流水止于 {blotter_end}，距今 {blotter_age} 天")
    if stale:
        fresh_reason.append("交易流水中断，灯不会自行变化")
    freshness = {
        "sourceGeneratedAt": vault_at,
        "historyLatest": (history[-1] or {}).get("date") if history else None,
        "tradeBlotterLastEntry": blotter_end,
        "ageDays": age,
        "stale": stale,
        "reason": "；".join(fresh_reason) if fresh_reason else "因子库与交易流水为近期快照。",
    }

    pre = _live_preconditions(factors, preconditions_override)
    live_blocked_reason = _live_blocked_reason(pre)

    revived = [
        factors[fid]
        for fid in SATELLITE_IDS
        if fid in factors and factors[fid].get("status") == "revived"
    ]
    confirmed = [row for row in revived if (_as_int(row.get("position")) or 0) > 0]
    unconfirmed = [row for row in revived if row not in confirmed]
    suggested = max((_as_int(row.get("position")) or 0 for row in confirmed), default=0)
    suggested = min(MAX_LIVE_WEIGHT, suggested)
    suggested_weight = 0

    if decayed:
        action = "advise_exit"
        reason = (
            "检测到跟随中的卫星因子衰减："
            + "；".join(str(d.get("reason") or d.get("id")) for d in decayed)
            + "。退出该因子/袖套，回母港 M30 基线等待。"
        )
        suggested_weight = 0
        suggested_live = 0
    elif confirmed:
        action = "advise_follow"
        suggested_weight = suggested
        if pre.get("allPass"):
            suggested_live = suggested
            reason = (
                f"卫星因子复活且实盘三前置已满足：建议按规则权重 {suggested}% 跟随，"
                f"上限 {MAX_LIVE_WEIGHT}%；仍为建议，不自动改模式或下单。"
            )
        else:
            suggested_live = 0
            reason = (
                f"卫星因子复活，规则建议权重 {suggested}%，但实盘前置未满："
                f"{live_blocked_reason} 灯可以看，仓位不跟。"
            )
    elif unconfirmed:
        action = "wait_in_baseline"
        suggested_weight = 0
        suggested_live = 0
        reason = "卫星因子刚复活但未完成双确认（建议权重 0%），先坐母港 M30 等待。"
    else:
        action = "wait_in_baseline"
        suggested_weight = 0
        suggested_live = 0
        reason = (
            "相关卫星因子为冷库/观察，未复活；坐在母港 M30 基线，不自动跟星舰，"
            "也不把参考灯当成下单指令。"
        )

    live_blocked = action != "advise_follow" or not pre.get("allPass")
    if not live_blocked_reason:
        live_blocked_reason = "实盘三前置已满足；仍为建议，不自动下单。"

    focus = _focus(factors, decayed, transitions)
    recent = [
        {
            "date": t.get("date"),
            "kind": t.get("kind"),
            "eventType": t.get("event_type"),
            "factorId": t.get("factor_id"),
            "name": t.get("name"),
            "from": t.get("from"),
            "to": t.get("to"),
        }
        for t in transitions[-TRANSITIONS_LIMIT:]
        if isinstance(t, dict)
    ]

    payload: dict[str, Any] = {
        "available": True,
        "generatedAt": generated_at,
        "freshness": freshness,
        "summary": summary,
        "policy": {
            "action": action,
            "baseline": BASELINE,
            "baselineLabel": BASELINE_LABEL,
            "suggestedWeight": suggested_weight,
            "suggestedLiveWeight": suggested_live,
            "maxLiveWeight": MAX_LIVE_WEIGHT,
            "liveBlocked": bool(live_blocked),
            "liveBlockedReason": live_blocked_reason,
            "reason": reason,
            "decayedFactors": decayed,
            "revivedFactors": [
                {
                    "id": row.get("id"),
                    "name": row.get("name"),
                    "status": row.get("status"),
                    "position": _as_int(row.get("position")) or 0,
                    "percentile": _as_float(row.get("percentile")),
                }
                for row in revived
            ],
            "preconditions": pre,
        },
        "focus": focus,
        "recentTransitions": recent,
        "adviceOnly": ADVICE_ONLY,
    }
    return compact_factor_system(payload) if compact else payload


def _coerce_date(now: str | date | None) -> date:
    if isinstance(now, date):
        return now
    if now:
        try:
            return date.fromisoformat(str(now)[:10])
        except ValueError:
            pass
    return date.today()


def render_factor_system_markdown(payload: dict[str, Any]) -> str:
    """Markdown rendering of the same contract (mirrors judgment-brief.md)."""
    if not payload.get("available"):
        return (
            "# 因子系统 factor system\n\n"
            f"- 可用: 否\n- 错误: {payload.get('error')}\n"
            "- 政策: 母港 M30 等待（因子库不可用，fail-open）\n"
        )
    summary = payload.get("summary") or {}
    policy = payload.get("policy") or {}
    fresh = payload.get("freshness") or {}
    lines = [
        "# 因子系统 factor system",
        "",
        f"- 生成: {payload.get('generatedAt')}",
        f"- 因子: {summary.get('nFactors')} 个（冷库 {summary.get('nCold')} / "
        f"观察 {summary.get('nWatch')} / 复活 {summary.get('nRevived')}）",
        f"- 新鲜度: {'陈旧' if fresh.get('stale') else '正常'} — {fresh.get('reason')}",
        "",
        "## 政策（仅建议 advice only）",
        "",
        f"- 动作: `{policy.get('action')}`",
        f"- 基线: {policy.get('baselineLabel')} (`{policy.get('baseline')}`)",
        f"- 规则建议权重: {policy.get('suggestedWeight')}%（上限 {policy.get('maxLiveWeight')}%）",
        f"- 建议实盘权重: {policy.get('suggestedLiveWeight')}%",
        f"- 实盘受阻: {'是' if policy.get('liveBlocked') else '否'} — "
        f"{policy.get('liveBlockedReason')}",
        f"- 理由: {policy.get('reason')}",
        "",
        "## 关注 focus",
        "",
    ]
    focus = payload.get("focus") or []
    if not focus:
        lines.append("- （无）")
    for item in focus:
        lines.append(
            f"- {item.get('name')} (`{item.get('id')}`): {item.get('badge')} · "
            f"role={item.get('role')} · pos={item.get('position')}% · {item.get('note')}"
        )
    if payload.get("recentTransitions"):
        lines.extend(["", "## 最近转换", ""])
        for t in payload["recentTransitions"]:
            lines.append(f"- {t.get('date')} {t.get('factorId')} {t.get('from')} -> {t.get('to')}")
    lines.append("")
    return "\n".join(lines).strip() + "\n"
