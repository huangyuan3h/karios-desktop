"""Sector Rotation Index (SRV): composite 0-100 rotation risk score.

Higher score = more violent rotation (dangerous / no main line).
Built from four dimensions over the last 3 trading days' Top5 industry
net-inflow sets:
  1. 3-day triple overlap (0-30)
  2. adjacent-day pairwise overlap avg (0-25)
  3. unique industries across 3 days (0-25)
  4. Top1 leader stability (0-20)

Levels (validated against 60d history: min score 51.5, no main-line days in
the window — Stable requires a real consensus, Elevated covers 45-65):
  score < 45   → Stable       (主线共识极强)
  score < 65   → Elevated     (高热震荡分歧)
  score >= 65  → Extreme_High (恶性电风扇绞肉机)
"""

from __future__ import annotations

from typing import Any

SRV_LEVEL_STABLE = "Stable"
SRV_LEVEL_ELEVATED = "Elevated"
SRV_LEVEL_EXTREME_HIGH = "Extreme_High"

_LABEL_ZH: dict[str, str] = {
    SRV_LEVEL_STABLE: "主线共识极强",
    SRV_LEVEL_ELEVATED: "高热震荡分歧",
    SRV_LEVEL_EXTREME_HIGH: "恶性电风扇绞肉机",
}

# Linear interpolation helpers
_TOTAL = 100.0


def _score_triple_overlap(overlap: int) -> float:
    """0-30: overlap 0 (no shared industry in 3d Top5) → 30."""
    return 30.0 * (5 - max(0, min(5, overlap))) / 5.0


def _score_pairwise_overlap(pair_avg: float) -> float:
    """0-25: avg adjacent-day Top5 overlap 0 → 25 (never repeats)."""
    pair_avg = max(0.0, min(5.0, pair_avg))
    return 25.0 * (5.0 - pair_avg) / 5.0


def _score_unique_industries(unique_count: int) -> float:
    """0-25: 5 unique (identical sets) → 0; 15 (all different) → 25."""
    unique_count = max(5, min(15, unique_count))
    return 25.0 * (unique_count - 5) / 10.0


def _score_leader_stability(top1_appearances: int) -> float:
    """0-20: same industry leads all 3 days → 0; 3 different leaders → 20."""
    appearances = max(1, min(3, top1_appearances))
    return 20.0 * (3 - appearances) / 2.0


def classify_srv_score(score: float) -> str:
    if score < 45.0:
        return SRV_LEVEL_STABLE
    if score < 65.0:
        return SRV_LEVEL_ELEVATED
    return SRV_LEVEL_EXTREME_HIGH


def _top_set_for_date(top_by_date: list[dict[str, Any]], date_str: str) -> set[str] | None:
    for item in top_by_date:
        if str(item.get("date") or "") != date_str:
            continue
        top = item.get("top")
        if not isinstance(top, list) or not top:
            return None
        names = {str(x).strip() for x in top if str(x).strip()}
        return names if names else None
    return None


def _top1_for_date(top_by_date: list[dict[str, Any]], date_str: str) -> str | None:
    for item in top_by_date:
        if str(item.get("date") or "") != date_str:
            continue
        top = item.get("top")
        if not isinstance(top, list) or not top:
            return None
        return str(top[0]).strip()
    return None


def compute_srv_index(*, top_by_date: list[dict[str, Any]], as_of_date: str) -> dict[str, Any]:
    """
    Compute SRV from Top5 industry inflow sets on T, T-1, T-2 (last 3 dates <= as_of_date).

    Returns a composite rotation score 0-100 plus the derived level.
    """
    as_of = str(as_of_date or "").strip()
    if not as_of:
        return _empty_srv_index("")

    dated_entries: list[tuple[str, dict[str, Any]]] = []
    for item in top_by_date:
        d = str(item.get("date") or "").strip()
        if d and d <= as_of:
            dated_entries.append((d, item))
    dated_entries.sort(key=lambda x: x[0])

    unique_dates = []
    seen: set[str] = set()
    for d, _ in dated_entries:
        if d not in seen:
            seen.add(d)
            unique_dates.append(d)

    if len(unique_dates) < 3:
        return _empty_srv_index(as_of)

    last3 = unique_dates[-3:]
    sets: list[set[str]] = []
    leaders: list[str | None] = []
    for d in last3:
        s = _top_set_for_date(top_by_date, d)
        if s is None:
            return _empty_srv_index(as_of)
        sets.append(s)
        leaders.append(_top1_for_date(top_by_date, d))

    triple = sets[0] & sets[1] & sets[2]
    overlap_count = len(triple)

    pair_a = len(sets[0] & sets[1])
    pair_b = len(sets[1] & sets[2])
    pair_avg = (pair_a + pair_b) / 2.0

    unique_all: set[str] = set()
    for s in sets:
        unique_all |= s
    unique_count = len(unique_all)

    leader_names = [l for l in leaders if l]
    top1_appearances = 0
    if leader_names:
        counts: dict[str, int] = {}
        for name in leader_names:
            counts[name] = counts.get(name, 0) + 1
        top1_appearances = max(counts.values())

    score = round(
        _score_triple_overlap(overlap_count)
        + _score_pairwise_overlap(pair_avg)
        + _score_unique_industries(unique_count)
        + _score_leader_stability(top1_appearances),
        1,
    )
    level = classify_srv_score(score)

    return {
        "asOfDate": as_of,
        "dates": last3,
        "score": score,
        "overlapCount": overlap_count,
        "pairwiseOverlap": round(pair_avg, 2),
        "uniqueIndustries": unique_count,
        "leaderStability": top1_appearances,
        "overlapSectors": sorted(triple),
        "level": level,
        "labelZh": _LABEL_ZH.get(level, ""),
    }


def _empty_srv_index(as_of_date: str) -> dict[str, Any]:
    return {
        "asOfDate": as_of_date,
        "dates": [],
        "score": None,
        "overlapCount": None,
        "pairwiseOverlap": None,
        "uniqueIndustries": None,
        "leaderStability": None,
        "overlapSectors": [],
        "level": None,
        "labelZh": None,
    }
