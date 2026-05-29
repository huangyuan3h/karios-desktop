"""Alpha Radar risk status fusion with Hot Industry and Mainline scores."""

from __future__ import annotations

import re
from typing import Any


def normalize_text(value: str) -> str:
    s = str(value or "").lower().strip()
    s = re.sub(r"\s+", "", s)
    return s


def keyword_matches_industry(keywords: list[str], industry_name: str) -> bool:
    ind_norm = normalize_text(industry_name)
    if not ind_norm:
        return False
    for kw in keywords:
        kw_norm = normalize_text(kw)
        if not kw_norm:
            continue
        if kw_norm in ind_norm or ind_norm in kw_norm:
            return True
        # Partial overlap for Chinese industry names (min 2 chars)
        if len(kw_norm) >= 2 and len(ind_norm) >= 2:
            if kw_norm[:2] in ind_norm or ind_norm[:2] in kw_norm:
                return True
    return False


def compute_risk_status(
    *,
    keywords: list[str],
    hot_industry_names: list[str],
    mainline_by_industry: dict[str, float],
    mainline_threshold: float = 80.0,
) -> str:
    """
    Return 'armed' when a keyword fuzzy-matches a hot industry AND mainline score > threshold.
    Otherwise 'waiting_v2_flow'.
    """
    for hot_name in hot_industry_names:
        if not keyword_matches_industry(keywords, hot_name):
            continue
        score = mainline_by_industry.get(hot_name)
        if score is None:
            for ind_name, ind_score in mainline_by_industry.items():
                if keyword_matches_industry(keywords, ind_name):
                    score = ind_score
                    break
        if score is not None and float(score) > mainline_threshold:
            return "armed"
    return "waiting_v2_flow"


def build_mainline_score_map(mainline_payload: dict[str, Any]) -> dict[str, float]:
    out: dict[str, float] = {}
    for row in mainline_payload.get("allScores") or []:
        name = str(row.get("industryName") or row.get("industry_name") or "").strip()
        score = row.get("totalScore") or row.get("total_score")
        if name and score is not None:
            try:
                out[name] = float(score)
            except (TypeError, ValueError):
                pass
    return out


def extract_hot_industry_names(hot_picks: list[dict[str, Any]]) -> list[str]:
    names: list[str] = []
    for pick in hot_picks:
        name = str(pick.get("industryName") or pick.get("industry_name") or "").strip()
        if name:
            names.append(name)
    return names
