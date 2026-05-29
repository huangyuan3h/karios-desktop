"""Alpha Radar topic filter for RSS headlines."""

from __future__ import annotations

import re
from typing import Any

# High-trust feeds: only apply exclude filter, skip include requirement.
TRUSTED_SOURCE_IDS = frozenset(
    {
        "stratechery",
        "semianalysis",
        "next-platform",
        "ieee-spectrum",
        "trendforce",
    }
)

INCLUDE_RE = re.compile(
    r"(?i)(semiconductor|semiconductors|chip|chips|gpu|gpus|cpu|cpus|datacenter|data\s*center|"
    r"\bai\b|artificial intelligence|llm|llms|machine learning|cloud|hyperscaler|"
    r"earnings|transcript|guidance|revenue|memory|hbm|dram|nand|foundry|tsmc|nvidia|amd|intel|"
    r"5g|network|networking|robot|robotics|storage|compute|infrastructure|"
    r"optical|module|packaging|advanced packaging|"
    r"半导体|芯片|算力|数据中心|存储|晶圆|光模块|财报|业绩)",
)

EXCLUDE_RE = re.compile(
    r"(?i)(biomed|biomedical|clinical trial|lung cancer|genetic|genome|pharma|pharmaceutical|"
    r"vaccine|oncology|precision medicine|rare variant|epitranscriptom|"
    r"sport|sports|crypto|bitcoin|ethereum|politics|election|celebrity|"
    r"travel|recipe|beauty|fashion)",
)


def passes_topic_filter(
    *,
    title: str,
    summary: str | None,
    source_id: str,
) -> bool:
    text = f"{title} {(summary or '')}".strip()
    if not text:
        return False
    if EXCLUDE_RE.search(text):
        return False
    if source_id in TRUSTED_SOURCE_IDS:
        return True
    return bool(INCLUDE_RE.search(text))


def filter_feed_items(
    items: list[dict[str, Any]],
    *,
    source_id: str,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    kept: list[dict[str, Any]] = []
    filtered_out = 0
    for item in items:
        if passes_topic_filter(
            title=str(item.get("title") or ""),
            summary=item.get("summary"),
            source_id=source_id,
        ):
            kept.append(item)
        else:
            filtered_out += 1
    return kept, {"fetched": len(items), "filteredOut": filtered_out, "stored": len(kept)}
