"""Alpha Radar topic filter for RSS headlines."""

from __future__ import annotations

import os
import re
from typing import Any, Callable

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
    r"travel|recipe|beauty|fashion|股评|荐股|涨停复盘|心灵鸡汤)",
)

CLS_POLICY_RE = re.compile(
    r"(【发改委】|【工信部】|【中国人民银行】|【国务院】|"
    r"发改委|工信部|央行|国务院|超长期特别国债|设备更新)",
)

POLICY_KEYWORD_RE = re.compile(
    r"(国务院|发改委|工信部|央行|货币政策|财政政策|产业政策|"
    r"设备更新|以旧换新|收储|专项债|特别国债|低空经济|数据要素)",
)

COMMODITY_RE = re.compile(
    r"(铜|铝|锂|纯碱|海运费|大宗商品|现货|期货|"
    r"commodity|copper|aluminum|lithium|freight|TC\b|加工费)",
)

COMMODITY_MOVE_RE = re.compile(
    r"(涨价|提价|上涨|暴涨|突破|新高|异动|库存|拐点|"
    r"停产|亏损|出清|供给|紧缺|TC暴跌|加工费)",
)

CYCLE_REVERSAL_RE = re.compile(
    r"(库存拐点|全行业亏损|现货提价|加工费|TC暴跌|"
    r"产能出清|停产|涨价|库存低位|现货上涨)",
)

CONSENSUS_RE = re.compile(
    r"(产能出清|反转|历史底部|左侧|右侧|景气拐点|"
    r"行业深度|研报|出清|底部)",
)


def filter_strict_enabled() -> bool:
    raw = os.getenv("ALPHA_RADAR_FILTER_STRICT", "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def _text_blob(title: str, summary: str | None) -> str:
    return f"{title} {(summary or '')}".strip()


def _passes_exclude(title: str, summary: str | None) -> bool:
    return not EXCLUDE_RE.search(_text_blob(title, summary))


def _filter_cls_policy(title: str, summary: str | None) -> bool:
    return bool(CLS_POLICY_RE.search(_text_blob(title, summary)))


def _filter_policy(title: str, summary: str | None) -> bool:
    return bool(POLICY_KEYWORD_RE.search(_text_blob(title, summary)))


def _filter_commodity(title: str, summary: str | None) -> bool:
    text = _text_blob(title, summary)
    return bool(COMMODITY_RE.search(text) and COMMODITY_MOVE_RE.search(text))


def _filter_cycle_reversal(title: str, summary: str | None) -> bool:
    return bool(CYCLE_REVERSAL_RE.search(_text_blob(title, summary)))


def _filter_consensus(title: str, summary: str | None) -> bool:
    return bool(CONSENSUS_RE.search(_text_blob(title, summary)))


SOURCE_FILTER_PROFILES: dict[str, Callable[[str, str | None], bool]] = {
    "cls-policy": _filter_cls_policy,
    "xinhua-policy": _filter_policy,
    "wallstreetcn-commodity": _filter_commodity,
    "smm-industry": _filter_cycle_reversal,
    "huibo-research": _filter_consensus,
}


def passes_topic_filter(
    *,
    title: str,
    summary: str | None,
    source_id: str,
) -> bool:
    text = _text_blob(title, summary)
    if not text:
        return False
    if not _passes_exclude(title, summary):
        return False

    profile = SOURCE_FILTER_PROFILES.get(source_id)
    if profile is not None:
        if not filter_strict_enabled():
            return True
        return profile(title, summary)

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
