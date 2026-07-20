from __future__ import annotations

from typing import Any

SW_TAXONOMY = "SW"
UNKNOWN_TAXONOMY = "UNKNOWN"
SW_L1_LEVEL = 1
DEFAULT_INDUSTRY_FLOW_SOURCE = "eastmoney_bkzj"

SW_L1_INDUSTRIES: frozenset[str] = frozenset(
    {
        "农林牧渔",
        "基础化工",
        "钢铁",
        "有色金属",
        "电子",
        "家用电器",
        "食品饮料",
        "纺织服饰",
        "轻工制造",
        "医药生物",
        "公用事业",
        "交通运输",
        "房地产",
        "商贸零售",
        "社会服务",
        "综合",
        "建筑材料",
        "建筑装饰",
        "电力设备",
        "国防军工",
        "计算机",
        "传媒",
        "通信",
        "银行",
        "非银金融",
        "汽车",
        "机械设备",
        "煤炭",
        "石油石化",
        "环保",
        "美容护理",
    }
)

SW_L1_INDUSTRY_NAMES: tuple[str, ...] = tuple(sorted(SW_L1_INDUSTRIES))


def normalize_industry_name(name: Any) -> str:
    return str(name or "").replace("\u3000", " ").strip()


def is_sw_l1_industry_name(name: Any) -> bool:
    return normalize_industry_name(name) in SW_L1_INDUSTRIES


def classify_sw_l1_industry(name: Any, raw: dict[str, Any] | None = None) -> dict[str, Any]:
    normalized = normalize_industry_name(name)
    is_allowed = normalized in SW_L1_INDUSTRIES
    return {
        "industry_name": normalized,
        "taxonomy": SW_TAXONOMY if is_allowed else UNKNOWN_TAXONOMY,
        "industry_level": SW_L1_LEVEL if is_allowed else None,
        "is_allowed": is_allowed,
        "source": DEFAULT_INDUSTRY_FLOW_SOURCE,
    }


def row_is_sw_l1(row: dict[str, Any]) -> bool:
    name = normalize_industry_name(row.get("industry_name"))
    if not name:
        return False
    taxonomy = str(row.get("taxonomy") or "").strip().upper()
    level = row.get("industry_level")
    try:
        level_int = int(level) if level is not None and str(level).strip() != "" else None
    except Exception:
        level_int = None
    if taxonomy == SW_TAXONOMY and level_int == SW_L1_LEVEL:
        return name in SW_L1_INDUSTRIES
    if taxonomy in {"", UNKNOWN_TAXONOMY}:
        return name in SW_L1_INDUSTRIES
    return False


def with_sw_l1_metadata(row: dict[str, Any], *, source: str = DEFAULT_INDUSTRY_FLOW_SOURCE) -> dict[str, Any]:
    out = dict(row)
    meta = classify_sw_l1_industry(out.get("industry_name"))
    out["industry_name"] = meta["industry_name"]
    out["taxonomy"] = out.get("taxonomy") or meta["taxonomy"]
    out["industry_level"] = out.get("industry_level") if out.get("industry_level") is not None else meta["industry_level"]
    out["source"] = out.get("source") or source
    return out
