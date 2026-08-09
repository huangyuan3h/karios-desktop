"""Portfolio correlation firewall (OPT-067 / L3-P5, V7.0-01).

Cross-asset concentration is invisible to the existing single-stock /
eastmoney-industry / sleeve caps: 恒生科技 ETF + 腾讯 + 通信 ETF are three
"asset classes" but one underlying Beta (Tech). This module groups positions
into **semantic factor clusters** (primary) and optionally confirms with an
**empirical 20-day return correlation** (secondary), then enforces a cluster
exposure cap:

    cluster exposure (held %) + the new BUY/ADD position % > 30%
    → CORRELATION_CAP_BLOCK for NEW entries in that cluster
    (never force-sells existing positions).

Design (from trading-improvement-checklist V7.0-01):

1. Semantic layer (primary): ETF → tracked index bucket (by ticker prefix);
   stocks → eastmoney industry (CN) or HK-tech ticker list. Clusters:
   tech_hk / semiconductor / tech_comm / metal / new_energy / consumer /
   health / financial / broad_cn. Anything else → ``other`` (not capped).
2. Empirical layer (secondary / confirm): 20-day close-return correlation
   across a UNION trading calendar (CN vs HK holidays differ). Sample too
   small (<15 aligned days) → fail-open back to the semantic layer.
3. Cap: semantic OR empirical cluster (r > 0.75) > 30% → block new entries.

No force-sell, no pure-statistical-only enforcement.
"""

from __future__ import annotations

import logging
import math
from typing import Any

from data_sync_service.db import get_connection

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cluster definitions (semantic layer)
# ---------------------------------------------------------------------------

CLUSTER_CAP_PCT = 30.0
CORRELATION_THRESHOLD = 0.75
MIN_ALIGNED_DAYS = 15
RETURN_WINDOW_DAYS = 20

# ETF ticker-prefix → tracked-index bucket (CN-listed ETFs).
ETF_CLUSTER_PREFIXES: dict[tuple[str, ...], str] = {
    ("513180", "159740", "513330", "513050", "159605"): "tech_hk",  # 恒生科技 / 恒生互联网 / 中概互联
    ("512480", "159995", "512760", "159813"): "semiconductor",  # 半导体 / 芯片
    ("512660", "159516"): "tech_comm",  # 军工通信（近似）— 通信 ETF 516880/159519 另列
    ("516880", "159519", "159383"): "tech_comm",  # 通信 / CPO ETF
    ("510880", "512800"): "financial",  # 红利低波偏金融 → 保守归类
    ("512000",): "financial",  # 券商 ETF
    ("510300", "510050", "510500"): "broad_cn",  # 宽基（不参与 cap）
}

# HK tech tickers (tech_hk cluster by ticker).
HK_TECH_TICKERS = {"00700", "01810", "09988", "03690", "09618", "01024", "02020", "02318"}

# Eastmoney industry substring → cluster (CN stocks).
# Order matters: the first matching needle wins, so broader/generic labels
# ("电子") must come AFTER more specific ones ("消费电子", "电子元件").
# K4 audit 2026-08-08: "电子" (216 stocks incl. 中芯/海光/寒武纪/澜起/长电),
# "印制电路板" (PCB/CPO chain, e.g. 沪电 002463), "元件", "光学光电子",
# "化学制药" and "小金属" were unmapped → those symbols fell through to
# "other" and were NOT cluster-protected on future BUY.
INDUSTRY_CLUSTER_RULES: list[tuple[str, str]] = [
    ("半导体", "semiconductor"),
    ("电子元件", "semiconductor"),
    ("元件", "semiconductor"),
    ("电子化学品", "semiconductor"),
    ("军工电子", "semiconductor"),
    ("消费电子", "tech_comm"),
    ("电子", "semiconductor"),  # eastmoney 一级电子 = 芯片/半导体硬件
    ("印制电路板", "tech_comm"),  # PCB / CPO 通信产业链
    ("通信", "tech_comm"),
    ("计算机设备", "tech_comm"),
    ("软件开发", "tech_comm"),
    ("互联网服务", "tech_comm"),
    ("游戏", "tech_comm"),
    ("光伏", "new_energy"),
    ("电池", "new_energy"),
    ("风电", "new_energy"),
    ("电力设备", "new_energy"),
    ("能源金属", "metal"),
    ("有色金属", "metal"),
    ("贵金属", "metal"),
    ("工业金属", "metal"),
    ("小金属", "metal"),
    ("银行", "financial"),
    ("证券", "financial"),
    ("保险", "financial"),
    ("多元金融", "financial"),
    ("白酒", "consumer"),
    ("食品饮料", "consumer"),
    ("家电", "consumer"),
    ("旅游零售", "consumer"),
    ("农牧", "consumer"),
    ("医药", "health"),
    ("中药", "health"),
    ("医疗服务", "health"),
    ("医疗器械", "health"),
    ("生物制品", "health"),
    ("化学制药", "health"),
]

CLUSTER_LABELS = {
    "tech_hk": "港股科技",
    "semiconductor": "半导体",
    "tech_comm": "通信科技(CPO)",
    "metal": "有色/金属",
    "new_energy": "新能源",
    "consumer": "消费",
    "health": "医药",
    "financial": "金融",
    "broad_cn": "宽基",
    "other": "其他",
}


def cluster_label(cluster: str) -> str:
    return CLUSTER_LABELS.get(cluster, cluster)


# ---------------------------------------------------------------------------
# Symbol → cluster resolution
# ---------------------------------------------------------------------------


def cluster_for_symbol(symbol: str, industry: str | None = None) -> str:
    """Semantic cluster for a watchlist symbol.

    ETF → tracked-index bucket by ticker prefix; HK → tech ticker list;
    CN → eastmoney industry substring rules. Falls back to ``other``.
    """
    s = str(symbol or "").strip().upper()
    if s.startswith("ETF:"):
        ticker = s.split(":", 1)[1].strip()
        if len(ticker) >= 6:
            prefix = ticker[:6]
        else:
            prefix = ticker
        for prefixes, cluster in ETF_CLUSTER_PREFIXES.items():
            if prefix in prefixes:
                return cluster
        return "other"
    if s.startswith("HK:"):
        ticker = s.split(":", 1)[1].strip()
        if ticker in HK_TECH_TICKERS:
            return "tech_hk"
        return "other"
    industry_text = str(industry or "").strip()
    for needle, cluster in INDUSTRY_CLUSTER_RULES:
        if needle in industry_text:
            return cluster
    return "other"


def em_industry_for_ts_code(ts_code: str) -> str | None:
    """Eastmoney industry name for a CN ts_code (best-effort)."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT industry_name FROM stock_eastmoney_industry WHERE ts_code = %s",
                    (ts_code,),
                )
                r = cur.fetchone()
        return str(r[0]) if r and r[0] else None
    except Exception as exc:  # noqa: BLE001
        logger.warning("em_industry lookup failed for %s: %s", ts_code, exc)
        return None


# ---------------------------------------------------------------------------
# Exposure aggregation + cap evaluation
# ---------------------------------------------------------------------------


def cluster_exposure(
    positions: list[dict[str, Any]],
    *,
    industries: dict[str, str] | None = None,
) -> dict[str, dict[str, Any]]:
    """Aggregate held pct by semantic cluster.

    ``positions``: [{symbol, positionPct}] (positionPct > 0 = held).
    ``industries``: optional {symbol: industryName} cache to avoid DB reads.
    """
    out: dict[str, dict[str, Any]] = {}
    for p in positions:
        sym = str(p.get("symbol") or "")
        try:
            pct = float(p.get("positionPct") or 0)
        except (TypeError, ValueError):
            continue
        if pct <= 0:
            continue
        industry = (industries or {}).get(sym)
        cluster = cluster_for_symbol(sym, industry)
        b = out.setdefault(cluster, {"exposurePct": 0.0, "symbols": [], "industries": set()})
        b["exposurePct"] += pct
        b["symbols"].append(sym)
        if industry:
            b["industries"].add(industry)
    for b in out.values():
        b["exposurePct"] = round(b["exposurePct"], 2)
        b["industries"] = sorted(b["industries"])
    for cname, b in out.items():
        b["label"] = cluster_label(cname)
    return out


def blocked_clusters(exposure: dict[str, dict[str, Any]]) -> list[str]:
    """Clusters whose held exposure already exceeds the cap (new entries blocked)."""
    return [c for c, b in exposure.items() if c != "other" and b["exposurePct"] > CLUSTER_CAP_PCT]


def evaluate_correlation_cap(
    positions: list[dict[str, Any]],
    *,
    industries: dict[str, str] | None = None,
    include_matrix: bool = False,
) -> dict[str, Any]:
    """Evaluate the correlation firewall for the current book.

    Returns:
      {
        "capPct": 30,
        "clusters": {cluster: {label, exposurePct, symbols, industries}},
        "overLimit": [cluster names],
        "blockedSymbols": symbols in over-limit clusters (new BUY/ADD blocked),
        "topPairs": [[symA, symB, r], ...] (empirical, sorted desc, r>0.75),
        "empiricalNote": "样本不足，仅语义层生效" | None,
        "ok": bool — book within the firewall
      }
    """
    exposure = cluster_exposure(positions, industries=industries)
    over = blocked_clusters(exposure)
    blocked_symbols: list[str] = []
    for c in over:
        blocked_symbols.extend(exposure[c]["symbols"])

    top_pairs: list[list[Any]] = []
    note = None
    if include_matrix and len(positions) >= 2:
        matrix, aligned = correlation_matrix(
            [str(p.get("symbol")) for p in positions],
            days=RETURN_WINDOW_DAYS,
        )
        if aligned < MIN_ALIGNED_DAYS or not matrix:
            note = "样本不足，仅语义层生效（fail-open）"
        else:
            for (a, b), r in sorted(matrix.items(), key=lambda x: -x[1]):
                if r > CORRELATION_THRESHOLD:
                    top_pairs.append([a, b, round(r, 3)])

    return {
        "capPct": CLUSTER_CAP_PCT,
        "clusters": exposure,
        "overLimit": over,
        "blockedSymbols": blocked_symbols,
        "topPairs": top_pairs[:5],
        "empiricalNote": note,
        "ok": not over,
    }


# ---------------------------------------------------------------------------
# Empirical correlation (secondary layer)
# ---------------------------------------------------------------------------


def _symbol_to_ts_code_corr(symbol: str) -> str | None:
    """Map watchlist symbol → ts_code for the daily table (CN/HK/ETF)."""
    s = str(symbol or "").strip().upper()
    if s.startswith("CN:"):
        t = s.split(":", 1)[1]
        if len(t) == 6 and t.isdigit():
            return f"{t}.{'SH' if t.startswith('6') else 'SZ'}"
    if s.startswith("HK:"):
        t = s.split(":", 1)[1]
        if 1 <= len(t) <= 5 and t.isdigit():
            return f"{t.zfill(5)}.HK"
    if s.startswith("ETF:"):
        t = s.split(":", 1)[1]
        if len(t) == 6 and t.isdigit():
            return f"{t}.{'SH' if t[0] in ('5','6','9') else 'SZ'}"
    return None


def correlation_matrix(symbols: list[str], days: int = 20) -> tuple[dict[tuple[str, str], float], int]:
    """20-day close-return correlation across a UNION calendar.

    Returns ((pair) -> pearson r, aligned_sample_count). Returns empty dict
    when fewer than MIN_ALIGNED_DAYS aligned samples exist (fail-open).
    """
    codes = {s: _symbol_to_ts_code_corr(s) for s in symbols}
    codes = {s: c for s, c in codes.items() if c}
    if len(codes) < 2:
        return {}, 0

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT ts_code, trade_date, close
                    FROM daily
                    WHERE ts_code = ANY(%s)
                      AND trade_date >= (SELECT MAX(trade_date) FROM daily) - INTERVAL '45 days'
                    ORDER BY ts_code, trade_date
                    """,
                    (list(codes.values()),),
                )
                rows = cur.fetchall()
    except Exception as exc:  # noqa: BLE001
        logger.warning("correlation daily fetch failed: %s", exc)
        return {}, 0

    # ts_code → {date: close}
    closes: dict[str, dict[str, float]] = {}
    for r in rows:
        ts = str(r[0])
        d = r[1].strftime("%Y-%m-%d") if hasattr(r[1], "strftime") else str(r[1])
        try:
            closes.setdefault(ts, {})[d] = float(r[2])
        except (TypeError, ValueError):
            continue

    # Union calendar (aligned trading days across all symbols).
    union_dates = sorted({d for ts in closes.values() for d in ts})
    series: dict[str, list[float]] = {}
    for sym, ts in codes.items():
        c = closes.get(ts)
        if not c:
            continue
        series[sym] = [c.get(d) for d in union_dates]
    if len(series) < 2:
        return {}, 0

    # Aligned sample: days where ALL series have a close.
    aligned_dates = [
        d for i, d in enumerate(union_dates)
        if all(v[i] is not None for v in series.values())
    ]
    if len(aligned_dates) < MIN_ALIGNED_DAYS:
        return {}, len(aligned_dates)

    # Returns over the aligned window (keep the last `days` returns).
    returns: dict[str, list[float]] = {}
    for sym, vals in series.items():
        rs: list[float] = []
        prev: float | None = None
        for i, d in enumerate(union_dates):
            if d not in aligned_dates:
                continue
            v = vals[i]
            if v is None:
                prev = None
                continue
            if prev is not None and prev > 0:
                rs.append((v - prev) / prev)
            prev = v
        returns[sym] = rs[-days:] if days else rs

    syms = [s for s in series if len(returns[s]) >= MIN_ALIGNED_DAYS - 1]
    out: dict[tuple[str, str], float] = {}
    for i, a in enumerate(syms):
        for b in syms[i + 1:]:
            n = min(len(returns[a]), len(returns[b]))
            if n < MIN_ALIGNED_DAYS - 1:
                continue
            r = _pearson(returns[a][-n:], returns[b][-n:])
            if r is not None:
                out[(a, b)] = r
    return out, len(aligned_dates)


def _pearson(x: list[float], y: list[float]) -> float | None:
    n = len(x)
    if n < 2:
        return None
    mx = sum(x) / n
    my = sum(y) / n
    num = sum((xi - mx) * (yi - my) for xi, yi in zip(x, y, strict=False))
    dx = math.sqrt(sum((xi - mx) ** 2 for xi in x))
    dy = math.sqrt(sum((yi - my) ** 2 for yi in y))
    if dx == 0 or dy == 0:
        return None
    r = num / (dx * dy)
    return max(-1.0, min(1.0, r))
