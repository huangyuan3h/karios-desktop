"""One-line Chinese summary of CN index + macro snapshot for Dashboard Markdown."""

from __future__ import annotations

import math
from typing import Any

# Series ids — keep aligned with macro_daily.SID_* and macro snapshot payloads.
_SID_IXIC = "IXIC"
_SID_USDCNH = "USDCNH.FXCM"
_SID_A50 = "A50"
_SID_COMM_ENERGY = "COMM_ENERGY"
_SID_COMM_GOLD = "COMM_GOLD"
_SID_COMM_COPPER = "COMM_COPPER"


def _finite_float(v: Any) -> float | None:
    try:
        f = float(v)
        return f if math.isfinite(f) else None
    except Exception:
        return None


def _fmt_pct_zh(pct: float) -> str:
    """Chinese phrasing for signed day-over-day percent."""
    return f"{'涨' if pct >= 0 else '跌'}{abs(pct):.2f}%"


def _signal_to_zh(sig: str) -> str:
    s = str(sig or "").strip().lower()
    if s == "red":
        return "红灯"
    if s == "yellow":
        return "黄灯"
    if s in ("green", "light_green"):
        return "绿灯"
    if s == "deep_green":
        return "深绿"
    if not s or s == "unknown":
        return "未知"
    return str(sig)


def _format_macro_close(sid: str, close: float) -> str:
    if sid == _SID_USDCNH:
        s = f"{close:.4f}".rstrip("0").rstrip(".")
        return s or f"{close:.4f}"
    if sid == _SID_COMM_COPPER:
        return f"{close:.0f}"
    return f"{close:.2f}"


def _macro_sentence(m: dict[str, Any], labels: dict[str, str]) -> str:
    sid = str(m.get("seriesId") or "")
    label = labels.get(sid) or str(m.get("name") or sid)
    close = _finite_float(m.get("close"))
    pct = _finite_float(m.get("pctChg"))
    as_of = str(m.get("asOfDate") or "").strip()

    if pct is not None:
        chunks: list[str] = [f"{label}{_fmt_pct_zh(pct)}"]
        if close is not None:
            n = _format_macro_close(sid, close)
            verb = "报" if sid == _SID_USDCNH else "收报"
            chunks.append(f"{verb}{n}")
        if as_of:
            chunks.append(f"截至{as_of}")
        return "，".join(chunks)

    if close is not None:
        n = _format_macro_close(sid, close)
        verb = "报" if sid == _SID_USDCNH else "收报"
        out = f"{label}{verb}{n}"
        if as_of:
            out += f"，截至{as_of}"
        return out
    return label


def format_market_environment_zh(snapshot: dict[str, Any] | None) -> str:
    """
    Build one Chinese sentence for copy-paste (Dashboard / agent context).
    Prioritizes day-over-day % change when present; uses GET /macro/snapshot shape.
    """
    if not snapshot:
        return ""
    parts: list[str] = []

    for it in snapshot.get("cnIndexSignals") or []:
        name = str(it.get("name") or it.get("tsCode") or "").strip()
        if not name:
            continue
        close = _finite_float(it.get("close"))
        pct = _finite_float(it.get("pctChg"))
        sig = _signal_to_zh(str(it.get("signal") or ""))
        pos = str(it.get("positionRange") or "—").strip()
        if close is not None:
            if pct is not None:
                parts.append(f"{name}{_fmt_pct_zh(pct)}，收报{close:.2f}（{sig}，建议仓位{pos}）")
            else:
                parts.append(f"{name}收报{close:.2f}（{sig}，建议仓位{pos}）")
        else:
            parts.append(f"{name}（{sig}，建议仓位{pos}）")

    macro_labels: dict[str, str] = {
        _SID_IXIC: "纳指",
        _SID_USDCNH: "离岸人民币",
        _SID_A50: "富时A50",
        _SID_COMM_ENERGY: "INE原油主力",
        _SID_COMM_GOLD: "沪金主力",
        _SID_COMM_COPPER: "沪铜主力",
    }

    for m in snapshot.get("macro") or []:
        if not isinstance(m, dict):
            continue
        parts.append(_macro_sentence(m, macro_labels))

    if not parts:
        return ""
    return "市场环境摘要：" + "；".join(parts) + "。"
