"""East Money upstream-unavailable classification (2026-11-06).

When the East Money realtime egress is down (RemoteDisconnected / empty
reply / IP-ban latch / proxy-exit 502s ...), downstream sync legs must
report explicit degradation — never a cascading FAILED with a raw
transport stack, and never fake healthy zeros.

This module is the single classifier + copy source so every EM-touching
leg degrades identically:

- :func:`is_em_source_unavailable` — True only for identifiably-East-Money
  transport/upstream failures. Data-shape errors (``no_rows``,
  ``missing_symbols``, ``no_iv_data`` ...) and unrelated vendor errors
  (tushare timeouts, programming errors) return False and MUST stay red.
- :func:`neutralize_em_substep` — turn an ``ok=False`` sub-result into an
  explicit ``ok=True`` degraded copy (raw error preserved for audit).
"""

from __future__ import annotations

from typing import Any

# Strong fingerprints: unambiguously East Money upstream surface.
_EM_UPSTREAM_MARKERS = (
    "eastmoney.com",
    "eastmoney_ip_ban",
    "push2delay",
    "push2his",
    "datacenter-web.eastmoney",
)

# Transport-level markers. Only count when paired with an "eastmoney"
# mention (bare timeouts from other vendors must stay failures).
_TRANSPORT_MARKERS = (
    "remotedisconnected",
    "empty reply",
    "(52)",
    "connection aborted",
    "connection reset",
    "reset by peer",
    "broken pipe",
    "timed out",
    "timeout",
    "temporarily unavailable",
    "name resolution",
    "network is unreachable",
    "connection refused",
    "bad gateway",
    "service unavailable",
    "gateway timeout",
    ": 502",
    ": 503",
    ": 504",
    " 502",
    " 503",
    " 504",
    "curl_http_",
    "urllib_http_",
)

# Exact signature of ``em_push2_http._em_get_json_no_proxy`` failure chains
# (direct requests/curl/urllib all failed) — the banner seen 2026-11-06.
_EM_CHAIN_MARKERS = ("direct:", "curl:", "urllib:")

#: Short user-facing note (Chinese). Shown in the UI instead of the raw stack.
EM_DEGRADED_ZH_SHORT = (
    "东方财富实时行情源暂时不可用（直连失败），已沿用上次有效数据，未写入空值；源恢复后自动恢复。"
)

#: Machine reason code for degraded EM legs.
EM_UNAVAILABLE_REASON = "em_source_unavailable"


def _texts_of(value: Any) -> list[str]:
    """Collect candidate error strings from a sub-result (never raises)."""
    out: list[str] = []
    try:
        if isinstance(value, dict):
            for key in ("error", "message", "fetchError", "reason"):
                v = value.get(key)
                if isinstance(v, str) and v.strip():
                    out.append(v)
            errors = value.get("errors")
            if isinstance(errors, list):
                for e in errors:
                    if isinstance(e, str) and e.strip():
                        out.append(e)
                    elif isinstance(e, dict):
                        m = e.get("error") or e.get("message")
                        if isinstance(m, str) and m.strip():
                            out.append(m)
            provider_errors = value.get("providerErrors")
            if isinstance(provider_errors, list):
                for e in provider_errors:
                    if isinstance(e, str) and e.strip():
                        out.append(e)
        elif isinstance(value, str) and value.strip():
            out.append(value)
        elif isinstance(value, BaseException):
            out.append(f"{type(value).__name__}: {value}")
    except Exception:  # noqa: BLE001 - classifier must never raise
        pass
    return out


def is_em_source_unavailable(error: Any) -> bool:
    """True only when the failure is identifiably East-Money-upstream.

    Data-shape / logic errors (``no_iv_data``, ``no_rows``,
    ``missing_symbols``, ``suspicious_empty_lhb`` ...) return False.
    """
    for text in _texts_of(error):
        low = text.lower()
        if any(m in low for m in _EM_UPSTREAM_MARKERS):
            return True
        if "eastmoney" in low and any(m in low for m in _TRANSPORT_MARKERS):
            return True
        if all(m in low for m in _EM_CHAIN_MARKERS):
            return True
    return False


def em_degraded_warning(*, leg: str) -> str:
    """Per-leg Chinese warning line (rides in ``warnings``; never flips ok)."""
    return f"{leg}：东方财富源暂时不可用，已沿用上次有效数据"


def neutralize_em_substep(result: dict[str, Any] | Any, *, leg: str) -> dict[str, Any] | None:
    """Return a degraded ``ok=True`` copy when ``result`` is EM-unavailable.

    Returns None when the result is not a dict, is not ``ok=False``, or the
    failure is not EM-upstream (those MUST stay red). The raw error text is
    preserved under ``rawError`` / ``providerErrors`` for audit.
    """
    if not isinstance(result, dict) or result.get("ok") is not False:
        return None
    if not is_em_source_unavailable(result):
        return None
    raw = result.get("error") or result.get("message") or "em_source_unavailable"
    degraded = {
        **result,
        "ok": True,
        "degraded": True,
        "unavailable": True,
        "reason": EM_UNAVAILABLE_REASON,
        "message": EM_DEGRADED_ZH_SHORT,
        "rawError": str(raw)[:500],
        "warning": em_degraded_warning(leg=leg),
    }
    return degraded
