from __future__ import annotations

import re
from typing import Any


def normalize_header(value: str) -> str:
    """
    Normalize TradingView headers so that they are stable across runs:
    - Replace NBSP with regular spaces
    - Replace newlines with spaces
    - Collapse whitespace
    """
    v = value.replace("\u00A0", " ").replace("\u202F", " ").replace("\n", " ").strip()
    v = re.sub(r"\s+", " ", v)
    return v


def normalize_headers(headers: list[str]) -> list[str]:
    out: list[str] = []
    for h in headers:
        nh = normalize_header(h)
        out.append(nh or "Column")
    return out


def split_symbol_cell(value: str) -> dict[str, str]:
    """
    TradingView's "Symbol" cell often contains multiple lines:
    - ticker
    - name
    - flags (e.g. D)
    v0: best-effort split; keep original value in Symbol as-is.
    """
    parts = [p.strip() for p in value.splitlines() if p.strip()]
    ticker = parts[0] if len(parts) > 0 else ""
    name = parts[1] if len(parts) > 1 else ""
    flags = " ".join(parts[2:]) if len(parts) > 2 else ""
    return {"Ticker": ticker, "Name": name, "Flags": flags}


def drop_empty_columns(
    headers: list[str],
    rows: list[dict[str, Any]],
) -> tuple[list[str], list[dict[str, Any]]]:
    """
    Drop columns that are empty across all rows. This removes TradingView padding columns
    like col_9/col_10.
    """
    if not rows:
        return headers, rows

    def is_empty(v: Any) -> bool:
        if v is None:
            return True
        s = str(v).strip()
        return s == ""

    non_empty: set[str] = set()
    for r in rows:
        for k, v in r.items():
            if not is_empty(v):
                non_empty.add(k)

    kept_headers = [h for h in headers if h in non_empty]
    out_rows: list[dict[str, Any]] = []
    for r in rows:
        out_rows.append({k: v for k, v in r.items() if k in non_empty})
    return kept_headers, out_rows


def enrich_symbol_columns(
    headers: list[str],
    rows: list[dict[str, Any]],
) -> tuple[list[str], list[dict[str, Any]]]:
    """
    Add Ticker/Name/Flags derived from Symbol when available.
    We keep the original Symbol for display/debugging.
    """
    if not rows:
        return headers, rows

    symbol_key = None
    for candidate in ("Symbol", "代码", "Ticker"):
        if candidate in headers:
            symbol_key = candidate
            break
    if symbol_key is None:
        return headers, rows

    out_headers = list(headers)
    for k in ("Ticker", "Name", "Flags"):
        if k not in out_headers:
            out_headers.append(k)

    out_rows: list[dict[str, Any]] = []
    for r in rows:
        rr = dict(r)
        raw = str(rr.get(symbol_key, "") or "")
        rr.update(split_symbol_cell(raw))
        out_rows.append(rr)

    return out_headers, out_rows

