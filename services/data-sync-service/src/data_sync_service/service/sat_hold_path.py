"""Satellite hold-path helpers: mark-to-entry on body day 1/2/3 closes.

Frozen S-gap: fill at T open (entry day = day 1), sell day-3 close.
This module does not change exits; it only attributes daily closes vs cost.
"""

from __future__ import annotations

from typing import Any

BODY = 3


def mark_to_entry_pct(close: float, entry: float) -> float:
    if not entry:
        raise ValueError("entry must be > 0")
    return round((close / entry - 1.0) * 100.0, 6)


def path_for_fill(
    ctx: dict[str, Any],
    *,
    ts: str,
    entry_date: str,
    entry_price: float,
    body: int = BODY,
) -> dict[str, Any] | None:
    """Return d1/d2/d3 close marks vs entry, or None if a close is missing."""
    cal: list[str] = ctx["cal"]
    idx_by_day: dict[str, int] = ctx["idx_by_day"]
    close_by_ts: dict[str, dict[str, float]] = ctx["close_by_ts"]
    ei = idx_by_day.get(entry_date)
    if ei is None or entry_price <= 0:
        return None
    if ei + body - 1 >= len(cal):
        return None
    days = [cal[ei + i] for i in range(body)]
    closes = [close_by_ts.get(ts, {}).get(d) for d in days]
    if any(c is None or c <= 0 for c in closes):
        return None
    pnls = [round(mark_to_entry_pct(float(c), entry_price), 4) for c in closes]  # type: ignore[arg-type]
    return {
        "ts": ts,
        "entryDate": entry_date,
        "d1": days[0],
        "d2": days[1],
        "d3": days[2],
        "pnl1": pnls[0],
        "pnl2": pnls[1],
        "pnl3": pnls[2],
    }


def _mean(xs: list[float]) -> float | None:
    if not xs:
        return None
    return round(sum(xs) / len(xs), 3)


def _median(xs: list[float]) -> float | None:
    if not xs:
        return None
    ys = sorted(xs)
    mid = len(ys) // 2
    if len(ys) % 2:
        return round(ys[mid], 3)
    return round((ys[mid - 1] + ys[mid]) / 2, 3)


def summarize_paths(paths: list[dict[str, Any]], *, protect_pct: float = 5.0) -> dict[str, Any]:
    """Path stats for completed body=3 fills. Marks are gross vs T-open, no costs."""
    n = len(paths)
    empty = {
        "n": 0,
        "mean": {"d1": None, "d2": None, "d3": None},
        "median": {"d1": None, "d2": None, "d3": None},
        "pctGreen": {"d1": None, "d2": None, "d3": None},
        "d2Red": {
            "n": 0,
            "pctOfFills": None,
            "recoveredGreen": 0,
            "pctRecoveredGreen": None,
            "improved": 0,
            "pctImproved": None,
            "meanD3": None,
            "meanD3Recovered": None,
            "meanD3StayedRed": None,
        },
        "hitProtectByD2": {
            "n": 0,
            "pctOfFills": None,
            "d3Green": 0,
            "pctD3Green": None,
            "meanD3": None,
        },
    }
    if n == 0:
        return empty
    p1 = [float(p["pnl1"]) for p in paths]
    p2 = [float(p["pnl2"]) for p in paths]
    p3 = [float(p["pnl3"]) for p in paths]
    d2_red = [p for p in paths if float(p["pnl2"]) < 0]
    recovered = [p for p in d2_red if float(p["pnl3"]) >= 0]
    improved = [p for p in d2_red if float(p["pnl3"]) > float(p["pnl2"])]
    stayed = [p for p in d2_red if float(p["pnl3"]) < 0]
    hit = [
        p
        for p in paths
        if min(float(p["pnl1"]), float(p["pnl2"])) <= -protect_pct
    ]
    hit_green = [p for p in hit if float(p["pnl3"]) >= 0]

    def _pct(k: int, den: int) -> float | None:
        if den <= 0:
            return None
        return round(100.0 * k / den, 1)

    return {
        "n": n,
        "mean": {"d1": _mean(p1), "d2": _mean(p2), "d3": _mean(p3)},
        "median": {"d1": _median(p1), "d2": _median(p2), "d3": _median(p3)},
        "pctGreen": {
            "d1": _pct(sum(1 for x in p1 if x >= 0), n),
            "d2": _pct(sum(1 for x in p2 if x >= 0), n),
            "d3": _pct(sum(1 for x in p3 if x >= 0), n),
        },
        "d2Red": {
            "n": len(d2_red),
            "pctOfFills": _pct(len(d2_red), n),
            "recoveredGreen": len(recovered),
            "pctRecoveredGreen": _pct(len(recovered), len(d2_red)),
            "improved": len(improved),
            "pctImproved": _pct(len(improved), len(d2_red)),
            "meanD3": _mean([float(p["pnl3"]) for p in d2_red]),
            "meanD3Recovered": _mean([float(p["pnl3"]) for p in recovered]),
            "meanD3StayedRed": _mean([float(p["pnl3"]) for p in stayed]),
        },
        "hitProtectByD2": {
            "n": len(hit),
            "pctOfFills": _pct(len(hit), n),
            "d3Green": len(hit_green),
            "pctD3Green": _pct(len(hit_green), len(hit)),
            "meanD3": _mean([float(p["pnl3"]) for p in hit]),
        },
    }


def entry_price_on(ctx: dict[str, Any], ts: str, entry_date: str) -> float | None:
    series = ctx["per_ts"].get(ts) or []
    di = ctx["date_idx"].get(ts, {}).get(entry_date, -1)
    if di < 0 or di >= len(series):
        return None
    open_px = series[di].get("open")
    if open_px is None or open_px <= 0:
        return None
    return float(open_px)


def paths_from_blotter(ctx: dict[str, Any], blotter: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in blotter:
        if row.get("kind") != "fill" or row.get("closeReason") != "body_exit":
            continue
        if int(row.get("heldDays") or 0) != BODY:
            continue
        ts = str(row.get("ts") or "")
        entry_date = str(row.get("entryDate") or "")
        px = entry_price_on(ctx, ts, entry_date)
        if px is None:
            continue
        path = path_for_fill(ctx, ts=ts, entry_date=entry_date, entry_price=px)
        if path is None:
            continue
        path["blotterPnlPct"] = row.get("pnlPct")
        out.append(path)
    return out
