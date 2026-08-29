"""Return attribution（涨跌归因）— where fused NAV returns came from.

Pick-strong track is 100% hard-switch: day t return belongs entirely to pick_t.

Metrics (locked by unit tests):
- dayRet: nav_t / nav_{t-1} - 1
- contrib_add[p]: Σ dayRet · 1{pick=p}  (additive; sum == Σ dayRet, ≠ geometric total)
- contrib_geo[p]: Π(1+dayRet|pick=p) - 1  (compounded sleeve while that pick was held)
- byMonth: month × pick → Σ dayRet
- topDays: largest |dayRet|
- stockBreakdown: on STOCK days, equal-weight legs get dayRet_sym / n (sums to basket dayRet)

User trades book is realized SELL pnl by symbol/bucket — not the same NAV path.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any

PICKS = ("STOCK", "GOLD", "OIL", "NASDAQ", "BOND10", "REPO")

# ETF symbol → pick bucket for user_trades classification
_ETF_BUCKET: dict[str, str] = {
    "518880": "GOLD",
    "513350": "OIL",
    "513100": "NASDAQ",
    "513110": "NASDAQ",
    "513500": "NASDAQ",
    "159941": "NASDAQ",
    "511260": "BOND10",
}


def day_returns_from_nav(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Derive per-day returns from cumulative navSingle (+ pick label).

    Skips the first row (no prior NAV). Prefer explicit ``dayRet`` if present.
    """
    out: list[dict[str, Any]] = []
    prev_nav: float | None = None
    for r in rows:
        date = str(r.get("date") or "")
        pick = str(r.get("pick") or "REPO")
        nav = r.get("navSingle")
        if nav is None:
            nav = r.get("navMulti")
        try:
            nav_f = float(nav) if nav is not None else None
        except (TypeError, ValueError):
            nav_f = None
        if "dayRet" in r and r["dayRet"] is not None:
            try:
                day_ret = float(r["dayRet"])
            except (TypeError, ValueError):
                day_ret = 0.0
            out.append({"date": date, "pick": pick, "dayRet": day_ret})
            if nav_f is not None:
                prev_nav = nav_f
            continue
        if nav_f is None:
            continue
        if prev_nav is None:
            # Timeline first row already embeds day-1 return vs unit NAV=1.
            if abs(nav_f - 1.0) > 1e-15:
                out.append({"date": date, "pick": pick, "dayRet": nav_f - 1.0})
            prev_nav = nav_f
            continue
        if prev_nav == 0:
            prev_nav = nav_f
            continue
        day_ret = nav_f / prev_nav - 1.0
        out.append({"date": date, "pick": pick, "dayRet": day_ret})
        prev_nav = nav_f
    return out


def _empty_pick_stat() -> dict[str, Any]:
    return {
        "days": 0,
        "pctDays": 0.0,
        "contribAddPct": 0.0,
        "contribGeoPct": 0.0,
    }


def attribute_by_pick(day_rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate additive + geometric contribution by pick."""
    n = len(day_rows)
    by: dict[str, dict[str, Any]] = {p: _empty_pick_stat() for p in PICKS}
    add_sums: dict[str, float] = defaultdict(float)
    geo_nav: dict[str, float] = defaultdict(lambda: 1.0)
    total_add = 0.0
    total_geo = 1.0

    for row in day_rows:
        pick = str(row.get("pick") or "REPO")
        if pick not in by:
            by[pick] = _empty_pick_stat()
        r = float(row.get("dayRet") or 0.0)
        by[pick]["days"] += 1
        add_sums[pick] += r
        geo_nav[pick] *= 1.0 + r
        total_add += r
        total_geo *= 1.0 + r

    for pick, st in by.items():
        days = int(st["days"])
        st["pctDays"] = round(100.0 * days / n, 2) if n else 0.0
        st["contribAddPct"] = round(add_sums.get(pick, 0.0) * 100.0, 4)
        st["contribGeoPct"] = round((geo_nav.get(pick, 1.0) - 1.0) * 100.0, 4)

    return {
        "byPick": by,
        "totalDays": n,
        "totalAddPct": round(total_add * 100.0, 4),
        "totalGeoPct": round((total_geo - 1.0) * 100.0, 4),
    }


def attribute_by_month(day_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Month × pick additive contribution (Σ dayRet * 100)."""
    months: dict[str, dict[str, float]] = {}
    for row in day_rows:
        date = str(row.get("date") or "")
        if len(date) < 7:
            continue
        month = date[:7]
        pick = str(row.get("pick") or "REPO")
        r = float(row.get("dayRet") or 0.0)
        bucket = months.setdefault(month, defaultdict(float))
        bucket[pick] += r
    out: list[dict[str, Any]] = []
    for month in sorted(months.keys()):
        raw = months[month]
        by_pick = {
            k: round(v * 100.0, 4) for k, v in sorted(raw.items(), key=lambda x: -abs(x[1]))
        }
        out.append(
            {
                "month": month,
                "byPick": by_pick,
                "totalAddPct": round(sum(raw.values()) * 100.0, 4),
            }
        )
    return out


def top_days(day_rows: list[dict[str, Any]], *, k: int = 10) -> list[dict[str, Any]]:
    """Largest |dayRet| days with pick labels."""
    ranked = sorted(day_rows, key=lambda r: abs(float(r.get("dayRet") or 0.0)), reverse=True)
    out: list[dict[str, Any]] = []
    for row in ranked[: max(0, k)]:
        out.append(
            {
                "date": row.get("date"),
                "pick": row.get("pick") or "REPO",
                "dayRetPct": round(float(row.get("dayRet") or 0.0) * 100.0, 4),
            }
        )
    return out


def attribute_stock_symbols(
    day_rows: list[dict[str, Any]],
    stock_legs_by_day: dict[str, list[dict[str, Any]]] | None,
) -> dict[str, Any] | None:
    """On STOCK days, attribute equal-weight leg returns.

    ``stock_legs_by_day[date]`` = [{symbol, dayRet}, ...] for that calendar day.
    Each leg gets contrib = dayRet_sym / n so Σ contrib == mean(dayRet_sym) ≈ basket dayRet.
    """
    if not stock_legs_by_day:
        return None
    days_by_sym: dict[str, int] = defaultdict(int)
    add_by_sym: dict[str, float] = defaultdict(float)
    stock_days = 0
    for row in day_rows:
        if str(row.get("pick") or "") != "STOCK":
            continue
        date = str(row.get("date") or "")
        legs = stock_legs_by_day.get(date) or []
        if not legs:
            continue
        stock_days += 1
        n = len(legs)
        for leg in legs:
            sym = str(leg.get("symbol") or leg.get("ts_code") or "")
            if not sym:
                continue
            r = float(leg.get("dayRet") or 0.0)
            days_by_sym[sym] += 1
            add_by_sym[sym] += r / n

    breakdown = {
        sym: {
            "days": days_by_sym[sym],
            "contribAddPct": round(add_by_sym[sym] * 100.0, 4),
        }
        for sym in sorted(add_by_sym.keys(), key=lambda s: -abs(add_by_sym[s]))
    }
    return {
        "stockDays": stock_days,
        "bySymbol": breakdown,
    }


def build_stock_legs_by_day(
    *,
    day_rows: list[dict[str, Any]],
    positions_by_day: list[dict[str, Any]],
    close_by_ts_day: dict[str, dict[str, float]],
    calendar: list[str],
) -> dict[str, list[dict[str, Any]]]:
    """Build equal-weight stock leg day returns for days where pick==STOCK.

    Positions are read from the *previous* calendar snap (same as pick_strong_track).
    """
    snap_by_day = {str(s.get("date")): s for s in positions_by_day}
    day_idx = {d: i for i, d in enumerate(calendar)}
    pick_by_date = {str(r.get("date")): str(r.get("pick") or "") for r in day_rows}
    out: dict[str, list[dict[str, Any]]] = {}

    for day, pick in pick_by_date.items():
        if pick != "STOCK":
            continue
        idx = day_idx.get(day)
        if idx is None or idx == 0:
            continue
        prev = calendar[idx - 1]
        snap = snap_by_day.get(prev)
        if not snap:
            continue
        legs: list[dict[str, Any]] = []
        for pos in snap.get("positions") or []:
            entry = str(pos.get("entry_date") or "")
            if entry and day <= entry:
                continue
            ts = str(pos.get("ts_code") or "")
            sym = str(pos.get("symbol") or ts)
            closes = close_by_ts_day.get(ts) or {}
            today_c, prev_c = closes.get(day), closes.get(prev)
            if today_c and prev_c and prev_c != 0:
                legs.append(
                    {
                        "symbol": sym,
                        "ts_code": ts,
                        "dayRet": today_c / prev_c - 1.0,
                    }
                )
        if legs:
            out[day] = legs
    return out


def attribute_pick_strong(
    timeline_rows: list[dict[str, Any]],
    *,
    stock_legs_by_day: dict[str, list[dict[str, Any]]] | None = None,
    top_k: int = 10,
    include_day_rows: bool = False,
) -> dict[str, Any]:
    """Full pick-strong attribution package from Timeline rows."""
    day_rows = day_returns_from_nav(timeline_rows)
    by_pick = attribute_by_pick(day_rows)
    out: dict[str, Any] = {
        **by_pick,
        "byMonth": attribute_by_month(day_rows),
        "topDays": top_days(day_rows, k=top_k),
        "stockBreakdown": attribute_stock_symbols(day_rows, stock_legs_by_day),
    }
    if include_day_rows:
        out["dayRows"] = day_rows
    return out


def classify_user_symbol(symbol: str) -> str:
    """Map a journal symbol to GOLD/OIL/NASDAQ/BOND10/STOCK_CN/STOCK_HK/OTHER."""
    sym = str(symbol or "").upper()
    bare = (
        sym.replace("ETF:", "")
        .replace(".SH", "")
        .replace(".SZ", "")
        .replace(".HK", "")
    )
    if bare in _ETF_BUCKET:
        return _ETF_BUCKET[bare]
    if sym.startswith("HK:"):
        return "STOCK_HK"
    if sym.startswith("CN:") or (bare.isdigit() and len(bare) == 6):
        return "STOCK_CN"
    if bare.isdigit() and len(bare) == 5:
        return "STOCK_HK"
    if sym.startswith("ETF:"):
        return "OTHER_ETF"
    return "OTHER"


def attribute_user_trades(
    sell_rows: list[dict[str, Any]],
    *,
    start: str,
    end: str,
    min_closed: int = 3,
) -> dict[str, Any]:
    """Realized SELL pnl attribution (gross) by symbol and coarse bucket."""
    in_window: list[dict[str, Any]] = []
    for r in sell_rows:
        d = str(r.get("tradeDate") or r.get("trade_date") or "")
        if not d or d < start or d > end:
            continue
        if r.get("pnlPct") is None and r.get("pnl_pct") is None:
            continue
        in_window.append(r)

    by_symbol: dict[str, dict[str, Any]] = {}
    by_bucket_add: dict[str, float] = defaultdict(float)
    by_bucket_n: dict[str, int] = defaultdict(int)

    for r in in_window:
        sym = str(r.get("symbol") or "")
        pnl = float(
            r.get("pnlPct") if r.get("pnlPct") is not None else r.get("pnl_pct") or 0.0
        )
        st = by_symbol.setdefault(
            sym, {"count": 0, "sumPnlPct": 0.0, "bucket": classify_user_symbol(sym)}
        )
        st["count"] += 1
        st["sumPnlPct"] = round(st["sumPnlPct"] + pnl, 4)
        bucket = st["bucket"]
        by_bucket_add[bucket] += pnl
        by_bucket_n[bucket] += 1

    by_bucket = {
        b: {
            "count": by_bucket_n[b],
            "sumPnlPct": round(by_bucket_add[b], 4),
        }
        for b in sorted(by_bucket_add.keys(), key=lambda x: -abs(by_bucket_add[x]))
    }
    by_symbol_sorted = dict(
        sorted(by_symbol.items(), key=lambda kv: -abs(float(kv[1]["sumPnlPct"])))
    )
    return {
        "closedCount": len(in_window),
        "bySymbol": by_symbol_sorted,
        "byBucket": by_bucket,
        "insufficient": len(in_window) < min_closed,
        "note": "Realized SELL gross pnl — not comparable to pick-strong NAV path.",
    }
