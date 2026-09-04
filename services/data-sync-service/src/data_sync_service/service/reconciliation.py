"""Weekly backtest-vs-paper reconciliation service (2026-08-11).

Answers for one trading day: what the S-3 backtest (CN + HK lines) says we
SHOULD hold at that day's close (engine end-of-day snapshots) vs what the
paper book ACTUALLY holds — per market, with entry-date alignment checks.
The Monday cron runs it for last Friday and persists the snapshot
(db/reconciliation), so drift between the backtest world and the real book
is measured weekly instead of silently diverging.

This is the "矫正操作" loop: any missing/extra/entry-skew row is a decision
point for the weekly review / decision agent, not a surprise.
"""

from __future__ import annotations

import logging
from statistics import median
from typing import Any

from data_sync_service.db.daily import fetch_ohlcv_batch_between
from data_sync_service.db.paper_trading import list_paper_trades
from data_sync_service.service.backtest_engine import BacktestConfig, BacktestData, simulate
from data_sync_service.service.paper_trading import _resolve_ts_code

logger = logging.getLogger(__name__)

# Same three fixed windows as run_walk_forward (S-3 audit standard).
WINDOWS: dict[str, tuple[str, str]] = {
    "OOS2": ("2024-08-01", "2025-08-01"),
    "train": ("2025-08-01", "2026-02-01"),
    "valid": ("2026-03-01", "2026-08-07"),
}

S3_CONFIG: dict[str, float | int | str] = {
    "score_threshold": 65.0,
    "max_hold_days": 60,
    "stop_loss_pct": -5.0,
    "target_pnl_pct": 100.0,
    "score_floor": 0.0,
    "market": "CN",
    "gates": "full",
    "trailing_stop_pct": -8.0,
    "position_pct": 0.10,
    "max_positions": 10,
    "rs_rank_min": 0.5,
    "diverging_scale": 1.0,
    "drawdown_circuit_pct": -25.0,
    "slippage_pct": 0.05,
    "pyramid_trigger_pct": 2.5,
    "pyramid_add_scale": 0.5,
    "pyramid_max_adds": 1,
    "exclude_boards": "300",
    # OPT-105 (2026-08-13): Strong-regime ATR stops — must mirror S3_CONFIG
    # so the audit replays the exact backtest rule set.
    "atr_stop_mult": 2.0,
    "atr_stop_strong_only": True,
    # TIP-014 (2026-08-14): weak/neutral-day entry block + env-aware entry
    # style — mirrors the S-3 audit config so the recon replays the exact
    # rule set.
    "neutral_block": True,
    "entry_style": "auto",
    "entry_style_rs_min": 0.7,
    "entry_style_dip_min": 3.0,
    "max_hold_env_shorten": 45,
    # D3 (2026-08-15): env-aware position sizing — uptrend 1.25x / fan 0.75x
    # (v4, 同 S3_CONFIG; 三窗全升, 长窗 +64pt)。
    "env_position_scale": "uptrend:1.25,fan:0.75",
    # E2 (2026-08-14): panic_cooldown 3→2 — 同 S3_CONFIG (弱市年冷却过严)。
    "panic_cooldown_days": 2,
}

HK_S3_CONFIG: dict[str, float | int | str] = {
    "score_threshold": 65.0,
    "max_hold_days": 60,
    "stop_loss_pct": -5.0,
    "target_pnl_pct": 100.0,
    "score_floor": 0.0,
    "market": "HK",
    "gates": "regime",
    "trailing_stop_pct": -12.0,
    "position_pct": 0.10,
    "max_positions": 20,
    "rs_rank_min": 0.6,
    "diverging_scale": 1.0,
    "drawdown_circuit_pct": -25.0,
    "slippage_pct": 0.05,
    "pyramid_trigger_pct": 2.5,
    "pyramid_add_scale": 0.5,
    "pyramid_max_adds": 1,
    "exclude_boards": "",
}


def _mk_config(market: str, start: str, end: str) -> BacktestConfig:
    base = HK_S3_CONFIG if market == "HK" else S3_CONFIG
    return BacktestConfig(start_date=start, end_date=end, **base)


def _entry(row: dict) -> str:
    """paper rows expose entryDate (camelCase via _row_to_dict)."""
    return str(row.get("entryDate") or row.get("entry_date") or "")


def _price(row: dict) -> float | None:
    """paper entry price (camelCase entryPrice via _row_to_dict)."""
    v = row.get("entryPrice") if row.get("entryPrice") is not None else row.get("entry_price")
    try:
        return float(v) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _closes_for(symbols: set[str], start: str, end: str) -> dict[str, dict[str, float]]:
    """symbol -> {date: close} window lookup (one query, HK/CN daily both live here).

    Positions carry the paper symbol (``HK:00622``); the daily table stores
    ts_codes (``00622.HK``) — resolve before querying (2026-08-11 fix).
    """
    if not symbols:
        return {}
    resolved: dict[str, str] = {}
    for s in symbols:
        r = _resolve_ts_code(s)
        if r:
            resolved[s] = r[1]
    rows = fetch_ohlcv_batch_between(sorted(resolved.values()), start, end)
    out: dict[str, dict[str, float]] = {}
    by_ts: dict[str, str] = {ts: sym for sym, ts in resolved.items()}
    for code, bars in rows.items():
        sym = by_ts.get(code, code)
        out[sym] = {
            str(d): float(c) for d, _, _, _, c, _ in bars if c not in ("", None)
        }
    return out


def _pct(entry: float, cur: float) -> float | None:
    """Return pct return entry→cur, None when uncomputable."""
    if not entry or not cur:
        return None
    return (cur - entry) / entry * 100.0


def _paper_holdings_on(day: str) -> dict[str, dict]:
    """symbol -> row for paper trades open on `day`."""
    out: dict[str, dict] = {}
    for row in list_paper_trades():
        if row.get("status") == "open" and _entry(row) <= day:
            out[str(row.get("symbol"))] = row
        elif (
            row.get("status") == "closed"
            and _entry(row) <= day
            and (not row.get("closeDate") or str(row.get("closeDate")) > day)
        ):
            out[str(row.get("symbol"))] = row
    return out


def reconcile_day(day: str, *, window: str = "valid", end_date: str | None = None) -> dict[str, Any]:
    """Full reconciliation for one trading day, per market (CN + HK).

    ``window`` picks the S-3 window config; ``end_date`` extends the window
    (for reconciling recent days beyond the fixed window end, e.g. today).
    Returns {reconDate, window, markets: {CN: {...}, HK: {...}}}.
    """
    if window not in WINDOWS:
        raise ValueError(f"unknown window {window!r} (valid: {list(WINDOWS)})")
    start, w_end = WINDOWS[window]
    end = max(end_date, w_end) if end_date else w_end
    if not (start <= day <= end):
        raise ValueError(f"{day} not in window {start}..{end}")

    paper = _paper_holdings_on(day)
    markets: dict[str, Any] = {}
    for market in ("CN", "HK"):
        cfg = _mk_config(market, start, end)
        data = BacktestData(cfg)
        run = simulate(cfg, data=data)
        snap = next((s for s in run.positions_by_day if s["date"] == day), None)
        if snap is None:
            markets[market] = {"available": False, "reason": f"no snapshot for {day}"}
            continue
        expect = {p["symbol"]: p for p in snap["positions"]}
        actual = {k: v for k, v in paper.items() if str(v.get("market") or "CN") == market}
        aligned, missing_h, extra = [], [], []
        # C4 half-way metric (2026-08-11): return diff backtest-vs-paper on
        # aligned names — one query for entry-day and recon-day closes.
        all_symbols = set(expect) | set(actual)
        closes = _closes_for(all_symbols, start, day)
        diff_vals: list[float] = []
        bt_vals: list[float] = []
        pp_vals: list[float] = []
        for s in sorted(set(expect) & set(actual)):
            bt_entry = expect[s]["entry_date"]
            bt_map = closes.get(s, {})
            bt_entry_close = bt_map.get(bt_entry)
            day_close = bt_map.get(day)
            bt_ret = _pct(bt_entry_close, day_close) if bt_entry_close and day_close else None
            pp_entry_price = _price(actual[s])
            pp_ret = _pct(pp_entry_price, day_close) if pp_entry_price and day_close else None
            d = (pp_ret - bt_ret) if (pp_ret is not None and bt_ret is not None) else None
            if d is not None:
                diff_vals.append(d)
            if bt_ret is not None:
                bt_vals.append(bt_ret)
            if pp_ret is not None:
                pp_vals.append(pp_ret)
            aligned.append({
                "symbol": s,
                "entry": bt_entry,
                "paperEntry": _entry(actual[s]),
                "entrySkew": _entry(actual[s]) != bt_entry,
                "score": expect[s].get("score_at_entry"),
                "btReturnPct": round(bt_ret, 2) if bt_ret is not None else None,
                "paperReturnPct": round(pp_ret, 2) if pp_ret is not None else None,
                "returnDiffPct": round(d, 2) if d is not None else None,
            })
        for s in sorted(set(expect) - set(actual)):
            p = expect[s]
            missing_h.append({
                "symbol": s,
                "entry": p["entry_date"],
                "score": p.get("score_at_entry"),
                "positionPct": p.get("position_pct"),
            })
        for s in sorted(set(actual) - set(expect)):
            a = actual[s]
            extra.append({
                "symbol": s,
                "entry": _entry(a),
                "source": a.get("source"),
            })
        markets[market] = {
            "available": True,
            "expected": len(expect),
            "actual": len(actual),
            "aligned": len(aligned),
            "missing": len(missing_h),
            "extra": len(extra),
            "alignedList": aligned,
            "missingList": missing_h,
            "extraList": extra,
            # C4 half-way: median return gap (paper − backtest) over aligned
            # names; positive = paper running ahead of the backtest replay.
            "alignedReturnDiffPct": round(median(diff_vals), 2) if diff_vals else None,
            "btReturnMedianPct": round(median(bt_vals), 2) if bt_vals else None,
            "paperReturnMedianPct": round(median(pp_vals), 2) if pp_vals else None,
        }
    return {"reconDate": day, "window": window, "markets": markets}


def run_and_persist(day: str, *, window: str = "valid") -> dict[str, Any]:
    """reconcile_day + persist (idempotent per day+market). Cron entry point."""
    from data_sync_service.db.reconciliation import insert_recon

    out = reconcile_day(day, window=window)
    for market, m in out["markets"].items():
        if not m.get("available"):
            continue
        detail = [
            {"type": "missing", **x} for x in m.get("missingList", [])
        ] + [
            {"type": "extra", **x} for x in m.get("extraList", [])
        ] + [
            {"type": "aligned", **x} for x in m.get("alignedList", [])
        ]
        insert_recon(
            recon_date=out["reconDate"],
            market=market,
            window=out["window"],
            expected=m["expected"],
            actual=m["actual"],
            aligned=m["aligned"],
            missing=m["missing"],
            extra=m["extra"],
            aligned_return_diff_pct=m.get("alignedReturnDiffPct"),
            bt_return_median_pct=m.get("btReturnMedianPct"),
            paper_return_median_pct=m.get("paperReturnMedianPct"),
            detail=detail,
        )
    return out


def _registry_holdings_on(day: str) -> dict[str, dict]:
    """symbol -> registry row for holdings open on ``day`` (real book).

    The user's ACTUAL positions (watchlist registry, positionPct > 0) — the
    behavior audit compares these against the backtest "should hold" set.
    """
    from data_sync_service.db.watchlist_automation import list_registry

    out: dict[str, dict] = {}
    for row in list_registry():
        sym = str(row.get("symbol") or "").upper()
        pct = row.get("positionPct")
        entry = str(row.get("entryDate") or "")
        try:
            held = pct is not None and float(pct) > 0 and bool(entry) and entry <= day
        except (TypeError, ValueError):
            held = False
        if held:
            out[sym] = row
    return out


def _sym_from_ts(ts: str) -> str:
    """ts code (600000.SH / 00700.HK) -> registry symbol (CN:600000 / HK:00700)."""
    code, _, suffix = str(ts or "").partition(".")
    if suffix.upper() == "HK":
        return f"HK:{code}"
    return f"CN:{code}"


def _leg_ctx(day: str) -> dict[str, Any]:
    """pick + satellite ts sets for the leg split. Never raises —
    on failure everything stays S-3 (legacy behavior)."""
    from datetime import date as _date

    pick: str | None = None
    sat_ts: set[str] = set()
    book_ts: set[str] = set()
    try:
        from data_sync_service.service.multi_asset_sleeve import _pick as sleeve_pick

        p = sleeve_pick()
        pick = p.get("key") if isinstance(p, dict) else getattr(p, "key", None)
    except Exception:  # noqa: BLE001
        pass
    try:
        from data_sync_service.service.twin_star_daily import (
            live_sat_ts_codes,
            sat_book_ts_codes,
        )

        d = _date.fromisoformat(day[:10])
        sat_ts = set(live_sat_ts_codes(d))
        book_ts = set(sat_book_ts_codes(d))
    except Exception:  # noqa: BLE001
        pass
    return {"pick": pick, "sat_ts": sat_ts, "book_ts": book_ts}


def reconcile_registry(
    day: str, *, window: str = "valid", end_date: str | None = None,
    mode: str = "twin_star", leg_ctx: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """BEHAVIOR AUDIT (2026-08-13): the user's REAL holdings vs the S-3
    backtest "should hold" set for one trading day.

    Unlike reconcile_day (paper book), this compares the watchlist registry
    (the user's actual buys/sells) against the engine's end-of-day snapshot:

      - ``extra``  → CORE-leg holding the backtest does NOT hold:
        · backtest never entered it  → "买了不该买"
        · backtest already exited it → "该卖没卖" (backtest closed, user still in)
      - ``missing``→ backtest holds it, the user does not (info: 该持没买)

    OPT-140 (twin_star mode): satellite-leg holdings are split out via the
    shared ``holding_book`` predicate — they are compared against the
    twin-star engine book instead (``satExtra``/``satMissing``) and NEVER
    counted as S-3 ``extra`` (no more "买了不该买" on satellite names).
    ``mode="single_track"`` keeps the legacy all-S-3 behavior.
    Pass ``leg_ctx`` explicitly in tests to avoid live pick/engine calls.

    Returns {reconDate, window, markets: {CN: {…, extraList: [{symbol, name,
    costPrice, entryDate, pnlPct?, kind: 'never_entered'|'exited'}],
    satExtraList, satMissingList}}}. The caller persists it
    (db.behavior_audit) and the watchlist page renders it.
    """
    from data_sync_service.service.twin_star_daily import holding_book

    if window not in WINDOWS:
        raise ValueError(f"unknown window {window!r} (valid: {list(WINDOWS)})")
    start, w_end = WINDOWS[window]
    end = max(end_date or day, w_end, day)  # audit day always covered
    if not (start <= day <= end):
        raise ValueError(f"{day} not in window {start}..{end}")

    leg = leg_ctx if leg_ctx is not None else (_leg_ctx(day) if mode == "twin_star" else None)
    pick = (leg or {}).get("pick")
    sat_ts: set[str] = set((leg or {}).get("sat_ts") or set())
    book_syms = {_sym_from_ts(ts) for ts in ((leg or {}).get("book_ts") or set())}

    real = _registry_holdings_on(day)
    markets: dict[str, Any] = {}
    for market in ("CN", "HK"):
        cfg = _mk_config(market, start, end)
        data = BacktestData(cfg)
        run = simulate(cfg, data=data)
        snap = next((s for s in run.positions_by_day if s["date"] == day), None)
        if snap is None:
            markets[market] = {"available": False, "reason": f"no snapshot for {day}"}
            continue
        expect = set(p["symbol"] for p in snap["positions"])
        # symbols the backtest ENTERED at some point <= day — whether still
        # open or already closed — mark "the backtest held this name": an
        # extra holding with this flag is 该卖没卖 (backtest exited, user in).
        was_held: set[str] = set()
        for t in run.trades:
            if t.entry_date <= day:
                was_held.add(t.symbol)
        was_held |= expect

        in_market = {
            s: r for s, r in real.items() if _resolve_ts_code(s) is not None
            and _resolve_ts_code(s)[0] == market
        }
        extra_list: list[dict[str, Any]] = []
        sat_extra_list: list[dict[str, Any]] = []
        for s in sorted(set(in_market) - expect):
            r = in_market[s]
            cost = r.get("costPrice")
            entry = str(r.get("entryDate") or "")
            item = {
                "symbol": s,
                "name": str(r.get("name") or ""),
                "costPrice": float(cost) if cost else None,
                "entryDate": entry,
            }
            book = holding_book(mode, pick, market, s, sat_ts) if leg is not None else "s3"
            if book == "sat":
                sat_extra_list.append({**item, "kind": "sat_leg"})
            else:
                extra_list.append({
                    **item,
                    "kind": "exited" if s in was_held else "never_entered",
                })
        missing_list: list[dict[str, Any]] = [
            {
                "symbol": s,
                "entry": next(
                    (p.get("entry_date") for p in snap["positions"] if p["symbol"] == s),
                    None,
                ),
                "score": next(
                    (p.get("score_at_entry") for p in snap["positions"] if p["symbol"] == s),
                    None,
                ),
            }
            for s in sorted(expect - set(in_market))
        ]
        # Satellite leg: engine book should-hold vs actually held.
        sat_expect = {
            s for s in book_syms
            if _resolve_ts_code(s) is not None and _resolve_ts_code(s)[0] == market
        } if leg is not None else set()
        sat_missing_list = [{"symbol": s} for s in sorted(sat_expect - set(in_market))]
        sat_actual = len(set(in_market) & (sat_expect | {e["symbol"] for e in sat_extra_list}))
        markets[market] = {
            "available": True,
            "expected": len(expect),
            "actual": len(in_market),
            "extra": len(extra_list),
            "missing": len(missing_list),
            "extraList": extra_list,
            "missingList": missing_list,
            "satExpected": len(sat_expect),
            "actualSat": sat_actual,
            "satExtra": len(sat_extra_list),
            "satMissing": len(sat_missing_list),
            "satExtraList": sat_extra_list,
            "satMissingList": sat_missing_list,
        }
    return {"reconDate": day, "window": window, "markets": markets}


def run_registry_and_persist(day: str, *, window: str = "valid", mode: str = "twin_star") -> dict[str, Any]:
    """reconcile_registry + persist to db.behavior_audit (idempotent)."""
    from data_sync_service.db.behavior_audit import insert_audit

    out = reconcile_registry(day, window=window, mode=mode)
    for market, m in out["markets"].items():
        if not m.get("available"):
            continue
        insert_audit(
            audit_date=out["reconDate"],
            market=market,
            expected=m["expected"],
            actual=m["actual"],
            extra=m["extra"],
            missing=m["missing"],
            extra_list=m.get("extraList") or [],
            missing_list=m.get("missingList") or [],
            sat_expected=m.get("satExpected", 0),
            sat_actual=m.get("actualSat", 0),
            sat_extra=m.get("satExtra", 0),
            sat_missing=m.get("satMissing", 0),
            sat_extra_list=m.get("satExtraList") or [],
            sat_missing_list=m.get("satMissingList") or [],
        )
    return out
