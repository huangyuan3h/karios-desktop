# ruff: noqa: E701,E722
"""Multi-asset sleeve: who is strong buy who (replaces single NASDAQ sleeve).

Candidates (all tradable in CN stock account, fund_daily 2023-01):
- GOLD 518880.SH 华安黄金
- OIL 513350.SH 富国油气QDII (proxy for crude)
- NASDAQ 513100.SH 纳指100
- BOND 511260.SH 10年国债 (low-vol ballast, rarely top but keeps sharpe)

Rule (G2 prototype 2026-08-23, 661d 2023-11~2026-08):
  lookback = 60d return (mom60) > 20d, filtered by price > MA200 (avoid falling knife)
  daily pick = argmax mom60 among above-MA200 candidates, traded next day, 0.05% cost
  Prototype: mom60+MA200 ann31.2% vol30 sharpe1.03 vs fixed NASDAQ 10.9% / equal4 19.9% sharpe1.51
  => rotation beats single asset, equal weight best sharpe. We keep rotation as tactical sleeve,
     equal4 as strategic note.

States: Harbor parking — park the idle fraction in the mom60+MA200 ETF argmax
(GOLD/OIL/NASDAQ/BOND10); no candidate above MA200 -> REPO.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import numpy as np

from data_sync_service.db.daily import fetch_last_bars
from data_sync_service.service.harbor import (
    HYST_BAND,
    MULTI_TS,
    NAMES,
    NASDAQ_ALIASES,
    TRAIL_PCT,
    held_mom,
    pick_parking,
)

logger = logging.getLogger(__name__)


def _parking_candidates() -> list[dict[str, str]]:
    """Parking universe derived from the canonical ``harbor`` definitions.

    Single source: ``harbor.MULTI_TS``/``NASDAQ_ALIASES``/``NAMES`` own the
    keys/ts_codes/names. A local copy here used to drift from the engine
    universe (OPT-206).
    """
    out = [
        {
            "key": key,
            "ts": ts,
            "symbol": f"ETF:{ts.split('.')[0]}",
            "name": NAMES.get(key, key),
        }
        for key, ts in MULTI_TS.items()
    ]
    known_ts = {c["ts"] for c in out}
    for alias in NASDAQ_ALIASES:
        if alias in known_ts:
            continue
        out.append(
            {
                "key": "NASDAQ",
                "ts": alias,
                "symbol": f"ETF:{alias.split('.')[0]}",
                "name": NAMES.get("NASDAQ", "NASDAQ"),
            }
        )
    return out


CANDIDATES = _parking_candidates()

# NASDAQ has two tradable aliases (513110/513100) – keep both, dedupe by key in _pick.
MULTI_ASSET_SYMBOLS = {c["symbol"] for c in CANDIDATES}
MULTI_ASSET_TS_CODES = {c["ts"] for c in CANDIDATES}


def multi_key_for_symbol(symbol: str | None) -> str | None:
    """Canonical parking key for a holding symbol/ts_code (None = not parked).

    Accepts ``ETF:513100`` / ``513100.SH`` / ``513100`` shapes. Returns the
    engine key (GOLD/OIL/NASDAQ/BOND10) so the Watchlist never re-derives its
    own mapping and can never disagree with ``harbor.pick_parking``.
    """
    s = str(symbol or "").upper()
    if not s:
        return None
    code = s.replace("ETF:", "").split(".")[0]
    for c in CANDIDATES:
        if s == c["symbol"] or s == c["ts"] or code == c["ts"].split(".")[0]:
            return c["key"]
    return None


def _etf_symbol_to_ts(symbol: str | None) -> str:
    """Parking ts_code for a holding symbol ('' when not a parking ETF)."""
    key = multi_key_for_symbol(symbol)
    if not key:
        return ""
    for c in CANDIDATES:
        if c["key"] == key:
            return c["ts"]
    return ""


def is_multi_asset_symbol(symbol: str) -> bool:
    sym = str(symbol or "").upper()
    # also treat ts_code with .SH/.SZ as symbol; accept both 513110/513100 as NASDAQ
    if sym in MULTI_ASSET_SYMBOLS or sym in MULTI_ASSET_TS_CODES:
        return True
    bare = sym.replace(".SH", "").replace(".SZ", "").replace("ETF:", "")
    return bare in {s.replace("ETF:", "") for s in MULTI_ASSET_SYMBOLS} or bare in {
        "513110",
        "513100",
        "513500",
    }


def _etf_market_data(ts: str) -> dict[str, Any]:
    """Fetch ETF bars and compute close/MA200 for holding display."""
    try:
        closes = _closes(ts, 260)
        if len(closes) < MA_WINDOW:
            return {"ok": False, "n": len(closes)}
        ma200 = sum(closes[-MA_WINDOW:]) / MA_WINDOW
        close = closes[-1]
        return {"ok": True, "close": close, "ma200": ma200, "above": close >= ma200}
    except Exception:
        return {"ok": False, "n": 0}


LOOKBACK = 60
MA_WINDOW = 200
COST = 0.0005
# Harbor ETF risk exit (causal: trigger on the decision print, no same-day
# credit). Single source: harbor.TRAIL_PCT — never a local 8.0 copy (OPT-206).
TRAILING_PCT = TRAIL_PCT


def _etf_trail_exit(held: dict[str, Any], *, day: str) -> dict[str, Any] | None:
    """If held ETF close < peak × (1 − TRAILING_PCT%), return SELL_TO_REPO.

    Peak reference = the frozen engine's: the parking replay holds the leg
    from the rotation SIGNAL-day close (``parking_replay`` initialises peak at
    ``prev``), while the paper/Live book fills at the NEXT open (entryDate =
    fill day). Include the session before ``entryDate`` so Live reproduces the
    engine's trail decisions (2026-09-17 audit: a 12.0% engine drawdown read
    as 7.8% live on the fill-day reference and the leg was not exited).

    Causal clock: the signal is evaluated on **completed closes only**. A
    breach on the latest completed session (`day` bar present) signals the
    next open. Once fired, the exit is **sticky** until the leg leaves the
    holdings: on a later morning/session where `day` has no bar yet (or the
    close bounced back inside the band) the same SELL_TO_REPO is still
    returned as a pending open sell. Before 2026-11-05 the function only
    looked at ``d == day``; without today's bar ``cur_close`` stayed 0 and a
    previously-triggered trail read as HOLD (the 513350 miss).
    """
    held_sym = str(held.get("symbol") or "").upper()
    held_ts = str(held.get("ts_code") or held_sym.replace("ETF:", "") + ".SH")
    entry = str(held.get("entryDate") or held.get("entry_date") or "")
    if not entry:
        return None
    try:
        canon = _parking_ts_for(held)
        mp = _series(canon, 500, as_of=day) if canon else {}
        if not mp:
            mp = _raw_series(held_ts, as_of=day, days=505)
        dates = sorted(mp)
        prior = [d for d in dates if d < entry[:10]]
        start = prior[-1] if prior else entry[:10]
        peak = 0.0
        signal_date = ""
        signal_dd = 0.0
        for d in dates:
            if d < start:
                continue
            c = float(mp[d])
            if c > peak:
                peak = c
            # Sticky: the FIRST breached completed close owns the signal.
            if not signal_date and peak > 0 and c < peak * (1 - TRAILING_PCT / 100):
                signal_date = d
                signal_dd = (peak - c) / peak * 100
        if not signal_date or signal_dd <= 0:
            return None
        # `day` itself has a completed bar → fresh close signal, execute next
        # open. Otherwise the signal predates `day` and is still unexecuted.
        pending = signal_date < str(day)[:10]
        execute_hint = (
            "待开盘卖出转逆回购（信号已触发，未执行）"
            if pending
            else "次日开盘卖出转逆回购"
        )
        message = (
            f"{held_sym}峰值回撤{signal_dd:.1f}% ≥{TRAILING_PCT:.0f}% → "
            + (
                f"次日开盘卖转逆回购（信号日 {signal_date}，待开盘执行）"
                if pending
                else "转逆回购"
            )
        )
        return {
            "active": True,
            "action": "SELL_TO_REPO",
            "message": message,
            "label": "卖出转repo(止损)",
            "symbol": held_sym,
            "trailExit": True,
            "pendingOpenSell": pending,
            "signalDate": signal_date,
            "executeHint": execute_hint,
        }
    except Exception as exc:
        logger.warning("multi-sleeve trailing check failed: %s", exc)
    return None


def _parking_ts_for(held: dict[str, Any]) -> str | None:
    """Canonical parking ts_code for a holding (None = not a parking leg)."""
    sym = str(held.get("symbol") or "").upper()
    ts = str(held.get("ts_code") or "").upper()
    for c in CANDIDATES:
        if sym == c["symbol"] or ts == c["ts"]:
            return c["ts"]
    key = multi_key_for_symbol(sym) or multi_key_for_symbol(ts)
    if key:
        for c in CANDIDATES:
            if c["key"] == key:
                return c["ts"]
    return None


# 14:30 intraday warning (2026-11-05): "near" = within this % above the trail
# threshold. Warning only — the trading clock stays close-signal → next-open.
TRAIL_WARN_NEAR_PCT = 1.0


def parking_trail_levels(held: dict[str, Any], *, day: str) -> dict[str, Any] | None:
    """Peak / threshold price for a held parking leg, on the RAW print basis.

    ``bar_5min`` 14:30 prints are raw, and ETF ``daily`` closes are raw too
    (``adj_factor`` NULL) — using the raw series keeps the threshold directly
    comparable to the intraday print. The trail RATIO is basis-invariant
    (peak × (1 − 8%)), so this is the same rule the close-based
    ``_etf_trail_exit`` applies. Fail-open: no bars → None (no warning).
    """
    held_sym = str(held.get("symbol") or "").upper()
    held_ts = str(held.get("ts_code") or held_sym.replace("ETF:", "") + ".SH")
    entry = str(held.get("entryDate") or held.get("entry_date") or "")
    try:
        mp = _raw_series(held_ts, as_of=day, days=505)
        if not mp:
            return None
        prior = [d for d in sorted(mp) if d < (entry[:10] if entry else "")]
        start = prior[-1] if prior else (entry[:10] if entry else sorted(mp)[0])
        window = [d for d in sorted(mp) if d >= start]
        if not window:
            return None
        peak = max(float(mp[d]) for d in window)
        if peak <= 0:
            return None
        return {
            "symbol": held_sym,
            "ts_code": held_ts,
            "entryDate": entry,
            "windowStart": start,
            "bars": len(window),
            "peak": round(peak, 4),
            "thresholdPx": round(peak * (1 - TRAILING_PCT / 100), 4),
        }
    except Exception as exc:  # noqa: BLE001
        logger.warning("multi-sleeve trail levels failed: %s", exc)
        return None


def intraday_trail_watch(
    held: dict[str, Any], *, day: str, px: float | None
) -> dict[str, Any] | None:
    """Conditional Must-watch when a 14:30 print is at/under the trail line.

    Does NOT change the trading clock: only a completed close finalizes the
    signal (``_etf_trail_exit``). This is the early warning so the user is not
    surprised after the close.
    """
    if not px or float(px) <= 0:
        return None
    levels = parking_trail_levels(held, day=day)
    if not levels:
        return None
    peak = float(levels["peak"])
    threshold = float(levels["thresholdPx"])
    px = float(px)
    if px > threshold * (1 + TRAIL_WARN_NEAR_PCT / 100.0):
        return None
    dd = (peak - px) / peak * 100.0
    gap = (px / threshold - 1.0) * 100.0
    status = "breached" if px <= threshold else "near"
    sym = levels["symbol"]
    if status == "breached":
        message = (
            f"{sym}现价{px:.3f} ≤ 门槛价{threshold:.3f}"
            f"（峰值{peak:.3f} 回撤{dd:.1f}%）→ 现价已达卖出条件"
        )
    else:
        message = (
            f"{sym}现价{px:.3f} 逼近门槛价{threshold:.3f}"
            f"（高{gap:.1f}% 回撤{dd:.1f}%）→ 将触线，收盘确认"
        )
    return {
        "active": True,
        "status": status,
        "symbol": sym,
        "px": round(px, 4),
        "thresholdPx": levels["thresholdPx"],
        "peak": levels["peak"],
        "drawdownPct": round(dd, 1),
        "gapPct": round(gap, 1),
        "message": message,
        "executeHint": "若收盘仍低于门槛价 → 明日开盘卖出转逆回购（不盘中卖）",
        "tradeDate": str(day)[:10],
    }


def latest_1430_print(ts_code: str, *, day: str) -> float | None:
    """Today's 14:30 print for one ts_code (raw), None when not captured yet."""
    if not ts_code:
        return None
    try:
        from data_sync_service.db.bar_5min import fetch_1430_closes

        rows = fetch_1430_closes([ts_code], str(day)[:10], str(day)[:10])
        return (rows.get(ts_code) or {}).get(str(day)[:10])
    except Exception as exc:  # noqa: BLE001
        logger.warning("multi-sleeve 14:30 print read failed: %s", exc)
        return None


def intraday_trail_watch_for_day(held: dict[str, Any], *, day: str) -> dict[str, Any] | None:
    """Read today's 14:30 print (or the latest intraday print) then evaluate.

    Source order (2026-11-05 steer: 当时价**或最新价**):
      1. ``live_1430`` snapshot row for ``day`` (the 14:30 print);
      2. newest stored intraday print of ``day`` (``bar_5min``, any slot) when
         the snapshot row is missing for this symbol.
    No print → no warning (never guess, never network).
    """
    held_ts = str(held.get("ts_code") or str(held.get("symbol") or "").upper().replace("ETF:", "") + ".SH")
    px = latest_1430_print(held_ts, day=day)
    source = "live_1430"
    if not px:
        px = latest_intraday_print(held_ts, day=day)
        source = "bar_5min_latest"
    warn = intraday_trail_watch(held, day=day, px=px)
    if warn:
        warn["pxSource"] = source
    return warn


def latest_intraday_print(ts_code: str, *, day: str) -> float | None:
    """Newest stored intraday print for one ts_code today (raw, no network)."""
    if not ts_code:
        return None
    try:
        from data_sync_service.db.bar_5min import fetch_latest_intraday_close

        return fetch_latest_intraday_close(ts_code, str(day)[:10])
    except Exception as exc:  # noqa: BLE001
        logger.warning("multi-sleeve latest intraday print read failed: %s", exc)
        return None



_MERGED_CACHE: dict[str, Any] = {"at": 0.0, "data": {}}
_MERGED_TTL_SECONDS = 60.0


def _merged_source() -> dict[str, dict[str, float]]:
    """Engine-basis parking series: research panel + scaled DB tail.

    ``daily`` ETF closes are RAW (ETF ``adj_factor`` is NULL), while the
    frozen engine and every Harbor-family Timeline read
    ``harbor.load_etf_closes()`` (CSV ``close_adj`` + ratio-scaled tail).
    Feeding the raw series into ``pick_parking``/trail made Live decisions
    diverge from the backtest on coupon-paying ETFs (2026-09-17 audit).

    60s cache: the panel is a 4MB CSV read + one query, and the dashboard
    hint calls this several times per request.
    """
    now = time.monotonic()
    cached = _MERGED_CACHE.get("data") or {}
    if cached and now - float(_MERGED_CACHE.get("at") or 0.0) < _MERGED_TTL_SECONDS:
        return cached
    try:
        from data_sync_service.service.harbor import load_etf_closes

        data = load_etf_closes() or {}
    except Exception as exc:  # noqa: BLE001
        logger.warning("multi-sleeve adjusted series load failed: %s", exc)
        data = {}
    if data:
        _MERGED_CACHE.update({"at": now, "data": data})
    return data


def _adjusted_series(ts: str) -> dict[str, float]:
    """Adjusted {date: close} for one parking ts (empty on failure)."""
    return dict(_merged_source().get(ts) or {})


def _raw_series(ts: str, *, as_of: str, days: int) -> dict[str, float]:
    """Raw ``daily`` fallback, cut at ``as_of`` (pre-2026-09-17 behavior)."""
    try:
        bars = fetch_last_bars(ts, days=days)
    except Exception:
        return {}
    out: dict[str, float] = {}
    for b in bars:
        d = str(b.get("date") or b.get("trade_date") or "")
        if d and d > as_of:
            continue
        try:
            c = float(b.get("close"))
        except (TypeError, ValueError):
            continue
        if c > 0:
            out[d] = c
    return out


def _series(ts: str, days: int = 260, *, as_of: str | None = None) -> dict[str, float]:
    """{date: close} through the latest COMPLETED bar at ``as_of``.

    Engine basis first (adjusted panel + scaled tail); raw ``daily`` fallback
    only when the panel has nothing for this ts.
    """
    from data_sync_service.service.trade_calendar_utils import shanghai_today

    cutoff = str(as_of or shanghai_today().isoformat())[:10]
    mp: dict[str, float] = {}
    for d, c in (_adjusted_series(ts) or {}).items():
        if not d or d > cutoff:
            continue
        try:
            v = float(c)
        except (TypeError, ValueError):
            continue
        if v > 0:
            mp[d] = v
    if not mp:
        mp = _raw_series(ts, as_of=cutoff, days=days + 5)
    if days and len(mp) > days:
        for d in sorted(mp)[: len(mp) - days]:
            mp.pop(d, None)
    return mp


def _closes(ts: str, days: int = 260) -> list[float]:
    """Latest closes including today's bar if present (display layer)."""
    mp = _series(ts, days)
    return [mp[d] for d in sorted(mp)]


def _signal_closes(ts: str, days: int = 260, *, as_of: str | None = None) -> list[float]:
    """Closes through the latest COMPLETED bar at ``as_of`` (default: today).

    Harbor clock: signal at T close (job runs 18:20) -> execute T+1 open.
    During the session the daily table has no today bar, so this naturally
    falls back to the previous session. Historical callers (recon / past-day
    views) must pass ``as_of`` or they would leak the latest closes.
    """
    mp = _series(ts, days, as_of=as_of)
    return [mp[d] for d in sorted(mp)]


def _signal_series(ts: str, days: int = 260, *, as_of: str | None = None) -> dict[str, float]:
    """{date: close} through the latest COMPLETED bar at ``as_of``.

    Same source/cut as ``_signal_closes``; the series form lets the shared
    ``harbor.pick_parking`` rule run on the exact same data as the timeline.
    """
    return _series(ts, days, as_of=as_of)


def _pick(*, as_of: str | None = None) -> dict[str, Any] | None:
    """The ETF-leg pick as of ``as_of`` (default: today) via the shared rule.

    Delegates to ``harbor.pick_parking`` so the Live decision, the frozen
    backtest (`build_harbor_timeline`) and the evaluation scripts all use ONE
    implementation (mom60 index, MA200 gate, NASDAQ alias selection, coverage).
    """
    etf_close = {c["ts"]: _signal_series(c["ts"], 260, as_of=as_of) for c in CANDIDATES}
    pick_as_of = max((max(mp) for mp in etf_close.values() if mp), default="")
    if not pick_as_of:
        return None
    return pick_parking(etf_close, pick_as_of)


def _held_leg_mom(ts: str | None, day: str | None) -> float | None:
    """Held leg's own mom60 as of ``day`` (same formula as the state machine).

    Used by the unified H2 rotation gate; ``None`` fails open (rotate).
    """
    if not ts:
        return None
    series = _signal_series(ts, 260, as_of=day)
    as_of = day or (max(series) if series else "")
    return held_mom({ts: series}, ts, as_of)


def _rsi(closes: list[float], period: int = 14) -> float | None:
    if len(closes) < period + 1:
        return None
    gains = 0.0
    losses = 0.0
    for i in range(1, period + 1):
        d = closes[-i] - closes[-i - 1]
        if d > 0:
            gains += d
        else:
            losses += -d
    if losses == 0:
        return 100.0
    rs = (gains / period) / (losses / period)
    return 100 - 100 / (1 + rs)


PULSE_STATS = {
    "oil_rsi80": {
        "n": 35,
        "mean": 3.85,
        "median": 4.70,
        "win": 82.9,
        "vs_nas_mean": 2.13,
        "recent_nas_mean": -1.06,
    },
    "nas_mom20_neg5": {"n": 63, "mean": 4.66, "win": 71.4, "vs_nas_mean": 0.59},
    "oil_vol_low": {"n": 129, "mean_gn": 2.46, "win": 65.1},
}


def build_pulse_hints(*, day: str | None = None) -> list[dict[str, Any]]:
    """Observation layer for §22.7 R1-R5 (no position change, hint only).

    Returns 3 hints with active flag for today (t-1 close, no lookahead):
    - R4 oil RSI>80 -> gold>oil +3.85% win82.9% (but gold>nas -1% recent -> vs sleeve≈0)
    - R2 nas mom20<-5% -> gold>oil +4.66% win71.4%
    - R3 oil vol20 low20% -> gold>nas +2.46% win65.1% (most stable vs sleeve)
    """
    hints: list[dict[str, Any]] = []
    try:
        closes_oil = _signal_closes("513350.SH", 260, as_of=day)
        closes_nas = _signal_closes("513100.SH", 260, as_of=day)
        # R4 oil RSI>80
        rsi_oil = _rsi(closes_oil) if len(closes_oil) >= 15 else None  # t-1 close
        active_rsi = rsi_oil is not None and rsi_oil > 80
        hints.append(
            {
                "id": "R4_oil_rsi80",
                "label": "油超买 RSI>80",
                "active": bool(active_rsi),
                "value": round(rsi_oil, 1) if rsi_oil is not None else None,
                "threshold": 80,
                "stats": PULSE_STATS["oil_rsi80"],
                "note": "金强于油 +3.85% win82.9% n35，但金>纳指 +2.1%全期/-1.06%近期，vs sleeve≈0（纳指强势年）",
                "action": "观察：若sleeve持有OIL，可考虑切GOLD；持有NASDAQ则不切",
            }
        )
        # R2 nas mom20<-5%
        mom_nas = None
        if len(closes_nas) >= 21:
            mom_nas = closes_nas[-1] / closes_nas[-21] - 1 if closes_nas[-21] != 0 else None
        active_nas = mom_nas is not None and mom_nas < -0.05
        hints.append(
            {
                "id": "R2_nas_mom20_neg5",
                "label": "纳指弱势 mom20<-5%",
                "active": bool(active_nas),
                "value": round(mom_nas * 100, 2) if mom_nas is not None else None,
                "threshold": -5.0,
                "stats": PULSE_STATS["nas_mom20_neg5"],
                "note": "金强于油 +4.66% win71.4% n63，但金>纳指 +0.59% vs sleeve≈0",
                "action": "观察：纳指弱时金相对油安全",
            }
        )
        # R3 oil vol low20% (approx threshold 0.0126 from 2023-11+ distribution)
        vol_oil = None
        if len(closes_oil) >= 21:
            rets = [
                closes_oil[i] / closes_oil[i - 1] - 1
                for i in range(len(closes_oil) - 20, len(closes_oil))
            ]
            vol_oil = float(np.std(rets)) if rets else None
        active_vol = vol_oil is not None and vol_oil < 0.0126
        hints.append(
            {
                "id": "R3_oil_vol_low",
                "label": "油低波 vol20<20%分位",
                "active": bool(active_vol),
                "value": round(vol_oil, 4) if vol_oil is not None else None,
                "threshold": 0.0126,
                "stats": PULSE_STATS["oil_vol_low"],
                "note": "金强于纳指 +2.46% win65.1% n129（双期稳定 early+2.26%/recent+2.75%），唯一金>纳指>2%天平",
                "action": "观察：油低波时金相对纳指 +2.5%",
            }
        )
    except Exception as exc:
        logger.warning("pulse hints failed: %s", exc)
    return hints


def _idle_pct(holdings: list[dict[str, Any]]) -> float:
    deployed = 0.0
    for h in holdings:
        try:
            deployed += float(h.get("positionPct", h.get("sleeve_pct") or 0))
        except:
            pass
    return max(0.0, 100 - min(deployed, 100))


def build_multi_asset_sleeve(
    *, day: str, cn_block: dict[str, Any], holdings_override=None
) -> dict[str, Any]:
    """Live Harbor hint: park idle cash in the mom60+MA200 ETF argmax.

    Rule (frozen by B11, `docs/backtests/stable/etf-parking-baseline-2026-09-13.md`):
    no STOCK gate, no idle floor; no ETF above MA200 -> REPO (exit if held).
    Exit = causal trail8. Position size = idle% (paper sleeve_pct).
    """
    holdings = (
        holdings_override if holdings_override is not None else (cn_block.get("holdings") or [])
    )
    idle = _idle_pct(holdings)
    parked = 0.0
    for h in holdings:
        sym = str(h.get("symbol") or "").upper()
        ts = str(h.get("ts_code") or "").upper()
        if any(sym == c["symbol"] or ts == c["ts"] for c in CANDIDATES):
            try:
                parked += float(h.get("positionPct", h.get("sleeve_pct") or 0))
            except (TypeError, ValueError):
                pass
    # Parking target size = stock-idle + the previously parked fraction: on a
    # ROTATE the held leg funds its successor, so the new leg must not be sized
    # at `idle` alone (which is 0 when fully parked).
    park_pct = max(0.0, min(100.0, idle + parked))
    regime = cn_block.get("regime")
    panic = bool((cn_block.get("panicCooldown") or {}).get("active"))
    circuit = bool(cn_block.get("circuitBlocked"))
    cands = cn_block.get("s3Candidates") or []
    gate_open = regime in ("Strong", "Diverging") and not panic and not circuit
    s3_buy_setup = gate_open and len(cands) > 0

    held = None
    held_c = None
    for h in holdings:
        # Registry rows are watchlist entries too: a parking ETF with 0% is NOT
        # a held leg. Picking it made the sleeve "rotate" a ghost leg (513110
        # at 0% in the 2026-11-05 screenshot) and skip the real 63.5% OIL trail.
        try:
            pos = float(h.get("positionPct", h.get("sleeve_pct") or 0) or 0)
        except (TypeError, ValueError):
            pos = 0.0
        if pos <= 0:
            continue
        sym = str(h.get("symbol") or "").upper()
        ts = str(h.get("ts_code") or "").upper()
        for c in CANDIDATES:
            if sym == c["symbol"] or ts == c["ts"]:
                held = h
                held_c = c
                break
        if held is not None:
            break

    etf_pick = _pick(as_of=day)
    out: dict[str, Any] = {
        "active": False,
        "action": "NONE",
        "idlePct": round(idle, 1),
        "parkPct": round(park_pct, 1),
        "s3BuySetup": s3_buy_setup,
        "mode": "harbor",
        "strategy": "港湾",
    }

    # Harbor (P1, B11): park idle cash in the mom60+MA200 argmax ETF —
    # no STOCK gate, no idle floor. No candidate above MA200 -> REPO.
    out.update({"pick": etf_pick, "holding": bool(held), "etfPick": etf_pick})

    if held:
        trail = _etf_trail_exit(held, day=day)
        if trail is not None:
            out.update(trail)
            return out
        # 14:30 early warning (2026-11-05): a print at/under the trail line is a
        # conditional Must-watch. It never changes `action` — only a completed
        # close finalizes the exit (close signal → next open).
        warn = intraday_trail_watch_for_day(held, day=day)
        if warn:
            out["intradayWarn"] = warn
    held_sym = str((held or {}).get("symbol") or "").upper()
    pick_sym = str((etf_pick or {}).get("symbol") or "").upper()

    if etf_pick is None:
        if held:
            out.update(
                {
                    "active": True,
                    "action": "SELL_TO_REPO",
                    "message": f"全候选跌破200日线 → 卖出 {held_sym} 转逆回购",
                    "label": "卖出转repo",
                    "symbol": held_sym,
                    "signalDate": day,
                    "pendingOpenSell": False,
                    "executeHint": "次日开盘卖出转逆回购",
                }
            )
            return out
        out.update(
            {
                "active": True,
                "action": "DONT_BUY",
                "message": "无 ETF 站上200日线 → 闲置留现金",
                "label": "不买",
            }
        )
        return out

    if held and held_sym == pick_sym:
        out.update(
            {
                "active": True,
                "action": "HOLD",
                "message": f"港湾停车：持有 {etf_pick['symbol']}（mom60 {etf_pick['mom60']}%）",
                "label": "持有",
            }
        )
        return out
    if held:
        # Unified H2 (H-H2-UNIFY, 2026-09-18): only rotate when the challenger
        # leads the incumbent's own mom60 by >= HYST_BAND; otherwise keep it.
        held_ts = str((held_c or {}).get("ts") or held.get("ts_code") or "").upper()
        hm = _held_leg_mom(held_ts, day)
        gap_pct = etf_pick["mom60"] - (hm * 100.0 if hm is not None else 0.0)
        if hm is not None and gap_pct < HYST_BAND * 100.0:
            out.update(
                {
                    "active": True,
                    "action": "HOLD",
                    "hystBlocked": True,
                    "message": (
                        f"港湾停车：维持持有 {held_sym}（挑战者 {etf_pick['symbol']} "
                        f"领先 {gap_pct:.1f}pt < 2pt 门槛，不换仓）"
                    ),
                    "label": "持有（迟滞）",
                }
            )
            return out
        out.update(
            {
                "active": True,
                "action": "ROTATE",
                "message": (
                    f"港湾停车轮动：卖出 {held_sym} → {etf_pick['symbol']} "
                    f"({etf_pick['name']} mom60 {etf_pick['mom60']}%)"
                ),
                "label": f"轮动至 {etf_pick['key']}",
                "symbol": held_sym,
                "signalDate": day,
                "pendingOpenSell": False,
                "executeHint": "次日开盘执行轮动",
            }
        )
        return out
    if idle <= 0:
        out.update(
            {
                "active": True,
                "action": "DONT_BUY",
                "message": "无闲置现金（股票仓已满）",
                "label": "不买",
            }
        )
        return out
    out.update(
        {
            "active": True,
            "action": "BUY",
            "message": (
                f"港湾停车：{etf_pick['symbol']}（{etf_pick['name']} mom60 {etf_pick['mom60']}%）"
                f" · 闲置 {idle:.0f}% → 买入"
            ),
            "label": f"买入 {etf_pick['key']}",
            "symbol": etf_pick.get("symbol"),
            "signalDate": day,
            "pendingOpenSell": False,
            "executeHint": "次日开盘买入",
        }
    )
    return out
