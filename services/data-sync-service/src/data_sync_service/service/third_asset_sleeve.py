"""T6 third-asset sleeve hint — NASDAQ-100 ETF on idle cash.

Design: docs/designs/third-asset-sleeve.md (2026-08-19, pending decision).

When the S-3 CN line holds no positions (idle cash), the idle capital may
sit in a low-correlation third asset instead of earning ~0 in cash. Three-window
pre-study (OOS2/train/valid) picked: hold a NASDAQ-100 ETF while its own close
stays above its 200-day MA; break the line -> back to GC001; when the S-3 line
re-opens with buy candidates -> sell the ETF and switch back to A-shares.

The backtest reference symbol is 513100 (华安); the user may hold any NASDAQ-100
QDII ETF (513110 华泰柏瑞, 159941 广发, ...) — the module tracks whatever is
actually held and only hints with 513100 when nothing is held yet.

This module only *hints* / *tracks* — it never moves money. The decision and
execution stay with the user / paper system.

States surfaced (always active when ETF data is available):
  BUY_513100         idle capital + gate open + above MA200     -> buy the ETF
  SELL_TO_A_SHARE    S-3 buy setup active                       -> switch back to A-shares
  SELL_TO_REPO       broke MA200 (holding)                      -> sell to repo
  DONT_BUY           gate closed / broke MA (not holding) /
                     fully deployed (above MA200)               -> stay out today
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from data_sync_service.db.daily import fetch_last_bars, get_last_trade_date

logger = logging.getLogger(__name__)

# Backtest reference (2026-08-19 study) — 华安纳斯达克100ETF.
THIRD_ASSET_TS = "513100.SH"
THIRD_ASSET_SYMBOL = "ETF:513100"
# NASDAQ-100 QDII ETFs the user may actually hold (same index, near-identical
# backtest behavior). Keep the reference symbol first.
THIRD_ASSET_SYMBOLS = {
    "ETF:513100", "ETF:513110", "ETF:159941", "ETF:159501", "ETF:513310", "ETF:159697",
}
THIRD_ASSET_TS_CODES = {s.replace("ETF:", "") + (".SH" if s.startswith("ETF:5") else ".SZ") for s in THIRD_ASSET_SYMBOLS}
MA_WINDOW = 200
# Require >= this many % of capital idle before suggesting a new ETF buy.
MIN_IDLE_PCT = 20.0
# Re-sync ETF bars from tushare when the latest local bar is older than this.
STALE_DAYS = 2

# actions surfaced to the UI
ACTION_BUY = "BUY_513100"
ACTION_SELL_TO_A_SHARE = "SELL_TO_A_SHARE"
ACTION_SELL_TO_REPO = "SELL_TO_REPO"
ACTION_DONT_BUY = "DONT_BUY"
ACTION_HOLD = "HOLD"
ACTION_NONE = "NONE"

_STRONG_REGIMES = ("Strong", "Diverging")

_ACTION_LABELS = {
    ACTION_BUY: "建议买入 513100",
    ACTION_SELL_TO_A_SHARE: "卖出 513100 · 换回 A 股",
    ACTION_SELL_TO_REPO: "卖出 513100 · 转逆回购",
    ACTION_DONT_BUY: "今日不买 513100",
    ACTION_HOLD: "持有（站上200日线）",
    ACTION_NONE: "暂不提示",
}


def action_label(action: str | None) -> str:
    return _ACTION_LABELS.get(action or "", ACTION_NONE)


def _float_or_none(v: Any) -> float | None:
    try:
        f = float(v)
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None


def _sma(closes: list[float], window: int) -> float | None:
    if len(closes) < window:
        return None
    return sum(closes[-window:]) / float(window)


def _ensure_fresh(ts: str) -> None:
    """Best-effort tushare sync when the local ETF bars are missing/stale."""
    try:
        last = get_last_trade_date(ts)
        if last is None or (date.today() - last).days > STALE_DAYS:
            from data_sync_service.service.etf_daily import sync_etf_daily_for_ts_code

            sync_etf_daily_for_ts_code(ts)
    except Exception as exc:  # noqa: BLE001
        logger.warning("third-asset sleeve: %s sync failed, using local data (%s)", ts, exc)


def is_third_asset_symbol(symbol: str) -> bool:
    """True for a NASDAQ-100 QDII ETF symbol (used to EXCLUDE it from the CN
    A-share holdings block — the sleeve is a separate region, not A股)."""
    return str(symbol or "").upper() in THIRD_ASSET_SYMBOLS


def _etf_symbol_to_ts(sym: str) -> str:
    """ETF:513110 -> 513110.SH ; ETF:159941 -> 159941.SZ (SH for 5xx, SZ for 1xx)."""
    code = str(sym or "").upper().replace("ETF:", "")
    return f"{code}.SH" if code.startswith("5") else f"{code}.SZ"


def resolve_held_third_asset(holdings: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Find the held NASDAQ-100 ETF in a holdings list (registry or paper).

    Returns the holding row (with symbol/ts_code/positionPct/costPrice/entryDate)
    or None when the user does not hold any sleeve ETF.
    """
    for h in holdings:
        if not isinstance(h, dict):
            continue
        sym = str(h.get("symbol") or "").upper()
        ts = str(h.get("ts_code") or "").upper()
        if sym in THIRD_ASSET_SYMBOLS or ts in THIRD_ASSET_TS_CODES:
            if not ts:
                h["ts_code"] = _etf_symbol_to_ts(sym)
            return h
    return None


def _idle_from_holdings(holdings: list[dict[str, Any]]) -> float:
    """Idle % of capital from a holdings list (positionPct or sleeve_pct)."""
    deployed = 0.0
    for h in holdings:
        if not isinstance(h, dict):
            continue
        pct = h.get("positionPct", h.get("sleeve_pct"))
        try:
            deployed += float(pct or 0.0)
        except (TypeError, ValueError):
            pass
    return max(0.0, 100.0 - min(deployed, 100.0))


def _market_state(cn_block: dict[str, Any]) -> dict[str, Any]:
    regime = cn_block.get("regime")
    panic_active = bool((cn_block.get("panicCooldown") or {}).get("active"))
    circuit = bool(cn_block.get("circuitBlocked"))
    candidates = cn_block.get("s3Candidates") or []
    gate_open = regime in _STRONG_REGIMES and not panic_active and not circuit
    return {
        "regime": regime,
        "panic_active": panic_active,
        "circuit": circuit,
        "candidates": candidates,
        "gate_open": gate_open,
        "s3_buy_setup": gate_open and len(candidates) > 0,
    }


def _etf_market_data(ts: str) -> dict[str, Any]:
    """Fetch the ETF bars and compute close / MA200 / above flags.

    Returns {} (empty) when data is insufficient — callers must handle it.
    """
    _ensure_fresh(ts)
    try:
        bars = fetch_last_bars(ts, days=MA_WINDOW + 10)
    except Exception as exc:  # noqa: BLE001
        logger.warning("third-asset sleeve: fetch %s bars failed (%s)", ts, exc)
        bars = []
    closes = [_float_or_none(b.get("close")) for b in bars]
    closes = [c for c in closes if c is not None]
    if len(closes) < MA_WINDOW:
        return {"ok": False, "ts": ts, "n": len(closes)}
    ma200 = _sma(closes, MA_WINDOW)
    if ma200 is None:
        return {"ok": False, "ts": ts, "n": len(closes)}
    close = closes[-1]
    pct_chg = (close / closes[-2] - 1.0) * 100.0 if len(closes) >= 2 and closes[-2] > 0 else None
    return {
        "ok": True,
        "ts": ts,
        "close": close,
        "ma200": ma200,
        "above_ma200": close >= ma200,
        "as_of": bars[-1].get("date", ""),
        "pct_chg": pct_chg,
    }


def build_third_asset_sleeve(
    *,
    day: str,
    cn_block: dict[str, Any],
    holdings_override: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Compute the idle-cash hint for a book.

    ``cn_block`` is the CN market-state block of ``build_portfolio_health``
    (regime, panicCooldown, circuitBlocked, s3Candidates, holdings...).
    Pass ``holdings_override`` to evaluate a different book (e.g. the paper
    trades) instead of the registry holdings inside ``cn_block``.

    Uses the held NASDAQ-100 ETF when the user already holds one, otherwise the
    backtest reference 513100.
    """
    holdings = holdings_override if holdings_override is not None else (cn_block.get("holdings") or [])
    held = resolve_held_third_asset(holdings)
    ts = str(held.get("ts_code") or "") if held else THIRD_ASSET_TS
    sym = str(held.get("symbol") or THIRD_ASSET_SYMBOL).upper() if held else THIRD_ASSET_SYMBOL

    out: dict[str, Any] = {
        "active": False,
        "action": ACTION_NONE,
        "message": "",
        "label": "",
        "etf": sym,
        "tsCode": ts,
        "holding": bool(held),
    }

    md = _etf_market_data(ts or THIRD_ASSET_TS)
    if not md.get("ok"):
        out["note"] = f"{sym} 本地数据不足 {MA_WINDOW} 根日线（{md.get('n', 0)}），暂不提示"
        return out

    close, ma200, above_ma200 = md["close"], md["ma200"], md["above_ma200"]
    idle_pct = _idle_from_holdings(holdings)
    st = _market_state(cn_block)
    holding_etf = bool(held)

    out.update(
        {
            "price": round(close, 3),
            "ma200": round(ma200, 3),
            "aboveMa200": above_ma200,
            "asOfDate": md["as_of"],
            "pctChg": round(md["pct_chg"], 2) if md["pct_chg"] is not None else None,
            "idlePct": round(idle_pct, 1),
            "s3BuySetup": st["s3_buy_setup"],
            "gateOpen": st["gate_open"],
            "holding513100": holding_etf,
        }
    )

    if st["s3_buy_setup"]:
        out["active"] = True
        out["action"] = ACTION_SELL_TO_A_SHARE
        reason = "（且已跌破200日线）" if not above_ma200 else "（仍在200日线上）"
        verb = f"卖出 {sym}" if holding_etf else "闲置资金改投 A 股候选"
        out["message"] = (
            f"A股有买点（{st['regime']} · {len(st['candidates'])} 个候选）→ {verb}{reason}"
        )
        out["label"] = action_label(ACTION_SELL_TO_A_SHARE)
        return out

    if not above_ma200:
        out["active"] = True
        out["action"] = ACTION_SELL_TO_REPO if holding_etf else ACTION_DONT_BUY
        if holding_etf:
            out["message"] = (
                f"{sym} 跌破200日线（现价 {close:.3f} < MA200 {ma200:.3f}）"
                f"→ 卖出转逆回购/货币ETF，等站回均线再买"
            )
        else:
            out["message"] = (
                f"{sym} 跌破200日线（现价 {close:.3f} < MA200 {ma200:.3f}）"
                f"→ 今天别买，资金留逆回购/现金"
            )
        out["label"] = action_label(out["action"])
        return out

    if not st["gate_open"]:
        # 2026-08-19: market gate closed (Weak/panic/circuit) → never suggest a
        # fresh buy on a day the A-share line itself is hiding. The ETF is
        # correlated to the same risk-off that closed the gate.
        out["active"] = True
        out["action"] = ACTION_DONT_BUY
        if st["regime"] not in _STRONG_REGIMES:
            reason = f"市场闸门关闭（regime={st['regime']}）"
        elif st["panic_active"]:
            reason = "市场闸门关闭（恐慌冷却中）"
        elif st["circuit"]:
            reason = "市场闸门关闭（回撤熔断）"
        else:
            reason = "市场闸门关闭"
        out["message"] = (
            f"{reason} · 今日不买 {sym}（即使站上200日线，资金留逆回购/现金）"
        )
        out["label"] = action_label(ACTION_DONT_BUY)
        return out

    if idle_pct >= MIN_IDLE_PCT:
        out["active"] = True
        out["action"] = ACTION_BUY
        out["message"] = (
            f"当前闲置资金 {idle_pct:.0f}% 且 {sym} 在200日线上"
            f"（现价 {close:.3f} > MA200 {ma200:.3f}）→ 建议用闲置资金买入（破线或 A 股有买点时卖出）"
        )
        out["label"] = action_label(ACTION_BUY)
        return out

    out["active"] = True
    out["action"] = ACTION_DONT_BUY
    out["message"] = (
        f"{sym} 在200日线上（现价 {close:.3f} > MA200 {ma200:.3f}）"
        f"但资金已部署（闲置 {idle_pct:.0f}%）→ 今日不买，专注 A 股持仓"
    )
    out["label"] = action_label(ACTION_DONT_BUY)
    return out


def build_third_asset_holding(
    *,
    day: str,
    cn_block: dict[str, Any],
    holdings_override: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    """Track the HELD NASDAQ-100 ETF with the T6 exit rules.

    Returns None when the user holds no sleeve ETF. Otherwise a holding-style
    block with the entry info, live price vs MA200, and the T6 action:
      HOLD             above MA200 + no A-share buy setup
      SELL_TO_A_SHARE  A-share buy setup (switch back)
      SELL_TO_REPO     broke MA200
    """
    holdings = holdings_override if holdings_override is not None else (cn_block.get("holdings") or [])
    held = resolve_held_third_asset(holdings)
    if held is None:
        return None

    sym = str(held.get("symbol") or "").upper()
    ts = str(held.get("ts_code") or "")
    md = _etf_market_data(ts)
    if not md.get("ok"):
        return {
            "symbol": sym, "tsCode": ts, "active": False,
            "note": f"{sym} 本地数据不足 {MA_WINDOW} 根日线（{md.get('n', 0)}），暂不跟踪",
        }

    st = _market_state(cn_block)
    cost = _float_or_none(held.get("costPrice"))
    position_pct = _float_or_none(held.get("positionPct", held.get("sleeve_pct")))
    pnl = (md["close"] / cost - 1.0) * 100.0 if cost and cost > 0 else None

    if st["s3_buy_setup"]:
        action, label, message = (
            ACTION_SELL_TO_A_SHARE,
            action_label(ACTION_SELL_TO_A_SHARE),
            f"A股有买点（{st['regime']} · {len(st['candidates'])} 个候选）→ 卖出 {sym}，资金换回 A 股",
        )
    elif not md["above_ma200"]:
        action, label, message = (
            ACTION_SELL_TO_REPO,
            action_label(ACTION_SELL_TO_REPO),
            f"{sym} 跌破200日线（现价 {md['close']:.3f} < MA200 {md['ma200']:.3f}）→ 卖出转逆回购",
        )
    else:
        action, label, message = (
            ACTION_HOLD,
            action_label(ACTION_HOLD),
            f"{sym} 站上200日线（现价 {md['close']:.3f} > MA200 {md['ma200']:.3f}）→ 持有，破线或 A 股有买点时卖出",
        )

    return {
        "active": True,
        "symbol": sym,
        "tsCode": ts,
        "name": held.get("name") or sym,
        "entryDate": held.get("entryDate"),
        "costPrice": round(cost, 4) if cost is not None else None,
        "positionPct": round(position_pct, 2) if position_pct is not None else None,
        "price": round(md["close"], 3),
        "ma200": round(md["ma200"], 3),
        "aboveMa200": md["above_ma200"],
        "asOfDate": md["as_of"],
        "pctChg": round(md["pct_chg"], 2) if md["pct_chg"] is not None else None,
        "pnlPct": round(pnl, 2) if pnl is not None else None,
        "action": action,
        "label": label,
        "message": message,
        "s3BuySetup": st["s3_buy_setup"],
        "gateOpen": st["gate_open"],
    }


def build_third_asset_sleeve_for_paper(*, day: str) -> dict[str, Any]:
    """Sleeve hint evaluated against the PAPER book (open S-3 trades).

    The paper system has no cash pool, so idle = 100 - sum(open sleeve_pct).
    Market state (regime / panic / circuit / candidates) is the same CN block
    used for the real book — the sleeve rule is market-driven, not book-driven.
    """
    from data_sync_service.db.paper_trading import list_paper_trades
    from data_sync_service.service.portfolio_health import _health_block

    cn_block = _health_block(market="CN", day=day)
    open_trades = list_paper_trades(status="open")
    holdings = []
    for t in open_trades:
        if not str(t.get("symbol") or "").upper().startswith(("CN:", "ETF:")):
            continue
        holdings.append(
            {
                "symbol": t.get("symbol"),
                "ts_code": t.get("ts_code"),
                "sleeve_pct": t.get("sleeve_pct") or 0.0,
            }
        )
    return build_third_asset_sleeve(day=day, cn_block=cn_block, holdings_override=holdings)