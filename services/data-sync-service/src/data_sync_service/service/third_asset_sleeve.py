"""T6 third-asset sleeve hint — NASDAQ-100 ETF (513100) on idle cash.

Design: docs/designs/third-asset-sleeve.md (2026-08-19, pending decision).

When the S-3 CN line holds no positions (idle cash), the idle capital may
sit in a low-correlation third asset instead of earning ~0 in cash. Three-window
pre-study (OOS2/train/valid) picked: hold 513100 while its own close stays
above its 200-day MA; break the line -> back to GC001; when the S-3 line
re-opens with buy candidates -> sell 513100 and switch back to A-shares.

This module only *hints* in the watchlist/health/briefing surfaces — it never
moves money. The decision and execution stay with the user / paper system.

Every state is surfaced (always active when 513100 data is available):
  BUY_513100         idle capital + above MA200          -> buy 513100
  SELL_TO_A_SHARE    S-3 buy setup active                -> switch back to A-shares
  SELL_TO_REPO       broke MA200 (holding)               -> sell to repo
  DONT_BUY           broke MA200 (not holding) OR fully
                     deployed (above MA200)              -> stay out today
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from data_sync_service.db.daily import fetch_last_bars, get_last_trade_date

logger = logging.getLogger(__name__)

THIRD_ASSET_TS = "513100.SH"
THIRD_ASSET_SYMBOL = "ETF:513100"
MA_WINDOW = 200
# Require >= this many % of capital idle before suggesting a new 513100 buy.
MIN_IDLE_PCT = 20.0
# Re-sync ETF bars from tushare when the latest local bar is older than this.
STALE_DAYS = 2

# actions surfaced to the UI
ACTION_BUY = "BUY_513100"
ACTION_SELL_TO_A_SHARE = "SELL_TO_A_SHARE"
ACTION_SELL_TO_REPO = "SELL_TO_REPO"
ACTION_DONT_BUY = "DONT_BUY"
ACTION_NONE = "NONE"

_STRONG_REGIMES = ("Strong", "Diverging")

_ACTION_LABELS = {
    ACTION_BUY: "建议买入 513100",
    ACTION_SELL_TO_A_SHARE: "卖出 513100 · 换回 A 股",
    ACTION_SELL_TO_REPO: "卖出 513100 · 转逆回购",
    ACTION_DONT_BUY: "今日不买 513100",
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
    """Best-effort tushare sync when the local 513100 bars are missing/stale."""
    try:
        last = get_last_trade_date(ts)
        if last is None or (date.today() - last).days > STALE_DAYS:
            from data_sync_service.service.etf_daily import sync_etf_daily_for_ts_code

            sync_etf_daily_for_ts_code(ts)
    except Exception as exc:  # noqa: BLE001
        logger.warning("third-asset sleeve: 513100 sync failed, using local data (%s)", exc)


def _idle_from_holdings(holdings: list[dict[str, Any]]) -> tuple[float, bool]:
    """Return (idle_pct, holding_513100) from a holdings list.

    Holdings items may carry ``positionPct`` (registry) or ``sleeve_pct``
    (paper trades); both are treated as deployed % of capital.
    """
    deployed = 0.0
    holding_513100 = False
    for h in holdings:
        if not isinstance(h, dict):
            continue
        pct = h.get("positionPct", h.get("sleeve_pct"))
        try:
            deployed += float(pct or 0.0)
        except (TypeError, ValueError):
            pass
        sym = str(h.get("symbol") or "").upper()
        ts = str(h.get("ts_code") or "").upper()
        if sym == THIRD_ASSET_SYMBOL.upper() or ts == THIRD_ASSET_TS.upper():
            holding_513100 = True
    return max(0.0, 100.0 - min(deployed, 100.0)), holding_513100


def build_third_asset_sleeve(
    *,
    day: str,
    cn_block: dict[str, Any],
    holdings_override: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Compute the 513100 idle-cash hint for a book.

    ``cn_block`` is the CN market-state block of ``build_portfolio_health``
    (regime, panicCooldown, circuitBlocked, s3Candidates, holdings...).
    Pass ``holdings_override`` to evaluate a different book (e.g. the paper
    trades) instead of the registry holdings inside ``cn_block``.
    """
    out: dict[str, Any] = {
        "active": False,
        "action": ACTION_NONE,
        "message": "",
        "label": "",
        "etf": THIRD_ASSET_SYMBOL,
        "tsCode": THIRD_ASSET_TS,
    }

    _ensure_fresh(THIRD_ASSET_TS)
    try:
        bars = fetch_last_bars(THIRD_ASSET_TS, days=MA_WINDOW + 10)
    except Exception as exc:  # noqa: BLE001
        logger.warning("third-asset sleeve: fetch 513100 bars failed (%s)", exc)
        bars = []

    closes = [_float_or_none(b.get("close")) for b in bars]
    closes = [c for c in closes if c is not None]
    if len(closes) < MA_WINDOW:
        out["note"] = f"{THIRD_ASSET_SYMBOL} 本地数据不足 {MA_WINDOW} 根日线（{len(closes)}），暂不提示"
        return out

    ma200 = _sma(closes, MA_WINDOW)
    close = closes[-1]
    as_of = bars[-1].get("date", "")
    pct_chg = (close / closes[-2] - 1.0) * 100.0 if len(closes) >= 2 and closes[-2] > 0 else None
    above_ma200 = bool(ma200 is not None and close >= ma200)
    if ma200 is None:
        out["note"] = f"{THIRD_ASSET_SYMBOL} MA200 计算失败，暂不提示"
        return out

    # --- deployed capital / 513100 holding from the chosen book --------------
    holdings = holdings_override if holdings_override is not None else (cn_block.get("holdings") or [])
    idle_pct, holding_513100 = _idle_from_holdings(holdings)

    # --- S-3 gate state (switch back to A-shares / buy-sleeve eligibility) --
    regime = cn_block.get("regime")
    panic_active = bool((cn_block.get("panicCooldown") or {}).get("active"))
    circuit = bool(cn_block.get("circuitBlocked"))
    candidates = cn_block.get("s3Candidates") or []
    # gate closed = market not tradeable (Weak/unknown regime, panic cooldown,
    # or drawdown circuit breaker) — matches frontend isMarketGateClosed.
    gate_open = regime in _STRONG_REGIMES and not panic_active and not circuit
    s3_buy_setup = gate_open and len(candidates) > 0

    out.update(
        {
            "price": round(close, 3),
            "ma200": round(ma200, 3),
            "aboveMa200": above_ma200,
            "asOfDate": as_of,
            "pctChg": round(pct_chg, 2) if pct_chg is not None else None,
            "idlePct": round(idle_pct, 1),
            "s3BuySetup": s3_buy_setup,
            "gateOpen": gate_open,
            "holding513100": holding_513100,
        }
    )

    # --- state machine (priority: switch-back > break-MA > gate-closed > buy) --
    if s3_buy_setup:
        out["active"] = True
        out["action"] = ACTION_SELL_TO_A_SHARE
        reason = "（且已跌破200日线）" if not above_ma200 else "（仍在200日线上）"
        verb = "卖出 513100" if holding_513100 else "闲置资金改投 A 股候选"
        out["message"] = (
            f"A股有买点（{regime} · {len(candidates)} 个候选）→ {verb}{reason}"
        )
        out["label"] = action_label(ACTION_SELL_TO_A_SHARE)
        return out

    if not above_ma200:
        out["active"] = True
        out["action"] = ACTION_SELL_TO_REPO if holding_513100 else ACTION_DONT_BUY
        if holding_513100:
            out["message"] = (
                f"{THIRD_ASSET_SYMBOL} 跌破200日线（现价 {close:.3f} < MA200 {ma200:.3f}）"
                f"→ 卖出转逆回购/货币ETF，等站回均线再买"
            )
        else:
            out["message"] = (
                f"{THIRD_ASSET_SYMBOL} 跌破200日线（现价 {close:.3f} < MA200 {ma200:.3f}）"
                f"→ 今天别买，资金留逆回购/现金"
            )
        out["label"] = action_label(out["action"])
        return out

    if not gate_open:
        # 2026-08-19: market gate closed (Weak/panic/circuit) → never suggest a
        # fresh 513100 buy on a day the A-share line itself is hiding. The ETF
        # is correlated to the same risk-off that closed the gate.
        out["active"] = True
        out["action"] = ACTION_DONT_BUY
        reason = "市场闸门关闭"
        if regime not in _STRONG_REGIMES:
            reason = f"市场闸门关闭（regime={regime}）"
        elif panic_active:
            reason = "市场闸门关闭（恐慌冷却中）"
        elif circuit:
            reason = "市场闸门关闭（回撤熔断）"
        out["message"] = (
            f"{reason} · 今日不买 {THIRD_ASSET_SYMBOL}（即使站上200日线，资金留逆回购/现金）"
        )
        out["label"] = action_label(ACTION_DONT_BUY)
        return out

    if idle_pct >= MIN_IDLE_PCT:
        out["active"] = True
        out["action"] = ACTION_BUY
        out["message"] = (
            f"当前闲置资金 {idle_pct:.0f}% 且 {THIRD_ASSET_SYMBOL} 在200日线上"
            f"（现价 {close:.3f} > MA200 {ma200:.3f}）→ 建议用闲置资金买入（破线或 A 股有买点时卖出）"
        )
        out["label"] = action_label(ACTION_BUY)
        return out

    out["active"] = True
    out["action"] = ACTION_DONT_BUY
    out["message"] = (
        f"{THIRD_ASSET_SYMBOL} 在200日线上（现价 {close:.3f} > MA200 {ma200:.3f}）"
        f"但资金已部署（闲置 {idle_pct:.0f}%）→ 今日不买，专注 A 股持仓"
    )
    out["label"] = action_label(ACTION_DONT_BUY)
    return out


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