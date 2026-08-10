"""Portfolio health check aligned to the S-3 backtest exit rules.

For every real holding in the watchlist registry (positionPct > 0) compute the
S-3 exit conditions from the SAME constants the paper system uses
(db/paper_trading.py): fixed stop -5% · trailing -8% from peak · 60-day cap.
Market state (regime / sentiment / panic cooldown / S-3 candidates) is
attached so a decision agent can answer "should I cut?" exactly the way the
backtest would.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from data_sync_service.db.paper_trading import (
    MAX_HOLD_DAYS,
    STOP_LOSS_PCT,
    TRAILING_STOP_PCT,
)
from data_sync_service.db.watchlist_automation import list_registry

logger = logging.getLogger(__name__)


def _resolve_holding_ts(symbol: str) -> str | None:
    """CN/HK resolve via the paper engine; ETF by exchange code prefix."""
    from data_sync_service.service.paper_trading import _resolve_ts_code

    parsed = _resolve_ts_code(symbol)
    if parsed:
        return parsed[1]
    if symbol.startswith("ETF:"):
        code = symbol.removeprefix("ETF:")
        if len(code) == 6 and code.isdigit():
            return f"{code}.SH" if code.startswith(("5", "6")) else f"{code}.SZ"
    return None


def _lookup_stock_basic(ts_codes: list[str]) -> tuple[dict[str, str], dict[str, str]]:
    """{ts_code: name}, {ts_code: industry} — best-effort, empty on failure."""
    from data_sync_service.db.stock_basic import fetch_all

    names: dict[str, str] = {}
    industries: dict[str, str] = {}
    try:
        for r in fetch_all():
            ts = r.get("ts_code")
            if not ts:
                continue
            if ts in ts_codes:
                if r.get("name"):
                    names[ts] = str(r["name"])
                if r.get("industry"):
                    industries[ts] = str(r["industry"])
    except Exception as exc:  # noqa: BLE001
        logger.warning("portfolio health stock basic failed: %s", exc)
    return names, industries


def _holding_check(
    *,
    name: str,
    cost: float,
    entry_date: str,
    ts: str,
    trade_date: str,
    trailing_pct: float | None = None,
    stop_pct: float | None = None,
    max_hold: int | None = None,
) -> dict[str, Any]:
    """S-3 exit-condition check for one holding (same constants as paper).

    2026-08-10 (HK parallel line): per-market rule overrides — HK uses the
    HK line's trailing -12% (stop -5% / hold 60 unchanged).
    """
    from data_sync_service.db import get_connection

    stop = stop_pct if stop_pct is not None else STOP_LOSS_PCT
    trail = trailing_pct if trailing_pct is not None else TRAILING_STOP_PCT
    hold = max_hold if max_hold is not None else MAX_HOLD_DAYS
    out: dict[str, Any] = {
        "symbol": name,
        "costPrice": cost,
        "entryDate": entry_date,
        "tsCode": ts,
        "checkDate": trade_date,
    }
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT trade_date, high, close FROM daily
                WHERE ts_code = %s AND trade_date >= %s AND trade_date <= %s
                ORDER BY trade_date
                """,
                (ts, entry_date, trade_date),
            )
            bars = cur.fetchall()
    if not bars:
        out["status"] = "no-price-data"
        out["action"] = "HOLD"
        out["note"] = "无价格数据，继续持有观察"
        return out

    last_date, _h, last_close = bars[-1]
    last_close = float(last_close)
    # Trailing stop is evaluated on CLOSE prices (same as backtest engine).
    peak = max(float(b[2]) for b in bars if b[2] is not None)
    peak_date = max(bars, key=lambda b: float(b[2]))[0]
    pnl = (last_close - cost) / cost * 100.0
    drawdown = (last_close - peak) / peak * 100.0
    try:
        days = (date.fromisoformat(trade_date) - date.fromisoformat(entry_date)).days
    except ValueError:
        days = 0

    out["lastClose"] = last_close
    out["lastDate"] = str(last_date)
    out["peakPrice"] = peak
    out["peakDate"] = str(peak_date)
    out["pnlPct"] = round(pnl, 2)
    out["drawdownFromPeakPct"] = round(drawdown, 2)
    out["holdingDays"] = days
    out["stopLossLine"] = round(cost * (1 + stop / 100.0), 3)
    out["trailingLine"] = round(peak * (1 + trail / 100.0), 3)
    expire = date.fromisoformat(entry_date) + __import__("datetime").timedelta(days=hold)
    out["maxHoldDate"] = expire.isoformat()
    out["expireDate"] = expire.isoformat()

    reasons: list[str] = []
    if pnl <= stop:
        reasons.append(f"stop_loss（净亏{abs(pnl):.1f}% >= {abs(stop):.0f}% 阈值）")
    if drawdown <= trail:
        reasons.append(f"trailing_stop（峰值回撤{abs(drawdown):.1f}% >= {abs(trail):.0f}% 阈值）")
    if days >= hold:
        reasons.append(f"max_hold（已持{days}天 >= {hold} 天）")
    if reasons:
        out["action"] = "EXIT"
        out["reason"] = "；".join(reasons)
    else:
        out["action"] = "HOLD"
    return out


def _build_holdings_block(market: str, day: str) -> list[dict[str, Any]]:
    """Holdings for one market vs its S-3 exit rules.

    2026-08-10 (HK parallel line): CN uses the CN rules (trail -8); HK uses
    the HK line rules (trail -12). Holdings are split by symbol prefix.
    """
    from data_sync_service.service.paper_s3 import PYRAMID_TRIGGER_PCT

    trail = -12.0 if market == "HK" else None
    prefix = f"{market}:"
    holdings: list[dict[str, Any]] = []
    try:
        pyramid_syms = _pyramided_symbols()
        for r in list_registry():
            sym = str(r.get("symbol") or "").upper()
            if not sym.startswith(prefix):
                continue
            payload = r.get("payload") or {}
            pct = payload.get("positionPct", r.get("positionPct"))
            cost = payload.get("costPrice", r.get("costPrice"))
            entry = payload.get("entryDate", r.get("entryDate"))
            name = payload.get("name", r.get("name"))
            if not (isinstance(pct, (int, float)) and pct > 0 and cost and entry):
                continue
            ts = _resolve_holding_ts(sym)
            check = _holding_check(
                name=str(name or sym),
                cost=float(cost),
                entry_date=str(entry),
                ts=ts or "",
                trade_date=day,
                trailing_pct=trail,
            )
            check["symbol"] = sym
            check["name"] = str(name or "")
            check["positionPct"] = pct
            check["pyramidTriggerLine"] = round(
                float(cost) * (1 + PYRAMID_TRIGGER_PCT / 100.0), 3
            )
            check["pyramidAdded"] = sym in pyramid_syms
            if ts is None:
                check["action"] = "HOLD"
                check["note"] = "无法解析标的代码，人工核对"
            holdings.append(check)
    except Exception as exc:  # noqa: BLE001
        logger.warning("portfolio health holdings failed: %s", exc)
    return holdings


def _health_block(*, market: str, day: str) -> dict[str, Any]:
    """One market's S-3 health block (CN = current live system, HK = parallel line)."""
    from data_sync_service.service.backtest_engine import BacktestConfig, _load_regime_by_day
    from data_sync_service.service.market_sentiment import get_cn_sentiment, get_panic_cooldown
    from data_sync_service.service.paper_s3 import (
        PYRAMID_ADD_SCALE,
        PYRAMID_TRIGGER_PCT,
        build_s3_candidates,
    )

    if market == "HK":
        from data_sync_service.service.market_regime import get_hk_regime

        regime = None
        try:
            regime = str(get_hk_regime(as_of_date=day).get("regime") or "")
        except Exception:  # noqa: BLE001
            pass
        sentiment = None
        panic = get_panic_cooldown(days=10, cooldown_days=3, as_of_date=day)
        candidates: list[dict[str, Any]] = []
        try:
            candidates = build_s3_candidates(trade_date=day, market="HK")
        except Exception as exc:  # noqa: BLE001
            logger.warning("portfolio health HK candidates failed: %s", exc)
        rules: dict[str, Any] = {
            "entryScore": 65,
            "rsMin": 0.6,
            "stopLossPct": -5.0,
            "trailingStopPct": -12.0,
            "maxHoldDays": 60,
            "pyramidTriggerPct": PYRAMID_TRIGGER_PCT,
            "pyramidAddScale": PYRAMID_ADD_SCALE,
            "gates": "regime",
        }
    else:
        cfg = BacktestConfig(
            start_date=day, end_date=day,
            score_threshold=65.0, gates="full", rs_rank_min=0.5,
        )
        regime = None
        try:
            regime = _load_regime_by_day(cfg, [day]).get(day)
        except Exception:  # noqa: BLE001
            pass
        sentiment = None
        panic = None
        candidates: list[dict[str, Any]] = []
        try:
            items = get_cn_sentiment(days=1, as_of_date=day)["items"]
            sentiment = items[-1].get("riskMode") if items else None
            panic = get_panic_cooldown(days=10, cooldown_days=3, as_of_date=day)
            candidates = build_s3_candidates(trade_date=day)
        except Exception as exc:  # noqa: BLE001
            logger.warning("portfolio health s3 candidates failed: %s", exc)
        rules: dict[str, Any] = {
            "entryScore": 65,
            "rsMin": 0.5,
            "stopLossPct": STOP_LOSS_PCT,
            "trailingStopPct": TRAILING_STOP_PCT,
            "maxHoldDays": MAX_HOLD_DAYS,
            "pyramidTriggerPct": PYRAMID_TRIGGER_PCT,
            "pyramidAddScale": PYRAMID_ADD_SCALE,
        }

    try:
        if candidates:
            ts_codes = [c["ts_code"] for c in candidates]
            names = _lookup_stock_basic(ts_codes)[0]
            for c in candidates:
                c["name"] = names.get(c["ts_code"])
    except Exception as exc:  # noqa: BLE001
        logger.warning("portfolio health candidate names failed: %s", exc)

    return {
        "regime": regime,
        "sentiment": sentiment,
        "panicCooldown": panic,
        "s3Candidates": candidates,
        "s3Rules": rules,
        "holdings": _build_holdings_block(market=market, day=day),
    }


def build_portfolio_health(
    *,
    trade_date: str | None = None,
    markets: tuple[str, ...] = ("CN",),
) -> dict[str, Any]:
    """Full S-3-aligned health report for the real holdings.

    2026-08-10 (HK parallel line): ``markets`` selects which strategy lines to
    include. Top-level fields stay CN (backward compatible for the decision
    agent); ``hkHealth`` carries the HK line block (null when not requested).
    """
    from data_sync_service.db.paper_trading import today_iso

    day = trade_date or today_iso()
    blocks: dict[str, dict[str, Any]] = {}
    for m in markets:
        if m in ("CN", "HK"):
            blocks[m] = _health_block(market=m, day=day)

    cn = blocks.get("CN") or _health_block(market="CN", day=day)
    return {
        "tradeDate": day,
        **cn,
        "hkHealth": blocks.get("HK"),
    }


def _pyramided_symbols() -> set[str]:
    """Symbols that already have an open S-3 pyramid-add leg."""
    from data_sync_service.db.paper_trading import list_paper_trades

    out: set[str] = set()
    try:
        for r in list_paper_trades(status="open"):
            if r.get("source") == "S3" and "pyramid-add" in str(r.get("whyAtEntry") or ""):
                out.add(str(r.get("symbol") or ""))
    except Exception as exc:  # noqa: BLE001
        logger.warning("portfolio health pyramided lookup failed: %s", exc)
    return out
