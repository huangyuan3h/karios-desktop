"""Core-holding audit: did the USER's operations follow the strategy rules?

The S-3 line trades are executed automatically (paper_s3), but the core
holdings (registry watchlist rows like CN:300628 / ETF:513110) are managed
manually. This module answers "was my trade on the rule, or off it?" for
every user_trades row.

Legs (OPT-149): each journal row carries ``leg`` ('s3' | 'sat').
- S-3 legs use the backtest-engine rules below (pyramid / stop / panic gate).
- Satellite legs (twin-star 12.5% slots) are reconciled against the paper
  satellite book (paper_trades source='twin_star'), NOT indicator rules:
  BUY must match a paper signal (same symbol+day, which implies the R-wide
  gate was open); SELL must land on the body=3 exit-due session
  (entry = day 1); ADD is always off-rule (no pyramiding on satellites).

  - pyramid ADD: close >= cost * (1 + pyramid_trigger_pct) on the PREVIOUS
    close and not yet added -> add half sleeve (regime-independent, max 1)
  - SELL: stop-loss line (cost * (1 + stop_loss_pct)) / trailing line /
    max-hold expiry (max_hold_days) — selling below the stop is a stop
    execution; selling above it early is a discretionary move
  - BUY/ADD in a panic-cooldown window: the CN gate blocks new A-share
    entries; the Nasdaq sleeve is NOT gated by the CN regime (2026-08-21)

Verdicts: ok / warn / violation. Violations surface as a red list.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

PYRAMID_TRIGGER_PCT = 2.5
PYRAMID_ADD_SCALE = 0.5
STOP_LOSS_PCT = -5.0
MAX_HOLD_DAYS = 60
PANIC_COOLDOWN_DAYS = 2


def _load_user_trades(symbol: str) -> list[dict[str, Any]]:
    from psycopg.rows import dict_row

    from data_sync_service.db import get_connection

    try:
        with get_connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                try:
                    cur.execute(
                        """SELECT id, symbol, side, trade_date, price, position_pct,
                                  cost_basis, entry_date, pnl_pct, holding_days, source,
                                  market, note, leg, created_at
                           FROM user_trades WHERE symbol = %s ORDER BY trade_date, created_at""",
                        (symbol,),
                    )
                except Exception:
                    # Pre-0041 DBs have no leg column: fall back, all legs read as s3.
                    conn.rollback()
                    cur.execute(
                        """SELECT id, symbol, side, trade_date, price, position_pct,
                                  cost_basis, entry_date, pnl_pct, holding_days, source,
                                  market, note, created_at
                           FROM user_trades WHERE symbol = %s ORDER BY trade_date, created_at""",
                        (symbol,),
                    )
                rows = list(cur.fetchall())
                for r in rows:
                    r.setdefault("leg", "s3")
                return rows
    except Exception as exc:  # noqa: BLE001
        logger.warning("core_holding_audit: user_trades load failed for %s (%s)", symbol, exc)
        return []


def _market_gate(as_of: str) -> dict[str, Any]:
    """CN regime + panic state for the audit day (used for BUY/ADD checks)."""
    from data_sync_service.service.market_regime import get_market_regime
    from data_sync_service.service.market_sentiment import get_panic_cooldown

    try:
        regime = str(get_market_regime(as_of_date=as_of).get("regime") or "")
    except Exception:  # noqa: BLE001
        regime = ""
    try:
        panic = get_panic_cooldown(days=10, cooldown_days=PANIC_COOLDOWN_DAYS, as_of_date=as_of)
    except Exception:  # noqa: BLE001
        panic = {}
    return {
        "regime": regime,
        "panicActive": bool(panic.get("active")),
        "gateOpen": regime in ("Strong", "Diverging") and not panic.get("active"),
    }


def _judge_add(op: dict[str, Any], state: dict[str, Any], gate: dict[str, Any]) -> dict[str, Any]:
    """ADD verdict vs the pyramid rule, judged against the PRE-trade state
    (reverse-replayed). Pyramiding is regime-independent — a panic window does
    NOT block an A-share ADD (engine L2342-2357 / paper_s3._pyramid_adds)."""
    sym = op["symbol"]
    is_etf = str(sym).startswith("ETF:")
    price = float(op["price"] or 0)
    add_pct = float(op["position_pct"] or 0)
    cost = float(state.get("cost") or 0)
    pct_before = float(state.get("pct") or 0)

    if is_etf:
        ma = state.get("ma200")
        above = bool(state.get("aboveMa200"))
        if ma:
            verdict = "ok" if above else "warn"
            detail = (
                f"套筒管理（ETF 不适用金字塔）：加仓价 {price:.3f} vs 200dMA {float(ma):.3f} "
                f"——{'站上均线，符合' if above else '跌破均线，本应等待'}；"
                f"CN 闸门对美股 ETF 无效（2026-08-21 拍板）"
            )
        else:
            verdict, detail = "ok", "套筒管理（ETF 不适用金字塔）；CN 闸门对美股 ETF 无效"
        return {"date": op["trade_date"], "side": op["side"], "price": price,
                "positionPct": add_pct, "verdict": verdict, "rule": "sleeve", "detail": detail}

    trigger = cost * (1 + PYRAMID_TRIGGER_PCT / 100.0)
    if cost > 0 and pct_before > 0:
        if price >= trigger:
            half = pct_before * PYRAMID_ADD_SCALE
            ratio = add_pct / half if half > 0 else 0.0
            if ratio <= 1.5:
                verdict, rule, detail = "ok", "pyramid", (
                    f"收盘触发金字塔（{price:.2f} ≥ 触发线 {trigger:.3f} = 成本 {cost:.3f}×1.025），"
                    f"加仓 {add_pct:.1f}% ≈ 半仓 {half:.1f}%"
                )
            else:
                verdict, rule, detail = "warn", "pyramid", (
                    f"金字塔触发（{price:.2f} ≥ {trigger:.3f}），但幅度 {add_pct:.1f}% "
                    f"为半仓 {half:.1f}% 的 {ratio:.1f} 倍，超量加仓"
                )
        else:
            verdict, rule, detail = "warn", "pyramid", (
                f"加仓价 {price:.2f} < 金字塔触发线 {trigger:.3f}（成本 {cost:.3f}×1.025）"
                f"——触发线内提前加仓"
            )
    else:
        verdict, rule, detail = "ok", "pyramid", "成本/仓位缺失，跳过金字塔核对"

    return {"date": op["trade_date"], "side": op["side"], "price": price,
            "positionPct": add_pct, "verdict": verdict, "rule": rule, "detail": detail}


def _judge_sell(op: dict[str, Any], state: dict[str, Any], gate: dict[str, Any]) -> dict[str, Any]:
    """SELL verdict: stop execution vs discretionary (strategy-visible) exit.
    The stop line is the engine-fixed -5% on the PRE-trade cost; trailing /
    max-hold are informational (the engine's fixed -8% peak line / 60 days)."""
    price = float(op["price"] or 0)
    cost = float(state.get("cost") or 0)
    stop = cost * (1 + STOP_LOSS_PCT / 100.0) if cost > 0 else None

    if stop is not None and price <= stop:
        verdict, rule, detail = "ok", "stop", (
            f"卖价 {price:.3f} ≤ 止损线 {stop:.3f}（成本 {cost:.3f}×0.95）——止损执行"
        )
    elif stop is not None and price > stop:
        verdict, rule, detail = "warn", "discretionary", (
            f"卖价 {price:.3f} 高于止损线 {stop:.3f}（成本 {cost:.3f}×0.95）"
            f"——策略外主动退出（引擎对核心仓无主动卖出规则）"
        )
    elif gate["panicActive"]:
        verdict, rule, detail = "ok", "panic_de_risk", (
            f"恐慌期（{gate['regime']}）卖出——符合弱市降仓策略精神"
        )
    else:
        verdict, rule, detail = "ok", "discretionary", "策略外操作（无对应规则可核）"

    return {"date": op["trade_date"], "side": op["side"], "price": price,
            "positionPct": float(op["position_pct"] or 0), "verdict": verdict,
            "rule": rule, "detail": detail}


def _judge_open(op: dict[str, Any], holding: dict[str, Any], gate: dict[str, Any]) -> dict[str, Any]:
    """BUY verdict: A-share entries must wait for an open CN gate."""
    sym = op["symbol"]
    is_etf = str(sym).startswith("ETF:")
    if is_etf:
        verdict, rule, detail = "ok", "sleeve", "ETF 开仓——CN 闸门无效（2026-08-21 拍板），以 200dMA 为闸门"
    elif gate["gateOpen"]:
        verdict, rule, detail = "ok", "regime", f"CN 闸门开（{gate['regime']}）——符合"
    else:
        verdict, rule, detail = "warn", "regime", (
            f"CN 闸门关（{gate['regime']}·panic={gate['panicActive']}）——恐慌期开 A股新仓"
        )
    return {"date": op["trade_date"], "side": op["side"], "price": float(op["price"] or 0),
            "positionPct": float(op["position_pct"] or 0), "verdict": verdict,
            "rule": rule, "detail": detail}


def _load_sat_book() -> set[tuple[str, str]]:
    """Paper satellite fills as (symbol, entry_date). Read-only reconcile set.

    A paper fill exists only when the R-wide gate was open and the strict
    C1 candidate filled, so a match certifies the whole satellite entry path.
    """
    from data_sync_service.db import get_connection

    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT symbol, entry_date FROM paper_trades WHERE source = 'twin_star'"
                )
                return {(str(s), str(d)) for s, d in cur.fetchall() if s and d}
    except Exception as exc:  # noqa: BLE001
        logger.warning("core_holding_audit: sat book load failed (%s)", exc)
        return set()


def _sat_exit_due(entry_date: str) -> str | None:
    """Body=3 exit-due session (entry day counts as day 1). None if unknown."""
    from datetime import date as _date
    from datetime import timedelta as _td

    from data_sync_service.db.trade_calendar import get_open_dates

    try:
        d0 = _date.fromisoformat(str(entry_date)[:10])
        sessions = get_open_dates("SSE", d0, d0 + _td(days=10))
        if len(sessions) >= 3:
            return sessions[2].isoformat() if hasattr(sessions[2], "isoformat") else str(sessions[2])
        return None
    except Exception:  # noqa: BLE001
        return None


def _judge_sat_open(op: dict[str, Any], sat_book: set[tuple[str, str]]) -> dict[str, Any]:
    """Satellite BUY verdict.

    Following the paper signal is the default path (same symbol+day implies the
    R-wide gate was open). A satellite buy that does NOT mirror a paper signal
    is a **self-directed pick** and is treated as compliant ("ok"), not a warn —
    the satellite book is user-managed and round-lot sizing never lands exactly
    on 12.5%. Both paths read ok; the detail distinguishes them (2026-09-10).
    """
    sym = str(op["symbol"])
    day = str(op["trade_date"])
    if (sym, day) in sat_book:
        verdict, rule, detail = "ok", "sat_signal", (
            "跟随卫星信号（paper 同票同日，R-wide 开闸日，body=3，第 3 日 14:30 卖）"
        )
    else:
        verdict, rule, detail = "ok", "sat_manual", (
            f"主动自选卫星单（paper {day} 无 {sym} 信号）——按自选处理，非偏离"
        )
    return {"date": op["trade_date"], "side": op["side"], "price": float(op["price"] or 0),
            "positionPct": float(op["position_pct"] or 0), "verdict": verdict,
            "rule": rule, "detail": detail}


def _judge_sat_sell(op: dict[str, Any], open_entry: str | None) -> dict[str, Any]:
    """Satellite SELL: must land on the body=3 exit-due session."""
    day = str(op["trade_date"])
    base = {"date": op["trade_date"], "side": op["side"], "price": float(op["price"] or 0),
            "positionPct": float(op["position_pct"] or 0)}
    if not open_entry:
        return {**base, "verdict": "warn", "rule": "sat_exit",
                "detail": "卫星卖出无对应开仓记录——先补开仓腿"}
    due = _sat_exit_due(open_entry)
    if due is None:
        return {**base, "verdict": "ok", "rule": "sat_exit",
                "detail": f"卫星卖出（{open_entry} 开仓，日历缺数未核到期日）"}
    if day == due:
        return {**base, "verdict": "ok", "rule": "sat_exit",
                "detail": f"第 3 日到期卖（{open_entry}→{due}，body=3）——符合"}
    if day < due:
        return {**base, "verdict": "warn", "rule": "sat_exit",
                "detail": f"提前卖（到期 {due}，body 不足 3 日）——锁死减亏段且打乱周转"}
    return {**base, "verdict": "warn", "rule": "sat_exit",
            "detail": f"延迟卖（到期 {due}，多占槽位）——赢家多拿一天已三窗拒收"}


def _judge_sat_add(op: dict[str, Any]) -> dict[str, Any]:
    return {"date": op["trade_date"], "side": op["side"], "price": float(op["price"] or 0),
            "positionPct": float(op["position_pct"] or 0), "verdict": "warn",
            "rule": "sat_exit", "detail": "卫星无加仓规则（body=3 拿满即卖，加仓=占槽）"}


def _etf_trend_state(symbol: str) -> dict[str, Any]:
    """MA200 / above flags for a sleeve ETF (from the production state machine)."""
    from data_sync_service.service.third_asset_sleeve import (
        _etf_market_data,
        _etf_symbol_to_ts,
    )

    try:
        md = _etf_market_data(_etf_symbol_to_ts(symbol))
        if not md.get("ok"):
            return {}
        return {"ma200": md.get("ma200"), "aboveMa200": bool(md.get("above_ma200"))}
    except Exception:  # noqa: BLE001
        return {}


def _replay_ops(
    ops: list[dict[str, Any]],
    state: dict[str, Any],
    gate: dict[str, Any],
    sat_book: set[tuple[str, str]] | None = None,
) -> list[dict[str, Any]]:
    """Judge each op against its PRE-trade state by reverse-replaying from the
    current (cost, pct) backwards — so the 8/21 ADD is checked against the
    8/20 cost (39.90), not today's blended one.

    Satellite legs (leg='sat') skip the S-3 rulebook: BUY reconciles against
    the paper satellite book, SELL against the body=3 exit-due session.
    """
    book = sat_book if sat_book is not None else set()
    # Chronological pre-pass: pair each SELL/ADD with its open entry date.
    open_entry: str | None = None
    entry_for: dict[int, str | None] = {}
    for idx, op in enumerate(ops):
        leg = str(op.get("leg") or "s3")
        if leg != "sat":
            continue
        if op["side"] == "BUY" and open_entry is None:
            open_entry = str(op["trade_date"])
        elif op["side"] == "SELL":
            entry_for[idx] = open_entry
            open_entry = None
    cur_cost = float(state.get("costPrice") or 0)
    cur_pct = float(state.get("positionPct") or 0)
    verdicts: list[dict[str, Any]] = []
    for rev_idx, op in enumerate(reversed(ops)):
        idx = len(ops) - 1 - rev_idx
        side = op["side"]
        price = float(op["price"] or 0)
        op_pct = float(op["position_pct"] or 0)
        leg = str(op.get("leg") or "s3")

        if leg == "sat":
            if side == "BUY":
                verdicts.append(_judge_sat_open(op, book))
            elif side == "SELL":
                verdicts.append(_judge_sat_sell(op, entry_for.get(idx)))
            else:
                verdicts.append(_judge_sat_add(op))
            continue

        if side == "ADD":
            # Roll BACK first: judge against the pre-trade blended state.
            prev_pct = cur_pct - op_pct
            prev_cost = (
                (cur_cost * cur_pct - price * op_pct) / prev_pct if prev_pct > 0 else cur_cost
            )
            pre = {"cost": prev_cost, "pct": prev_pct}
            verdicts.append(_judge_add(op, pre, gate))
            cur_pct, cur_cost = prev_pct, prev_cost
        elif side == "SELL":
            prev_pct = cur_pct + op_pct  # before the sell we held more
            pre = {"cost": cur_cost, "pct": prev_pct}
            verdicts.append(_judge_sell(op, pre, gate))
            cur_pct = prev_pct
        elif side == "BUY":
            prev_pct = cur_pct - op_pct  # before the open we held less
            pre = {"cost": 0.0, "pct": prev_pct}
            verdicts.append(_judge_open(op, pre, gate))
            cur_pct = prev_pct
            cur_cost = 0.0  # a fresh open has no prior cost basis to roll to

    verdicts.reverse()
    # OPT-150: carry the journal row id so the UI can patch a misfiled leg.
    for v, op in zip(verdicts, ops, strict=True):
        v.setdefault("id", op.get("id"))
    return verdicts


def audit_core_holdings(*, day: str) -> dict[str, Any]:
    """Audit the manual core book against the strategy rules for ``day``.

    Returns per-holding state (from portfolio_health, so the lines the engine
    tracks match) + per-operation verdicts + a flat violation list.
    """
    from data_sync_service.db.watchlist_automation import list_registry
    from data_sync_service.service.portfolio_health import build_portfolio_health

    gate = _market_gate(day)
    health = build_portfolio_health(trade_date=day)
    health_by_symbol = {h.get("symbol"): h for h in (health.get("holdings") or [])}
    etf_trend_cache: dict[str, dict[str, Any]] = {}
    sat_book = _load_sat_book()

    holdings_out: list[dict[str, Any]] = []
    violations: list[dict[str, Any]] = []
    for row in list_registry():
        sym = str(row.get("symbol") or "")
        if not sym or sym.startswith("CN:99"):
            continue
        state = health_by_symbol.get(sym) or {}
        ops_raw = _load_user_trades(sym)
        if not ops_raw:
            continue  # no manual trades -> nothing to audit
        pct = state.get("positionPct") or row.get("positionPct")
        try:
            has_position = float(pct or 0) > 0
        except (TypeError, ValueError):
            has_position = False
        if not has_position and not any(o["side"] == "SELL" for o in ops_raw):
            continue  # pure watchlist row, never traded / never held

        if str(sym).startswith("ETF:"):
            etf_trend_cache[sym] = _etf_trend_state(sym)
        ops = _replay_ops(ops_raw, state, gate, sat_book)
        # Holding leg = leg of the most recent BUY (sat and s3 never blend rules).
        leg = "s3"
        for o in reversed(ops_raw):
            if o["side"] == "BUY":
                leg = str(o.get("leg") or "s3")
                break
        for o in ops:
            if o["side"] == "ADD" and str(sym).startswith("ETF:"):
                trend = etf_trend_cache[sym]
                o["detail"] = o["detail"].split("——")[0]
                if trend.get("ma200"):
                    above = bool(trend["aboveMa200"])
                    o["verdict"] = "ok" if above else "warn"
                    o["detail"] = (
                        f"套筒管理（ETF 不适用金字塔）：加仓价 {o['price']:.3f} vs "
                        f"200dMA {float(trend['ma200']):.3f} ——{'站上均线，符合' if above else '跌破均线，本应等待'}；"
                        f"CN 闸门对美股 ETF 无效（2026-08-21 拍板）"
                    )

        for op in ops:
            if op["verdict"] == "violation":
                violations.append({"symbol": sym, **op})
            elif op["verdict"] == "warn":
                violations.append({"symbol": sym, "severity": "warn", **op})

        holdings_out.append(
            {
                "symbol": sym,
                "name": state.get("name") or row.get("name"),
                "leg": leg,
                "positionPct": state.get("positionPct") or row.get("positionPct"),
                "costPrice": state.get("costPrice") or row.get("costPrice"),
                "lastClose": state.get("lastClose"),
                "pnlPct": state.get("pnlPct"),
                "stopLossLine": state.get("stopLossLine"),
                "trailingLine": state.get("trailingLine"),
                "maxHoldDate": state.get("maxHoldDate"),
                # Satellite legs have no pyramid rule: hide the S-3 line (OPT-149).
                "pyramidTriggerLine": None if leg == "sat" else state.get("pyramidTriggerLine"),
                "pyramidAdded": bool(state.get("pyramidAdded")) and leg != "sat",
                "ops": ops,
            }
        )

    return {
        "day": day,
        "gate": gate,
        "holdings": holdings_out,
        "violations": violations,
        "counts": {
            "ok": sum(1 for h in holdings_out for o in h["ops"] if o["verdict"] == "ok"),
            "warn": sum(1 for h in holdings_out for o in h["ops"] if o["verdict"] == "warn"),
            "violation": sum(1 for h in holdings_out for o in h["ops"] if o["verdict"] == "violation"),
        },
    }