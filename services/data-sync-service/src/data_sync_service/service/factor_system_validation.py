"""Repeatable walk-forward validation for the factor-system policy (2026-11-11).

This is the system's validation loop, NOT a new alpha search. Thresholds are
preregistered from H2k and must not be retuned here:
- K2: N=40, X=75, off below 50, t>=0, two-step 0 -> 10 -> 20 (20% cap),
  step gap >= 20 trades, immediate step-down, single-trade -8% breaker.
- Costs are already in ``h2f_trades.contrib``: 32.28bp/trade satellite cost;
  transfer/B-leg 5bp/side is documented and already inside M30 NAV.

Three arms:
1. ``gatedK2``: follow only when the prior 40-trade window is revived.
2. ``alwaysOnStarship``: 100% of the same satellite sleeve, for contrast.
3. ``waitInM30Baseline``: M30 NAV when the satellite is cold (the policy's
   baseline). If an M30 NAV is unavailable, the arm is reported as
   ``available: false`` with a clear caveat instead of inventing numbers.

No DB, no network, no broker/order calls. Frozen inputs only.
"""

from __future__ import annotations

import json
import math
from datetime import date
from pathlib import Path
from typing import Any

from data_sync_service.service import sgap_decay

SAT_COST_BP = 32.28
TRANSFER_COST_BP = 5.0
SATELLITE_CAP_PCT = 20

# K2 frozen parameters (H2k Sec 4 / sgap_decay).
REVIVAL_N = sgap_decay.REVIVAL_N
REVIVAL_X = sgap_decay.REVIVAL_X
REVIVAL_OFF = sgap_decay.REVIVAL_OFF
REVIVAL_STEP_GAP = sgap_decay.REVIVAL_STEP_GAP
SINGLE_TRADE_STOP_PCT = -8.0

WINDOW_ORDER: tuple[str, ...] = ("OOS2", "train", "valid", "holdout")
HOLDOUT_START = sgap_decay.WINDOWS["holdout"]["start"]


def _t_stat(xs: list[float]) -> float:
    n = len(xs)
    if n < 2:
        return 0.0
    mean = sum(xs) / n
    var = sum((x - mean) ** 2 for x in xs) / (n - 1)
    if not math.isfinite(var) or var <= 0:
        return 0.0
    return mean / (math.sqrt(var) / math.sqrt(n))


def normalize_trades(raw: Any) -> list[dict[str, Any]]:
    """Normalize frozen per-trade rows to ``{entry, contrib, ret}`` sorted by date.

    ``contrib`` is already net of the 32.28bp satellite cost. ``ret`` is
    reconstructed only so the single-trade -8% breaker can be applied:
    contrib = 25 * (ret - 0.003228) => ret = contrib / 25 + 0.003228.
    """
    if isinstance(raw, dict):
        raw = raw.get("trades", [])
    rows: list[dict[str, Any]] = []
    for item in raw or []:
        if not isinstance(item, dict):
            continue
        entry = item.get("entry") or item.get("entryDate")
        value = item.get("contrib", item.get("contribPct"))
        if value is None:
            continue
        try:
            contrib = float(value)
        except (TypeError, ValueError):
            continue
        if not entry or not math.isfinite(contrib):
            continue
        rows.append(
            {
                "entry": str(entry)[:10],
                "contrib": contrib,
                "ret": contrib / 25.0 + SAT_COST_BP / 10_000.0,
            }
        )
    rows.sort(key=lambda row: row["entry"])
    return rows


def k2_positions(
    contribs: list[float],
    *,
    trades: list[dict[str, Any]] | None = None,
    single_trade_stop_pct: float | None = SINGLE_TRADE_STOP_PCT,
) -> list[dict[str, Any]]:
    """Prior-window K2 state machine (T-1 style, no lookahead).

    The position applied to trade ``i`` is decided only from the previous
    N=40 trades ``[i-N, i-1]``. Warmup is 0. Step-down is immediate one level
    per window below 50. Step-up needs two consecutive revived windows and a
    >= 20-trade gap. A single-trade <= -8% breaker forces the next position
    to 0. Other FINAL breakers need daily NAV / same-day data and are listed
    as unavailable in the artifact rather than invented.
    """
    n = len(contribs)
    out: list[dict[str, Any]] = []
    pos = 0
    last_change = -(10**9)
    prev_ok = False
    for i in range(n):
        breaker: str | None = None
        if i > 0 and trades is not None and single_trade_stop_pct is not None:
            prev_ret = trades[i - 1].get("ret")
            if isinstance(prev_ret, (int, float)) and prev_ret * 100.0 <= single_trade_stop_pct:
                if pos != 0:
                    breaker = "single_trade_-8"
                pos = 0
                last_change = i
                prev_ok = False

        window = contribs[max(0, i - REVIVAL_N) : i]
        pct: float | None = None
        t_stat: float | None = None
        window_sum: float | None = None
        if len(window) == REVIVAL_N:
            window_sum = sum(window)
            pct = sgap_decay.percentile_of_sum(window_sum, REVIVAL_N)
            t_stat = _t_stat(window)
            ok = pct >= REVIVAL_X and t_stat >= 0
            if pct < REVIVAL_OFF:
                if pos == 20:
                    pos = 10
                    last_change = i
                elif pos == 10:
                    pos = 0
                    last_change = i
                prev_ok = False
            elif ok and prev_ok:
                if pos == 0 and (i - last_change) >= REVIVAL_STEP_GAP:
                    pos = 10
                    last_change = i
                elif pos == 10 and (i - last_change) >= REVIVAL_STEP_GAP:
                    pos = 20
                    last_change = i
            prev_ok = ok
        else:
            prev_ok = False
        out.append(
            {
                "i": i,
                "position": pos,
                "windowSum": window_sum,
                "percentile": pct,
                "t": t_stat,
                "breaker": breaker,
            }
        )
    return out


def _window_of(entry: str) -> str:
    for label in WINDOW_ORDER:
        win = sgap_decay.WINDOWS[label]
        if win["start"] <= entry <= win["end"]:
            return label
    if entry < HOLDOUT_START:
        return "longPreHoldout"
    return "other"


def assemble_arms(
    trades: list[dict[str, Any]],
    positions: list[dict[str, Any]],
) -> dict[str, Any]:
    """Combine trades + per-trade positions into the two satellite arms."""
    if len(trades) != len(positions):
        raise ValueError("trades and positions must have the same length")
    by_window_gated = {label: 0.0 for label in (*WINDOW_ORDER, "longPreHoldout", "other")}
    by_window_always = {label: 0.0 for label in (*WINDOW_ORDER, "longPreHoldout", "other")}
    gated_total = 0.0
    always_total = 0.0
    n_on10 = n_on20 = n_off = 0
    exposure_sum = 0.0
    changes: list[dict[str, Any]] = []
    prev_pos: int | None = None
    for trade, state in zip(trades, positions, strict=True):
        contrib = float(trade["contrib"])
        pos = int(state.get("position") or 0)
        pos = max(0, min(SATELLITE_CAP_PCT, pos))
        weight = pos / SATELLITE_CAP_PCT
        gated = contrib * weight
        label = _window_of(str(trade["entry"]))
        gated_total += gated
        always_total += contrib
        by_window_gated[label] += gated
        by_window_always[label] += contrib
        exposure_sum += weight
        if pos == 10:
            n_on10 += 1
        elif pos == 20:
            n_on20 += 1
        else:
            n_off += 1
        if prev_pos != pos:
            changes.append(
                {
                    "i": state.get("i"),
                    "date": trade["entry"],
                    "from": prev_pos,
                    "to": pos,
                    "percentile": state.get("percentile"),
                    "t": state.get("t"),
                    "breaker": state.get("breaker"),
                }
            )
            prev_pos = pos
    n = len(trades) or 1
    return {
        "gatedK2": {
            "unit": "satellite points",
            "total": round(gated_total, 4),
            "byWindow": {k: round(v, 4) for k, v in by_window_gated.items()},
            "exposurePct": round(exposure_sum / n * 100.0, 2),
            "counts": {"nOn10": n_on10, "nOn20": n_on20, "nOff": n_off},
            "positionChanges": changes,
        },
        "alwaysOnStarship": {
            "unit": "satellite points",
            "total": round(always_total, 4),
            "byWindow": {k: round(v, 4) for k, v in by_window_always.items()},
            "exposurePct": 100.0,
        },
    }


def _window_return(cal: list[Any], nav: list[Any]) -> float | None:
    if not cal or not nav or len(cal) != len(nav):
        return None
    try:
        first = float(nav[0])
        last = float(nav[-1])
    except (TypeError, ValueError):
        return None
    if first <= 0 or not math.isfinite(first) or not math.isfinite(last):
        return None
    return last / first - 1.0


def load_m30_nav(path: Path | None) -> dict[str, Any]:
    """Load an already-used M30 NAV artifact (H2d windows or dates/nav).

    Returns ``available: false`` with a caveat when missing; never fabricates
    a baseline. The frozen H2d artifact stores ``windows.<label>.cal`` and
    ``m30_h2`` (70% harbor + 30% B3, the product baseline).
    """
    if path is None:
        return {
            "available": False,
            "unit": "NAV %",
            "source": None,
            "byWindow": {},
            "caveat": "未提供 M30 NAV 文件；基线臂按 REPO/0% 处理，不能当作真实 M30 收益。",
        }
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return {
            "available": False,
            "unit": "NAV %",
            "source": str(path),
            "byWindow": {},
            "caveat": f"M30 NAV 不可读（{exc}）；基线臂按 REPO/0% 处理。",
        }
    windows = raw.get("windows") if isinstance(raw, dict) else None
    if isinstance(windows, dict):
        by_window: dict[str, float] = {}
        for label in (*WINDOW_ORDER, "long"):
            row = windows.get(label) or {}
            ret = _window_return(row.get("cal") or [], row.get("m30_h2") or [])
            if ret is not None:
                by_window[label] = round(ret * 100.0, 4)
        # Total across long + holdout: compounding the two frozen slices is
        # the closest honest full-span M30 number available.
        total: float | None = None
        if "long" in by_window and "holdout" in by_window:
            total = ((1 + by_window["long"] / 100) * (1 + by_window["holdout"] / 100) - 1) * 100
        return {
            "available": bool(by_window),
            "unit": "NAV %",
            "source": str(path),
            "byWindow": by_window,
            "total": round(total, 4) if total is not None else None,
            "caveat": (
                "M30 为 NAV %（70%港湾+30%B3，费用已含），不是卫星点；"
                "与卫星臂并列展示，不能直接相加。"
            ),
        }
    dates = raw.get("dates") if isinstance(raw, dict) else None
    nav = raw.get("nav") if isinstance(raw, dict) else None
    if isinstance(dates, list) and isinstance(nav, list) and len(dates) == len(nav):
        by_window = {}
        for label in WINDOW_ORDER:
            win = sgap_decay.WINDOWS[label]
            pairs = [
                (d, n)
                for d, n in zip(dates, nav, strict=False)
                if win["start"] <= str(d)[:10] <= win["end"]
            ]
            if pairs:
                ret = _window_return([p[0] for p in pairs], [p[1] for p in pairs])
                if ret is not None:
                    by_window[label] = round(ret * 100.0, 4)
        total_ret = _window_return(dates, nav)
        return {
            "available": bool(by_window),
            "unit": "NAV %",
            "source": str(path),
            "byWindow": by_window,
            "total": round(total_ret * 100.0, 4) if total_ret is not None else None,
            "caveat": "M30 为 NAV %，不是卫星点；与卫星臂并列展示，不能直接相加。",
        }
    return {
        "available": False,
        "unit": "NAV %",
        "source": str(path),
        "byWindow": {},
        "caveat": "M30 NAV 文件结构不认识；基线臂按 REPO/0% 处理。",
    }


def run_walk_forward(
    trades_raw: Any,
    *,
    m30_nav_path: Path | str | None = None,
    as_of: str | None = None,
) -> dict[str, Any]:
    """Run the three preregistered arms and return a JSON-ready artifact."""
    trades = normalize_trades(trades_raw)
    if not trades:
        return {
            "generatedAt": str(as_of or date.today().isoformat()),
            "available": False,
            "error": "no usable trades (entry + contrib)",
            "adviceOnly": True,
        }
    contribs = [t["contrib"] for t in trades]
    positions = k2_positions(contribs, trades=trades)
    arms = assemble_arms(trades, positions)
    # Diagnostic only: the same K2 state machine without the single-trade
    # -8% breaker. This is what makes the H2k report's 429pt gated figure
    # comparable; it is NOT an alternative policy or a retuned threshold.
    diag_positions = k2_positions(contribs, trades=trades, single_trade_stop_pct=None)
    diag_arms = assemble_arms(trades, diag_positions)
    baseline = load_m30_nav(Path(m30_nav_path) if m30_nav_path else None)
    n_on = sum(1 for p in positions if int(p.get("position") or 0) > 0)
    artifact: dict[str, Any] = {
        "generatedAt": str(as_of or date.today().isoformat()),
        "available": True,
        "adviceOnly": True,
        "preregistered": {
            "rule": "H2k K2",
            "N": REVIVAL_N,
            "X": REVIVAL_X,
            "offBelow": REVIVAL_OFF,
            "steps": [0, 10, 20],
            "maxWeightPct": SATELLITE_CAP_PCT,
            "stepGapTrades": REVIVAL_STEP_GAP,
            "twoStepConfirm": True,
            "immediateStepDown": True,
            "retuned": False,
        },
        "costs": {
            "satelliteBpPerTrade": SAT_COST_BP,
            "transferBpPerSide": TRANSFER_COST_BP,
            "note": "contrib is already net of 32.28bp; transfer cost is inside M30 NAV",
        },
        "input": {
            "nTrades": len(trades),
            "firstTrade": trades[0]["entry"],
            "lastTrade": trades[-1]["entry"],
            "source": "frozen h2f_trades / already-used blotter; no new download",
        },
        "arms": {
            **arms,
            "waitInM30Baseline": baseline,
        },
        "diagnostics": {
            "gatedK2NoBreaker": {
                "unit": "satellite points",
                "total": diag_arms["gatedK2"]["total"],
                "byWindow": diag_arms["gatedK2"]["byWindow"],
                "exposurePct": diag_arms["gatedK2"]["exposurePct"],
                "note": (
                    "same K2 state machine with the single-trade -8% breaker disabled; "
                    "diagnostic for H2k comparability, not a retuned arm"
                ),
            }
        },
        "breakers": {
            "applied": ["single_trade_-8"],
            "notAvailable": [
                "forward_60d_-10 (needs daily NAV)",
                "three_same_day_full_losses (needs intraday)",
                "weekly_-5_halve (needs daily NAV)",
                "data_link_alarm (needs live health feed)",
                "holdout_-25_stop (needs daily holdout NAV)",
            ],
        },
        "summary": {
            "nTrades": len(trades),
            "nWithSatelliteOn": n_on,
            "gatedVsAlwaysPoints": round(
                arms["gatedK2"]["total"] - arms["alwaysOnStarship"]["total"], 4
            ),
            "gatedExposurePct": arms["gatedK2"]["exposurePct"],
        },
        "caveats": [
            "卫星两臂为交易空间点数（同一25%槽口径），基线为 M30 NAV %，不可直接相加。",
            "K2 分位沿用 H2k 的 F long R1 正态近似（±10pt），阈值冻结未调。",
            "未混入官方 COMPARE 版本 / catalog 窗口；只复用 h2f_trades 与已用 NAV。",
            "这是系统的验证循环，不是新 alpha 搜索；未改任何 live 参数或下单。",
        ],
    }
    return artifact


def render_validation_markdown(result: dict[str, Any]) -> str:
    """Short Chinese-readable report for the JSON artifact."""
    if not result.get("available"):
        return (
            "# 因子系统 walk-forward 验证\n\n"
            f"- 不可用: {result.get('error')}\n"
            "- advice only；未改任何 live 设置或下单。\n"
        )
    arms = result.get("arms") or {}
    gated = arms.get("gatedK2") or {}
    always = arms.get("alwaysOnStarship") or {}
    base = arms.get("waitInM30Baseline") or {}
    pre = result.get("preregistered") or {}
    inp = result.get("input") or {}
    lines = [
        "# 因子系统 walk-forward 验证（2026-11-11）",
        "",
        "> 这是系统的验证循环，不是新的 alpha 研究。阈值预注册冻结（"
        f"K2 N={pre.get('N')} / X={pre.get('X')} / off<{pre.get('offBelow')} / "
        f"两步 {pre.get('steps')} / 上限 {pre.get('maxWeightPct')}%），跑后未调。",
        "",
        "## 输入与成本",
        "",
        f"- 交易: {inp.get('nTrades')} 笔，{inp.get('firstTrade')} .. {inp.get('lastTrade')}"
        "（冻结 h2f_trades，未新下载）",
        f"- 成本: 卫星 {SAT_COST_BP}bp/笔（contrib 已扣）；转移 {TRANSFER_COST_BP}bp/边"
        "（已含在 M30 NAV）",
        "",
        "## 三臂结果（卫星臂=交易空间点数；基线=NAV %）",
        "",
        "| 臂 | 合计 | OOS2 | train | valid | holdout | 暴露 |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for name, arm in (("K2 gated", gated), ("Always-on 星舰", always)):
        bw = arm.get("byWindow") or {}
        lines.append(
            f"| {name} | {arm.get('total')} | {bw.get('OOS2')} | {bw.get('train')} | "
            f"{bw.get('valid')} | {bw.get('holdout')} | {arm.get('exposurePct')}% |"
        )
    if base.get("available"):
        bw = base.get("byWindow") or {}
        lines.append(
            f"| 等待 M30 基线 | {base.get('total')} | {bw.get('OOS2')} | {bw.get('train')} | "
            f"{bw.get('valid')} | {bw.get('holdout')} | 100% |"
        )
    else:
        lines.append("| 等待 M30 基线 | 不可用 | — | — | — | — | — |")
    lines.extend(
        [
            "",
            f"- K2 gated vs 一直持有：{result.get('summary', {}).get('gatedVsAlwaysPoints')} 点"
            "（负数=保险税，正数=避险收益）。",
            f"- K2 有仓交易数：{result.get('summary', {}).get('nWithSatelliteOn')} / "
            f"{result.get('summary', {}).get('nTrades')}。",
            "",
        ]
    )
    diag = (result.get("diagnostics") or {}).get("gatedK2NoBreaker") or {}
    if diag:
        lines.append(
            f"- 诊断（不是第四臂）：去掉单笔 -8% 熔断后 gated={diag.get('total')} 点、"
            f"暴露 {diag.get('exposurePct')}%，更接近 H2k 报告的 429；差异来自 T-1 窗口"
            "与步进细节，未调阈值。"
        )
        lines.append("")
    lines.extend(
        [
            "## 基线臂 caveat",
            "",
            f"- {base.get('caveat')}",
            "",
            "## 未实现的熔断（诚实声明）",
            "",
            "- " + "；".join(result.get("breakers", {}).get("notAvailable") or []),
            "",
            "## 结论",
            "",
            "- 系统政策仍是：冷库/观察 -> 母港 M30 等待；复活且实盘三前置满足才谈跟随。",
            "- 本次没有改 TRAIL_PCT (8%)、cooldown k=0、任何权重、任何订单或用户模式。",
            "- 冻结的交易流水中断（最后交易 "
            f"{inp.get('lastTrade')}），灯不会自行变化；这是下一刀要解决的新鲜度问题。",
            "",
        ]
    )
    return "\n".join(lines)
