"""H2 Harbor shadow ledger (Live-track, display-only).

Daily paper shadow account for the H-HARBOR-H2 validation line: same S-3
engine returns and idle fractions as the canonical Harbor Timeline, with the
idle-cash parking leg switched to H2 hysteresis
(``parking_sleeve.blend_harbor_h2_timeline``).

Boundary (read OPT-215): this ledger NEVER touches Live. No writes to
``paper_trades`` / ``user_trades`` / watchlist registry; no orders, no fills,
no recon against the real book. It is a NAV shadow account whose only
consumers are the daily ``harbor_h2_shadow`` scheduler job (append-only),
``GET /api/backtest/harbor-h2-shadow/latest`` and the compare-tab follow
card. Harbor Live keeps canonical ``harbor.parking_replay``.

Paper window starts at the first successful run (``inception``): NAVs are
re-based to 1.0 there, so MDD monitoring and the rollback line measure the
paper track only — never the backtest history (already visible in Timeline).
"""

from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

REPORT_NAME = "harbor_h2_shadow_latest.json"
# Trailing-year rebuild window for the daily job (matches Timeline default).
WINDOW_DAYS = 365
# Paper rows kept in the report (display history; monitoring uses all of it).
HISTORY_KEEP = 120

# Rollback line (H-HARBOR-H2 paper gate, aligned with
# docs/backtests/stable/harbor-h2-parking-2026-09-16.md §3: "paper long-MDD
# 比同期港湾深超 2pt 即回滚"): H2 trails Live by >2pt cumulative, or its
# paper-window drawdown runs >2pt deeper than Live's. WATCH is halfway.
# Statuses are recomputed fresh every day — the ledger history preserves past
# triggers.
SPREAD_WATCH_PT = -1.0
SPREAD_ROLLBACK_PT = -2.0
MDD_GAP_WATCH_PT = 1.0
MDD_GAP_ROLLBACK_PT = 2.0

STATUS_TRACKING = "tracking"
STATUS_WATCH = "watch"
STATUS_ROLLBACK = "rollback"

ACTION_LABEL = {
    "hold": "持有不动",
    "enter": "建仓",
    "rotate": "换仓",
    "trail_exit": "回撤清仓",
}
STATUS_LABEL = {
    STATUS_TRACKING: "跟踪中",
    STATUS_WATCH: "观察",
    STATUS_ROLLBACK: "已回滚",
}


def report_path() -> Path:
    return Path(__file__).resolve().parents[3] / "data" / "backtest_reports" / REPORT_NAME


def load_shadow_report() -> dict[str, Any] | None:
    """Return the persisted shadow report, or None (never run / unreadable)."""
    try:
        data = json.loads(report_path().read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict) or not data.get("ok") or not isinstance(data.get("rows"), list):
        return None
    return data


def save_shadow_report(report: dict[str, Any]) -> Path:
    """Atomically persist the shadow report (tmp + rename, never partial)."""
    path = report_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(report, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)
    return path


def status_for(spread_pt: float, mdd_gap_pt: float) -> str:
    """Paper-gate status from cumulative spread (H2-Live, pt) and MDD gap (pt).

    Positive ``mdd_gap_pt`` means H2's drawdown runs deeper than Live's.
    """
    if spread_pt <= SPREAD_ROLLBACK_PT or mdd_gap_pt >= MDD_GAP_ROLLBACK_PT:
        return STATUS_ROLLBACK
    if spread_pt <= SPREAD_WATCH_PT or mdd_gap_pt >= MDD_GAP_WATCH_PT:
        return STATUS_WATCH
    return STATUS_TRACKING


def extract_day_series(
    harbor_result: dict[str, Any], h2_result: dict[str, Any]
) -> list[dict[str, Any]]:
    """Align Harbor (Live) and H2 rows by date into one daily series.

    Day ``t`` carries each leg's window-cumulative NAV; the caller derives the
    day return from consecutive points and compounds it into the paper NAVs.
    """
    h_rows = {str(r.get("date")): r for r in (harbor_result.get("rows") or []) if r.get("date")}
    b_rows = {str(r.get("date")): r for r in (h2_result.get("rows") or []) if r.get("date")}
    out: list[dict[str, Any]] = []
    for day in sorted(set(h_rows) & set(b_rows)):
        h, b = h_rows[day], b_rows[day]
        try:
            nav_live = float(h.get("navSingle") or 0.0)
            nav_h2 = float(b.get("navSingle") or 0.0)
        except (TypeError, ValueError):
            continue
        if nav_live <= 0 or nav_h2 <= 0:
            continue
        out.append(
            {
                "date": day,
                "nav_live_win": nav_live,
                "nav_h2_win": nav_h2,
                "pick_live": str(h.get("pick") or "—"),
                "pick_h2": str(b.get("pick") or "—"),
                "idle_pct": float(h.get("idlePct") or 0.0),
                "trail_h2": bool(b.get("parkedTrail")),
                "sides_h2": int(b.get("parkedSides") or 0),
            }
        )
    return out


def _action_for(prev_pick: str | None, day: dict[str, Any]) -> str:
    if day["trail_h2"]:
        return "trail_exit"
    if prev_pick is None or prev_pick != day["pick_h2"]:
        return "enter" if (prev_pick in (None, "REPO")) else "rotate"
    return "hold"


def build_shadow_report(
    end: str,
    harbor_result: dict[str, Any],
    h2_result: dict[str, Any],
    prev: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Append missing paper days (up to ``end``) onto the previous ledger.

    First run seeds a single inception row (NAVs = 1.0, no fake history).
    Returns the full report dict (also saved by the caller).
    """
    series = extract_day_series(harbor_result, h2_result)
    if not series:
        raise ValueError("empty H2/Live day series — cannot update shadow ledger")
    prev_rows: list[dict[str, Any]] = list((prev or {}).get("rows") or [])
    last_date = str(prev_rows[-1]["date"]) if prev_rows else None
    if last_date is not None and last_date >= end:
        out = dict(prev or {})
        out["note"] = f"up to date ({last_date})"
        return out

    by_date = {s["date"]: i for i, s in enumerate(series)}
    if end not in by_date:
        raise ValueError(f"day series does not cover end={end}")
    new_dates = [
        d
        for d in sorted(by_date)
        if (last_date is None and d == end) or (last_date is not None and last_date < d <= end)
    ]
    if not new_dates:
        raise ValueError(f"nothing to append (last={last_date}, end={end})")

    if prev_rows:
        last = prev_rows[-1]
        nav_live = float(last["navLive"])
        nav_h2 = float(last["navH2"])
        peak_live = float(last.get("peakLive") or nav_live)
        peak_h2 = float(last.get("peakH2") or nav_h2)
        prev_pick = str(last.get("pickH2") or "")
        inception = str((prev or {}).get("inception") or prev_rows[0]["date"])
    else:
        nav_live = nav_h2 = peak_live = peak_h2 = 1.0
        prev_pick = ""
        inception = end

    appended: list[dict[str, Any]] = []
    first_ledger_row = not prev_rows
    for day in new_dates:
        i = by_date[day]
        cur = series[i]
        if i > 0 and not first_ledger_row:
            p = series[i - 1]
            r_live = cur["nav_live_win"] / p["nav_live_win"] - 1.0 if p["nav_live_win"] else 0.0
            r_h2 = cur["nav_h2_win"] / p["nav_h2_win"] - 1.0 if p["nav_h2_win"] else 0.0
        else:
            # Inception (fresh ledger): rebase to 1.0 — the window's return up
            # to this day belongs to the backtest history, not the paper track.
            r_live = r_h2 = 0.0
        nav_live *= 1.0 + r_live
        nav_h2 *= 1.0 + r_h2
        peak_live = max(peak_live, nav_live)
        peak_h2 = max(peak_h2, nav_h2)
        dd_live = (peak_live - nav_live) / peak_live * 100.0 if peak_live > 0 else 0.0
        dd_h2 = (peak_h2 - nav_h2) / peak_h2 * 100.0 if peak_h2 > 0 else 0.0
        spread_pt = (nav_h2 - nav_live) * 100.0
        mdd_gap_pt = dd_h2 - dd_live
        action = _action_for(prev_pick or None, cur)
        prev_pick = cur["pick_h2"]
        appended.append(
            {
                "date": day,
                "navLive": round(nav_live, 6),
                "navH2": round(nav_h2, 6),
                "peakLive": round(peak_live, 6),
                "peakH2": round(peak_h2, 6),
                "dayLivePct": round(r_live * 100, 3),
                "dayH2Pct": round(r_h2 * 100, 3),
                "spreadPt": round(spread_pt, 2),
                "ddLivePct": round(dd_live, 2),
                "ddH2Pct": round(dd_h2, 2),
                "mddGapPt": round(mdd_gap_pt, 2),
                "pickLive": cur["pick_live"],
                "pickH2": cur["pick_h2"],
                "actionH2": action,
                "idlePct": round(cur["idle_pct"], 1),
                "status": status_for(spread_pt, mdd_gap_pt),
            }
        )

    rows = (prev_rows + appended)[-HISTORY_KEEP:]
    latest = rows[-1]
    return {
        "ok": True,
        "inception": inception,
        "generatedAt": datetime.now(UTC).isoformat(),
        "rows": rows,
        "latest": latest,
        "appended": len(appended),
        "thresholds": {
            "spreadWatchPt": SPREAD_WATCH_PT,
            "spreadRollbackPt": SPREAD_ROLLBACK_PT,
            "mddGapWatchPt": MDD_GAP_WATCH_PT,
            "mddGapRollbackPt": MDD_GAP_ROLLBACK_PT,
        },
        "note": (
            "港湾H2影子账本（paper NAV，只看不动手）：S-3引擎与闲置比例复用港湾"
            "Timeline，停车腿为H2迟滞套筒。paper窗口自inception起按1.0计，回滚线："
            "累计落后>2pt或回撤深过Live>1pt。Live=港湾canonical，不受影响。"
        ),
    }
