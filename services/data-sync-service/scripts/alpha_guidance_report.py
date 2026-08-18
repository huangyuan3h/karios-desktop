"""Alpha Radar guidance report vs the user's REAL trades (OPT-109).

For every closed user_trade, look up alpha_radar_trends events for the same
symbol whose createdAt predates the ENTRY date (30d window) — did the
catalyst radar back this trade before it happened? Then bucket trades by
"had alpha backing" vs "no alpha backing" and compare realised pnl / win
rate. Small-sample warning: output is directional until C4 sample grows.

Usage:
  PYTHONPATH=src python3 scripts/alpha_guidance_report.py
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))


def _parse_dt(v: str | None) -> datetime | None:
    if not v:
        return None
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except ValueError:
        return None


def main() -> int:
    from data_sync_service.db import user_trades as ut
    from data_sync_service.db.alpha_radar import fetch_trends

    ut.ensure_tables()
    with ut.get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT symbol, side, source, pnl_pct, entry_date, trade_date
            FROM user_trades ORDER BY trade_date
            """
        )
        trades = cur.fetchall()
    if not trades:
        print("no user_trades yet — nothing to evaluate")
        return 0

    total, trends = fetch_trends(limit=2000, max_age_days=90)
    # symbol -> list of (created_at, grade, confidence)
    backing: dict[str, list[tuple[datetime | None, str, float]]] = {}
    for t in trends:
        grade = str(t.get("catalystGrade") or "")
        if grade not in ("S", "A"):
            continue
        conf = float(t.get("mappingConfidence") or 0.0)
        created = _parse_dt(str(t.get("createdAt") or ""))
        for m in (t.get("cnSymbols") or []) + (t.get("hkSymbols") or []):
            sym = str(m.get("symbol") or "")
            if sym:
                backing.setdefault(sym, []).append((created, grade, conf))

    print("## Alpha Radar guidance vs real user trades (OPT-109)")
    print(f"- user trades: {len(trades)} · alpha S/A events scanned (90d): {len(trends)}")
    print("| Symbol | Side | PnL% | 买入日 | α事件(≤买入日) | 最高级 | 置信度 | 有α背书 |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- |")
    backed, unbacked = [], []
    for sym, side, _source, pnl, entry_date, _trade_date in trades:
        entry_dt = None
        if entry_date:
            try:
                entry_dt = datetime.fromisoformat(str(entry_date).replace("Z", "+00:00"))
                if entry_dt.tzinfo is None:
                    entry_dt = entry_dt.replace(tzinfo=UTC)
            except ValueError:
                pass
        events = []
        for created, grade, conf in backing.get(str(sym), []):
            if created is None or entry_dt is None:
                continue
            if created.tzinfo is None:
                created = created.replace(tzinfo=UTC)
            if created <= entry_dt or created - entry_dt <= timedelta(days=30):
                events.append((grade, conf))
        events = sorted(events, key=lambda e: e[1], reverse=True)
        has_backing = bool(events)
        max_grade = max((g for g, _ in events), default="—") if events else "—"
        max_conf = max((c for _, c in events), default=0.0) if events else 0.0
        (backed if has_backing else unbacked).append(float(pnl) if pnl is not None else 0.0)
        print(
            f"| {sym} | {side} | {pnl if pnl is not None else '—':>6} | {entry_date or '—'} | "
            f"{len(events)} | {max_grade} | {max_conf:.2f} | {'✅' if has_backing else '—'} |"
        )

    def _stat(name: str, vals: list[float]) -> None:
        if not vals:
            print(f"- {name}: 样本 0")
            return
        wins = sum(1 for v in vals if v > 0)
        print(
            f"- {name}: n={len(vals)} · 均 PnL {sum(vals)/len(vals):+.2f}% · "
            f"胜率 {wins/len(vals)*100:.0f}%"
        )

    print()
    _stat("有 α 背书", backed)
    _stat("无 α 背书", unbacked)
    print()
    if len(trades) < 20:
        print("⚠ 样本 <20 笔：方向性参考，不作定案（C4 口径等 ≥20 笔平仓）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
