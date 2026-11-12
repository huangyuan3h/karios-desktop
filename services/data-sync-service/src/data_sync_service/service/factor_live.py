"""Factor-vault live overlay — near-daily refresh of the rolling window.

Why this exists
---------------
The vault's four audit windows (OOS2 / train / valid / holdout through
2026-09-30) are frozen truth and stay frozen. But the "last 40 trades" health
check (cold / watch / revived) used to read only the frozen audit blotters, so
the lights could never move: S-gap stopped at 2026-09-23, the Harbor stock leg
at 2026-05-29, and the whole file was generated 2026-10-03.

This module owns one generated file in the reports dir::

    data/backtest_reports/factor_live_trades.json   (gitignored, generated)

Contents:
- ``appended.sgap``   new *closed* S-gap trades after the frozen h2f blotter,
  produced by the frozen habit recipe (``state_bucket_track.HABIT_RECIPE``:
  gap>3%, amp_1430 asc, top 1/3 bucket, C1 skip, body=3, gate 0.5,
  contrib = 0.25 * (ret - 32.28bp) * 100 — the same recipe that produced the
  1130-trade audit blotter).
- ``appended.harbor`` new *closed* S-3 stock-leg trades after the frozen H2d
  long blotter, produced by the frozen S-3 config (truth:
  ``scripts/run_walk_forward.py`` ``S3_CONFIG``; reused here via
  ``service/reconciliation.py`` which mirrors it for the weekly recon).
- ``parking.episodes`` live Harbor parking-leg episodes recomputed through the
  latest complete ETF session with ``harbor.build_harbor_timeline`` (which
  uses ``parking_replay``). Contribution points = the exact additive points
  that ``build_harbor_timeline`` adds to Harbor NAV:
  ``idle * (parking_ret - COST*sides) * 100``. Recomputed wholesale each run
  (a deterministic path, not an append-only ledger).
- ``meta``            as-of watermark, expected sessions, frozen-prefix
  fingerprints, per-input ok/reason.

Design invariants
-----------------
- The frozen wealth-ideas audit files are **read-only inputs**; this module
  never writes to the audit tree. The frozen prefix is referenced by path +
  sha256 + counts, never copied into the live file, so a refresh cannot
  mutate the frozen window.
- The replay windows restart *after the frozen last exit*, so the frozen book
  is fully closed and the incremental replay never double-counts a leg.
  Sessions between the frozen last entry and last exit are deliberately not
  replayed: the frozen book still occupied slots then, and re-entering them
  with an empty replay book would fabricate fills the audit never made.
  The daily pilots restart after their frozen last *signal* day instead
  (their pkl prefix stores neither ts_code nor dates), which is the same
  boundary once the next session's entry is required.
- Only trades whose exit already happened on a complete daily bar are
  appended (``exit <= as_of``).
- Idempotent: appended rows are merged by ``(family, ts, entry, exit)`` and
  re-running the same day yields the same list.
- Fail-open: a family whose replay/inputs fail keeps its previous appended
  list and is marked ``ok: false``; the live file is never deleted/truncated.
  The scheduler job records the failure and pages (HIGH_JOB_TYPES).
- No strategy/parameter/order changes: this is display data only.

Consumers:
- ``scripts/generate_factor_vault.py`` — last-40 overrides (live preferred).
- ``scripts/generate_sgap_decay.py`` — same live S-gap series.
- ``service/factor_system.py`` — freshness + recent-performance as-of.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
from collections.abc import Callable
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from data_sync_service.service import factor_live_daily as fld

logger = logging.getLogger(__name__)

LIVE_FILENAME = "factor_live_trades.json"
LIVE_VERSION = 1
SGAP_FROZEN_FILENAME = "h2f_trades.json"
HARBOR_FROZEN_FILENAME = "h2d_blotter_long.json"

# Same frozen rolling window as the H2k revival rule (factor_vault.REVIVAL_N).
ROLLING_N = 40

# The live append job runs at 19:00 Asia/Shanghai; before that hour the
# previous session is an acceptable as-of (the "same-day lag" in the API).
LIVE_JOB_HOUR_CN = 19

# A 14:30 panel day needs at least one print to be replayable at all.
MIN_PANEL_PRINTS = 1

# H01/H02/H03 daily event pilots ride the same overlay (mean %/笔 window).
DAILY_FAMILIES: tuple[str, ...] = fld.FAMILIES

_FAMILIES = ("sgap", "harbor", *DAILY_FAMILIES)

SgapReplay = Callable[[str, str], dict[str, Any]]
HarborReplay = Callable[[str, str], dict[str, Any]]
DailyReplay = Callable[[str, str, str], dict[str, Any]]


# ---------------------------------------------------------------------------
# Paths / IO
# ---------------------------------------------------------------------------


def audit_scratch_dir() -> Path:
    """Read-only wealth-ideas audit scratch dir (frozen prefix lives here)."""
    return Path.home() / "Projects" / "wealth-ideas" / "karios-audit-2026-10" / "scratch"


def live_path(reports_dir: Path | str | None = None) -> Path:
    base = Path(reports_dir) if reports_dir is not None else _default_reports_dir()
    return base / LIVE_FILENAME


def _default_reports_dir() -> Path:
    return Path(__file__).resolve().parents[3] / "data" / "backtest_reports"


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def load_live_payload(path: Path | str | None = None) -> dict[str, Any] | None:
    """Load the live blotter (None when absent or unreadable)."""
    p = Path(path) if path is not None else live_path()
    try:
        raw = _read_json(p)
    except (OSError, ValueError):
        return None
    if not isinstance(raw, dict):
        return None
    if not isinstance(raw.get("appended"), dict):
        raw["appended"] = {}
    for fam in _FAMILIES:
        if not isinstance(raw["appended"].get(fam), list):
            raw["appended"][fam] = []
    if not isinstance(raw.get("parking"), dict):
        raw["parking"] = {"episodes": []}
    return raw


# ---------------------------------------------------------------------------
# Frozen prefix (read-only)
# ---------------------------------------------------------------------------


def _finite(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _norm_sgap(raw: dict[str, Any]) -> dict[str, Any] | None:
    entry = raw.get("entry") or raw.get("entryDate")
    exit_ = raw.get("exit") or raw.get("exitDate")
    contrib = _finite(raw.get("contrib", raw.get("contribPct")))
    if not entry or contrib is None:
        return None
    row: dict[str, Any] = {
        "ts": str(raw.get("ts") or raw.get("symbol") or ""),
        "entry": str(entry)[:10],
        "exit": str(exit_)[:10] if exit_ else None,
        "contrib": round(contrib, 4),
        "win": contrib > 0,
    }
    # Keep the optional audit attribution fields so the S-gap decay crowding /
    # large-order panels stay populated for the frozen prefix rows.
    for key in ("large_pct", "amt_w", "circ"):
        val = _finite(raw.get(key))
        if val is not None:
            row[key] = val
    return row


def _norm_harbor(raw: dict[str, Any]) -> dict[str, Any] | None:
    """H2d blotter row -> NAV points (pnl% x position weight)."""
    entry = raw.get("entry") or raw.get("entryDate")
    exit_ = raw.get("exit") or raw.get("exitDate")
    if raw.get("contrib") is not None:
        contrib = _finite(raw.get("contrib"))
    else:
        pnl = _finite(raw.get("pnl", raw.get("pnl_pct")))
        pos = _finite(raw.get("pos", raw.get("position_pct")))
        contrib = pnl * pos if pnl is not None and pos is not None else None
    if not entry or contrib is None:
        return None
    return {
        "ts": str(raw.get("ts") or raw.get("symbol") or ""),
        "entry": str(entry)[:10],
        "exit": str(exit_)[:10] if exit_ else None,
        "contrib": round(contrib, 4),
        "win": contrib > 0,
    }


def _load_rows(path: Path, norm: Callable[[dict[str, Any]], dict[str, Any] | None]) -> list[dict]:
    try:
        raw = _read_json(path)
    except (OSError, ValueError):
        return []
    if isinstance(raw, dict):
        raw = raw.get("trades", [])
    if not isinstance(raw, list):
        return []
    rows = [norm(t) for t in raw if isinstance(t, dict)]
    return [r for r in rows if r is not None]


def load_frozen_sgap(path: Path | str | None = None) -> list[dict[str, Any]]:
    p = Path(path) if path is not None else audit_scratch_dir() / SGAP_FROZEN_FILENAME
    return _load_rows(p, _norm_sgap)


def load_frozen_harbor(path: Path | str | None = None) -> list[dict[str, Any]]:
    p = Path(path) if path is not None else audit_scratch_dir() / HARBOR_FROZEN_FILENAME
    return _load_rows(p, _norm_harbor)


def frozen_fingerprint(path: Path, rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Reference the frozen prefix without copying it into the live file."""
    payload = json.dumps(rows, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    exits = [str(r.get("exit") or "") for r in rows if r.get("exit")]
    entries = [str(r.get("entry") or "") for r in rows if r.get("entry")]
    return {
        "path": str(path),
        "entries": len(rows),
        "lastEntry": max(entries, default=None),
        "lastExit": max(exits, default=None),
        "sha256": hashlib.sha256(payload.encode("utf-8")).hexdigest() if rows else None,
        "readable": bool(rows),
    }


def last_frozen_entry(rows: list[dict[str, Any]]) -> str | None:
    return max((str(r.get("entry")) for r in rows if r.get("entry")), default=None)


def last_frozen_exit(rows: list[dict[str, Any]]) -> str | None:
    return max((str(r.get("exit")) for r in rows if r.get("exit")), default=None)


# ---------------------------------------------------------------------------
# Merge / rolling stats (pure)
# ---------------------------------------------------------------------------


def trade_key(family: str, trade: dict[str, Any]) -> str:
    ts = str(trade.get("ts") or "")
    if not ts:
        # Frozen daily-prefix rows have no ts_code (the audit pkl never stored
        # one); they carry a stable ``key`` built from (family, signal, seq).
        key = trade.get("key")
        if key:
            return str(key)
    return "|".join(
        [
            str(family),
            ts,
            str(trade.get("entry") or ""),
            str(trade.get("exit") or ""),
        ]
    )


def _sorted(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    # Stable by entry date only: same-day ties keep audit/blotter order, which
    # is what the frozen H2k/H2d replays assume.
    return sorted(rows, key=lambda t: str(t.get("entry") or ""))


def merge_appended(
    existing: list[dict[str, Any]],
    incoming: list[dict[str, Any]],
    *,
    family: str,
) -> list[dict[str, Any]]:
    """Idempotent union of appended rows (existing order wins on conflict)."""
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for row in list(existing or []) + list(incoming or []):
        if not isinstance(row, dict):
            continue
        key = trade_key(family, row)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return _sorted(out)


def merge_trades(
    frozen: list[dict[str, Any]],
    appended: list[dict[str, Any]],
    *,
    family: str,
) -> list[dict[str, Any]]:
    """Frozen prefix + appended overlay, deduped and entry-sorted."""
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for row in list(frozen or []) + list(appended or []):
        if not isinstance(row, dict):
            continue
        key = trade_key(family, row)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return _sorted(out)


def rolling_stats(
    trades: list[dict[str, Any]],
    *,
    n: int = ROLLING_N,
    mu: float,
    sigma: float,
    as_of: str | None = None,
) -> dict[str, Any]:
    """Last-``n`` closed trades: net / win rate / placebo percentile.

    ``net`` is the additive contribution sum in the family's frozen unit
    (satellite points / harbor NAV points). ``percentile`` reuses the frozen
    normal-approx calibration (``factor_vault.percentile_of_sum``).
    ``as_of`` is the replay watermark when supplied (the vault displays it as
    "as of the latest complete session"); ``last_trade_as_of`` is the last
    closed trade's exit so the window's real end stays visible.
    """
    from data_sync_service.service import factor_vault as fv

    rows = _sorted([t for t in trades or [] if _finite(t.get("contrib")) is not None])
    tail = rows[-n:] if n > 0 else rows
    net = sum(float(t["contrib"]) for t in tail)
    wins = sum(1 for t in tail if float(t["contrib"]) > 0)
    last_exit = max((str(t.get("exit")) for t in tail if t.get("exit")), default=None)
    last_entry = max((str(t.get("entry")) for t in tail if t.get("entry")), default=None)
    return {
        "trades": len(tail),
        "net": round(net, 4),
        "win_rate": round(wins / len(tail), 4) if tail else None,
        "percentile": fv.percentile_of_sum(net, len(tail), mu, sigma) if tail else None,
        "as_of": as_of or last_exit or last_entry,
        "last_trade_as_of": last_exit or last_entry,
        "first_trade_as_of": tail[0].get("entry") if tail else None,
        "total_trades": len(rows),
    }


def parking_as_trades(episodes: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    """Closed parking episodes in the same additive-points shape as trades.

    The still-held leg (``closed=False``) is MTM-only and is excluded from the
    rolling window so the health check keeps the "closed trades only" rule.
    """
    out: list[dict[str, Any]] = []
    for ep in episodes or []:
        if not isinstance(ep, dict) or ep.get("closed") is False:
            continue
        out.append(
            {
                "ts": str(ep.get("ts") or ""),
                "entry": ep.get("entry"),
                "exit": ep.get("exit"),
                "contrib": _finite(ep.get("contrib")) or 0.0,
                "win": (_finite(ep.get("contrib")) or 0.0) > 0,
                "leg": "parking",
            }
        )
    return out


def harbor_rolling_trades(
    frozen: list[dict[str, Any]], payload: dict[str, Any] | None
) -> list[dict[str, Any]]:
    """Frozen H2d stock leg + live stock-leg appends + live parking episodes.

    The frozen H2d blotter only recorded the stock leg, so the frozen prefix
    stays stock-only; everything after it carries ``leg="stock"`` or
    ``leg="parking"``. Both contributions are Harbor NAV points, which keeps
    the rolling sum in one unit.
    """
    appended = list((payload or {}).get("appended", {}).get("harbor") or [])
    parking = parking_as_trades(((payload or {}).get("parking") or {}).get("episodes"))
    return merge_trades(frozen, appended + parking, family="harbor")


# ---------------------------------------------------------------------------
# Replay engines (reuse the frozen recipes; no new rules)
# ---------------------------------------------------------------------------


def replay_sgap_live(start: str, end: str) -> dict[str, Any]:
    """Run the frozen habit recipe over [start, end] (DB reads only).

    Uses ``state_bucket_track.load_sgap_context`` + ``replay_sgap_from_context``
    with the frozen ``HABIT_RECIPE`` (same kwargs that produced the 1130-trade
    audit blotter). Only ``kind="fill"`` rows closed inside the window count.
    """
    from data_sync_service.service.state_bucket_track import (
        HABIT_RECIPE,
        load_sgap_context,
        replay_sgap_from_context,
    )

    ctx = load_sgap_context(start, end)
    sat = replay_sgap_from_context(ctx, start=start, end=end, **HABIT_RECIPE)
    trades: list[dict[str, Any]] = []
    for row in sat.get("blotter") or []:
        if row.get("kind") != "fill":
            continue
        entry = row.get("entryDate")
        exit_ = row.get("exitDate")
        if not entry or not exit_ or str(exit_) > end:
            continue
        contrib = _finite(row.get("contribPct"))
        if contrib is None:
            continue
        trades.append(
            {
                "ts": str(row.get("ts") or ""),
                "entry": str(entry)[:10],
                "exit": str(exit_)[:10],
                "contrib": round(contrib, 4),
                "win": contrib > 0,
                "source": "sgap_replay",
            }
        )
    panel_prints = {
        d: sum(1 for mp in (ctx.get("px_1430") or {}).values() if mp.get(d))
        for d in (ctx.get("cal") or [])
        if start <= d <= end
    }
    end_prints = int(panel_prints.get(end, 0))
    return {
        "ok": end_prints >= MIN_PANEL_PRINTS,
        "reason": None
        if end_prints >= MIN_PANEL_PRINTS
        else f"no 14:30 bars for {end} (live panel not synced)",
        "asOf": end,
        "trades": trades,
        "openPositions": len(sat.get("openPositions") or []),
        "decisionAvailable": bool(sat.get("decisionAvailable")),
        "decisionUnavailableDays": list(sat.get("decisionUnavailableDays") or []),
        "panelPrints": end_prints,
        "gateOpenDays": sum(1 for r in sat.get("rows") or [] if r.get("gateOpen")),
        "gapDays": sum(1 for r in sat.get("rows") or [] if int(r.get("gapCount") or 0) > 0),
        "filledToday": sum(int(r.get("filledToday") or 0) for r in sat.get("rows") or []),
    }


def frozen_s3_config() -> dict[str, Any]:
    """Frozen CN S-3 config (truth: scripts/run_walk_forward.py S3_CONFIG).

    ``service/reconciliation.py`` mirrors that dict for the weekly recon; the
    two fields it omits (next-open entry + the 0.7亿 liquidity gate) are added
    back so the live Harbor stock-leg replay matches the frozen S-3 recipe.
    """
    from data_sync_service.service.reconciliation import S3_CONFIG

    return {**S3_CONFIG, "entry_mode": "next_open", "min_avg_amount": 0.7}


def replay_harbor_live(start: str, end: str) -> dict[str, Any]:
    """Replay the Harbor stock leg + parking path through ``end``.

    - Stock leg: frozen S-3 config -> closed ``BacktestTrade`` rows, scored as
      ``pnl_pct * position_pct`` points (same unit as the H2d blotter).
    - Parking path: ``harbor.build_harbor_timeline`` (which calls
      ``parking_replay``) over the same calendar; episodes carry the exact
      additive points the timeline adds to Harbor NAV.
    """
    from data_sync_service.service.backtest_engine import (
        BacktestConfig,
        BacktestData,
        simulate,
    )
    from data_sync_service.service.harbor import build_harbor_timeline, load_etf_closes
    from data_sync_service.service.portfolio_nav_sim import engine_nav_by_day_from_run

    cfg = BacktestConfig(start_date=start, end_date=end, **frozen_s3_config())
    data = BacktestData(cfg)
    run = simulate(cfg, data)
    trades: list[dict[str, Any]] = []
    for tr in run.trades:
        if not tr.close_date or str(tr.close_date) > end:
            continue
        trades.append(
            {
                "ts": str(tr.symbol or ""),
                "entry": str(tr.entry_date)[:10],
                "exit": str(tr.close_date)[:10],
                "contrib": round(float(tr.pnl_pct) * float(tr.position_pct), 4),
                "win": float(tr.pnl_pct) > 0,
                "source": "harbor_replay",
                "leg": "stock",
            }
        )

    etf_close = load_etf_closes()
    timeline = build_harbor_timeline(
        calendar=list(data.calendar),
        positions_by_day=run.positions_by_day,
        engine_nav_by_day=engine_nav_by_day_from_run(data.calendar, run.nav_curve),
        etf_close=etf_close,
    )
    rows = timeline.get("rows") or []
    episodes = parking_episodes(rows)
    etf_as_of = max((max(mp) for mp in etf_close.values() if mp), default=None)
    return {
        "ok": bool(rows),
        "reason": None if rows else "parking replay produced no rows (ETF series missing)",
        "asOf": str(rows[-1]["date"]) if rows else end,
        "etfAsOf": etf_as_of,
        "trades": trades,
        "parking": episodes,
        "calendarDays": len(rows),
        "parkingTrades": int(timeline.get("parkingTrades") or 0),
    }


def parking_episodes(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group ``build_harbor_timeline`` rows into parking-leg episodes.

    Contribution per day is exactly the Harbor NAV additive term
    ``idle * (parking_ret - COST * sides)`` scaled to points; a trail-exit day
    (REPO + sides>0) charges its sell side to the episode that just closed.
    """
    from data_sync_service.service.harbor import COST

    episodes: list[dict[str, Any]] = []
    cur: dict[str, Any] | None = None

    def _close(ep: dict[str, Any] | None, *, closed: bool = True) -> None:
        if ep is not None:
            ep["contrib"] = round(float(ep["contrib"]), 4)
            ep["closed"] = closed
            episodes.append(ep)

    for row in rows or []:
        day = str(row.get("date") or "")
        pick = str(row.get("pick") or "")
        pick_ts = str(row.get("pickTs") or "")
        sides = int(row.get("parkedSides") or 0)
        idle = float(row.get("idlePct") or 0.0) / 100.0
        ret = float(row.get("parkedRetPct") or 0.0) / 100.0
        held = bool(pick_ts) and pick not in ("", "REPO")
        if not held:
            if cur is not None:
                cur["contrib"] += idle * (-COST * sides) * 100.0
                _close(cur)
                cur = None
            continue
        contrib = idle * (ret - COST * sides) * 100.0
        if cur is not None and cur["ts"] == pick_ts:
            cur["exit"] = day
            cur["contrib"] += contrib
            cur["days"] += 1
            continue
        _close(cur)
        cur = {
            "ts": pick_ts,
            "entry": day,
            "exit": day,
            "contrib": contrib,
            "days": 1,
            "idlePct": round(idle * 100.0, 2),
        }
    # The leg still held on the last row is open (MTM only, not a closed
    # trade), so the rolling window does not count it.
    _close(cur, closed=False)
    for ep in episodes:
        ep["win"] = float(ep.get("contrib") or 0.0) > 0
    return episodes


# ---------------------------------------------------------------------------
# Calendar / session helpers (read-only DB, fail-open)
# ---------------------------------------------------------------------------


def _read_only_conn():
    from data_sync_service.db import get_connection

    return get_connection()


def latest_complete_session() -> str | None:
    """Latest A-share session with an ingested ``daily`` row (read-only).

    Joined to ``stock_basic`` so an ETF-only day cannot pass for a stock
    session; the 45-day scan window keeps the query cheap.
    """
    try:
        with _read_only_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT max(d.trade_date)
                    FROM daily d
                    JOIN stock_basic sb ON sb.ts_code = d.ts_code
                    WHERE d.trade_date >= current_date - interval '45 days'
                      AND d.close > 0
                    """
                )
                row = cur.fetchone()
                return str(row[0])[:10] if row and row[0] else None
    except Exception as exc:  # noqa: BLE001 - display freshness, never a gate
        logger.warning("factor_live: latest_complete_session failed: %s", exc)
        return None


def latest_etf_session() -> str | None:
    """Latest session with an ingested ETF ``daily`` row (read-only)."""
    try:
        from data_sync_service.service.harbor import MULTI_TS

        codes = sorted(set(MULTI_TS.values()) | {"513100.SH"})
        with _read_only_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT max(trade_date) FROM daily
                    WHERE ts_code = ANY(%s)
                      AND close > 0
                      AND trade_date >= current_date - interval '45 days'
                    """,
                    (codes,),
                )
                row = cur.fetchone()
                return str(row[0])[:10] if row and row[0] else None
    except Exception as exc:  # noqa: BLE001
        logger.warning("factor_live: latest_etf_session failed: %s", exc)
        return None


def first_session_after(day: str) -> str | None:
    """First open session strictly after ``day`` (read-only, fail-open)."""
    try:
        with _read_only_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT min(cal_date) FROM trade_calendar WHERE is_open = 1 AND cal_date > %s",
                    (day,),
                )
                row = cur.fetchone()
                if row and row[0]:
                    return str(row[0])[:10]
    except Exception as exc:  # noqa: BLE001
        logger.warning("factor_live: first_session_after failed: %s", exc)
    try:
        nxt = date.fromisoformat(day) + timedelta(days=1)
    except ValueError:
        return None
    while nxt.weekday() >= 5:
        nxt += timedelta(days=1)
    return nxt.isoformat()


# ---------------------------------------------------------------------------
# Refresh
# ---------------------------------------------------------------------------


def _empty_payload() -> dict[str, Any]:
    return {
        "version": LIVE_VERSION,
        "meta": {},
        "appended": {fam: [] for fam in _FAMILIES},
        "parking": {"episodes": []},
    }


def _iso_now(now: datetime | None) -> datetime:
    return now if now is not None else datetime.now()


def refresh_live_blotter(
    reports_dir: Path | str | None = None,
    *,
    now: datetime | None = None,
    latest_session: str | None = None,
    etf_session: str | None = None,
    frozen_sgap_path: Path | str | None = None,
    frozen_harbor_path: Path | str | None = None,
    frozen_daily_prefix: dict[str, Any] | None = None,
    sgap_replay: SgapReplay | None = None,
    harbor_replay: HarborReplay | None = None,
    daily_replay: DailyReplay | None = None,
    session_after: Callable[[str], str | None] | None = None,
    write: bool = True,
) -> dict[str, Any]:
    """Append new closed trades and rewrite the live blotter (fail-open).

    Every external dependency is injectable (paths, session dates, replay
    callables) so the merge/append logic is unit-testable with no DB:
    ``frozen_daily_prefix`` is the H01/H02/H03 sidecar payload (``None``
    rebuilds/loads it, ``{}`` means live-only fallback), and ``daily_replay``
    is ``(family, start, end) -> {ok, trades, asOf}``.
    """
    base = Path(reports_dir) if reports_dir is not None else _default_reports_dir()
    path = base / LIVE_FILENAME
    existing = load_live_payload(path) or _empty_payload()
    stamped = _iso_now(now)

    sgap_frozen_path = (
        Path(frozen_sgap_path)
        if frozen_sgap_path is not None
        else audit_scratch_dir() / SGAP_FROZEN_FILENAME
    )
    harbor_frozen_path = (
        Path(frozen_harbor_path)
        if frozen_harbor_path is not None
        else audit_scratch_dir() / HARBOR_FROZEN_FILENAME
    )
    sgap_frozen = load_frozen_sgap(sgap_frozen_path)
    harbor_frozen = load_frozen_harbor(harbor_frozen_path)
    if frozen_daily_prefix is None:
        try:
            frozen_daily_prefix = fld.load_frozen_prefix(base, write=write)
        except Exception as exc:  # noqa: BLE001 - live-only fallback, never fatal
            logger.warning("factor_live: daily frozen prefix unavailable: %s", exc)
            frozen_daily_prefix = None

    watermark = latest_session if latest_session is not None else latest_complete_session()
    etf_mark = etf_session if etf_session is not None else latest_etf_session()

    inputs: dict[str, dict[str, Any]] = {}
    appended = {
        fam: _sorted(list(existing.get("appended", {}).get(fam) or [])) for fam in _FAMILIES
    }
    parking = existing.get("parking") or {}
    if not isinstance(parking, dict) or not isinstance(parking.get("episodes"), list):
        parking = {"episodes": []}

    # --- S-gap -------------------------------------------------------------
    sgap_entry = _input_meta(sgap_frozen, watermark)
    if not sgap_frozen:
        sgap_entry.update(ok=False, reason=f"frozen prefix missing ({sgap_frozen_path})")
    elif watermark is None:
        sgap_entry.update(ok=False, reason="latest complete trading session unknown")
    else:
        last_exit = last_frozen_exit(sgap_frozen) or last_frozen_entry(sgap_frozen)
        start = (session_after or first_session_after)(last_exit) if last_exit else None
        sgap_entry["replayStart"] = start
        if start is None or start > watermark:
            sgap_entry.update(ok=True, newTrades=0, note="no session after frozen prefix")
        else:
            try:
                result = (sgap_replay or replay_sgap_live)(start, watermark)
            except Exception as exc:  # noqa: BLE001 - keep yesterday's rows
                logger.warning("factor_live: sgap replay failed: %s", exc)
                sgap_entry.update(ok=False, reason=f"sgap replay failed: {exc}")
            else:
                last_entry = last_frozen_entry(sgap_frozen) or ""
                new_rows = [
                    t
                    for t in result.get("trades") or []
                    if str(t.get("entry") or "") > last_entry
                    and t.get("exit")
                    and str(t["exit"]) <= watermark
                ]
                appended["sgap"] = _prune_appended(
                    merge_appended(appended["sgap"], new_rows, family="sgap"),
                    last_entry,
                    watermark,
                    family="sgap",
                )
                sgap_entry.update(
                    ok=bool(result.get("ok", True)),
                    reason=result.get("reason"),
                    newTrades=len(new_rows),
                    openPositions=result.get("openPositions"),
                    decisionAvailable=result.get("decisionAvailable"),
                    panelPrints=result.get("panelPrints"),
                    gateOpenDays=result.get("gateOpenDays"),
                    gapDays=result.get("gapDays"),
                    asOf=result.get("asOf") or watermark,
                )
    inputs["sgap"] = sgap_entry

    # --- Harbor (stock leg + parking path) ---------------------------------
    harbor_entry = _input_meta(harbor_frozen, watermark)
    if not harbor_frozen:
        harbor_entry.update(ok=False, reason=f"frozen prefix missing ({harbor_frozen_path})")
    elif watermark is None:
        harbor_entry.update(ok=False, reason="latest complete trading session unknown")
    elif etf_mark is None:
        harbor_entry.update(ok=False, reason="ETF daily session unknown")
    elif etf_mark < watermark:
        harbor_entry.update(
            ok=False,
            reason=f"ETF daily ends {etf_mark} (latest session {watermark})",
        )
    else:
        last_exit = last_frozen_exit(harbor_frozen) or last_frozen_entry(harbor_frozen)
        start = (session_after or first_session_after)(last_exit) if last_exit else None
        harbor_entry["replayStart"] = start
        if start is None or start > watermark:
            harbor_entry.update(ok=True, newTrades=0, note="no session after frozen prefix")
        else:
            try:
                result = (harbor_replay or replay_harbor_live)(start, watermark)
            except Exception as exc:  # noqa: BLE001 - keep yesterday's rows
                logger.warning("factor_live: harbor replay failed: %s", exc)
                harbor_entry.update(ok=False, reason=f"harbor replay failed: {exc}")
            else:
                last_entry = last_frozen_entry(harbor_frozen) or ""
                new_rows = [
                    t
                    for t in result.get("trades") or []
                    if str(t.get("entry") or "") > last_entry
                    and t.get("exit")
                    and str(t["exit"]) <= watermark
                ]
                appended["harbor"] = _prune_appended(
                    merge_appended(appended["harbor"], new_rows, family="harbor"),
                    last_entry,
                    watermark,
                    family="harbor",
                )
                episodes = [
                    ep
                    for ep in result.get("parking") or []
                    if ep.get("exit") and str(ep["exit"]) <= watermark
                ]
                if episodes:
                    parking = {"episodes": episodes}
                closed_eps = [ep for ep in episodes if ep.get("closed") is not False]
                harbor_entry.update(
                    ok=bool(result.get("ok", True)),
                    reason=result.get("reason"),
                    newTrades=len(new_rows),
                    parkingTrades=len(closed_eps),
                    parkingOpen=len(episodes) - len(closed_eps),
                    parkingNet=round(sum(float(ep.get("contrib") or 0.0) for ep in closed_eps), 4),
                    asOf=result.get("asOf") or watermark,
                    etfAsOf=result.get("etfAsOf") or etf_mark,
                )
    inputs["harbor"] = harbor_entry

    # --- H01/H02/H03 daily event pilots ------------------------------------
    # Fail-open per pilot: a failing family keeps yesterday's appended rows and
    # never blocks the others (or the S-gap/Harbor inputs above).
    for family in DAILY_FAMILIES:
        frozen_rows = fld.family_rows(frozen_daily_prefix, family)
        entry = _input_meta(frozen_rows, watermark)
        entry["frozenFingerprint"] = fld.prefix_fingerprint(frozen_daily_prefix, family)
        frozen_signal = fld.last_signal(frozen_rows)
        entry["frozenLastSignal"] = frozen_signal
        # Without the audit pkl the replay still starts after the frozen
        # holdout edge and the window uses live closed trades only.
        base_signal = frozen_signal or fld.HOLDOUT_END
        if watermark is None:
            entry.update(ok=False, reason="latest complete trading session unknown")
        else:
            start = (session_after or first_session_after)(base_signal)
            entry["replayStart"] = start
            if start is None or start > watermark:
                entry.update(ok=True, newTrades=0, note="no session after frozen prefix")
            else:
                try:
                    result = (daily_replay or fld.replay_family)(family, start, watermark)
                except Exception as exc:  # noqa: BLE001 - keep yesterday's rows
                    logger.warning("factor_live: %s replay failed: %s", family, exc)
                    entry.update(ok=False, reason=f"{family} replay failed: {exc}")
                else:
                    ok = bool(result.get("ok", True))
                    entry.update(
                        ok=ok,
                        reason=result.get("reason"),
                        asOf=result.get("asOf") or watermark,
                        codes=result.get("codes"),
                    )
                    if ok:
                        last_entry = fld.last_entry(frozen_rows) or fld.HOLDOUT_END
                        new_rows = [
                            t
                            for t in result.get("trades") or []
                            if t.get("ts")
                            and str(t.get("signal") or "") > base_signal
                            and t.get("exit")
                            and str(t["exit"]) <= watermark
                        ]
                        appended[family] = _prune_appended(
                            merge_appended(appended[family], new_rows, family=family),
                            last_entry,
                            watermark,
                            family=family,
                        )
                        # Rolling ledger: 15x the window is plenty for the
                        # vault override; the frozen prefix is never trimmed.
                        appended[family] = fld.trim_appended(appended[family])
                        entry["newTrades"] = len(new_rows)
        inputs[family] = entry

    bad = [f"{fam}: {meta.get('reason')}" for fam, meta in inputs.items() if not meta.get("ok")]
    meta: dict[str, Any] = {
        "version": LIVE_VERSION,
        "updatedAt": stamped.isoformat(),
        "asOf": watermark,
        "expectedSession": watermark,
        "etfSession": etf_mark,
        "status": "degraded" if bad else "ok",
        "reason": "；".join(bad) if bad else None,
        "generatedBy": "factor_live_append",
        "rollingN": ROLLING_N,
        "frozenPrefix": {
            "sgap": frozen_fingerprint(sgap_frozen_path, sgap_frozen),
            "harbor": frozen_fingerprint(harbor_frozen_path, harbor_frozen),
            **{fam: fld.prefix_fingerprint(frozen_daily_prefix, fam) for fam in DAILY_FAMILIES},
        },
        "inputs": inputs,
        "parkingAsOf": (
            max(
                (str(ep.get("exit")) for ep in parking.get("episodes") or [] if ep.get("exit")),
                default=None,
            )
        ),
        "note": (
            "appended = new closed trades after the frozen prefix; the frozen audit "
            "windows stay frozen. stock/harbor contributions are additive points; "
            "parking episodes are the same additive Harbor NAV points; h01/h02/h03 "
            "net is a decimal per-trade return displayed as mean %/笔."
        ),
    }

    payload = {
        "version": LIVE_VERSION,
        "meta": meta,
        "appended": {fam: appended[fam] for fam in _FAMILIES},
        "parking": parking,
    }
    if write:
        _write_atomic(path, payload)
    return payload


def _input_meta(frozen: list[dict[str, Any]], watermark: str | None) -> dict[str, Any]:
    return {
        "ok": True,
        "reason": None,
        "newTrades": 0,
        "asOf": watermark,
        "frozenEntries": len(frozen),
        "frozenLastEntry": last_frozen_entry(frozen),
        "frozenLastExit": last_frozen_exit(frozen),
    }


def _prune_appended(
    rows: list[dict[str, Any]],
    frozen_last_entry: str,
    watermark: str,
    *,
    family: str,
) -> list[dict[str, Any]]:
    """Drop rows that are already in the frozen prefix or not closed yet."""
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        entry = str(row.get("entry") or "")
        exit_ = str(row.get("exit") or "")
        if not entry or entry <= frozen_last_entry:
            continue
        if exit_ and exit_ > watermark:
            continue
        key = trade_key(family, row)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return _sorted(out)


def _write_atomic(path: Path, payload: dict[str, Any]) -> None:
    """Atomic replace: a crash mid-write must never truncate yesterday's file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


# ---------------------------------------------------------------------------
# Read-side helpers for the vault / decay scripts
# ---------------------------------------------------------------------------


def live_override_stats(
    payload: dict[str, Any] | None,
    *,
    family: str,
    frozen: list[dict[str, Any]],
    mu: float,
    sigma: float,
) -> dict[str, Any] | None:
    """Rolling stats for a family, live overlay preferred over frozen-only.

    Returns None when the frozen prefix is missing (the vault then keeps its
    embedded frozen aggregates — never invent a window).
    """
    if not frozen:
        return None
    if family == "sgap":
        merged = merge_trades(
            frozen,
            list((payload or {}).get("appended", {}).get("sgap") or []),
            family="sgap",
        )
    else:
        merged = harbor_rolling_trades(frozen, payload)
    meta = (payload or {}).get("meta") or {}
    return rolling_stats(
        merged,
        mu=mu,
        sigma=sigma,
        as_of=meta.get("asOf"),
    )


def daily_override_stats(
    payload: dict[str, Any] | None,
    *,
    family: str,
    frozen: list[dict[str, Any]],
    as_of: str | None = None,
) -> dict[str, Any] | None:
    """Last-40 for a daily event pilot as mean %/笔 (live overlay preferred).

    Deliberately not ``rolling_stats``: S-gap/Harbor sum additive points, but
    these bricks are ``%/笔`` so the window is the *mean* per-trade return.
    Returns None when there is neither a frozen prefix nor an appended row
    (the vault then keeps its embedded frozen proxy — never invent a window).
    """
    appended = [
        r
        for r in ((payload or {}).get("appended", {}) or {}).get(family) or []
        if isinstance(r, dict)
    ]
    if not frozen and not appended:
        return None
    meta = (payload or {}).get("meta") or {}
    return fld.rolling_stats(
        frozen,
        appended,
        as_of=as_of or meta.get("asOf"),
    )
