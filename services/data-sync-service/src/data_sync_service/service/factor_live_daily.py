"""H01/H02/H03 daily event replay — live continuation of the factor vault.

Why this exists
---------------
The vault's four audit windows (OOS2 / train / valid / holdout through
2026-09-30) are frozen truth. S-gap and the Harbor legs already follow the
latest complete session through ``factor_live.py``; the three daily event
pilots (``h01`` 5连阴 hold5, ``h02`` BIAS6 hold5, ``h03`` RSI30 hold5) stayed
stuck on the frozen holdout mean, so their "最近表现" light could never move.

This module ports the exact recipe from the frozen audit script
``~/Projects/wealth-ideas/karios-audit-2026-10/scratch/h2_daily.py`` (read-only,
never written back, never copied into git):

- cost ``COST = 0.003228`` per closed trade, ``net = sell/buy - 1 - COST``;
- universe ``stock_basic.market`` in 主板/创业板/科创板, no ``ST``, no delisted
  name, listed >= 60 calendar days at the **signal** date;
- signal-day filters ``amount >= 20000`` and ``pct_chg < 9.5``;
- entry = next session open, exit = close of the 5th session from entry
  (``si = bi + 5 - 1``), skip when the entry day ``pct_chg > 9.5``;
- H01: five consecutive down days including the signal bar;
- H02: ``close / MA6 - 1 < -0.06`` (MA6 finite and > 0);
- H03: RSI(14) (pandas ``rolling(n, min_periods=n)`` mean of gains/losses)
  below 30;
- no TopN / no intra-day cap: every passing event counts (``add_ev``).

Only trades whose exit is already a complete session are appended; an open
leg is MTM-only and never enters the closed last-40 window.

Frozen prefix
-------------
``h2_records.pkl`` ``toprec`` holds the audit's per-signal-day rows
``(pct|bias|rsi, buy, sell, amt)`` **without** ``ts_code`` and only for the
audit windows. ``build_frozen_prefix`` extracts a compact tail per family into
``data/backtest_reports/factor_live_daily_prefix.json`` (gitignored, like
``factor_live_trades.json``) once, keyed by ``(family, signal, seq)`` and
fingerprinted with the source sha256 + counts. The pkl / ``h2_daily.json``
are read-only inputs; the full audit book is never copied into the live file.

When the pkl is unavailable the replay still runs from the frozen holdout end
(2026-09-30) and the rolling window uses live closed trades only — honest and
fail-open, never inventing a prefix. The live overlay keeps only the most
recent ``KEEP_ROWS`` closed rows per family (15x the 40-trade window); the
full replay is rebuilt from the frozen prefix each run, so trimming is
deterministic and the frozen prefix itself never moves.

Units
-----
These bricks are ``unit: "%/笔"``. ``rolling_stats`` therefore reports the
**mean percent per trade** of the last 40 closed trades (``mean(net) * 100``),
not the additive-points sum used by S-gap / Harbor. ``percentile`` stays
``None``: no H2 500-draw window calibration is recoverable here, and inventing
``mu``/``sigma`` would fake a revival.

No DB writes, no network, no orders, no strategy/parameter changes: display
data only.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import pickle
from collections.abc import Callable, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any, NamedTuple

logger = logging.getLogger(__name__)

# Pilot ids used by the vault / live overlay.
FAMILIES: tuple[str, ...] = ("h01", "h02", "h03")
LABELS: dict[str, str] = {"h01": "H01", "h02": "H02", "h03": "H03"}

# Frozen recipe constants (h2_daily.py — do not tune).
COST: float = 0.003228
HOLD_SESSIONS: int = 5
MIN_AMOUNT: float = 20000.0
MAX_SIGNAL_PCT: float = 9.5
MAX_ENTRY_PCT: float = 9.5
MIN_LIST_DAYS: int = 60
LOOKBACK_SESSIONS: int = 80

# Calendar days loaded before the replay start so every code has the same
# >= 80-bar warm-up the audit loop assumes (i >= 80).
LOOKBACK_CALENDAR_DAYS: int = 160
BAR_BATCH: int = 600
MIN_BARS: int = LOOKBACK_SESSIONS + 1

# Frozen holdout edge: the live continuation starts on the next open session.
HOLDOUT_END: str = "2026-09-30"
HOLDOUT_START: str = "2026-08-10"

MARKETS: frozenset[str] = frozenset({"主板", "创业板", "科创板"})

PREFIX_FILENAME = "factor_live_daily_prefix.json"
FROZEN_PKL = "h2_records.pkl"
FROZEN_JSON = "h2_daily.json"
# Enough frozen rows that the first post-freeze window can still be a real
# 40-trade tail before the replay has closed 40 trades of its own.
PREFIX_ROWS = 200
# The live overlay keeps the most recent closed rows only (15x the rolling
# window). The full replay is rebuilt from the frozen prefix each run, so the
# trim is deterministic and idempotent; the frozen prefix itself never moves.
KEEP_ROWS = 600
PREFIX_VERSION = 1

DailyReplay = Callable[[str, str, str], dict[str, Any]]


class Bar(NamedTuple):
    """One daily bar: only the fields the three recipes read."""

    day: str
    open: float
    close: float
    pct: float | None
    amount: float | None


# ---------------------------------------------------------------------------
# Paths / IO
# ---------------------------------------------------------------------------


def audit_scratch_dir() -> Path:
    """Read-only wealth-ideas audit scratch dir (frozen prefix lives here)."""
    return Path.home() / "Projects" / "wealth-ideas" / "karios-audit-2026-10" / "scratch"


def default_reports_dir() -> Path:
    return Path(__file__).resolve().parents[3] / "data" / "backtest_reports"


def prefix_path(reports_dir: Path | str | None = None) -> Path:
    base = Path(reports_dir) if reports_dir is not None else default_reports_dir()
    return base / PREFIX_FILENAME


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


# ---------------------------------------------------------------------------
# Pure recipe (port of h2_daily.py)
# ---------------------------------------------------------------------------


def _finite(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def compute_rsi(closes: Sequence[float], n: int = 14) -> list[float | None]:
    """RSI(n) exactly like ``pandas rolling(n, min_periods=n).mean()``.

    ``delta = close.diff()``; gains/losses are clipped; the means start at the
    ``n``-th delta. An all-loss window returns 0, an all-gain window 100 and a
    flat window None (the audit's NaN).
    """
    out: list[float | None] = [None] * len(closes)
    if len(closes) <= n:
        return out
    gains = [0.0] * len(closes)
    losses = [0.0] * len(closes)
    for i in range(1, len(closes)):
        delta = closes[i] - closes[i - 1]
        if delta > 0:
            gains[i] = delta
        elif delta < 0:
            losses[i] = -delta
    for i in range(n, len(closes)):
        avg_gain = sum(gains[i - n + 1 : i + 1]) / n
        avg_loss = sum(losses[i - n + 1 : i + 1]) / n
        if avg_loss == 0:
            out[i] = None if avg_gain == 0 else 100.0
        else:
            out[i] = 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)
    return out


def _parse_day(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def listed_enough(list_date: str | None, signal: str) -> bool:
    """``list_ok`` from the audit: >= 60 calendar days listed, unknown -> ok."""
    ld = _parse_day(list_date)
    sig = _parse_day(signal)
    if ld is None or sig is None:
        return True
    return (sig - ld).days >= MIN_LIST_DAYS


def universe_from_rows(
    rows: Sequence[Sequence[Any]],
) -> tuple[list[str], dict[str, str | None]]:
    """``stock_basic`` rows -> (codes, list_date map) after the audit filters.

    Rows are ``(ts_code, market, name, list_date, delist_date)``. ST names and
    delisted names are dropped for the whole replay (the audit built its
    universe once, at the cut date, and reused it).
    """
    codes: list[str] = []
    list_dates: dict[str, str | None] = {}
    for ts, market, name, list_date, delist_date in rows:
        ts = str(ts)
        list_dates[ts] = str(list_date)[:10] if list_date else None
        if market not in MARKETS:
            continue
        if delist_date:
            continue
        if name and "ST" in str(name):
            continue
        codes.append(ts)
    return codes, list_dates


def _five_down(pcts: Sequence[float | None], i: int) -> bool:
    if i < 4:
        return False
    window = pcts[i - 4 : i + 1]
    return len(window) == 5 and all(p is not None and p < 0 for p in window)


def _bias6(closes: Sequence[float], i: int) -> float | None:
    if i < 5:
        return None
    ma6 = sum(closes[i - 5 : i + 1]) / 6.0
    if ma6 <= 0:
        return None
    return closes[i] / ma6 - 1.0


def _forward_trade(ts: str, i: int, bars: Sequence[Bar]) -> dict[str, Any] | None:
    """``fwd(hold=5, delay=0)``: next-open buy, close of the 5th session."""
    buy_i = i + 1
    sell_i = buy_i + HOLD_SESSIONS - 1
    if sell_i >= len(bars):
        return None
    buy_bar = bars[buy_i]
    sell_bar = bars[sell_i]
    buy = _finite(buy_bar.open)
    sell = _finite(sell_bar.close)
    if buy is None or buy <= 0 or sell is None:
        return None
    if buy_bar.pct is not None and buy_bar.pct > MAX_ENTRY_PCT:
        # Limit-up entry day: the audit skips the trade entirely.
        return None
    net = sell / buy - 1.0 - COST
    return {
        "ts": ts,
        "signal": bars[i].day,
        "entry": buy_bar.day,
        "exit": sell_bar.day,
        "buy": round(buy, 4),
        "sell": round(sell, 4),
        "net": round(net, 6),
        "win": net > 0,
    }


def panel_events(
    family: str,
    ts: str,
    bars: Sequence[Bar],
    *,
    start: str,
    end: str,
    list_date: str | None = None,
) -> list[dict[str, Any]]:
    """Every passing event for one code inside ``[start, end]`` (signal days).

    Mirrors the audit loop: signals are evaluated from ``i >= 80`` (warm-up),
    the universe/listing filters use the signal date, and the forward trade
    only exists when its exit bar is already in the loaded frame.
    """
    if family not in FAMILIES:
        raise ValueError(f"unknown daily pilot: {family}")
    n = len(bars)
    if n < MIN_BARS:
        return []
    pcts = [b.pct for b in bars]
    closes = [b.close for b in bars]
    rsi = compute_rsi(closes) if family == "h03" else None
    out: list[dict[str, Any]] = []
    for i in range(LOOKBACK_SESSIONS, n):
        bar = bars[i]
        if bar.day < start:
            continue
        if bar.day > end:
            break
        if not listed_enough(list_date, bar.day):
            continue
        amount = bar.amount
        if amount is None or amount < MIN_AMOUNT:
            continue
        pct = bar.pct
        if pct is None or pct >= MAX_SIGNAL_PCT:
            continue
        if family == "h01":
            hit = _five_down(pcts, i)
        elif family == "h02":
            bias = _bias6(closes, i)
            hit = bias is not None and bias < -0.06
        else:
            assert rsi is not None
            hit = rsi[i] is not None and rsi[i] < 30.0
        if not hit:
            continue
        trade = _forward_trade(ts, i, bars)
        if trade is not None:
            out.append(trade)
    return out


# ---------------------------------------------------------------------------
# Read-only DB inputs
# ---------------------------------------------------------------------------


def _read_only_query(sql: str, args: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    """Read-only SELECT in an explicit read-only transaction.

    ``SET TRANSACTION READ ONLY`` (not ``conn.read_only``) keeps the pooled
    connection's default state untouched, so a later writer on the same pool
    slot is unaffected.
    """
    from data_sync_service.db import get_connection

    with get_connection() as conn, conn.transaction():
        conn.execute("SET TRANSACTION READ ONLY")
        with conn.cursor() as cur:
            cur.execute(sql, args)
            return list(cur.fetchall())


def load_universe() -> tuple[list[str], dict[str, str | None]]:
    rows = _read_only_query(
        "SELECT ts_code, market, name, list_date::text, delist_date::text FROM stock_basic"
    )
    return universe_from_rows(rows)


def load_sessions(start: str, end: str) -> list[str]:
    rows = _read_only_query(
        "SELECT cal_date::text FROM trade_calendar "
        "WHERE is_open = 1 AND cal_date BETWEEN %s AND %s ORDER BY cal_date",
        (start, end),
    )
    return [str(r[0])[:10] for r in rows]


def load_bars(
    codes: Sequence[str], since: str, until: str
) -> dict[str, list[Bar]]:
    """Daily bars per code, ordered by trade date (batched, read-only)."""
    bars: dict[str, list[Bar]] = {}
    for start in range(0, len(codes), BAR_BATCH):
        batch = list(codes[start : start + BAR_BATCH])
        rows = _read_only_query(
            "SELECT ts_code, trade_date::text, open, close, pct_chg, amount "
            "FROM daily WHERE ts_code = ANY(%s) AND trade_date BETWEEN %s AND %s "
            "ORDER BY ts_code, trade_date",
            (batch, since, until),
        )
        for ts, day, open_, close, pct, amount in rows:
            open_f = _finite(open_)
            close_f = _finite(close)
            if open_f is None or close_f is None:
                continue
            bars.setdefault(str(ts), []).append(
                Bar(
                    str(day)[:10],
                    open_f,
                    close_f,
                    _finite(pct),
                    _finite(amount),
                )
            )
    return bars


class DailyContext(NamedTuple):
    since: str
    until: str
    bars: dict[str, list[Bar]]
    list_dates: dict[str, str | None]


# One-entry cache: the three pilots share the same bars/universe per run.
_CONTEXT_CACHE: DailyContext | None = None


def _shift_days(day: str, days: int) -> str:
    parsed = _parse_day(day)
    if parsed is None:
        return day
    from datetime import timedelta

    return (parsed + timedelta(days=days)).isoformat()


def load_context(start: str, end: str) -> DailyContext:
    global _CONTEXT_CACHE
    since = _shift_days(start, -LOOKBACK_CALENDAR_DAYS)
    if (
        _CONTEXT_CACHE is not None
        and _CONTEXT_CACHE.until == end
        and _CONTEXT_CACHE.since <= since
    ):
        return _CONTEXT_CACHE
    codes, list_dates = load_universe()
    bars = load_bars(codes, since, end)
    ctx = DailyContext(since=since, until=end, bars=bars, list_dates=list_dates)
    _CONTEXT_CACHE = ctx
    return ctx


def reset_context_cache() -> None:
    """Test helper: drop the shared bars/universe cache."""
    global _CONTEXT_CACHE
    _CONTEXT_CACHE = None


def replay_family(family: str, start: str, end: str) -> dict[str, Any]:
    """Replay one pilot's closed events with signal day in ``[start, end]``."""
    ctx = load_context(start, end)
    if not ctx.bars:
        return {
            "ok": False,
            "reason": f"no daily bars between {ctx.since} and {end}",
            "asOf": end,
            "trades": [],
        }
    trades: list[dict[str, Any]] = []
    for ts, bars in ctx.bars.items():
        trades.extend(
            panel_events(
                family,
                ts,
                bars,
                start=start,
                end=end,
                list_date=ctx.list_dates.get(ts),
            )
        )
    trades.sort(key=lambda t: (t["entry"], t["ts"]))
    return {
        "ok": True,
        "reason": None,
        "asOf": end,
        "trades": trades,
        "codes": len(ctx.bars),
        "since": ctx.since,
    }


# ---------------------------------------------------------------------------
# Frozen prefix sidecar (read-only pkl -> compact tail, never copied to git)
# ---------------------------------------------------------------------------


def _holdout_mean(rows: Sequence[dict[str, Any]]) -> tuple[int, float]:
    vals = [r["net"] for r in rows if _finite(r.get("net")) is not None]
    if not vals:
        return 0, 0.0
    return len(vals), sum(vals) / len(vals)


def build_frozen_prefix(
    *,
    scratch_dir: Path | str | None = None,
    rows: int = PREFIX_ROWS,
    sessions: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Extract the frozen prefix tail + integrity fingerprint (read-only).

    ``sessions`` is injectable for tests; production reads ``trade_calendar``
    (read-only). The audit pkl has no dates, so entry/exit are derived with the
    standard 5-session hold and are display/ordering aids only — the frozen
    windows themselves are never recomputed or rewritten.
    """
    scratch = Path(scratch_dir) if scratch_dir is not None else audit_scratch_dir()
    pkl_path = scratch / FROZEN_PKL
    json_path = scratch / FROZEN_JSON
    with pkl_path.open("rb") as fh:
        raw = pickle.load(fh)  # noqa: S301 - local, trusted audit artifact (read-only)
    toprec = raw.get("toprec") or {}
    daily = _read_json(json_path) if json_path.exists() else {}

    if sessions is None:
        try:
            sessions = load_sessions("2026-07-01", "2026-11-30")
        except Exception as exc:  # noqa: BLE001 - calendar is a display aid here
            logger.warning("factor_live_daily: calendar unavailable: %s", exc)
            sessions = []
    cal = list(sessions)
    cal_idx = {d: i for i, d in enumerate(cal)}

    def _session_after(day: str, offset: int) -> str | None:
        idx = cal_idx.get(day)
        if idx is None or idx + offset >= len(cal):
            return None
        return cal[idx + offset]

    families: dict[str, dict[str, Any]] = {}
    for family in FAMILIES:
        label = LABELS[family]
        by_day = toprec.get(label) or {}
        all_rows: list[dict[str, Any]] = []
        for signal in sorted(by_day):
            for seq, tup in enumerate(by_day[signal]):
                if not tup or len(tup) < 3:
                    continue
                buy = _finite(tup[1])
                sell = _finite(tup[2])
                if buy is None or buy <= 0 or sell is None:
                    continue
                entry = _session_after(signal, 1)
                exit_day = _session_after(entry, HOLD_SESSIONS - 1) if entry else None
                net = sell / buy - 1.0 - COST
                all_rows.append(
                    {
                        "key": f"{family}|{signal}|{seq}",
                        "signal": signal,
                        "entry": entry,
                        "exit": exit_day,
                        "buy": round(buy, 4),
                        "sell": round(sell, 4),
                        "net": round(net, 6),
                        "win": net > 0,
                        "frozen": True,
                    }
                )
        holdout = [
            r for r in all_rows if HOLDOUT_START <= str(r["signal"]) <= HOLDOUT_END
        ]
        n, mean = _holdout_mean(holdout)
        expected = ((daily.get("events") or {}).get(label) or {}).get("holdout") or {}
        expected_n = int(expected.get("n") or 0)
        expected_mean = _finite(expected.get("mean"))
        families[family] = {
            "label": label,
            "rows": all_rows[-rows:] if rows > 0 else all_rows,
            "total": len(all_rows),
            "lastSignal": max((str(r["signal"]) for r in all_rows), default=None),
            "lastEntry": max((str(r["entry"]) for r in all_rows if r["entry"]), default=None),
            "lastExit": max((str(r["exit"]) for r in all_rows if r["exit"]), default=None),
            "holdout": {
                "start": HOLDOUT_START,
                "end": HOLDOUT_END,
                "n": n,
                "mean": round(mean, 6),
                "expectedN": expected_n,
                "expectedMean": expected_mean,
                "ok": bool(
                    expected_n
                    and n == expected_n
                    and expected_mean is not None
                    and abs(mean - expected_mean) < 1e-6
                ),
            },
        }

    return {
        "version": PREFIX_VERSION,
        "generatedAt": datetime.now().isoformat(timespec="seconds"),
        "source": {
            "pkl": str(pkl_path),
            "pklSha256": _sha256_file(pkl_path),
            "dailyJson": str(json_path),
            "dailyJsonSha256": _sha256_file(json_path) if json_path.exists() else None,
            "counts": {family: families[family]["total"] for family in FAMILIES},
        },
        "keepRows": rows,
        "families": families,
    }


def load_frozen_prefix(
    reports_dir: Path | str | None = None,
    *,
    write: bool = True,
    scratch_dir: Path | str | None = None,
    refresh: bool = False,
) -> dict[str, Any] | None:
    """Load (or one-shot rebuild) the frozen-prefix sidecar.

    Returns ``None`` when neither the sidecar nor the audit pkl is usable; the
    caller then replays live-only from ``HOLDOUT_END`` (fail-open).
    """
    path = prefix_path(reports_dir)
    cached: dict[str, Any] | None = None
    if not refresh:
        try:
            raw = _read_json(path)
            if isinstance(raw, dict) and isinstance(raw.get("families"), dict):
                cached = raw
        except (OSError, ValueError):
            cached = None

    scratch = Path(scratch_dir) if scratch_dir is not None else audit_scratch_dir()
    pkl_path = scratch / FROZEN_PKL
    if not pkl_path.exists():
        return cached
    try:
        pkl_sha = _sha256_file(pkl_path)
    except OSError:
        return cached
    if cached is not None:
        src = cached.get("source") or {}
        if src.get("pklSha256") == pkl_sha:
            return cached
    try:
        prefix = build_frozen_prefix(scratch_dir=scratch)
    except Exception as exc:  # noqa: BLE001 - keep yesterday's rows on failure
        logger.warning("factor_live_daily: frozen prefix rebuild failed: %s", exc)
        return cached
    if write:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(prefix, ensure_ascii=False), encoding="utf-8")
        tmp.replace(path)
    return prefix


def family_rows(prefix: dict[str, Any] | None, family: str) -> list[dict[str, Any]]:
    fam = ((prefix or {}).get("families") or {}).get(family) or {}
    rows = fam.get("rows")
    return [r for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []


def last_signal(rows: Sequence[dict[str, Any]]) -> str | None:
    return max((str(r["signal"]) for r in rows if r.get("signal")), default=None)


def last_entry(rows: Sequence[dict[str, Any]]) -> str | None:
    return max((str(r["entry"]) for r in rows if r.get("entry")), default=None)


def last_exit(rows: Sequence[dict[str, Any]]) -> str | None:
    return max((str(r["exit"]) for r in rows if r.get("exit")), default=None)


def prefix_fingerprint(prefix: dict[str, Any] | None, family: str) -> dict[str, Any]:
    """Reference the frozen prefix by path + sha256 + counts (never copy it)."""
    src = (prefix or {}).get("source") or {}
    fam = ((prefix or {}).get("families") or {}).get(family) or {}
    rows = family_rows(prefix, family)
    return {
        "path": src.get("pkl"),
        "sha256": src.get("pklSha256"),
        "entries": fam.get("total"),
        "keptRows": len(rows),
        "lastSignal": fam.get("lastSignal"),
        "lastEntry": fam.get("lastEntry"),
        "lastExit": fam.get("lastExit"),
        "holdoutOk": (fam.get("holdout") or {}).get("ok"),
        "readable": bool(rows),
    }


# ---------------------------------------------------------------------------
# Rolling window (mean % per trade — NOT the additive points sum)
# ---------------------------------------------------------------------------


def _sorted(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda r: (
            str(r.get("exit") or ""),
            str(r.get("entry") or ""),
            str(r.get("key") or ""),
        ),
    )


def trim_appended(
    rows: Sequence[dict[str, Any]], n: int = KEEP_ROWS
) -> list[dict[str, Any]]:
    """Keep the most recent ``n`` closed rows (deterministic, idempotent)."""
    ordered = _sorted([r for r in rows if isinstance(r, dict)])
    if n <= 0 or len(ordered) <= n:
        return ordered
    return ordered[-n:]


def rolling_stats(
    frozen: Sequence[dict[str, Any]],
    appended: Sequence[dict[str, Any]],
    *,
    as_of: str | None,
    n: int | None = None,
) -> dict[str, Any] | None:
    """Last-``n`` closed trades as mean %/trade (``mean(net) * 100``).

    ``net`` is deliberately a mean, not a sum: these bricks are ``%/笔`` and a
    40-trade sum would read ~80 and break the unit. ``percentile`` stays None
    (no recoverable H2 placebo calibration -> never revived on net alone).
    """
    from data_sync_service.service import factor_vault as fv

    window = int(n if n is not None else fv.REVIVAL_N)
    rows = _sorted(
        [
            r
            for r in list(frozen or []) + list(appended or [])
            if isinstance(r, dict) and _finite(r.get("net")) is not None
        ]
    )
    tail = rows[-window:] if window > 0 else rows
    if not tail:
        return None
    vals = [float(r["net"]) for r in tail]
    mean = sum(vals) / len(vals)
    wins = sum(1 for v in vals if v > 0)
    last_exit_day = max((str(r["exit"]) for r in tail if r.get("exit")), default=None)
    last_entry_day = max((str(r["entry"]) for r in tail if r.get("entry")), default=None)
    return {
        "trades": len(tail),
        "net": round(mean * 100.0, 4),
        "win_rate": round(wins / len(tail), 4),
        "percentile": None,
        "as_of": as_of or last_exit_day or last_entry_day,
        "last_trade_as_of": last_exit_day or last_entry_day,
        "first_trade_as_of": tail[0].get("entry"),
        "total_trades": len(rows),
        "unit": "%",
    }
