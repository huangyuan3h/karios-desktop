"""Twin-Star intraday approximate satellite signal.

During the CN session (09:30–15:00) a full A-share clist snapshot is merged
as *today's last bar* so the S-gap screen tracks live prices. After 15:00 the
last snapshot is frozen and served until 09:00 the next calendar morning
(weekend: keep serving the last session file).

Formulas match the frozen S-gap engine. Live also returns limit-up names in
the bucket plus fillable alternates so the user can swap.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from data_sync_service.service.em_push2_http import em_get_json
from data_sync_service.service.state_bucket_track import (
    BUCKET_Q,
    MAX_POS,
    R_WIDE_THRESHOLD,
    _day_features,
    _load_calendar,
    _load_mv,
    _load_rows,
    select_live_gap_picks,
)
from data_sync_service.service.twin_star_daily import fill_candidate_names

logger = logging.getLogger(__name__)

TOP_N = MAX_POS
# Habit recipe (sat-exit-hhmm 2026-09-03): C1 3% + day-3 14:30 sell.
# Mirrors state_bucket_track replay params: fill_mode=same_1430, fill_hhmm=1430,
# max_open_to_1430_pct=0.03, exit_hhmm=1430. Frozen T-open engine untouched.
HABIT_C1_PCT = 0.03
HABIT_FILL_MODE = "same_1430"
HABIT_FILL_HHMM = "1430"
HABIT_EXIT_HHMM = "1430"
WARMUP_DAYS = 120
SNAPSHOT_PAGE_SIZE = 200
SNAPSHOT_MAX_PAGES = 40
_EM_SPOT_URLS = (
    "https://push2delay.eastmoney.com/api/qt/clist/get",
    "https://push2.eastmoney.com/api/qt/clist/get",
)
_EM_SPOT_URL = _EM_SPOT_URLS[1]
_EM_REFERER = "https://quote.eastmoney.com/center/gridlist.html"
# A-share markets: 深主板 m:0+t:6 · 创业板 m:0+t:80 · 沪主板 m:1+t:2 · 科创板 m:1+t:23
_EM_A_SHARE_FS = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23"
_EM_FIELDS = ",".join(["f12", "f13", "f14", "f2", "f15", "f16", "f17", "f18", "f6"])

_CACHE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))),
    "data",
    "twin_star_intraday",
)
CN_TZ = ZoneInfo("Asia/Shanghai")
LIVE_TAPE_START_MIN = 9 * 60 + 30
LIVE_TAPE_END_MIN = 15 * 60
# 12:30 is the first snapshot the 14:30 fill must have; missing after this is a
# hard failure (East Money hang = no satellite that day).
SNAPSHOT_EXPECT_MIN = 12 * 60 + 30
SNAPSHOT_LIVE_STALE_SEC = 20 * 60
REFRESH_MAX_AGE_SEC = 60
_REFRESH_LOCK = threading.Lock()


def _cache_path(today: date) -> str:
    return os.path.join(_CACHE_DIR, f"{today.isoformat()}.json")


def _em_snapshot_params(page_number: int) -> dict[str, str]:
    return {
        "pn": str(page_number),
        "pz": str(SNAPSHOT_PAGE_SIZE),
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "wbp2u": "|0|0|0|web",
        "fid": "f12",
        "fs": _EM_A_SHARE_FS,
        "fields": _EM_FIELDS,
        "_": str(int(time.time() * 1000)),
    }


def _em_snapshot_request(params: dict[str, str]) -> dict[str, Any]:
    errors: list[str] = []
    for url in _EM_SPOT_URLS:
        try:
            return em_get_json(url, params=params, referer=_EM_REFERER)
        except Exception as exc:  # noqa: BLE001
            host = url.split("//", 1)[-1].split("/", 1)[0]
            errors.append(f"{host}:{exc}")
    raise RuntimeError("; ".join(errors))


def fetch_market_snapshot() -> dict[str, dict[str, Any]]:
    """Full A-share quote snapshot: {ts_code: {open, high, low, close, pre_close, amount, name?}}.

    ``close`` = the price at snapshot time (12:30 lunch break → simulated close).
    """
    out: dict[str, dict[str, float | None]] = {}
    total = None
    for page in range(1, SNAPSHOT_MAX_PAGES + 1):
        j = _em_snapshot_request(_em_snapshot_params(page))
        data = (j or {}).get("data") or {}
        if total is None:
            total = int(data.get("total") or 0)
        diff = data.get("diff") or []
        if not diff:
            break
        for row in diff:
            code = str(row.get("f12") or "")
            market = str(row.get("f13") or "")
            if not code or code.startswith(("BJ", "SH", "SZ")):
                continue
            exch = "SH" if market == "1" else "SZ"
            ts = f"{code}.{exch}"
            close = _f(row.get("f2"))
            pre_close = _f(row.get("f18"))
            if close is None or close <= 0 or pre_close is None or pre_close <= 0:
                continue
            name_raw = row.get("f14")
            name = str(name_raw).strip() if name_raw not in (None, "", "-") else ""
            packed: dict[str, Any] = {
                "open": _f(row.get("f17")),
                "high": _f(row.get("f15")),
                "low": _f(row.get("f16")),
                "close": close,
                "pre_close": pre_close,
                "amount": _f(row.get("f6")),
            }
            if name:
                packed["name"] = name
            out[ts] = packed
        if total and len(out) >= total:
            break
    logger.info("twin_star_intraday: snapshot %s rows", len(out))
    return out


def _is_trading_day(day: str) -> bool:
    """True when the trade_calendar says the SH exchange is open that day.

    The daily table has no bar for the current day intraday, so the calendar
    table (not `_load_calendar`) is the source of truth here.
    """
    try:
        import psycopg

        from data_sync_service.config import get_settings

        conn = psycopg.connect(get_settings().database_url)
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT is_open FROM trade_calendar WHERE exchange='SSE' AND cal_date=%s",
                (day,),
            )
            row = cur.fetchone()
            return bool(row and row[0])
        finally:
            conn.close()
    except Exception:  # noqa: BLE001
        return True


def _f(val: Any) -> float | None:
    try:
        if val is None or val == "-":
            return None
        return float(val)
    except (TypeError, ValueError):
        return None


def now_cn() -> datetime:
    return datetime.now(CN_TZ)


def session_date(now: datetime | None = None) -> date:
    """Session the live tape belongs to.

    Before 09:00 Asia/Shanghai the previous calendar date is still "last
    session" so overnight analysis keeps yesterday's freeze.
    """
    now = now or now_cn()
    if now.tzinfo is None:
        now = now.replace(tzinfo=CN_TZ)
    else:
        now = now.astimezone(CN_TZ)
    if now.hour < 9:
        return (now.date() - timedelta(days=1))
    return now.date()


def in_live_tape_window(now: datetime | None = None) -> bool:
    now = now or now_cn()
    if now.tzinfo is None:
        now = now.replace(tzinfo=CN_TZ)
    else:
        now = now.astimezone(CN_TZ)
    from data_sync_service.service.trade_calendar_utils import is_non_trading_day

    if is_non_trading_day(now.date()):
        return False
    mins = now.hour * 60 + now.minute
    return LIVE_TAPE_START_MIN <= mins <= LIVE_TAPE_END_MIN


def _is_cn_session_day(day: date) -> bool:
    """True when ``day`` is on the A-share calendar (fail-open on load error)."""
    try:
        cal = _load_calendar(day.isoformat(), day.isoformat())
        return bool(cal)
    except Exception:  # noqa: BLE001
        return day.weekday() < 5


def intraday_snapshot_status(*, now: datetime | None = None) -> dict[str, Any]:
    """Whether today's East Money tape is usable for the 14:30 satellite fill.

    Required after 12:30 on a weekday session. Overnight / weekend / holidays
    do not cry stale. A lookback file from yesterday is *not* today's snapshot.
    """
    now = now or now_cn()
    if now.tzinfo is None:
        now = now.replace(tzinfo=CN_TZ)
    else:
        now = now.astimezone(CN_TZ)
    session = session_date(now)
    mins = now.hour * 60 + now.minute
    weekend = now.weekday() >= 5
    after_expect = mins >= SNAPSHOT_EXPECT_MIN
    session_day = False if weekend else _is_cn_session_day(session)
    required = session_day and after_expect
    cached = _read_cache(session)
    snapshot_at = cached.get("snapshotAt") if isinstance(cached, dict) else None
    age = snapshot_age_seconds(cached, now) if cached else None
    missing = required and cached is None
    stale = False
    reason: str | None = None
    if missing:
        stale = True
        reason = "no_session_snapshot"
    elif (
        required
        and in_live_tape_window(now)
        and age is not None
        and age > SNAPSHOT_LIVE_STALE_SEC
    ):
        stale = True
        reason = "snapshot_stale"
    return {
        "ok": not missing and not stale,
        "session": session.isoformat(),
        "missing": missing,
        "stale": stale,
        "snapshotAt": snapshot_at if isinstance(snapshot_at, str) else None,
        "ageSeconds": age,
        "reason": reason,
        "required": required,
        "sessionDay": session_day,
    }


def snapshot_age_seconds(sat: dict[str, Any] | None, now: datetime | None = None) -> float | None:
    if not sat:
        return None
    raw = sat.get("snapshotAt")
    if not isinstance(raw, str) or not raw:
        return None
    try:
        ts = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=CN_TZ)
    now = now or now_cn()
    return (now.astimezone(CN_TZ) - ts.astimezone(CN_TZ)).total_seconds()


def build_intraday_sat(today: date | None = None) -> dict[str, Any] | None:
    """Satellite screen using a full-market snapshot as today's last bar.

    Returns the same shape as twin_star_daily._sat_signal plus ``approx: True``,
    or None when the snapshot / data is unavailable.
    """
    today = today or session_date()
    w_start = (today - timedelta(days=WARMUP_DAYS)).isoformat()
    try:
        snapshot = fetch_market_snapshot()
        if not snapshot:
            logger.warning("twin_star_intraday: empty snapshot for %s", today)
            return None
        cal = _load_calendar(w_start, today.isoformat())
        per_ts = _load_rows(w_start, (today - timedelta(days=1)).isoformat())
        mv_map = _load_mv(w_start, (today - timedelta(days=1)).isoformat())
    except Exception as exc:  # noqa: BLE001
        logger.warning("twin_star_intraday: data load failed for %s (%s)", today, exc)
        return None
    if not cal:
        return None
    today_s = today.isoformat()
    if not _is_trading_day(today_s):
        logger.info("twin_star_intraday: %s not a trading day", today_s)
        return None

    # Merge the snapshot as today's approximate bar.
    mv_latest = sorted(mv_map.keys())[-1] if mv_map else None
    for ts, row in snapshot.items():
        series = per_ts.setdefault(ts, [])
        series.append(
            {
                "date": today_s,
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "pre_close": row["pre_close"],
                "amount": row["amount"],
            }
        )
        if mv_latest is not None:
            mv = mv_map.get(mv_latest, {}).get(ts)
            if mv is not None:
                mv_map.setdefault(today_s, {})[ts] = float(mv)

    date_idx = {ts: {r["date"]: i for i, r in enumerate(series)} for ts, series in per_ts.items()}
    day_all, breadth = _day_features(per_ts, mv_map, cal, today_s, date_idx)

    def _limit_locked(ts: str) -> bool:
        di = date_idx.get(ts, {}).get(today_s, -1)
        if di < 0:
            return False
        r = per_ts.get(ts, [])[di]
        pc = r.get("pre_close")
        if not pc or pc <= 0:
            return False
        lim = 0.20 if str(ts).startswith(("3", "68")) else 0.10
        return float(r["close"]) >= pc * (1 + lim - 0.004)

    gap_stocks = [(ts, d["amp"], d["gap"]) for ts, d in day_all.items() if d["is_gap"]]
    locked = {ts for ts, _amp, _gap in gap_stocks if _limit_locked(ts)}
    picks = select_live_gap_picks(
        gap_stocks, locked, bucket_q=BUCKET_Q, top_n=TOP_N
    )

    def _pack(rows: list, *, blocked: bool = False) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for ts, amp, gap in rows:
            series = per_ts.get(ts)
            idx = date_idx.get(ts, {}).get(today_s, -1)
            close = series[idx]["close"] if idx >= 0 and series else None
            open_px = series[idx]["open"] if idx >= 0 and series else None
            snap_name = snapshot.get(ts, {}).get("name")
            run_up = None
            try:
                if close and open_px and float(open_px) > 0:
                    run_up = float(close) / float(open_px) - 1.0
            except (TypeError, ValueError):
                run_up = None
            item: dict[str, Any] = {
                "ts": ts,
                "amp": round(float(amp) * 100, 2),
                "gapPct": round(float(gap) * 100, 2),
                "close": float(close) if close else None,
                "limitLocked": blocked,
                "openPx": float(open_px) if open_px else None,
                "runUpPct": round(run_up * 100, 2) if run_up is not None else None,
            }
            if isinstance(snap_name, str) and snap_name.strip():
                item["name"] = snap_name.strip()
            out.append(item)
        return out

    snapshot_at = datetime.now(CN_TZ).isoformat(timespec="seconds")
    raw_candidates = _pack(picks["primary"])
    # Habit C1 (sat-entry-c1 2026-09-03): skip when 14:30/open-1 > 3%.
    # Strict pool: no refill from worse ranks; idle notional stays with core.
    candidates: list[dict[str, Any]] = []
    skipped_c1: list[dict[str, Any]] = []
    for row in raw_candidates:
        run_up = None
        try:
            px = row.get("close")
            op = row.get("openPx")
            if px and op and float(op) > 0:
                run_up = float(px) / float(op) - 1.0
        except (TypeError, ValueError):
            run_up = None
        if run_up is not None and run_up > HABIT_C1_PCT:
            skipped_c1.append({**row, "skipReason": "skip_1430_run"})
        else:
            candidates.append(row)
    blocked = _pack(picks["blocked"], blocked=True)
    alternates = _pack(picks["alternates"])
    fill_candidate_names(candidates, blocked, alternates, skipped_c1)
    return {
        "asOf": today_s,
        "gateOpen": breadth > R_WIDE_THRESHOLD,
        "breadth": round(float(breadth), 3),
        "gapCount": len(gap_stocks),
        "candidates": candidates,
        "blocked": blocked,
        "alternates": alternates,
        "skippedC1": skipped_c1,
        "skippedC1Count": len(skipped_c1),
        "entryFilter": "c1_3pct",
        "exitHhmm": HABIT_EXIT_HHMM,
        "approx": True,
        "snapshotAt": snapshot_at,
        "note": None,
        "frozen": False,
    }


def _read_cache(day: date) -> dict[str, Any] | None:
    path = _cache_path(day)
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def cache_intraday_sat(sat: dict[str, Any], today: date | None = None) -> None:
    today = today or session_date()
    os.makedirs(_CACHE_DIR, exist_ok=True)
    with open(_cache_path(today), "w", encoding="utf-8") as fh:
        json.dump(sat, fh, ensure_ascii=False)


def load_intraday_sat(
    today: date | None = None,
    *,
    now: datetime | None = None,
    lookback: bool = True,
) -> dict[str, Any] | None:
    """Load the session cache. ``lookback`` walks back up to 7 days (overnight / weekend)."""
    now = now or now_cn()
    today = today or session_date(now)
    sat = _read_cache(today)
    if sat is not None or not lookback:
        return sat
    for i in range(1, 8):
        prev = _read_cache(today - timedelta(days=i))
        if prev is not None:
            return {**prev, "heldOvernight": True}
    return None


def maybe_refresh_intraday_sat(
    *,
    force: bool = False,
    now: datetime | None = None,
) -> dict[str, Any] | None:
    """Refresh the live tape at most once a minute in-session; freeze after close.

    Overnight (before 09:00) and after 15:00: return the last cached session
    without hitting East Money.
    """
    now = now or now_cn()
    session = session_date(now)
    cached_today = _read_cache(session)

    if not force and not in_live_tape_window(now):
        return cached_today or load_intraday_sat(session, now=now)

    age = snapshot_age_seconds(cached_today, now)
    path = _cache_path(session)
    mtime_age: float | None = None
    try:
        mtime_age = time.time() - os.path.getmtime(path)
    except OSError:
        pass
    fresh = (age is not None and 0 <= age < REFRESH_MAX_AGE_SEC) or (
        mtime_age is not None and 0 <= mtime_age < REFRESH_MAX_AGE_SEC
    )
    if not force and cached_today and fresh:
        return cached_today

    with _REFRESH_LOCK:
        cached_today = _read_cache(session)
        age = snapshot_age_seconds(cached_today, now)
        try:
            mtime_age = time.time() - os.path.getmtime(path)
        except OSError:
            mtime_age = None
        fresh = (age is not None and 0 <= age < REFRESH_MAX_AGE_SEC) or (
            mtime_age is not None and 0 <= mtime_age < REFRESH_MAX_AGE_SEC
        )
        if not force and cached_today and fresh:
            return cached_today
        sat = build_intraday_sat(session)
        if sat is None:
            return cached_today or load_intraday_sat(session, now=now)
        mins = now.hour * 60 + now.minute
        if mins >= LIVE_TAPE_END_MIN:
            sat["frozen"] = True
            sat["note"] = "收盘冻结 · 保留至次日 09:00"
        cache_intraday_sat(sat, session)
        return sat