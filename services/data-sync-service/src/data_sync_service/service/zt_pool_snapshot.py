"""Forward-only snapshot of East Money limit-up pools (涨停/炸板/强势/昨日涨停).

AkShare's pool endpoints only retain ~2 weeks, so history cannot be backfilled;
this accumulates per-stock 封板资金 / 首封时间 / 最后封板 / 炸板次数 / 连板数
daily for future research (P0-13 B18 follow-up: what full-day 5min cannot give).

Output: data/zt_pool/zt_pool_YYYYMMDD.csv (one row per pool stock, `pool` column).
Override dir via ``ZT_POOL_DIR`` (tests / deploy).
"""

from __future__ import annotations

import logging
import os
from datetime import date, timedelta
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

POOLS = (
    ("zt", "stock_zt_pool_em"),
    ("zbgc", "stock_zt_pool_zbgc_em"),
    ("strong", "stock_zt_pool_strong_em"),
    ("previous", "stock_zt_pool_previous_em"),
)


def out_dir() -> Path:
    override = os.environ.get("ZT_POOL_DIR")
    if override:
        return Path(override).expanduser()
    return Path(__file__).resolve().parents[3] / "data" / "zt_pool"


def _akshare() -> Any:
    import akshare as ak  # type: ignore[import-not-found]

    return ak


def snapshot_day(day: date, *, force: bool = False) -> dict[str, Any]:
    """Fetch all pools for one day and write one CSV. Never raises."""
    dest = out_dir() / f"zt_pool_{day:%Y%m%d}.csv"
    if dest.exists() and not force:
        return {"ok": True, "skipped": True, "path": str(dest), "rows": 0, "date": day.isoformat()}
    try:
        ak = _akshare()
        frames: list[Any] = []
        errors: list[str] = []
        for pool, attr in POOLS:
            fn = getattr(ak, attr, None)
            if fn is None:
                errors.append(f"missing {attr}")
                continue
            try:
                df = fn(date=day.strftime("%Y%m%d"))
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{attr}: {str(exc)[:120]}")
                continue
            if df is None or getattr(df, "empty", True):
                continue
            df = df.copy()
            df.insert(0, "pool", pool)
            frames.append(df)
        if not frames:
            return {
                "ok": False,
                "error": "no pool data",
                "errors": errors,
                "date": day.isoformat(),
            }
        import pandas as pd

        out = pd.concat(frames, ignore_index=True)
        dest.parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(dest, index=False)
        return {
            "ok": True,
            "skipped": False,
            "path": str(dest),
            "rows": int(len(out)),
            "errors": errors,
            "date": day.isoformat(),
        }
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": str(exc)[:200], "date": day.isoformat()}


def snapshot_recent(days: int = 1, *, force: bool = False) -> dict[str, Any]:
    """Snapshot the last ``days`` calendar days (weekdays only). Never raises."""
    today = date.today()
    saved: list[str] = []
    skipped: list[str] = []
    failed: list[str] = []
    for back in range(max(1, days)):
        d = today - timedelta(days=back)
        if d.weekday() >= 5:
            continue
        res = snapshot_day(d, force=force)
        if res.get("ok") and res.get("skipped"):
            skipped.append(res["date"])
        elif res.get("ok"):
            saved.append(res["date"])
        else:
            failed.append(f"{res['date']}: {res.get('error', 'unknown')}")
    return {
        "ok": True,
        "saved": saved,
        "skipped": skipped,
        "failed": failed,
        "dir": str(out_dir()),
    }
