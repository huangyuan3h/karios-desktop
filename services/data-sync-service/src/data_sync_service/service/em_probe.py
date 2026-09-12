"""East Money egress black-box probe (OPT-126).

Five jobs share one egress IP; when it gets banned the failures surface as
silent `skipped`/`ok=False` scattered across jobs. This probe hits each EM
host directly (same path as the business code: ``ProxyHandler({})`` direct,
no proxy) every 10 minutes and raises one high-severity system event +
webhook when a host fails 3 times in a row.

Anti-patterns (locked): probe never touches the proxy; never faster than
10min (self-inflicted ban); failures go to the independent `em_probe`
job_type, never to a business `sync_job_record`.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

JOB_TYPE = "em_probe"
PROBE_TIMEOUT = 10.0
PROBE_FAIL_STREAK_ALERT = 3

# One light GET per host, mirroring the business call shape (params copied
# from the production call sites so a 200 here means the job path works).
PROBE_TARGETS: tuple[dict[str, Any], ...] = (
    {
        "host": "push2.eastmoney.com",
        "url": "https://push2.eastmoney.com/api/qt/clist/get",
        "params": {
            "pn": "1",
            "pz": "1",
            "po": "1",
            "np": "1",
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": "2",
            "invt": "2",
            "fid": "f12",
            "fs": "m:0+t:6",
            "fields": "f12,f14,f2",
        },
        "referer": "https://quote.eastmoney.com/center/gridlist.html",
    },
    {
        "host": "data.eastmoney.com",
        "url": "https://data.eastmoney.com/dataapi/bkzj/getbkzj",
        "params": {"key": "f62", "code": "m:90 t:2"},
        "referer": "https://data.eastmoney.com/bkzj/hy.html",
    },
    {
        "host": "push2his.eastmoney.com",
        "url": "https://push2his.eastmoney.com/api/qt/stock/kline/get",
        "params": {
            "secid": "1.000001",
            "ut": "fa5fd1943c7b386f172bc3b47b105b",
            "fields1": "f1,f2,f3,f4,f5",
            "fields2": "f51,f52,f53,f54,f55",
            "klt": "101",
            "fqt": "1",
            "beg": "20260101",
            "end": "20260110",
            "lmt": "10",
        },
        "referer": "https://quote.eastmoney.com/",
    },
)

_lock = threading.Lock()
_streaks: dict[str, int] = {}
_last_run: dict[str, Any] = {"at": None, "checks": []}


def _probe_host(target: dict[str, Any]) -> dict[str, Any]:
    """Single direct GET; returns {host, ok, ms, error} (never raises)."""
    from data_sync_service.service.em_push2_http import _urllib_get_json

    start = time.monotonic()
    try:
        _urllib_get_json(
            target["url"],
            params=target["params"],
            referer=target["referer"],
            timeout=PROBE_TIMEOUT,
            use_proxy=False,
        )
        return {
            "host": target["host"],
            "ok": True,
            "ms": int((time.monotonic() - start) * 1000),
            "error": None,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "host": target["host"],
            "ok": False,
            "ms": int((time.monotonic() - start) * 1000),
            "error": str(exc)[:300],
        }


def probe_once() -> dict[str, Any]:
    """Run all host checks now (network I/O; isolated per host)."""
    checks = [_probe_host(t) for t in PROBE_TARGETS]
    return {
        "at": time.time(),
        "checks": checks,
        "failed": [c["host"] for c in checks if not c["ok"]],
    }


def run_em_probe() -> dict[str, Any]:
    """Probe + streak accounting + alerting. Returns the run summary.

    A host alert fires exactly once per outage as a system event (when its
    streak REACHES 3 — insert_event has no dedupe), while the webhook emit
    repeats every failing run (emit_event dedupes same-day keys downstream,
    so a missed delivery still gets re-queued). A success resets the streak.
    """
    from data_sync_service.db.system_events import insert_event
    from data_sync_service.db.webhook import emit_event

    result = probe_once()
    with _lock:
        for check in result["checks"]:
            host = check["host"]
            if check["ok"]:
                _streaks[host] = 0
            else:
                _streaks[host] = _streaks.get(host, 0) + 1
        _last_run["at"] = result["at"]
        _last_run["checks"] = result["checks"]
        streaks = dict(_streaks)
        new_outage = [
            c
            for c in result["checks"]
            if not c["ok"] and streaks.get(c["host"]) == PROBE_FAIL_STREAK_ALERT
        ]
        still_down = [
            c
            for c in result["checks"]
            if not c["ok"] and streaks.get(c["host"], 0) > PROBE_FAIL_STREAK_ALERT
        ]
    for check in new_outage:
        host = check["host"]
        title = f"East Money egress failing: {host} ({PROBE_FAIL_STREAK_ALERT}x)"
        try:
            insert_event(
                event_type="em_probe_failed",
                severity="high",
                title=title,
                detail=check["error"] or "",
                payload={"host": host, "streak": PROBE_FAIL_STREAK_ALERT},
                dedupe_key=f"em_probe:{host}",
            )
        except Exception:  # noqa: BLE001 — probe must not die on observability
            logger.warning("em_probe event insert failed for %s", host, exc_info=True)
        logger.warning("em_probe: %s", title)
    for check in new_outage + still_down:
        host = check["host"]
        try:
            emit_event(
                "em_probe_failed",
                {"host": host, "error": check["error"]},
                dedupe_key=f"em_probe:{host}",
            )
        except Exception:  # noqa: BLE001
            logger.warning("em_probe emit failed for %s", host, exc_info=True)
    result["alerted"] = [c["host"] for c in new_outage]
    result["streaks"] = dict(_streaks)
    return result


def probe_status() -> dict[str, Any]:
    """Last-run snapshot for the health endpoint (never raises, no I/O)."""
    try:
        with _lock:
            checks = [dict(c) for c in _last_run.get("checks", [])]
            at = _last_run.get("at")
            streaks = dict(_streaks)
        return {
            "at": at,
            "checks": checks,
            "streaks": streaks,
            "failing": [c["host"] for c in checks if not c["ok"]],
        }
    except Exception:  # noqa: BLE001
        return {"at": None, "checks": [], "streaks": {}, "failing": []}


def reset_probe_state() -> None:
    """Clear streaks/last-run (test helper)."""
    with _lock:
        _streaks.clear()
        _last_run["at"] = None
        _last_run["checks"] = []
