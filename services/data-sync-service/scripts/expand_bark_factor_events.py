"""Expand the live Bark subscription with factor/S-gap event types (2026-10-25).

Reversible, idempotent: loads webhook_subscriptions id=280, merges
``factor_status_up`` / ``factor_revived`` / ``sgap_revival`` into event_types,
removes the retired ``twin_star_reminder`` slot, keeps ``job_failed`` +
``satellite_action``, and rewrites via the existing ``upsert_subscription``
with the SAME url/secret/provider/enabled (no credential rotation).

Revert: re-run with --restore-twin-star to put ``twin_star_reminder`` back
(and optionally --drop-factor-events to remove the three new types).

Never touches trading tables; only webhook_subscriptions row 280.

Usage:
  cd services/data-sync-service && PYTHONPATH=src python3 scripts/expand_bark_factor_events.py
  cd services/data-sync-service && PYTHONPATH=src python3 scripts/expand_bark_factor_events.py --sub-id 280 --dry-run
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_sync_service.db import webhook as webhook_db  # noqa: E402

SUB_ID = 280
ADD_TYPES = ("factor_status_up", "factor_revived", "sgap_revival")
DROP_TYPES = ("twin_star_reminder",)
KEEP_TYPES = ("job_failed", "satellite_action")


def main() -> int:
    ap = argparse.ArgumentParser(description="Expand Bark sub with factor events")
    ap.add_argument("--sub-id", type=int, default=SUB_ID)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--restore-twin-star", action="store_true")
    ap.add_argument("--drop-factor-events", action="store_true")
    args = ap.parse_args()

    subs = {s["id"]: s for s in webhook_db.list_subscriptions()}
    sub = subs.get(args.sub_id)
    if sub is None:
        print(
            f"subscription id={args.sub_id} not found — "
            "Yuan must create the Bark sub first (Settings → Webhook / Bark)",
            file=sys.stderr,
        )
        return 1
    before = sorted(sub.get("eventTypes") or [])
    after_set = set(before)
    if args.drop_factor_events:
        after_set -= set(ADD_TYPES)
    else:
        after_set |= set(ADD_TYPES)
    if args.restore_twin_star:
        after_set |= set(DROP_TYPES)
    else:
        after_set -= set(DROP_TYPES)
    after_set |= set(KEEP_TYPES)  # never drop the live essentials
    after = sorted(after_set)

    print(f"sub id={sub['id']} provider={sub.get('provider')} enabled={sub.get('enabled')}")
    print(f"before: {before}")
    print(f"after:  {after}")
    if before == after:
        print("no change (idempotent) — nothing to write")
        return 0
    if args.dry_run:
        print("dry-run — not writing")
        return 0
    updated = webhook_db.upsert_subscription(
        url=sub["url"],
        secret=sub["secret"],
        event_types=after,
        enabled=bool(sub.get("enabled", True)),
        provider=sub.get("provider") or "generic",
        sub_id=args.sub_id,
    )
    print(f"updated: {sorted(updated.get('eventTypes') or [])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
