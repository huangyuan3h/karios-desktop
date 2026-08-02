#!/usr/bin/env python3
"""Seed investment-grade news sources + disable legacy generic sources (Track 1).

Idempotent. Safe to run multiple times.

Usage:
    PYTHONPATH=src python scripts/seed_news_sources.py
    PYTHONPATH=src python scripts/seed_news_sources.py --dry-run
    PYTHONPATH=src python scripts/seed_news_sources.py --legacy-disable
"""

from __future__ import annotations

import argparse
import sys

from data_sync_service.db.news import (
    create_source,
    fetch_sources,
    update_source,
)
from data_sync_service.service.news import (
    DEFAULT_NEWS_SOURCES,
    LEGACY_DISABLED_SOURCES,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Show what would change")
    parser.add_argument(
        "--legacy-disable",
        action="store_true",
        help="Also disable legacy generic sources (bbc-world / nyt-world / hn-front / reddit-finance)",
    )
    args = parser.parse_args()

    existing = {s["id"]: s for s in fetch_sources(enabled_only=False)}
    print(f"Current sources ({len(existing)}):")
    for sid, src in sorted(existing.items()):
        print(f"  [{src.get('tier', 'D')}] {sid} ({src['name']}) enabled={src['enabled']}")

    print()
    print(f"Seed plan ({len(DEFAULT_NEWS_SOURCES)} investment-grade sources):")
    changes = []
    for sid, name, _url, tier, category in DEFAULT_NEWS_SOURCES:
        cur = existing.get(sid)
        if cur is None:
            changes.append(("INSERT", sid, name, tier, category))
        elif cur.get("tier") != tier or cur.get("category") != category:
            changes.append(("UPDATE tier/cat", sid, name, tier, category))
        else:
            print(f"  [skip] {sid} already tier={tier} cat={category}")

    if args.legacy_disable:
        for sid in LEGACY_DISABLED_SOURCES:
            if sid in existing:
                changes.append(("DISABLE legacy", sid, existing[sid]["name"], "D", None))

    if not changes:
        print("  (no changes)")
        return 0

    if args.dry_run:
        print()
        print(f"DRY-RUN — would apply {len(changes)} change(s):")
        for ch in changes:
            print(f"  {ch}")
        return 0

    print()
    print(f"Applying {len(changes)} change(s)...")
    for action, sid, name, tier, category in changes:
        if action == "DISABLE legacy":
            update_source(source_id=sid, enabled=False)
            print(f"  ✓ disabled legacy {sid}")
        else:
            create_source(
                source_id=sid,
                name=name,
                url=next(u for s, _, u, _, _ in DEFAULT_NEWS_SOURCES if s == sid),
                enabled=True,
                tier=tier,
                category=category,
            )
            print(f"  ✓ {action} {sid} tier={tier} cat={category}")

    print()
    print("Done. Final state:")
    final = {s["id"]: s for s in fetch_sources(enabled_only=False)}
    print(f"  Total: {len(final)} (enabled: {sum(1 for s in final.values() if s['enabled'])})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
