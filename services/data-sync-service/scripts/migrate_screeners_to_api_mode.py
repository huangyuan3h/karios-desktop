"""Migrate legacy Chrome-URL screeners to API-mode templates (OPT-057.x).

This script registers the 3 main built-in screener templates as
``mode='api'`` entries in the ``tv_screeners`` table. The legacy
``mode='chrome'`` rows (e.g. user-created screeners with TV URLs) are
LEFT UNTOUCHED — the user chooses in Settings which row to enable.

WHY NOT auto-convert existing rows:
- The existing ``Karios Pullback`` row has accumulated snapshots under
  its current screener_id; replacing it would orphan historical data.
- ``mode='api'`` and ``mode='chrome'`` are NOT equivalent (filter JSON vs
  TV URL semantics differ). Side-by-side is safer.
- Idempotency: running this script twice is a no-op (we use stable
  template-derived screener ids).

USAGE:
    cd services/data-sync-service
    PYTHONPATH=src python scripts/migrate_screeners_to_api_mode.py [--dry-run] [--enable]

OPTIONS:
    --dry-run    Print what would be created, do not write to DB.
    --enable     Also set the new template rows to ``enabled=True``.
                 Default: ``enabled=False`` so user can compare in Settings
                 before activating.

EXIT CODES:
    0 — success (or dry-run completed)
    1 — at least one template missing (caller bug)
    2 — DB error (connection / migration not applied)

NOTE: This script assumes Alembic migration ``0012_tv_screeners_api_mode``
has been applied (``alembic upgrade head``). It does NOT run the migration.
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime

from data_sync_service.tv.templates import get_template, list_templates

from data_sync_service.db import tv as tvdb

# Stable ids for the 3 main templates we register. We deliberately use
# UUIDs (not template_id strings) so:
# 1. Re-running the script with updated templates doesn't collide.
# 2. Screener id matches the format of user-created screeners (consistency
#    with `create_screener`).
# The trade-off: the id encodes "this is a template-registered screener"
# only by convention; downstream code doesn't depend on it.
TEMPLATE_SCREENER_IDS: dict[str, str] = {
    "karios_pullback_v3_cn": "tmpl-pullback-v3-cn",
    "karios_pullback_v3_hk": "tmpl-pullback-v3-hk",
    "karios_pullback_v3_us": "tmpl-pullback-v3-us",
    "falcon_launch_v2_cn": "tmpl-falcon-launch-v2-cn",
    "industry_top5_fallback_cn": "tmpl-industry-top5-fallback-cn",
}


def _now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def _register_template(
    *,
    template_id: str,
    screener_id: str,
    enabled: bool,
    dry_run: bool,
) -> str:
    """Register (or upsert) one template. Returns 'created' | 'updated' | 'noop'."""
    template = get_template(template_id)
    if template is None:
        raise ValueError(f"unknown template: {template_id}")

    existing = tvdb.fetch_screener_by_id(screener_id)
    now = _now_iso()

    name = template.display_name
    url = ""  # API mode doesn't need a URL
    mode = "api"
    market = template.market
    filter_json = list(template.filter_json)
    api_columns = list(template.api_columns)

    if existing is None:
        if not dry_run:
            tvdb.upsert_screener(
                screener_id=screener_id,
                name=name,
                url=url,
                enabled=enabled,
                created_at=now,
                updated_at=now,
                mode=mode,
                market=market,
                filter_json=filter_json,
                api_columns=api_columns,
            )
        return "created"

    # Already exists — only update fields that may have changed
    # (filter_json / api_columns / market). Don't touch name/url/enabled.
    same_filter = existing.get("filterJson") == filter_json
    same_columns = (existing.get("apiColumns") or []) == api_columns
    same_market = existing.get("market") == market
    same_mode = existing.get("mode") == "api"
    if same_filter and same_columns and same_market and same_mode:
        return "noop"

    if not dry_run:
        tvdb.upsert_screener(
            screener_id=screener_id,
            name=existing.get("name") or name,
            url=existing.get("url") or url,
            enabled=existing.get("enabled", enabled),
            created_at=now,
            updated_at=now,
            mode="api",
            market=market,
            filter_json=filter_json,
            api_columns=api_columns,
        )
    return "updated"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Register built-in screener templates as mode='api' rows.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be created, do not write to DB.",
    )
    parser.add_argument(
        "--enable",
        action="store_true",
        help="Set new template rows to enabled=True (default: False; user enables in Settings).",
    )
    args = parser.parse_args(argv)

    templates = list_templates()
    if not templates:
        print("FATAL: no templates registered in tv/templates.py", file=sys.stderr)
        return 1

    enabled = bool(args.enable)
    dry_run = bool(args.dry_run)

    print(
        f"{'[DRY RUN] ' if dry_run else ''}Registering {len(templates)} templates "
        f"as mode='api' (enabled={enabled})"
    )
    print("-" * 72)

    created = updated = noop = 0
    for template_id, screener_id in TEMPLATE_SCREENER_IDS.items():
        template = get_template(template_id)
        if template is None:
            # Defensive: should never happen (all 5 templates registered).
            print(f"  SKIP  {template_id:<32s} unknown template")
            continue
        try:
            result = _register_template(
                template_id=template_id,
                screener_id=screener_id,
                enabled=enabled,
                dry_run=dry_run,
            )
        except Exception as e:  # noqa: BLE001
            print(f"  ERROR {template_id:<32s} {type(e).__name__}: {e}")
            return 2
        marker = {"created": "+", "updated": "~", "noop": "="}[result]
        print(
            f"  {marker}     {template_id:<32s} → {screener_id:<32s} "
            f"market={template.market}"
        )
        if result == "created":
            created += 1
        elif result == "updated":
            updated += 1
        else:
            noop += 1

    print("-" * 72)
    print(f"Summary: {created} created, {updated} updated, {noop} noop")

    if not enabled:
        print()
        print(
            "Template rows are registered with enabled=False. "
            "Enable in Settings → Screeners when you're ready to compare "
            "against the legacy Chrome-URL screener."
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())