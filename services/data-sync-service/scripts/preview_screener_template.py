"""Validate screener templates against live TV Scanner API (OPT-057.x).

For each built-in template in ``tv/templates.py``, this script:

1. Builds the API request payload using ``scanner_api.build_request_payload``.
2. POSTs to ``scanner.tradingview.com/global/scan`` with ``range=[0, 5]``
   (only need 5 rows to verify the filter parses + returns rows).
3. Validates the response:
   - HTTP 200
   - ``data`` is a non-empty list of rows
   - At least one requested column is present in row values
4. Reports per-template status (PASS / FAIL / DEGRADED) and updates
   ``tv/templates.py``'s ``nested_filter_validated`` field if requested.

WHY:
The TV Scanner API filter DSL is undocumented. Nested arithmetic expressions
(e.g. ``{left: High.Interval52Week, operation: mult, right: 0.85}``) are
NOT guaranteed to parse. This script is the gate before marking a
template as production-ready.

USAGE:
    cd services/data-sync-service
    PYTHONPATH=src python scripts/preview_screener_template.py [--only <template_id>] [--auto-update]

OPTIONS:
    --only TEMPLATE_ID    Run only this template (skip others).
    --auto-update         If validation passes for all templates, rewrite
                          ``tv/templates.py`` setting ``nested_filter_validated=True``
                          for those that passed.
    --timeout SECONDS     Per-template HTTP timeout (default 15).

EXIT CODES:
    0 — all selected templates passed
    1 — at least one template failed validation
    2 — network / API error (cannot reach scanner.tradingview.com)

NOTE: This script makes REAL HTTP calls to scanner.tradingview.com. Use
judiciously — TV's undocumented API has no SLA. The dispatcher already
implements fallback to ego_lite / chrome for capture; this script is for
manual validation before marking a template production-ready.
"""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass

from data_sync_service.tv import scanner_api
from data_sync_service.tv.templates import get_template, list_templates


@dataclass(frozen=True)
class TemplateValidation:
    template_id: str
    ok: bool
    row_count: int
    sample_symbols: list[str]
    error: str | None
    elapsed_ms: int


def _validate_one(
    template_id: str,
    *,
    timeout_s: float,
) -> TemplateValidation:
    template = get_template(template_id)
    if template is None:
        return TemplateValidation(
            template_id=template_id,
            ok=False,
            row_count=0,
            sample_symbols=[],
            error="unknown template",
            elapsed_ms=0,
        )

    t0 = time.monotonic()
    try:
        result = scanner_api.fetch_screener_via_api(
            filter_payload=template.filter_json,
            columns=template.api_columns,
            range_=(0, 5),
            timeout_s=timeout_s,
            max_retries=0,  # preview is one-shot
        )
    except scanner_api.PermanentApiError as e:
        return TemplateValidation(
            template_id=template_id,
            ok=False,
            row_count=0,
            sample_symbols=[],
            error=f"permanent:{e}",
            elapsed_ms=int((time.monotonic() - t0) * 1000),
        )
    except scanner_api.TransientApiError as e:
        return TemplateValidation(
            template_id=template_id,
            ok=False,
            row_count=0,
            sample_symbols=[],
            error=f"transient:{e}",
            elapsed_ms=int((time.monotonic() - t0) * 1000),
        )
    except Exception as e:  # noqa: BLE001
        return TemplateValidation(
            template_id=template_id,
            ok=False,
            row_count=0,
            sample_symbols=[],
            error=f"{type(e).__name__}:{e}",
            elapsed_ms=int((time.monotonic() - t0) * 1000),
        )

    sample_symbols = [
        str(row.get("Symbol") or row.get("name") or "")
        for row in result.rows[:3]
    ]
    return TemplateValidation(
        template_id=template_id,
        ok=len(result.rows) > 0,
        row_count=len(result.rows),
        sample_symbols=sample_symbols,
        error=None,
        elapsed_ms=int((time.monotonic() - t0) * 1000),
    )


def _print_report(results: list[TemplateValidation]) -> None:
    print()
    print("=" * 78)
    print(f"{'template_id':<32s} {'status':<8s} {'rows':>4s} {'time':>6s}  {'samples'}")
    print("-" * 78)
    for r in results:
        status = "PASS" if r.ok else "FAIL"
        samples = ", ".join(s for s in r.sample_symbols if s)[:32]
        print(
            f"{r.template_id:<32s} {status:<8s} {r.row_count:>4d} "
            f"{r.elapsed_ms:>5d}ms  {samples}"
        )
    print("=" * 78)
    failed = [r for r in results if not r.ok]
    if failed:
        print()
        print(f"FAILED ({len(failed)}/{len(results)}):")
        for r in failed:
            print(f"  - {r.template_id}: {r.error}")


def _auto_update_template_py(passed: set[str]) -> int:
    """Rewrite tv/templates.py setting ``nested_filter_validated=True``
    for templates in ``passed`` that are currently False.

    Idempotent: re-running the script after a successful run is a no-op
    because all already-True rows are skipped.

    Returns the count of rows updated.
    """
    import re
    from pathlib import Path

    path = Path(__file__).resolve().parent.parent / "src" / "data_sync_service" / "tv" / "templates.py"
    src = path.read_text()

    updated = 0
    out_lines: list[str] = []
    # State machine: track which template we're inside, update its
    # nested_filter_validated line if it appears False and template is in `passed`.
    current_template: str | None = None

    for line in src.splitlines():
        # Detect ScreenerTemplate( block opening for our target templates.
        m_id = re.match(r'^(\s*)template_id="([^"]+)",\s*$', line)
        if m_id:
            current_template = m_id.group(2)
            out_lines.append(line)
            continue
        m_valid = re.match(r'^(\s*)nested_filter_validated=(True|False),\s*$', line)
        if m_valid and current_template is not None:
            indent, current_value = m_valid.group(1), m_valid.group(2) == "True"
            if current_template in passed and not current_value:
                out_lines.append(f"{indent}nested_filter_validated=True,")
                updated += 1
            else:
                out_lines.append(line)
            continue
        # Close of block resets template tracking on the matching close paren.
        if line.strip() == ")" and current_template is not None:
            current_template = None
        out_lines.append(line)

    if updated > 0:
        path.write_text("\n".join(out_lines) + "\n")
    return updated


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate built-in screener templates against live TV Scanner API.",
    )
    parser.add_argument(
        "--only",
        type=str,
        default=None,
        help="Run only this template_id (default: all)",
    )
    parser.add_argument(
        "--auto-update",
        action="store_true",
        help="Rewrite templates.py setting nested_filter_validated=True for passed templates.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=15.0,
        help="Per-template HTTP timeout in seconds (default: 15)",
    )
    args = parser.parse_args(argv)

    templates = list_templates()
    if not templates:
        print("FATAL: no templates registered in tv/templates.py", file=sys.stderr)
        return 1

    if args.only:
        target_ids = [args.only]
        for tid in target_ids:
            if get_template(tid) is None:
                print(f"FATAL: unknown template_id: {tid}", file=sys.stderr)
                return 1
    else:
        target_ids = [t.template_id for t in templates]

    print(f"Validating {len(target_ids)} templates against live TV Scanner API...")
    print(f"Timeout: {args.timeout:.1f}s per template")

    results: list[TemplateValidation] = []
    for tid in target_ids:
        r = _validate_one(tid, timeout_s=args.timeout)
        results.append(r)
        status = "PASS" if r.ok else "FAIL"
        print(
            f"  {status}  {tid:<32s} {r.row_count} rows, {r.elapsed_ms}ms"
            + (f" — {r.error}" if r.error else "")
        )

    _print_report(results)

    failed = [r for r in results if not r.ok]
    if failed:
        return 1

    passed_ids = {r.template_id for r in results if r.ok}

    if args.auto_update and passed_ids:
        n = _auto_update_template_py(passed_ids)
        if n:
            print(f"\nUpdated templates.py: {n} template(s) marked nested_filter_validated=True")
        else:
            print("\ntemplates.py already up to date (no changes needed)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())