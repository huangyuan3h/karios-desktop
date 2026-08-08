"""Coverage gate for the L4-Gate COV plan (§8 of l4-gate-audit).

Reads coverage.json produced by pytest-cov and fails the build when:
  - overall coverage is below the requested threshold, or
  - any core module is below its own threshold.

Output (always printed): module × coverage% × missed lines × gap, so a
failed gate is immediately actionable.

Usage:
  python3 scripts/coverage_gate.py                 # overall >= 75 (wave 1)
  python3 scripts/coverage_gate.py --overall 85    # wave 2
  python3 scripts/coverage_gate.py --overall 90    # wave 3
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
COVERAGE_JSON = Path(__file__).resolve().parents[1] / "coverage.json"

# Core business modules that must stay >= CORE_TARGET (wave 1: >=85%).
CORE_MODULES = [
    "service/decision.py",
    "service/correlation.py",
    "service/weekly_review.py",
    "service/backtest_engine.py",
    "service/paper_trading.py",
    "service/watchlist_automation.py",
    "service/execution_source.py",
    "service/trendok.py",
    "service/execution_journal.py",
    "service/research.py",
    "service/exit_attribution.py",
]
CORE_TARGET = 85.0

# Explicitly exempted from any gate (reason in comment; do not silently omit).
EXEMPTED = {
    "tv/capture.py": "ego-lite Chrome capture fallback — retired path (H10 audit)",
}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--overall", type=float, default=75.0, help="overall coverage threshold")
    ap.add_argument("--json", default=str(COVERAGE_JSON), help="path to coverage.json")
    args = ap.parse_args()

    data = json.loads(Path(args.json).read_text())
    files = data["files"]
    overall = data["totals"]["percent_covered"]
    overall_missed = data["totals"]["missing_lines"]

    failures: list[str] = []
    print(f"{'module':<38} {'cov%':>6} {'missed':>7} {'gap':>6}")
    print("-" * 62)
    for mod, target in [("OVERALL", args.overall)]:
        pass
    for fname, fdata in sorted(files.items()):
        short = fname.split("src/data_sync_service/")[-1] if "src/data_sync_service/" in fname else fname
        if "tests" in fname:
            continue
        t = fdata["summary"]
        if short in EXEMPTED:
            print(f"{short:<38} {'EXEMPT':>6} {t['missing_lines']:>7} {t['percent_covered']:>5.1f}%")
            continue
        core = short in CORE_MODULES
        if core:
            gap = max(0.0, CORE_TARGET - t["percent_covered"])
            flag = "CORE" if gap > 0 else "ok"
            print(f"{short:<38} {t['percent_covered']:>5.1f}% {t['missing_lines']:>7} {gap:>6.1f}  {flag}")
            if gap > 0:
                failures.append(f"{short}: {t['percent_covered']:.1f}% < {CORE_TARGET:.0f}% (missed {t['missing_lines']})")

    print("-" * 62)
    print(f"{'OVERALL':<38} {overall:>5.1f}% {overall_missed:>7}")
    if overall < args.overall:
        failures.append(
            f"overall {overall:.1f}% < {args.overall:.0f}% (missed {overall_missed}; need <= {int(data['totals']['num_statements'] * (100 - args.overall) / 100)})"
        )

    if failures:
        print("\nFAIL:")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)
    print("\nGATE PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
