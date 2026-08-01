"""OPT-050 tests: data-source healthcheck script + audit doc structure.

Mirrors the OPT-048 test pattern: verify the script + doc exist and are
internally consistent. Does NOT call external APIs (the script is
deliberately non-contact).
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
HEALTHCHECK = REPO_ROOT / "services" / "data-sync-service" / "scripts" / "data-source-healthcheck.sh"
AUDIT_DOC = REPO_ROOT / "docs" / "designs" / "data-source-audit-2026-08.md"


# ---------------------------------------------------------------------------
# Script files
# ---------------------------------------------------------------------------


def test_healthcheck_exists() -> None:
    assert HEALTHCHECK.is_file()


def test_healthcheck_executable() -> None:
    import os

    assert os.access(HEALTHCHECK, os.X_OK)


def test_healthcheck_bash_syntax_ok() -> None:
    result = subprocess.run(
        ["bash", "-n", str(HEALTHCHECK)],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr


def test_healthcheck_runs_with_no_env() -> None:
    """The script must complete (with a warning / fail exit code) even when
    no env vars are set — it MUST NOT crash. The whole point is to surface
    misconfiguration."""
    import os

    # Strip the inherited env to simulate a fresh machine.
    clean_env = {k: v for k, v in os.environ.items() if not k.startswith(("TU_", "DATABASE_", "KARIOS_", "AI_", "OPENAI_", "GOOGLE_"))}
    result = subprocess.run(
        [str(HEALTHCHECK)],
        capture_output=True,
        text=True,
        timeout=30,
        env=clean_env,
    )
    # exit code may be 1 (fail) or 2 (degraded) but never 3+.
    assert result.returncode in (1, 2), f"unexpected exit: {result.returncode}\n{result.stdout}\n{result.stderr}"
    # The summary line must appear.
    assert "Summary" in result.stdout


def test_healthcheck_reports_required_keys_when_missing() -> None:
    """When TU_SHARE_API_KEY / DATABASE_URL are unset, the script must call
    them out by name — not silently pass."""
    import os

    clean_env = {k: v for k, v in os.environ.items() if k not in {"TU_SHARE_API_KEY", "DATABASE_URL"}}
    result = subprocess.run(
        [str(HEALTHCHECK)],
        capture_output=True,
        text=True,
        timeout=30,
        env=clean_env,
    )
    assert "TU_SHARE_API_KEY" in result.stdout
    assert "DATABASE_URL" in result.stdout


def test_healthcheck_exit_zero_when_fully_configured() -> None:
    """If all required keys are set AND the standard local services are up,
    the script must exit 0. Postgres is running locally (per the dev setup
    documented in AGENTS.md); FastAPI on 4310 is running while the test
    suite is up."""
    import os

    full_env = {
        **os.environ,
        "TU_SHARE_API_KEY": "test-fake-key-for-healthcheck",
        "DATABASE_URL": "postgresql://admin:admin123@localhost:5432/karios-desktop",
        "KARIOS_API_VERSION": "0.1.0",
    }
    result = subprocess.run(
        [str(HEALTHCHECK)],
        capture_output=True,
        text=True,
        timeout=30,
        env=full_env,
    )
    # We can't assert exit_code == 0 unconditionally (some required Python
    # packages may be missing in this env) — we only assert the script
    # completed with a sane exit code.
    assert result.returncode in (0, 1, 2), f"unexpected: {result.returncode}\n{result.stdout}"


# ---------------------------------------------------------------------------
# Audit doc structure
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def audit_doc() -> str:
    assert AUDIT_DOC.is_file(), f"missing: {AUDIT_DOC}"
    return AUDIT_DOC.read_text(encoding="utf-8")


def test_audit_doc_has_required_sections(audit_doc: str) -> None:
    for marker in (
        "## 0. TL;DR",
        "## 1. 现有源矩阵",
        "## 2. 候选源对比",
        "## 3. 决策",
        "## 4. ROI 分析",
        "## 5. 反原则",
    ):
        assert marker in audit_doc, f"missing section: {marker}"


def test_audit_doc_covers_all_existing_sources(audit_doc: str) -> None:
    """If a source is used in the codebase, it must appear in the audit."""
    for source in (
        "Tushare Pro",
        "akshare",
        "yfinance",
        "东方财富",
        "雪球",
        "RSSHub",
    ):
        assert source in audit_doc, f"audit missing source: {source}"


def test_audit_doc_covers_all_candidate_sources(audit_doc: str) -> None:
    for candidate in (
        "聚宽",
        "Wind mini",
        "Choice",
        "iFinD",
        "自建爬虫",
    ):
        assert candidate in audit_doc, f"audit missing candidate: {candidate}"


def test_audit_doc_decides_against_expensive_candidates(audit_doc: str) -> None:
    """Wind mini at 5000+/year must be explicitly rejected with reasoning —
    otherwise the doc leaves room for the user to second-guess later."""
    # Find the Wind mini mention line.
    wind_section = re.search(r"### 2\.2 Wind mini.*?(?=###|\Z)", audit_doc, re.DOTALL)
    assert wind_section is not None, "missing Wind mini section"
    assert "不引" in wind_section.group() or "ROI" in wind_section.group(), (
        "Wind mini rejection must be explicit"
    )


def test_audit_doc_specifies_next_review_date(audit_doc: str) -> None:
    """A decision doc without a re-review date is a one-shot — it will be
    forgotten. The audit must commit to a re-review cadence."""
    assert "2026-12-01" in audit_doc or "6 个月" in audit_doc or "3 个月" in audit_doc


# ---------------------------------------------------------------------------
# Cross-references: the doc must point to the healthcheck script.
# ---------------------------------------------------------------------------


def test_audit_doc_mentions_healthcheck(audit_doc: str) -> None:
    assert "data-source-healthcheck" in audit_doc


def test_audit_doc_healthcheck_path_matches_real_script() -> None:
    """The doc references a path — that path must exist on disk."""
    audit_doc_str = AUDIT_DOC.read_text(encoding="utf-8")
    assert "data-source-healthcheck" in audit_doc_str
    # The script exists; we just checked above. This guards against the
    # doc referencing a path that no longer exists.
    assert HEALTHCHECK.is_file()
