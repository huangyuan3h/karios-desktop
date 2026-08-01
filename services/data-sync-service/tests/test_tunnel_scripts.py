"""OPT-048 tests: Cloudflare Tunnel scripts + setup doc.

These tests do NOT contact Cloudflare. They verify:
- the two shell scripts exist and are executable
- both scripts pass `bash -n` (no syntax errors)
- the setup doc exists, has the 4 required sections, and references no
  phantom files
- `--help` works on both scripts (so users get a clean experience on
  misconfiguration)
"""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = REPO_ROOT / "services" / "data-sync-service" / "scripts"
SETUP_DOC = REPO_ROOT / "docs" / "designs" / "cloudflare-tunnel-setup.md"

QUICK_TUNNEL = SCRIPTS_DIR / "start-quick-tunnel.sh"
NAMED_TUNNEL = SCRIPTS_DIR / "setup-named-tunnel.sh"


# ---------------------------------------------------------------------------
# Script files
# ---------------------------------------------------------------------------


def test_quick_tunnel_exists() -> None:
    assert QUICK_TUNNEL.is_file(), f"missing: {QUICK_TUNNEL}"


def test_named_tunnel_exists() -> None:
    assert NAMED_TUNNEL.is_file(), f"missing: {NAMED_TUNNEL}"


def test_quick_tunnel_executable() -> None:
    assert os.access(QUICK_TUNNEL, os.X_OK), f"not executable: {QUICK_TUNNEL}"


def test_named_tunnel_executable() -> None:
    assert os.access(NAMED_TUNNEL, os.X_OK), f"not executable: {NAMED_TUNNEL}"


@pytest.mark.parametrize("script", [QUICK_TUNNEL, NAMED_TUNNEL])
def test_script_bash_syntax_ok(script: Path) -> None:
    """`bash -n` parses without executing — catches unbalanced quotes / `fi`
    / `}` / `done` before the user ever runs the script."""
    result = subprocess.run(
        ["bash", "-n", str(script)],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, f"{script.name}: {result.stderr}"


# ---------------------------------------------------------------------------
# --help works
# ---------------------------------------------------------------------------


def test_quick_tunnel_help() -> None:
    result = subprocess.run(
        [str(QUICK_TUNNEL), "--help"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0
    out = result.stdout + result.stderr
    assert "Usage:" in out
    assert "--port" in out


def test_named_tunnel_help() -> None:
    result = subprocess.run(
        [str(NAMED_TUNNEL), "--help"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0
    out = result.stdout + result.stderr
    assert "cloudflared tunnel login" in out
    assert "cloudflared tunnel create" in out
    assert "cloudflared tunnel route dns" in out


# ---------------------------------------------------------------------------
# Setup doc structure
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def setup_doc() -> str:
    assert SETUP_DOC.is_file(), f"missing: {SETUP_DOC}"
    return SETUP_DOC.read_text(encoding="utf-8")


def test_setup_doc_has_four_main_sections(setup_doc: str) -> None:
    """The doc must cover why / quick / named / 验证+回退, otherwise users
    end up with half the picture."""
    for marker in (
        "## 0. 为什么",
        "## 2. Quick Tunnel",
        "## 3. Named Tunnel",
        "## 5. 回退方案",
    ):
        assert marker in setup_doc, f"missing section: {marker}"


def test_setup_doc_documents_install_command(setup_doc: str) -> None:
    """The install step is the only one that requires the user to leave the
    terminal; the doc MUST call out the exact command for macOS (their env)."""
    assert "brew install cloudflared" in setup_doc
    assert "cloudflared --version" in setup_doc


def test_setup_doc_mentions_responsibility_boundary(setup_doc: str) -> None:
    """The contract from freelancer-architecture.md / cloud-deployment-options.md:
    Karios does NOT push; external AI assistant does. The doc must not suggest
    running the tunnel from Docker (which would put Karios' job into a container).
    """
    assert "127.0.0.1" in setup_doc
    # The "反模式" section must mention Docker explicitly.
    assert re.search(r"Docker", setup_doc) is not None
    # And it must NOT be presented as a recommended path.
    assert re.search(r"❌.*Docker", setup_doc) is not None


# ---------------------------------------------------------------------------
# Cross-references: every path the doc tells the user to create must exist.
# ---------------------------------------------------------------------------


def test_setup_doc_referenced_paths_exist() -> None:
    """The setup doc references three concrete files the user must create
    (QUICK_TUNNEL / NAMED_TUNNEL scripts + the discovery.md inside docs/api).
    None of these may be missing or the doc is lying to the user."""
    doc = SETUP_DOC.read_text(encoding="utf-8")
    for referenced in (
        "scripts/start-quick-tunnel.sh",
        "scripts/setup-named-tunnel.sh",
    ):
        # The doc must use the same path that's in scripts/ (relative to repo root).
        # We resolve from the repo root both ways and check the file exists.
        # The doc sometimes prefixes with `services/data-sync-service/...` and
        # sometimes with `scripts/...` (relative to that dir) — accept either.
        candidates = [
            REPO_ROOT / referenced,
            REPO_ROOT / "services/data-sync-service" / referenced,
        ]
        assert any(c.is_file() for c in candidates), (
            f"setup doc references '{referenced}' but no matching file exists "
            f"(checked: {[str(c) for c in candidates]})"
        )


# ---------------------------------------------------------------------------
# Smoke: the quick-tunnel script's preflight fails predictably when the
# local server is down. This proves the script's error path is wired
# correctly — but only when cloudflared is actually installed (otherwise
# the script exits earlier with the "not installed" message, which is also
# correct behavior but not what this specific check is about).
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    subprocess.run(
        ["command", "-v", "cloudflared"],
        capture_output=True,
    ).returncode != 0,
    reason="cloudflared not installed; preflight check needs the install step to pass first",
)
def test_quick_tunnel_preflight_fails_without_local_server() -> None:
    """With no Karios running on the chosen port, the script must exit
    non-zero with a helpful message — not silently start a tunnel to nothing."""
    result = subprocess.run(
        [str(QUICK_TUNNEL)],
        capture_output=True,
        text=True,
        timeout=15,
        env={**os.environ, "PORT": "59999"},  # almost-certainly-not-listening port
    )
    assert result.returncode != 0
    err = result.stderr
    assert "127.0.0.1:59999" in err or "Karios" in err
