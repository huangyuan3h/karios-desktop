"""OPT-056 tests: Docker one-click bringup + UPS recovery.

These tests verify the SHELL SCRIPTS and SUPPORTING ARTIFACTS exist and are
well-formed. They do NOT actually start Docker — that requires Docker Desktop
to be running and would be slow + flaky in CI.

What we test:
- All 6 new shell scripts exist and are executable.
- All 6 pass `bash -n` (catches unbalanced quotes / fi / done).
- All 6 support `--help` returning Usage.
- The generated LaunchAgent plist XML is valid via `plutil -lint` (skipped on
  Linux).
- `docker compose config` parses the new compose file without error.
- `.env.example` contains all required keys referenced by docker-compose.yml.
- All 3 service Dockerfiles exist + use a pinned base image (no `:latest`).
- The desktop-ui nginx.conf exists and contains the expected reverse-proxy
  locations.
- `scripts/install-launchd.sh` has a `--help` short-circuit so running it
  twice doesn't re-install the LaunchAgent.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = REPO_ROOT / "scripts"
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"
ENV_EXAMPLE = REPO_ROOT / ".env.example"
DOCKERIGNORE = REPO_ROOT / ".dockerignore"
SETUP_DOC = REPO_ROOT / "docs" / "setup" / "docker-one-click.md"

DOCKER_UP = SCRIPTS_DIR / "docker-up.sh"
DOCKER_DOWN = SCRIPTS_DIR / "docker-down.sh"
DOCKER_STATUS = SCRIPTS_DIR / "docker-status.sh"
INSTALL_LAUNCHD = SCRIPTS_DIR / "install-launchd.sh"
UNINSTALL_LAUNCHD = SCRIPTS_DIR / "uninstall-launchd.sh"
UPS_SHUTDOWN = SCRIPTS_DIR / "ups-shutdown.sh"

DATASYNC_DOCKERFILE = REPO_ROOT / "services" / "data-sync-service" / "Dockerfile"
DATASYNC_DOCKERIGNORE = REPO_ROOT / "services" / "data-sync-service" / ".dockerignore"
AI_DOCKERFILE = REPO_ROOT / "apps" / "ai-service" / "Dockerfile"
AI_DOCKERIGNORE = REPO_ROOT / "apps" / "ai-service" / ".dockerignore"
DESKTOP_DOCKERFILE = REPO_ROOT / "apps" / "desktop-ui" / "Dockerfile"
DESKTOP_NGINX = REPO_ROOT / "apps" / "desktop-ui" / "nginx.conf"


# ---------------------------------------------------------------------------
# Required scripts
# ---------------------------------------------------------------------------

ALL_SCRIPTS = [
    DOCKER_UP,
    DOCKER_DOWN,
    DOCKER_STATUS,
    INSTALL_LAUNCHD,
    UNINSTALL_LAUNCHD,
    UPS_SHUTDOWN,
]


@pytest.mark.parametrize("script", ALL_SCRIPTS)
def test_script_exists(script: Path) -> None:
    assert script.is_file(), f"missing: {script}"


@pytest.mark.parametrize("script", ALL_SCRIPTS)
def test_script_executable(script: Path) -> None:
    assert os.access(script, os.X_OK), f"not executable: {script}"


@pytest.mark.parametrize("script", ALL_SCRIPTS)
def test_script_bash_syntax_ok(script: Path) -> None:
    """`bash -n` parses without executing — catches unbalanced quotes / fi
    / done before the user ever runs the script."""
    result = subprocess.run(
        ["bash", "-n", str(script)],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, f"{script.name}: {result.stderr}"


# ---------------------------------------------------------------------------
# --help on every script
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("script", ALL_SCRIPTS)
def test_script_help(script: Path) -> None:
    """`--help` returns 0 and prints Usage:. install-launchd.sh is a special
    case (exits early on --help, doesn't actually install)."""
    result = subprocess.run(
        [str(script), "--help"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, f"{script.name}: {result.stderr}"
    out = result.stdout + result.stderr
    assert "Usage:" in out, f"{script.name}: missing Usage: in --help output"


# ---------------------------------------------------------------------------
# LaunchAgent plist validity (macOS only)
# ---------------------------------------------------------------------------


def test_launchd_help_short_circuits() -> None:
    """install-launchd.sh must NOT actually install when --help is passed.
    We just check exit code + Usage — full plutil check is in test_launchd_xml below."""
    result = subprocess.run(
        [str(INSTALL_LAUNCHD), "--help"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0
    assert "Usage:" in (result.stdout + result.stderr)


@pytest.mark.skipif(shutil.which("plutil") is None, reason="plutil only on macOS")
def test_launchd_xml_template_is_valid(tmp_path: Path) -> None:
    """Validate the XML we generate inside install-launchd.sh by extracting
    the heredoc and writing it to a temp file, then running plutil -lint.

    Why: writing the plist via heredoc is the easiest way to ship the script
    without bundling XML separately. We can still lint the generated XML.
    """
    # Read install-launchd.sh, find the heredoc lines, write to temp plist.
    text = INSTALL_LAUNCHD.read_text(encoding="utf-8")
    # Match "cat > $PLIST_PATH <<EOF" ... "EOF" (the first heredoc block).
    m = re.search(r"cat\s*>\s*\"\$PLIST_PATH\"\s*<<'?EOF'?\s*\n(.*?)\nEOF", text, re.DOTALL)
    assert m, "could not find plist heredoc in install-launchd.sh"
    xml = m.group(1)
    # Substitute the variable references so plutil sees a complete plist.
    xml = xml.replace("${LABEL}", "com.karios.docker-up")
    xml = xml.replace("${DOCKER_UP}", "/tmp/docker-up.sh")
    xml = xml.replace("${LOG_DIR}", "/tmp/karios-logs")
    xml = xml.replace("${ROOT}", "/tmp/repo")

    plist = tmp_path / "com.karios.docker-up.plist"
    plist.write_text(xml, encoding="utf-8")
    result = subprocess.run(
        ["plutil", "-lint", str(plist)],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, f"plutil -lint failed: {result.stderr}\n{xml}"


# ---------------------------------------------------------------------------
# docker-compose.yml
# ---------------------------------------------------------------------------


def test_compose_file_exists() -> None:
    assert COMPOSE_FILE.is_file()


@pytest.mark.skipif(shutil.which("docker") is None, reason="docker not installed")
def test_compose_config_parses() -> None:
    """`docker compose config` resolves the file (with .env) and exits 0.

    Skips if Docker isn't installed (e.g., on a CI runner without Docker).
    """
    result = subprocess.run(
        ["docker", "compose", "config", "--quiet"],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, f"compose config failed: {result.stderr}"


def test_compose_declares_required_services() -> None:
    """All 4 new services must be declared: data-sync, ai-service, desktop-ui,
    migrate (plus the existing postgres / pgadmin / rsshub)."""
    text = COMPOSE_FILE.read_text(encoding="utf-8")
    for svc in [
        "postgres:",
        "pgadmin:",
        "rsshub:",
        "data-sync:",
        "ai-service:",
        "desktop-ui:",
        "migrate:",
    ]:
        assert svc in text, f"missing service {svc} in docker-compose.yml"


def test_compose_data_sync_binds_zero_oh_oh_oh() -> None:
    """data-sync MUST listen on 0.0.0.0 in the container (otherwise the
    healthcheck from the host can't reach it)."""
    text = COMPOSE_FILE.read_text(encoding="utf-8")
    assert re.search(r"data-sync:.*\n.*\n\s+HOST:\s*0\.0\.0\.0", text, re.DOTALL), (
        "data-sync service must set HOST=0.0.0.0 in compose env"
    )


# ---------------------------------------------------------------------------
# .env.example
# ---------------------------------------------------------------------------


def test_env_example_exists() -> None:
    assert ENV_EXAMPLE.is_file()


@pytest.mark.parametrize(
    "key",
    [
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "POSTGRES_DB",
        "TU_SHARE_API_KEY",
        "AI_SERVICE_PORT",
        "DATA_SYNC_PORT",
        "DESKTOP_UI_PORT",
        "RSSHUB_PORT",
        "PGADMIN_PORT",
        "NEXT_PUBLIC_DATA_SYNC_BASE_URL",
        "NEXT_PUBLIC_AI_BASE_URL",
        "KARIOS_API_KEYS",
    ],
)
def test_env_example_has_key(key: str) -> None:
    text = ENV_EXAMPLE.read_text(encoding="utf-8")
    assert re.search(rf"^{re.escape(key)}=", text, re.MULTILINE), f".env.example missing key: {key}"


# ---------------------------------------------------------------------------
# Dockerfiles
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "path",
    [DATASYNC_DOCKERFILE, AI_DOCKERFILE, DESKTOP_DOCKERFILE],
)
def test_dockerfile_exists(path: Path) -> None:
    assert path.is_file(), f"missing Dockerfile: {path}"


@pytest.mark.parametrize(
    "path",
    [DATASYNC_DOCKERFILE, AI_DOCKERFILE, DESKTOP_DOCKERFILE],
)
def test_dockerfile_no_latest_tag(path: Path) -> None:
    """No `:latest` base image — pin versions for reproducibility."""
    text = path.read_text(encoding="utf-8")
    # Look for "FROM ...:latest" or "FROM ... " (no tag at all).
    froms = re.findall(r"^FROM\s+(\S+)", text, re.MULTILINE)
    assert froms, f"{path.name}: no FROM lines"
    for img in froms:
        assert not img.endswith(":latest"), (
            f"{path.name}: base image uses :latest — pin a version: {img}"
        )
        # Must have a tag (after the colon) — except for the special buildx
        # syntax dockerfile and stage references.
        if img.startswith("docker/dockerfile:") or img.startswith("--"):
            continue
        assert ":" in img or "@sha256:" in img, f"{path.name}: base image has no tag: {img}"


def test_datasync_dockerfile_exposes_4330() -> None:
    text = DATASYNC_DOCKERFILE.read_text(encoding="utf-8")
    assert "EXPOSE 4330" in text


def test_ai_dockerfile_exposes_4310() -> None:
    text = AI_DOCKERFILE.read_text(encoding="utf-8")
    assert "EXPOSE 4310" in text


def test_desktop_dockerfile_uses_nginx() -> None:
    text = DESKTOP_DOCKERFILE.read_text(encoding="utf-8")
    # Final stage must use nginx:alpine (or similar) for serving static.
    assert re.search(r"^FROM\s+nginx:[\d.]+-alpine", text, re.MULTILINE), (
        "desktop-ui final stage must be nginx:alpine"
    )


def test_desktop_nginx_config_exists() -> None:
    assert DESKTOP_NGINX.is_file()
    text = DESKTOP_NGINX.read_text(encoding="utf-8")
    assert "location /api/" in text, "nginx.conf must reverse-proxy /api/ to data-sync"
    assert "location /ai/" in text, "nginx.conf must reverse-proxy /ai/ to ai-service"
    assert "data-sync:4330" in text
    assert "ai-service:4310" in text


def test_dockerignore_exists() -> None:
    assert DOCKERIGNORE.is_file()
    text = DOCKERIGNORE.read_text(encoding="utf-8")
    # Must exclude secrets.
    assert ".env" in text
    # Must exclude heavy build dirs.
    assert "node_modules" in text or "**/node_modules/" in text


# ---------------------------------------------------------------------------
# Documentation
# ---------------------------------------------------------------------------


def test_setup_doc_exists() -> None:
    assert SETUP_DOC.is_file()


def test_setup_doc_has_required_sections() -> None:
    text = SETUP_DOC.read_text(encoding="utf-8")
    required = [
        "前置条件",
        "一键启动",
        "状态检查",
        "停机",
        "换电脑",
        "UPS 自动恢复",
        "开机自启",
    ]
    for section in required:
        assert section in text, f"setup doc missing section: {section}"


def test_setup_doc_references_all_scripts() -> None:
    """The doc must reference every script so users know they exist."""
    text = SETUP_DOC.read_text(encoding="utf-8")
    for script in [
        "docker-up.sh",
        "docker-down.sh",
        "docker-status.sh",
        "install-launchd.sh",
        "uninstall-launchd.sh",
        "ups-shutdown.sh",
    ]:
        assert script in text, f"setup doc doesn't mention {script}"
