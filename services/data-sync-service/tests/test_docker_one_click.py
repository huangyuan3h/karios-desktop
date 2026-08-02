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

import json
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


@pytest.mark.parametrize(
    "path",
    [DATASYNC_DOCKERFILE, AI_DOCKERFILE, DESKTOP_DOCKERFILE],
)
def test_dockerfile_no_shell_in_copy(path: Path) -> None:
    """Regression guard: COPY/ADD do NOT run a shell. Any `2>/dev/null`,
    `|| true`, `&&`, `|`, or `$VAR` in a COPY/ADD source/destination would be
    interpreted by BuildKit as a literal path segment, leading to errors like
    'failed to compute cache key: ... "/2>/dev/null": not found'.

    See https://docs.docker.com/reference/dockerfile/#copy — COPY takes only
    paths and `--flag=value` pairs. Use RUN with `/bin/sh -c` for shell logic.
    """
    text = path.read_text(encoding="utf-8")
    # Match `COPY <src>` or `COPY --from=<x> <src>` — up to end of line.
    copy_lines = re.findall(r"^COPY\b[^\n]*", text, re.MULTILINE | re.IGNORECASE)
    assert copy_lines, f"{path.name}: no COPY lines found"
    forbidden = ("2>/dev/null", "||", "&&", "|", ">", "<", "$(")
    for line in copy_lines:
        for token in forbidden:
            assert token not in line, (
                f"{path.name}: shell syntax '{token}' inside COPY instruction:\n"
                f"  {line}\n"
                "Use a separate RUN step with /bin/sh -c for shell logic, "
                "or pre-create the file outside the Dockerfile."
            )


# ---------------------------------------------------------------------------
# Node service Dockerfile lockfile handling (regression for ERR_PNPM_NO_LOCKFILE)
# ---------------------------------------------------------------------------

ROOT_PNPM_LOCK = REPO_ROOT / "pnpm-lock.yaml"


def test_root_pnpm_lockfile_exists() -> None:
    """pnpm workspaces use a SINGLE root lockfile. Per-app lockfiles do NOT
    exist for apps/ai-service or apps/desktop-ui."""
    assert ROOT_PNPM_LOCK.is_file(), f"missing root pnpm-lock.yaml at {ROOT_PNPM_LOCK}"


@pytest.mark.parametrize(
    "no_lockfile",
    [
        REPO_ROOT / "apps" / "ai-service" / "pnpm-lock.yaml",
        REPO_ROOT / "apps" / "desktop-ui" / "pnpm-lock.yaml",
        REPO_ROOT / "services" / "data-sync-service" / "pnpm-lock.yaml",
    ],
)
def test_no_per_app_pnpm_lockfile(no_lockfile: Path) -> None:
    """Per-app lockfiles would diverge from root. They must not exist."""
    assert not no_lockfile.exists(), (
        f"per-app pnpm-lock.yaml exists at {no_lockfile} — pnpm workspaces use "
        "a single root lockfile; per-app lockfiles will diverge and break "
        "`pnpm install --frozen-lockfile`."
    )


@pytest.mark.parametrize("path", [AI_DOCKERFILE, DESKTOP_DOCKERFILE])
def test_node_dockerfile_copies_root_lockfile(path: Path) -> None:
    """Regression guard for `ERR_PNPM_NO_LOCKFILE Cannot install with
    "frozen-lockfile" because pnpm-lock.yaml is absent`.

    Both ai-service and desktop-ui Dockerfiles must COPY the root
    pnpm-lock.yaml (no wildcard `pnpm-lock.yaml*` which silently misses).
    """
    text = path.read_text(encoding="utf-8")
    # Look for a COPY line that includes pnpm-lock.yaml (NOT a glob).
    copy_lines = re.findall(r"^COPY\b[^\n]*pnpm-lock\.yaml[^\n]*", text, re.MULTILINE)
    assert copy_lines, (
        f"{path.name}: no COPY line for pnpm-lock.yaml — `pnpm install "
        "--frozen-lockfile` will fail with ERR_PNPM_NO_LOCKFILE"
    )
    # Make sure at least one COPY is the literal lockfile, not a glob.
    literal = [
        ln for ln in copy_lines if "pnpm-lock.yaml *" not in ln and "*pnpm-lock.yaml" not in ln
    ]
    assert literal, (
        f"{path.name}: only `pnpm-lock.yaml*` glob found; the lockfile is named "
        "`pnpm-lock.yaml` exactly and must be COPYed as a literal token."
    )


@pytest.mark.parametrize("path", [AI_DOCKERFILE, DESKTOP_DOCKERFILE])
def test_node_dockerfile_uses_root_context(path: Path) -> None:
    """If the ai-service or desktop-ui Dockerfile is built from a per-app
    context, the root pnpm-lock.yaml won't be reachable. The docker-compose
    must use `context: .` for these services."""
    compose_text = COMPOSE_FILE.read_text(encoding="utf-8")
    # Each node service's build block must have `context: .` or `context: ./`.
    if "ai-service" in path.name.lower() or path.parent.name == "ai-service":
        # The compose block for ai-service must use root context.
        assert re.search(
            r"ai-service:\s*\n\s*build:\s*\n\s*context:\s*\.?\s*\n",
            compose_text,
        ) or re.search(
            r"ai-service:\s*\n\s*build:\s*\n\s*context:\s*\.\s*\n",
            compose_text,
        ), "docker-compose ai-service must use `context: .` (root) for pnpm workspace"
    if path.parent.name == "desktop-ui":
        assert re.search(
            r"desktop-ui:\s*\n\s*build:\s*\n\s*context:\s*\.\s*\n",
            compose_text,
        ), "docker-compose desktop-ui must use `context: .` (root) for pnpm workspace"


# ---------------------------------------------------------------------------
# Regression: COPY sources must exist in the build context
# ---------------------------------------------------------------------------

# Each Node service: dockerfile context path -> repo-relative.
NODE_DOCKERFILE_CONTEXTS: dict[Path, Path] = {
    AI_DOCKERFILE: REPO_ROOT,  # ai-service uses root context
    DESKTOP_DOCKERFILE: REPO_ROOT,  # desktop-ui uses root context
}
DATA_SYNC_CONTEXT = REPO_ROOT / "services" / "data-sync-service"


def _parse_copy_sources(dockerfile: Path) -> list[str]:
    """Extract source path tokens from each `COPY <src>... <dest>` line.

    Skips:
    - `--from=<stage>` flags + all sources after them on the same line
      (those come from a previous build stage, not the build context).
    - `--chown=...`, `--link`, etc. flags.
    - Build-arg-style sources (`$VAR`).
    - Globs (containing `*` or `?`).

    Returns the list of literal source paths referenced FROM THE BUILD CONTEXT.
    """
    text = dockerfile.read_text(encoding="utf-8")
    copy_lines = re.findall(r"^COPY\b[^\n]*", text, re.MULTILINE | re.IGNORECASE)
    sources: list[str] = []
    for line in copy_lines:
        tokens = line.split()[1:]  # drop "COPY"
        from_prev_stage = False
        srcs: list[str] = []
        for token in tokens:
            if token.startswith("--from="):
                # All subsequent tokens on this line come from the named stage.
                from_prev_stage = True
                continue
            if token.startswith("--"):
                # Other flag (--chown=, --link, etc.).
                continue
            if from_prev_stage:
                # Sources after --from= are paths inside the named stage.
                continue
            srcs.append(token)
        if not srcs:
            continue
        # Last token is destination; sources are everything before.
        for s in srcs[:-1]:
            if s.startswith("$"):
                continue
            if "*" in s or "?" in s:
                continue
            sources.append(s)
    return sources


@pytest.mark.parametrize(
    "dockerfile,context",
    list(NODE_DOCKERFILE_CONTEXTS.items()),
)
def test_node_dockerfile_copy_sources_exist(dockerfile: Path, context: Path) -> None:
    """Regression guard: 'failed to compute cache key: "/<file>": not found'.

    Every literal (non-glob, non-build-arg) COPY source must exist in the
    build context directory. Skips `--from=` (previous-stage references).
    """
    sources = _parse_copy_sources(dockerfile)
    for src in sources:
        # Sources are relative to the build context root.
        target = context / src
        assert target.exists(), (
            f"{dockerfile.relative_to(REPO_ROOT)}: COPY source '{src}' does not "
            f"exist in build context {context}. Either the path is wrong, or "
            f"the dockerfile's `context:` in docker-compose.yml is wrong."
        )


def test_datasync_dockerfile_copy_sources_exist() -> None:
    """data-sync-service uses its own directory as build context."""
    sources = _parse_copy_sources(DATASYNC_DOCKERFILE)
    for src in sources:
        target = DATA_SYNC_CONTEXT / src
        assert target.exists(), (
            f"{DATASYNC_DOCKERFILE.relative_to(REPO_ROOT)}: COPY source '{src}' "
            f"does not exist in build context {DATA_SYNC_CONTEXT}."
        )


# ---------------------------------------------------------------------------
# Regression: pnpm install must include devDependencies when a build step
# follows (tsc, next build, etc.)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("path", [AI_DOCKERFILE, DESKTOP_DOCKERFILE])
def test_node_dockerfile_pnpm_install_includes_devdeps(path: Path) -> None:
    """Regression guard for `sh: tsc: not found` / `sh: next: not found`.

    When `NODE_ENV=production` is set in the Dockerfile and the file also runs
    a build step (tsc, next build), pnpm install will SKIP devDependencies by
    default and the build will fail. Two ways to fix:

    1. Add `--prod=false` (or `--dev`) to the pnpm install command.
    2. Don't set NODE_ENV=production in the build stage.

    This test passes if EITHER approach is used:
    - pnpm install includes --prod=false / --dev, OR
    - no `ENV NODE_ENV=production` line before the pnpm install RUN.
    """
    text = path.read_text(encoding="utf-8")
    # Find the RUN block whose command actually invokes pnpm install.
    # Dockerfile RUN may span multiple lines (backslash continuations).
    # Strategy: split into RUN blocks, find the one that contains "pnpm install".
    run_blocks = re.split(r"(?=\nRUN\b)", text)
    install_cmd = ""
    for block in run_blocks:
        if re.search(r"\bpnpm\s+install\b", block):
            install_cmd = block
            break
    assert install_cmd, f"{path.name}: no pnpm install RUN block found"

    # Find any ENV NODE_ENV=production line that comes BEFORE the install.
    install_pos = text.find(install_cmd)
    before_install = text[:install_pos]
    has_prod_env_before_install = bool(
        re.search(r"^\s*ENV\s+NODE_ENV\s*=\s*production", before_install, re.MULTILINE)
    )

    if not has_prod_env_before_install:
        # No NODE_ENV=production set before install — pnpm will install devDeps.
        return

    # NODE_ENV=production IS set — pnpm install must override with --prod=false or --dev.
    assert "--prod=false" in install_cmd or "--dev" in install_cmd, (
        f"{path.name}: NODE_ENV=production is set before `pnpm install`, but "
        "the install command does not include --prod=false (or --dev). "
        "pnpm will skip devDependencies (typescript, tsx, etc.) and the build "
        f"will fail.\n  install cmd: {install_cmd}"
    )


def test_ai_dockerfile_exposes_4310() -> None:
    text = AI_DOCKERFILE.read_text(encoding="utf-8")
    assert "EXPOSE 4310" in text


# ---------------------------------------------------------------------------
# Regression: monorepo Next.js builds need full workspace install
# ---------------------------------------------------------------------------

DESKTUI_PACKAGE_JSON = REPO_ROOT / "apps" / "desktop-ui" / "package.json"
SHARED_PACKAGE_JSON = REPO_ROOT / "packages" / "shared" / "package.json"
SHARED_SCHEMAS_DIR = REPO_ROOT / "packages" / "shared" / "src" / "schemas"


def test_desktop_ui_uses_karios_shared() -> None:
    """Sanity check: desktop-ui depends on @karios/shared (workspace)."""
    assert DESKTUI_PACKAGE_JSON.is_file()
    pkg = json.loads(DESKTUI_PACKAGE_JSON.read_text(encoding="utf-8"))
    deps = {**pkg.get("dependencies", {}), **pkg.get("devDependencies", {})}
    assert "@karios/shared" in deps, (
        "desktop-ui no longer depends on @karios/shared — if you removed this "
        "dependency, the test below (test_desktop_dockerfile_installs_full_workspace) "
        "may no longer apply."
    )


def test_shared_uses_zod() -> None:
    """Sanity check: shared package depends on zod in some way that Next.js
    Turbopack will need to resolve at build time."""
    pkg = json.loads(SHARED_PACKAGE_JSON.read_text(encoding="utf-8"))
    deps = {**pkg.get("dependencies", {}), **pkg.get("devDependencies", {})}
    if "zod" not in deps:
        pytest.skip("shared no longer depends on zod; test below may not apply")
    assert SHARED_SCHEMAS_DIR.is_dir(), f"missing {SHARED_SCHEMAS_DIR}"
    # At least one schema file imports zod.
    zod_imports = [
        f for f in SHARED_SCHEMAS_DIR.glob("*.ts") if "from 'zod'" in f.read_text(encoding="utf-8")
    ]
    assert zod_imports, (
        f"No schema in {SHARED_SCHEMAS_DIR} imports zod. The regression test "
        "for full-workspace install may no longer apply."
    )


def test_desktop_dockerfile_installs_full_workspace() -> None:
    """Regression guard: 'Module not found: Can't resolve zod' from
    packages/shared/src/schemas/*.ts during `next build`.

    Next.js Turbopack has known issues with pnpm `--filter` installs when
    a transpilePackages workspace package (here @karios/shared) has its own
    dependencies (here zod). The fix is to install the FULL workspace (no
    `--filter`), matching the host dev workflow.

    This test fails if the desktop-ui Dockerfile uses `--filter` on pnpm install.
    If you can prove Turbopack handles filter properly, feel free to add
    `--filter ./apps/desktop-ui...` back and update this test.
    """
    text = DESKTOP_DOCKERFILE.read_text(encoding="utf-8")
    # Find the pnpm install RUN block.
    run_blocks = re.split(r"(?=\nRUN\b)", text)
    install_cmd = ""
    for block in run_blocks:
        if re.search(r"\bpnpm\s+install\b", block):
            install_cmd = block
            break
    assert install_cmd, "desktop-ui Dockerfile has no pnpm install RUN"
    assert "--filter" not in install_cmd, (
        "desktop-ui Dockerfile uses `pnpm install --filter ...`. This causes "
        "Next.js Turbopack to fail with 'Module not found: Can't resolve zod' "
        "from packages/shared/src/schemas/*.ts at build time. Use full "
        "workspace install (no --filter) instead."
    )


# ---------------------------------------------------------------------------
# Regression: scripts with `set -u` must use ${arr[@]+...} for empty arrays
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "script",
    [DOCKER_UP, DOCKER_DOWN, DOCKER_STATUS, INSTALL_LAUNCHD, UNINSTALL_LAUNCHD, UPS_SHUTDOWN],
)
def test_script_no_bare_array_expansion(script: Path) -> None:
    """Regression guard: `set -euo pipefail` makes bash fail with
    'unbound variable' when expanding an empty array as "${arr[@]}".

    Empty arrays need the form `${arr[@]+"${arr[@]}"}` instead.
    """
    text = script.read_text(encoding="utf-8")
    # Find any bash array expansion. We care about `COMPOSE_ARGS` and similar
    # that can be empty.
    # Look for lines like `cmd "${ARR[@]}" ...` — bare expansion of possibly-empty.
    lines_with_bare = []
    for lineno, line in enumerate(text.splitlines(), 1):
        # Match `"${NAME[@]}"` but NOT `${NAME[@]+"${NAME[@]}"}` (safe form).
        if re.search(r'"\$\{[A-Z_][A-Z0-9_]*\[@\]\}"', line):
            lines_with_bare.append((lineno, line.strip()))

    if not lines_with_bare:
        return

    for lineno, line in lines_with_bare:
        # Allow if the script set `set -u` is not in effect (we know it is).
        # Allow if guarded by an `if` block — but we don't parse that here.
        # Allow if the entire script doesn't use set -u (rare for our scripts).
        # Simplest: require the safe form `${arr[@]+"${arr[@]}"}` on the same line.
        assert re.search(r'\$\{[A-Z_][A-Z0-9_]*\[@\]\+"\$\{[A-Z_][A-Z0-9_]*\[@\]\}"\}', line), (
            f"{script.name}:{lineno}: bare `${{ARR[@]}}` expansion of possibly-empty "
            f"array will trigger 'unbound variable' under `set -u`. Use the safe "
            f'form `${{ARR[@]+"${{ARR[@]}}"}}` instead.\n  line: {line}'
        )


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
