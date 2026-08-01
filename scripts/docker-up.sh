#!/usr/bin/env bash
# One-click bringup for the Karios Docker stack.
#
# What it does:
#   1. Pre-flight: docker / docker compose / .env
#   2. Build images (data-sync, ai-service, desktop-ui)
#   3. Start all services (postgres, rsshub, data-sync, ai-service, desktop-ui, pgadmin)
#   4. Run one-shot Alembic migration
#   5. Wait until /healthz is 200 on every service
#   6. Print connection URLs
#
# Usage:
#   scripts/docker-up.sh
#   scripts/docker-up.sh --rebuild           # force rebuild (no cache)
#   scripts/docker-up.sh --no-build          # skip image build (faster for restart)
#   scripts/docker-up.sh --detach            # start in background (default behavior)
#
# See docs/setup/docker-one-click.md for full instructions.

set -euo pipefail

# ---- arg parsing -----------------------------------------------------------

REBUILD=0
BUILD=1
DETACH=1
MIGRATE=0

while [ $# -gt 0 ]; do
    case "$1" in
        --rebuild)
            REBUILD=1
            shift
            ;;
        --no-build)
            BUILD=0
            shift
            ;;
        --detach)
            DETACH=1
            shift
            ;;
        --migrate)
            MIGRATE=1
            shift
            ;;
        --help|-h)
            cat <<'EOF'
Usage: docker-up.sh [--rebuild] [--no-build] [--detach] [--migrate]

Brings up the Karios Docker stack on this machine.

Options:
  --rebuild    Force rebuild images without using cache.
  --no-build   Skip image build (assumes images already built).
  --detach     Run in background (default; docker compose up -d).
  --migrate    Stop orphan containers from the old docker-compose.yml
               (postgres-db, pgadmin-web, karios-rsshub) before starting.
               Required when upgrading from a prior Karios install.

Requires:
  - docker >= 24 with docker compose v2
  - .env file at repo root (copy from .env.example)
EOF
            exit 0
            ;;
        *)
            echo "error: unknown argument '$1' (try --help)" >&2
            exit 64
            ;;
    esac
done

# ---- preflight -------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

if ! command -v docker >/dev/null 2>&1; then
    echo "error: docker is not installed. Install Docker Desktop for Mac first." >&2
    echo "  https://www.docker.com/products/docker-desktop/" >&2
    exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
    echo "error: 'docker compose' (v2) is required. Update Docker Desktop." >&2
    exit 1
fi

if [ ! -f .env ]; then
    echo "warning: .env not found at repo root." >&2
    echo "  Falling back to .env.example defaults (dev-only)." >&2
    if [ -f .env.example ]; then
        cp .env.example .env
        echo "  Created .env from .env.example. Edit it to fill in API keys." >&2
        echo "  Press Enter to continue with dev defaults, or Ctrl-C to abort..." >&2
        read -r _
    else
        echo "error: .env.example missing. Aborting." >&2
        exit 1
    fi
fi

# ---- migrate (stop orphan containers from old compose) -------------------

ORPHAN_CONTAINERS=(postgres-db pgadmin-web karios-rsshub)

if [ "$MIGRATE" = "1" ]; then
    echo "[docker-up] --migrate: stopping orphan containers from old compose..."
    for c in "${ORPHAN_CONTAINERS[@]}"; do
        if docker ps --format '{{.Names}}' | grep -qx "$c"; then
            echo "  stopping $c..."
            docker stop "$c" >/dev/null || true
        fi
    done
else
    # Check for orphans; warn if any are running on required ports.
    CONFLICT=0
    for c in "${ORPHAN_CONTAINERS[@]}"; do
        if docker ps --format '{{.Names}}' | grep -qx "$c"; then
            echo "[docker-up] detected orphan container: $c" >&2
            CONFLICT=1
        fi
    done
    if [ "$CONFLICT" = "1" ]; then
        echo "Re-run with --migrate to stop them automatically." >&2
        echo "  $0 --migrate" >&2
        exit 1
    fi
fi

# ---- build -----------------------------------------------------------------

COMPOSE_ARGS=()
if [ "$BUILD" = "1" ]; then
    if [ "$REBUILD" = "1" ]; then
        echo "[docker-up] building images (no cache)..."
        docker compose build --no-cache
    else
        echo "[docker-up] building images..."
        docker compose build
    fi
else
    COMPOSE_ARGS+=(--no-build)
fi

# ---- up --------------------------------------------------------------------

echo "[docker-up] starting services..."
docker compose up -d "${COMPOSE_ARGS[@]}" --remove-orphans

# ---- wait for healthz ------------------------------------------------------

echo "[docker-up] waiting for services to become healthy..."
TIMEOUT_SECONDS=180
ELAPSED=0

wait_for_health() {
    local url="$1"
    local label="$2"
    local retries=30
    while [ $retries -gt 0 ]; do
        if curl --silent --fail --max-time 3 "$url" >/dev/null 2>&1; then
            echo "  ✓ $label"
            return 0
        fi
        retries=$((retries - 1))
        sleep 2
        ELAPSED=$((ELAPSED + 2))
    done
    echo "  ✗ $label (timed out after ${ELAPSED}s)" >&2
    return 1
}

HEALTH_OK=1
wait_for_health "http://127.0.0.1:4330/healthz" "data-sync /healthz" || HEALTH_OK=0
wait_for_health "http://127.0.0.1:4310/healthz" "ai-service /healthz" || HEALTH_OK=0
wait_for_health "http://127.0.0.1:8080/healthz" "desktop-ui /healthz" || HEALTH_OK=0

echo
if [ "$HEALTH_OK" = "1" ]; then
    echo "Karios is up."
    echo
    echo "  Web UI:        http://127.0.0.1:8080"
    echo "  Data Sync API: http://127.0.0.1:4330"
    echo "  AI Service:    http://127.0.0.1:4310"
    echo "  pgAdmin:       http://127.0.0.1:8081"
    echo "  RSSHub:        http://127.0.0.1:1200"
    echo
    echo "Run scripts/docker-status.sh to inspect, scripts/docker-down.sh to stop."
else
    echo "One or more services failed healthcheck. Run scripts/docker-status.sh for details." >&2
    exit 1
fi