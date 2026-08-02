#!/usr/bin/env bash
# UPS low-battery shutdown hook for Karios.
#
# This script is INTENDED to be called by an external UPS monitor
# (Homebrew `nut`, `apcupsd`, or APC's PowerChute). It is NOT a UPS monitor.
#
# Why a hook instead of a monitor:
#   - macOS has no built-in UPS API.
#   - Building one here would duplicate the work of mature tools (nut, apcupsd).
#   - Keeping this script as a hook means the user can choose their preferred
#     monitoring tool without touching Karios.
#
# Behavior:
#   1. Run docker compose down (graceful — Postgres flushes WAL, uvicorn
#      handles SIGTERM via tini).
#   2. Trigger macOS shutdown via `pmset shutdown now` (or a configurable
#      command — KARIOS_UPS_SHUTDOWN_CMD env var).
#
# Usage:
#   scripts/ups-shutdown.sh
#
# Required configuration (in .env or environment):
#   KARIOS_UPS_SHUTDOWN_HOOK=<path-to-this-script>
#
# Then in your UPS monitor (e.g., /etc/nut/upsmon.conf):
#   SHUTDOWNCMD "<path>/scripts/ups-shutdown.sh"
# Or for apcupsd (/etc/apcupsd/apccontrol):
#   Replace the doshutdown line with a call to this script.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
    cat <<'EOF'
Usage: ups-shutdown.sh

Gracefully shuts down the Karios Docker stack and triggers macOS shutdown.
Intended to be called by an external UPS monitor (nut, apcupsd, etc.) on
low battery. See docs/setup/docker-one-click.md for monitor configuration.
EOF
    exit 0
fi

if [ "$(uname -s)" != "Darwin" ]; then
    echo "error: this script is macOS-only. Detected: $(uname -s)" >&2
    exit 1
fi

echo "[ups-shutdown] bringing down Karios Docker stack..."
docker compose down --remove-orphans || true

# Allow custom shutdown command. Default: pmset shutdown now (immediate shutdown).
SHUTDOWN_CMD="${KARIOS_UPS_SHUTDOWN_CMD:-pmset shutdown now}"

echo "[ups-shutdown] running: $SHUTDOWN_CMD"
sh -c "$SHUTDOWN_CMD" || true

# pmset shutdown is synchronous and rarely returns. If it does return, exit
# non-zero so the UPS monitor knows the shutdown did not complete.
echo "[ups-shutdown] warning: shutdown command returned. System may not be powering off." >&2
exit 1