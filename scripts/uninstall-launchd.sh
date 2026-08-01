#!/usr/bin/env bash
# Uninstall the Karios LaunchAgent.
#
# Usage:
#   scripts/uninstall-launchd.sh

set -euo pipefail

LABEL="com.karios.docker-up"
PLIST_PATH="$HOME/Library/LaunchAgents/${LABEL}.plist"

if [ "$(uname -s)" != "Darwin" ]; then
    echo "error: this script is macOS-only. Detected: $(uname -s)" >&2
    exit 1
fi

if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
    cat <<'EOF'
Usage: uninstall-launchd.sh

Unloads and removes the Karios LaunchAgent. Safe to run multiple times.
EOF
    exit 0
fi

if launchctl list | grep -q "$LABEL"; then
    echo "[launchd] unloading $LABEL..."
    launchctl unload "$PLIST_PATH" 2>/dev/null || true
fi

if [ -f "$PLIST_PATH" ]; then
    echo "[launchd] removing $PLIST_PATH..."
    rm -f "$PLIST_PATH"
fi

echo "Karios LaunchAgent uninstalled."