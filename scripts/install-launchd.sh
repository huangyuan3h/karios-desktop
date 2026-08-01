#!/usr/bin/env bash
# Install a macOS LaunchAgent that auto-starts the Karios Docker stack on login.
#
# What it does:
#   1. Writes ~/Library/LaunchAgents/com.karios.docker-up.plist
#   2. Loads it via launchctl (user-level, no sudo)
#   3. The agent runs scripts/docker-up.sh after login (RunAtLoad) and on every
#      subsequent login (LimitLoadSessions=1 keeps it from re-running in nested
#      sessions).
#
# Pair with scripts/uninstall-launchd.sh.
#
# Usage:
#   scripts/install-launchd.sh
#
# Requirements:
#   - macOS only. Won't run on Linux.
#   - Docker Desktop for Mac installed and configured to start at login.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DOCKER_UP="$ROOT/scripts/docker-up.sh"
LABEL="com.karios.docker-up"
PLIST_PATH="$HOME/Library/LaunchAgents/${LABEL}.plist"
LOG_DIR="$HOME/Library/Logs/karios"

if [ "$(uname -s)" != "Darwin" ]; then
    echo "error: this script is macOS-only. Detected: $(uname -s)" >&2
    exit 1
fi

if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
    cat <<'EOF'
Usage: install-launchd.sh

Installs ~/Library/LaunchAgents/com.karios.docker-up.plist and loads it.
After install, the Karios Docker stack starts automatically on every login.

macOS only. No arguments.
EOF
    exit 0
fi

if [ ! -x "$DOCKER_UP" ]; then
    echo "error: $DOCKER_UP is missing or not executable." >&2
    exit 1
fi

mkdir -p "$HOME/Library/LaunchAgents" "$LOG_DIR"

# Unload any prior copy so we don't end up with two.
if launchctl list | grep -q "$LABEL"; then
    echo "[launchd] unloading existing agent..."
    launchctl unload "$PLIST_PATH" 2>/dev/null || true
fi

# Write plist (XML — no plutil dependency at install time, but we plutil -lint
# at test time to verify the format).
cat > "$PLIST_PATH" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>${LABEL}</string>

    <key>ProgramArguments</key>
    <array>
        <string>${DOCKER_UP}</string>
    </array>

    <key>RunAtLoad</key>
    <true/>

    <key>LimitLoadSessions</key>
    <integer>1</integer>

    <key>StandardOutPath</key>
    <string>${LOG_DIR}/docker-up.out.log</string>

    <key>StandardErrorPath</key>
    <string>${LOG_DIR}/docker-up.err.log</string>

    <key>WorkingDirectory</key>
    <string>${ROOT}</string>
</dict>
</plist>
EOF

# Validate XML if plutil is available (it is on every modern macOS).
if command -v plutil >/dev/null 2>&1; then
    if ! plutil -lint "$PLIST_PATH" >/dev/null 2>&1; then
        echo "error: plist failed plutil -lint validation." >&2
        plutil -lint "$PLIST_PATH" >&2 || true
        exit 1
    fi
fi

# Load.
echo "[launchd] loading $LABEL..."
launchctl load -w "$PLIST_PATH"

echo
echo "Karios LaunchAgent installed."
echo
echo "  Plist:  $PLIST_PATH"
echo "  Logs:   $LOG_DIR/docker-up.{out,err}.log"
echo
echo "On next login (or right now if you ran this manually),"
echo "docker-up.sh will start the stack."
echo
echo "To uninstall: scripts/uninstall-launchd.sh"