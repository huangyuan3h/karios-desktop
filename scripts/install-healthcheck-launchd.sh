#!/usr/bin/env bash
# Install a macOS LaunchAgent that runs scripts/data_healthcheck.py daily
# (剩余风险 ②: 数据源健康告警).
#
# Schedule:
#   - 08:30 daily (StartCalendarInterval) — after the overnight backup (03:00)
#     and before a typical trading-day session.
#   - RunAtLoad=true — also runs once at login.
#
# Behavior:
#   - stdout/stderr appended to ~/.karios/logs/healthcheck.log (rotated by size).
#   - --notify: macOS notification when any check fails (exit code >= 2).
#
# Install:
#   bash scripts/install-healthcheck-launchd.sh
# Uninstall:
#   launchctl unload ~/Library/LaunchAgents/com.karios.healthcheck.plist
#   rm ~/Library/LaunchAgents/com.karios.healthcheck.plist

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HEALTHCHECK_SCRIPT="$REPO_ROOT/services/data-sync-service/scripts/data_healthcheck.py"
LABEL="com.karios.healthcheck"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
PYTHON_BIN="$(python3 -c 'import sys; print(sys.executable)')"
LOG_DIR="$HOME/.karios/logs"
LOG_FILE="$LOG_DIR/healthcheck.log"

mkdir -p "$LOG_DIR" "$HOME/Library/LaunchAgents"

cat > "$PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>$LABEL</string>
    <key>ProgramArguments</key>
    <array>
        <string>$PYTHON_BIN</string>
        <string>$HEALTHCHECK_SCRIPT</string>
        <string>--notify</string>
    </array>
    <key>WorkingDirectory</key>
    <string>$REPO_ROOT/services/data-sync-service</string>
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>8</integer>
        <key>Minute</key>
        <integer>30</integer>
    </dict>
    <key>RunAtLoad</key>
    <true/>
    <key>StandardOutPath</key>
    <string>$LOG_FILE</string>
    <key>StandardErrorPath</key>
    <string>$LOG_FILE</string>
</dict>
</plist>
EOF

launchctl unload "$PLIST" 2>/dev/null || true
launchctl load "$PLIST"
echo "Installed $LABEL → runs daily 08:30 (+ at login), log: $LOG_FILE"
echo "Verify with: launchctl list | grep $LABEL"
