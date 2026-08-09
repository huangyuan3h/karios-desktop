#!/usr/bin/env bash
# Install a macOS LaunchAgent that runs db_backup.sh on a schedule + on login
# (OPT-060).
#
# Schedule:
#   - 03:00 daily (StartCalendarInterval) — primary schedule for a Mac that
#     stays online overnight.
#   - RunAtLoad=true — also runs once when the agent loads (login / manual
#     `launchctl load`). Covers the "Mac was asleep, just woke up" case where
#     the user logs in mid-day and the script's last-age check decides
#     whether to actually dump.
#
# Sleep handling (the reason this exists at all):
#   launchd's StartCalendarInterval does NOT catch up on missed runs while the
#   Mac was asleep. The db_backup.sh script itself defends against this by
#   checking "is the most recent local backup > 25h old?" — if yes, it dumps
#   regardless of why it was triggered. This means:
#     - daily online → 03:00 cron + last-age skip (everything < 25h)
#     - daily online but cron missed (e.g. sleep at 03:00) → next login's
#       RunAtLoad triggers a fresh dump
#     - Mac asleep for a week, user opens Terminal → last-age triggers dump
#   Plus, the install script optionally appends a one-liner to ~/.zshenv
#   so even a fresh shell session (without a relaunch of the LaunchAgent) will
#   trigger the script — it self-skips if the last backup is fresh.
#
# What it does:
#   1. Writes ~/Library/LaunchAgents/com.karios.db-backup.plist
#   2. Loads it via launchctl (user-level, no sudo)
#   3. Optionally appends a self-skipping trigger to ~/.zshenv (offers
#      interactively; skips if declined)
#
# Usage:
#   scripts/install-db-backup-launchd.sh                 # install + load
#   scripts/install-db-backup-launchd.sh --unload        # uninstall
#   scripts/install-db-backup-launchd.sh --status        # show state
#
# Requirements:
#   - macOS only.
#   - docker + a running Postgres container (db_backup.sh auto-detects).

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Resolve ROOT robustly: prefer realpath; fall back to pwd-derived if invoked
# via `bash scripts/install-db-backup-launchd.sh` (BASH_SOURCE can be relative).
if command -v realpath >/dev/null 2>&1; then
  SCRIPT_DIR="$(realpath "$SCRIPT_DIR")"
fi
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DB_BACKUP="$ROOT/services/data-sync-service/scripts/db_backup.sh"

LABEL="com.karios.db-backup"
PLIST_PATH="$HOME/Library/LaunchAgents/${LABEL}.plist"
LOG_DIR="$HOME/Library/Logs/karios"
ZSHENV="$HOME/.zshenv"
ZSHENV_MARKER="# >>> karios db-backup >>>"
ZSHENV_FOOTER="# <<< karios db-backup <<<"

# ---- helpers ---------------------------------------------------------------
_log() { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }
_fail() { printf '[%s] ERROR: %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" >&2; exit 1; }

if [ "$(uname -s)" != "Darwin" ]; then
  _fail "this script is macOS-only (uses launchctl)"
fi

ACTION="install"
for arg in "$@"; do
  case "$arg" in
    --unload)   ACTION="unload" ;;
    --status)   ACTION="status" ;;
    --help|-h)
      sed -n '2,30p' "$0"; exit 0 ;;
    *) _fail "unknown arg: $arg" ;;
  esac
done

# ---- status ----------------------------------------------------------------
if [ "$ACTION" = "status" ]; then
  echo "== Karios db-backup status =="
  # grep -q would SIGPIPE launchctl on a long list; use process substitution.
  if grep -q "$LABEL" < <(launchctl list 2>/dev/null); then
    echo "LaunchAgent: LOADED ($LABEL)"
    launchctl list "$LABEL" 2>/dev/null | sed 's/^/  /'
  else
    echo "LaunchAgent: not loaded"
  fi
  echo
  echo "Plist:        $PLIST_PATH $([ -f "$PLIST_PATH" ] && echo '(exists)' || echo '(missing)')"
  echo "Logs:         $LOG_DIR/db-backup.{out,err}.log"
  echo "Last backup:  $(ls -1t "$HOME/.karios/backups/postgres"/karios-*.dump 2>/dev/null | head -1 || echo 'none')"
  echo "iCloud copy:  $(ls -1t "$HOME/Library/Mobile Documents/com~apple~CloudDocs/Karios/backups/postgres"/karios-*.dump 2>/dev/null | head -1 || echo 'none')"
  if grep -q "$ZSHENV_MARKER" "$ZSHENV" 2>/dev/null; then
    echo "zshenv hook:  installed"
  else
    echo "zshenv hook:  not installed"
  fi
  exit 0
fi

# ---- unload ----------------------------------------------------------------
if [ "$ACTION" = "unload" ]; then
  if launchctl list 2>/dev/null | grep -q "$LABEL"; then
    _log "unloading $LABEL..."
    launchctl unload "$PLIST_PATH" 2>/dev/null || true
    rm -f "$PLIST_PATH"
    _log "removed $PLIST_PATH"
  else
    _log "$LABEL is not loaded; nothing to unload"
  fi
  if [ -f "$ZSHENV" ] && grep -q "$ZSHENV_MARKER" "$ZSHENV"; then
    # Strip the marker block
    /usr/bin/sed -i.bak "/$ZSHENV_MARKER/,/$ZSHENV_FOOTER/d" "$ZSHENV"
    rm -f "$ZSHENV.bak"
    _log "removed zshenv hook"
  fi
  _log "uninstall complete"
  exit 0
fi

# ---- install ---------------------------------------------------------------
[ -x "$DB_BACKUP" ] || _fail "missing executable: $DB_BACKUP"
mkdir -p "$HOME/Library/LaunchAgents" "$LOG_DIR"

# Pull DATABASE_URL from .env so the LaunchAgent doesn't depend on a sourced
# shell environment.
DATABASE_URL=""
if [ -f "$ROOT/.env" ]; then
  DATABASE_URL="$(grep -E '^DATABASE_URL=' "$ROOT/.env" | head -1 | cut -d= -f2-)"
fi

# Write plist.
_log "writing $PLIST_PATH..."
cat > "$PLIST_PATH" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>${LABEL}</string>

    <key>ProgramArguments</key>
    <array>
        <string>${DB_BACKUP}</string>
    </array>

    <!-- Daily at 03:00 — primary trigger for a Mac that's online overnight. -->
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>3</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>

    <!--
        Wake=true: launchd is allowed to schedule the script even around
        sleep transitions. The script itself enforces a 25h last-age check,
        so we don't accumulate redundant dumps while the Mac is awake
        continuously.
    -->
    <key>Wake</key>
    <true/>

    <!-- Also run once when the agent loads (= on login / manual load). -->
    <key>RunAtLoad</key>
    <true/>

    <key>LimitLoadSessions</key>
    <integer>1</integer>

    <key>EnvironmentVariables</key>
    <dict>
        <key>DATABASE_URL</key>
        <string>${DATABASE_URL}</string>
        <key>PATH</key>
        <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>
    </dict>

    <key>StandardOutPath</key>
    <string>${LOG_DIR}/db-backup.out.log</string>

    <key>StandardErrorPath</key>
    <string>${LOG_DIR}/db-backup.err.log</string>

    <key>WorkingDirectory</key>
    <string>${ROOT}</string>
</dict>
</plist>
EOF

# Validate XML.
if command -v plutil >/dev/null 2>&1; then
  if ! plutil -lint "$PLIST_PATH" >/dev/null 2>&1; then
    _fail "plist failed plutil -lint validation"
  fi
fi

# Unload prior copy if present.
if grep -q "$LABEL" < <(launchctl list 2>/dev/null); then
  _log "unloading existing $LABEL first..."
  launchctl unload "$PLIST_PATH" 2>/dev/null || true
fi

# Load.
_log "loading $LABEL..."
launchctl load -w "$PLIST_PATH"

# ---- zshenv hook -----------------------------------------------------------
# This is the "Mac was asleep for a week, user opens a shell" safety net.
# The script self-skips if the last backup is fresh, so this is cheap.
if [ -t 0 ]; then
  printf '\nAdd a one-liner to ~/.zshenv so every new shell session triggers a backup check (it self-skips if fresh)? [Y/n] '
  read -r ans
  case "${ans:-y}" in
    n|N) _log "skipped zshenv hook" ;;
    *)
      if ! grep -q "$ZSHENV_MARKER" "$ZSHENV" 2>/dev/null; then
        {
          printf '\n%s\n' "$ZSHENV_MARKER"
          printf '%s\n' "# Self-skipping backup check — runs db_backup.sh which bails if <25h since last dump."
          printf '%s\n' "[[ -x \"$DB_BACKUP\" ]] && nohup \"$DB_BACKUP\" >/dev/null 2>&1 &"
          printf '%s\n' "$ZSHENV_FOOTER"
        } >> "$ZSHENV"
        _log "appended zshenv hook"
      else
        _log "zshenv hook already present"
      fi
      ;;
  esac
else
  _log "non-interactive shell — skipping zshenv hook (run with TTY to install)"
fi

_log "install complete."
echo
echo "  Plist:  $PLIST_PATH"
echo "  Logs:   $LOG_DIR/db-backup.{out,err}.log"
echo
echo "Quick checks:"
echo "  scripts/install-db-backup-launchd.sh --status"
echo "  tail -f $LOG_DIR/db-backup.out.log"
echo
echo "To uninstall: scripts/install-db-backup-launchd.sh --unload"