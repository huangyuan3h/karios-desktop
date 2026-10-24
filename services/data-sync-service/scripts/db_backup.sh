#!/usr/bin/env bash
# Karios Postgres backup — OPT-060.
#
# Goal: guarantee that "even if the Mac sleeps for a week", we never lose
# more than ~24h of data, and we always have an off-machine copy (iCloud
# Drive) ready for migration to a new laptop.
#
# Strategy:
#   - pg_dump (custom format, compressed) via `docker exec` against the
#     running Postgres container. We auto-detect the container instead of
#     hardcoding a name, because the dev box uses `postgres-db` and the
#     compose stack uses `karios-postgres`.
#   - Daily-guarantee check: skip only when the newest dump is already from
#     TODAY (local calendar date parsed from the karios-YYYYMMDD-HHMMSS
#     filename). A 03:00 launchd run the day after a successful dump (~24h
#     later) must still dump — a pure age threshold (e.g. 25h) skips those runs
#     and stretches the effective RPO to ~48h (seen 2026-10-16..24: every even
#     day skipped at ~85660s < 90000s). After a week asleep, the next 03:00 run
#     sees a different calendar date and dumps immediately.
#   - Local retention: keep last N days (plist: 7) in ~/.karios/backups/postgres.
#   - NAS: ~/bin/nas-sync.sh copies dumps to the G1 NAS daily (03:17); old iCloud
#     dumps are deleted only after a verified NAS copy exists.
#   - Off-machine copy: mirror to ~/Library/Mobile Documents/com~apple~CloudDocs/
#     Karios/backups/postgres (iCloud Drive). iCloud's own daemon syncs even
#     while the Mac is asleep, so this is the truly remote copy.
#   - Verification: every dump is validated with `pg_restore --list`. If the
#     TOC cannot be parsed, the backup file is moved aside as `.corrupt` and
#     we exit non-zero so launchd surfaces the failure.
#   - Manifest: a small JSON sidecar records timestamp / size / pg version /
#     container name / verification result for forensics.
#
# Idempotent: safe to run twice in a row (overwrites nothing — each run
# produces a timestamped file).
#
# Usage:
#   scripts/db_backup.sh                 # normal run
#   scripts/db_backup.sh --dry-run       # show what would happen
#   scripts/db_backup.sh --force         # skip the last-age check
#
# Env overrides:
#   KARIOS_PG_CONTAINER   — pin a specific container (default: auto-detect)
#   KARIOS_BACKUP_LOCAL_DIR  — local backup root (default: ~/.karios/backups/postgres)
#   KARIOS_BACKUP_ICLOUD_DIR — iCloud backup root (default: ~/Library/Mobile Documents/com~apple~CloudDocs/Karios/backups/postgres)
#   KARIOS_LOCAL_RETENTION_DAYS  (default 30; launchd plist sets 7)
#   KARIOS_ICLOUD_RETENTION_DAYS (default 14; only deletes dumps already verified on the NAS)
#   KARIOS_ICLOUD_INDEX / KARIOS_NAS_LEDGER — see retention section
#   KARIOS_ICLOUD_MIRROR  (default 0 = off since 2026-10-07, iCloud quota full; set 1 to re-enable iCloud mirror + iCloud retention)
#
# Exit codes:
#   0 — success
#   1 — required tool missing or DB unreachable
#   2 — backup verification failed (corrupt dump)
#   3 — iCloud copy failed (local copy still good)

set -uo pipefail

# ---- args ------------------------------------------------------------------
DRY_RUN=0
FORCE=0
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=1 ;;
    --force)   FORCE=1 ;;
    --help|-h)
      sed -n '2,40p' "$0"
      exit 0
      ;;
    *) echo "unknown arg: $arg" >&2; exit 1 ;;
  esac
done

# ---- config ----------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

LOCAL_DIR="${KARIOS_BACKUP_LOCAL_DIR:-$HOME/.karios/backups/postgres}"
ICLOUD_DIR="${KARIOS_BACKUP_ICLOUD_DIR:-$HOME/Library/Mobile Documents/com~apple~CloudDocs/Karios/backups/postgres}"
LOCAL_RETENTION="${KARIOS_LOCAL_RETENTION_DAYS:-30}"
ICLOUD_RETENTION="${KARIOS_ICLOUD_RETENTION_DAYS:-14}"
ICLOUD_MIRROR="${KARIOS_ICLOUD_MIRROR:-0}"   # 0 = skip iCloud mirror + iCloud retention (local + NAS only)
ICLOUD_INDEX="${KARIOS_ICLOUD_INDEX:-$(dirname "$LOCAL_DIR")/icloud-index.txt}"   # names mirrored to iCloud
NAS_LEDGER="${KARIOS_NAS_LEDGER:-$(dirname "$LOCAL_DIR")/nas-archived.txt}"       # dumps verified on NAS (written by ~/bin/nas-sync.sh)

# ---- helpers ---------------------------------------------------------------
_log() { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }
_fail() { printf '[%s] ERROR: %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" >&2; exit "${2:-1}"; }

_require_cmd() {
  command -v "$1" >/dev/null 2>&1 || _fail "required command not found: $1" 1
}

# Resolve DATABASE_URL from .env if not already exported.
if [ -z "${DATABASE_URL:-}" ] && [ -f "$ROOT/.env" ]; then
  # shellcheck disable=SC1090
  DATABASE_URL="$(grep -E '^DATABASE_URL=' "$ROOT/.env" | head -1 | cut -d= -f2-)"
  export DATABASE_URL
fi
[ -n "${DATABASE_URL:-}" ] || _fail "DATABASE_URL not set (also not in $ROOT/.env)" 1

# Parse host:port from DATABASE_URL.
if [[ "$DATABASE_URL" =~ @(localhost|127\.0\.0\.1|\[?([a-f0-9:.]+)\]?):([0-9]+)/ ]]; then
  PG_HOST="${BASH_REMATCH[1]}"
  PG_PORT="${BASH_REMATCH[3]}"
else
  _fail "DATABASE_URL is not localhost (backup script only handles local Postgres): $DATABASE_URL" 1
fi

# ---- detect container ------------------------------------------------------
detect_container() {
  # Find a running Postgres container exposing port $PG_PORT on host.
  docker ps --format '{{.Names}}\t{{.Image}}\t{{.Ports}}' 2>/dev/null \
    | awk -v port=":$PG_PORT->" '$1 != "" && index($3, port) > 0 && $2 ~ /postgres/ {print $1; exit}'
}

PG_CONTAINER="${KARIOS_PG_CONTAINER:-$(detect_container)}"
[ -n "$PG_CONTAINER" ] || _fail "no running Postgres container exposing :$PG_PORT (set KARIOS_PG_CONTAINER or start the DB)" 1

# ---- last-age check --------------------------------------------------------
# If the most recent backup is younger than the threshold, skip unless --force.
LAST_BACKUP_FILE="$(ls -1t "$LOCAL_DIR"/karios-*.dump 2>/dev/null | grep -v '\.corrupt$' | head -1 || true)"
NOW_EPOCH="$(date +%s)"

if [ -n "$LAST_BACKUP_FILE" ] && [ "$FORCE" -eq 0 ]; then
  LAST_NAME="$(basename "$LAST_BACKUP_FILE")"
  # Calendar date from the filename (karios-YYYYMMDD-HHMMSS.dump). Unparsable
  # names fail open toward dumping — one backup too many beats one too few.
  LAST_DATE="$(printf '%s' "$LAST_NAME" | sed -E 's/^karios-([0-9]{8})-[0-9]{6}\.dump$/\1/')"
  TODAY_DATE="$(date +%Y%m%d)"
  LAST_MTIME_EPOCH="$(stat -f %m "$LAST_BACKUP_FILE" 2>/dev/null || echo 0)"
  AGE_SEC=$((NOW_EPOCH - LAST_MTIME_EPOCH))
  if [ -n "$LAST_DATE" ] && [ "$LAST_DATE" != "$LAST_NAME" ] && [ "$LAST_DATE" = "$TODAY_DATE" ]; then
    _log "skip — already dumped today ($LAST_NAME, ${AGE_SEC}s old)"
    _log "use --force to override"
    exit 0
  fi
  _log "last dump is from ${LAST_DATE:-unknown-date} ($LAST_NAME, ${AGE_SEC}s old; today $TODAY_DATE) — dumping fresh"
fi

# ---- prepare ---------------------------------------------------------------
_require_cmd docker
_require_cmd awk

TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
BACKUP_FILE="$LOCAL_DIR/karios-$TIMESTAMP.dump"
MANIFEST_FILE="$LOCAL_DIR/karios-$TIMESTAMP.manifest.json"

if [ "$DRY_RUN" -eq 1 ]; then
  _log "DRY-RUN: would dump into $BACKUP_FILE"
  if [ "$ICLOUD_MIRROR" = "1" ]; then _log "DRY-RUN: would mirror to $ICLOUD_DIR"; else _log "DRY-RUN: iCloud mirror disabled (KARIOS_ICLOUD_MIRROR=$ICLOUD_MIRROR)"; fi
  _log "DRY-RUN: would keep last ${LOCAL_RETENTION}d local / ${ICLOUD_RETENTION}d iCloud"
  exit 0
fi

mkdir -p "$LOCAL_DIR"

# ---- dump ------------------------------------------------------------------
_log "dumping $DATABASE_URL from container=$PG_CONTAINER -> $BACKUP_FILE"
if ! docker exec -e PGPASSWORD="${PGPASSWORD:-}" "$PG_CONTAINER" \
    pg_dump -U "${POSTGRES_USER:-admin}" \
            -d "${POSTGRES_DB:-karios-desktop}" \
            -Fc -Z 9 --no-owner --no-acl \
    > "$BACKUP_FILE" 2>"$LOCAL_DIR/karios-$TIMESTAMP.dump.err"; then
  _fail "pg_dump failed — see $LOCAL_DIR/karios-$TIMESTAMP.dump.err" 1
fi
rm -f "$LOCAL_DIR/karios-$TIMESTAMP.dump.err"

BACKUP_SIZE_BYTES="$(stat -f %z "$BACKUP_FILE" 2>/dev/null || echo 0)"
BACKUP_SIZE_HUMAN="$(du -h "$BACKUP_FILE" 2>/dev/null | awk '{print $1}')"

# ---- verify ----------------------------------------------------------------
_log "verifying dump TOC..."
if ! docker exec -i "$PG_CONTAINER" pg_restore -l < "$BACKUP_FILE" >/dev/null 2>"$LOCAL_DIR/karios-$TIMESTAMP.verify.err"; then
  CORRUPT_PATH="$BACKUP_FILE.corrupt"
  mv "$BACKUP_FILE" "$CORRUPT_PATH"
  _fail "dump verification failed — moved to $CORRUPT_PATH" 2
fi
rm -f "$LOCAL_DIR/karios-$TIMESTAMP.verify.err"

# ---- manifest --------------------------------------------------------------
PG_VERSION="$(docker exec "$PG_CONTAINER" postgres --version 2>/dev/null | awk '{print $3}' || echo "unknown")"
TABLE_COUNT="$(docker exec "$PG_CONTAINER" psql -U "${POSTGRES_USER:-admin}" -d "${POSTGRES_DB:-karios-desktop}" -tAc \
  "SELECT count(*) FROM pg_tables WHERE schemaname='public'" 2>/dev/null || echo "unknown")"

cat > "$MANIFEST_FILE" <<EOF
{
  "timestamp": "$TIMESTAMP",
  "created_at_iso": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "database_url_host": "$PG_HOST",
  "database_url_port": $PG_PORT,
  "pg_container": "$PG_CONTAINER",
  "pg_version": "$PG_VERSION",
  "public_table_count": $TABLE_COUNT,
  "backup_file": "$(basename "$BACKUP_FILE")",
  "backup_size_bytes": $BACKUP_SIZE_BYTES,
  "backup_size_human": "$BACKUP_SIZE_HUMAN",
  "verify_ok": true
}
EOF

_log "verified — $BACKUP_SIZE_HUMAN ($BACKUP_SIZE_BYTES bytes) / $TABLE_COUNT tables / pg $PG_VERSION"

# ---- mirror to iCloud ------------------------------------------------------
if [ "$ICLOUD_MIRROR" != "1" ]; then
  _log "iCloud mirror disabled (KARIOS_ICLOUD_MIRROR=$ICLOUD_MIRROR) — local + NAS only"
elif [ -d "$HOME/Library/Mobile Documents/com~apple~CloudDocs" ]; then
  if mkdir -p "$ICLOUD_DIR" 2>/dev/null; then
    if cp "$BACKUP_FILE" "$MANIFEST_FILE" "$ICLOUD_DIR/" 2>"$LOCAL_DIR/karios-$TIMESTAMP.icp.err"; then
      rm -f "$LOCAL_DIR/karios-$TIMESTAMP.icp.err"
      _log "mirrored to iCloud: $ICLOUD_DIR"
      printf '%s\n' "$(basename "$BACKUP_FILE")" "$(basename "$MANIFEST_FILE")" >> "$ICLOUD_INDEX"
    else
      _log "WARN: iCloud copy failed (local backup is still good) — see $LOCAL_DIR/karios-$TIMESTAMP.icp.err"
      # Non-fatal: local backup is intact.
    fi
  else
    _log "WARN: cannot write to iCloud dir (not signed in?); skipping"
  fi
else
  _log "iCloud Drive not configured — skipping remote mirror (local-only backup)"
fi

# ---- retention -------------------------------------------------------------
# Dates are parsed from the filename (karios-YYYYMMDD-HHMMSS.*) instead of -mtime, and every
# rm failure is logged (no more `2>/dev/null`).
#
# Why the old iCloud cleanup always logged "deleted 0": when this script runs from launchd,
# macOS privacy (TCC) blocks *listing* the iCloud Drive folder ("Operation not permitted"),
# while creating/unlinking a file by its full path still works. `find ... 2>/dev/null` therefore
# saw no files at all. iCloud file names now come from $ICLOUD_INDEX (appended on every mirror),
# merged with a directory listing when one is permitted.
#
# Safety: an old iCloud dump is only removed once nas-sync.sh has verified a copy on the NAS
# (listed in $NAS_LEDGER). Otherwise it is kept and reported as "deferred".
_file_date() {  # karios-20260804-195239.dump -> 20260804
  local b; b="$(basename "$1")"
  if [[ "$b" =~ ^karios-([0-9]{8})-[0-9]{6}\. ]]; then echo "${BASH_REMATCH[1]}"; fi
}
_rm_logged() {  # path -> 0 on success; logs failures
  local err
  if err="$(rm -f -- "$1" 2>&1)" && [ ! -e "$1" ]; then return 0; fi
  _log "WARN: retention: could not delete $1: ${err:-still present}"
  return 1
}

LOCAL_CUTOFF="$(date -v-"${LOCAL_RETENTION}"d +%Y%m%d)"
DELETED_LOCAL=0; DELETED_LOCAL_MF=0; LOCAL_ERRS=0
LATEST_NAME="$(basename "$BACKUP_FILE")"
for f in "$LOCAL_DIR"/karios-*.dump "$LOCAL_DIR"/karios-*.manifest.json; do
  [ -e "$f" ] || continue
  d="$(_file_date "$f")"; [ -n "$d" ] || continue
  [ "$(basename "$f")" = "$LATEST_NAME" ] && continue
  if [ "$d" -lt "$LOCAL_CUTOFF" ]; then
    if _rm_logged "$f"; then
      case "$f" in *.dump) DELETED_LOCAL=$((DELETED_LOCAL+1)) ;; *) DELETED_LOCAL_MF=$((DELETED_LOCAL_MF+1)) ;; esac
    else LOCAL_ERRS=$((LOCAL_ERRS+1)); fi
  fi
done
_log "retention: deleted $DELETED_LOCAL dumps + $DELETED_LOCAL_MF manifests dated before $LOCAL_CUTOFF (${LOCAL_RETENTION}d), errors=$LOCAL_ERRS"

if [ "$ICLOUD_MIRROR" = "1" ] && [ -d "$ICLOUD_DIR" ]; then
  ICLOUD_CUTOFF="$(date -v-"${ICLOUD_RETENTION}"d +%Y%m%d)"
  LISTING="$(ls -1 "$ICLOUD_DIR" 2>&1)"; LIST_RC=$?
  [ "$LIST_RC" -eq 0 ] || { LISTING=""; _log "note: iCloud dir not listable here (TCC) — using index $ICLOUD_INDEX"; }
  NAMES="$( { cat "$ICLOUD_INDEX" 2>/dev/null; printf '%s\n' "$LISTING"; } | grep -E '^karios-[0-9]{8}-[0-9]{6}\.dump$' | sort -u )"
  DELETED_ICL=0; DELETED_ICL_MF=0; DEFERRED_ICL=0; ICL_ERRS=0; GONE=""
  for n in $NAMES; do
    d="$(_file_date "$n")"; [ -n "$d" ] && [ "$d" -lt "$ICLOUD_CUTOFF" ] || continue
    stem="${n%.dump}"
    if [ ! -e "$ICLOUD_DIR/$n" ]; then GONE="$GONE $stem"; continue; fi
    if ! grep -q "^$n	" "$NAS_LEDGER" 2>/dev/null; then DEFERRED_ICL=$((DEFERRED_ICL+1)); continue; fi
    if _rm_logged "$ICLOUD_DIR/$n"; then
      DELETED_ICL=$((DELETED_ICL+1)); GONE="$GONE $stem"
      if [ -e "$ICLOUD_DIR/$stem.manifest.json" ] && _rm_logged "$ICLOUD_DIR/$stem.manifest.json"; then DELETED_ICL_MF=$((DELETED_ICL_MF+1)); fi
    else ICL_ERRS=$((ICL_ERRS+1)); fi
  done
  # drop removed entries from the index
  if [ -n "$GONE" ] && [ -f "$ICLOUD_INDEX" ]; then
    pat="$(printf '%s\n' $GONE | sed 's/^/^/; s/$/\\./' | paste -sd'|' -)"
    grep -Ev "$pat" "$ICLOUD_INDEX" > "$ICLOUD_INDEX.tmp" && mv "$ICLOUD_INDEX.tmp" "$ICLOUD_INDEX"
  fi
  _log "retention (iCloud): deleted $DELETED_ICL dumps + $DELETED_ICL_MF manifests dated before $ICLOUD_CUTOFF (${ICLOUD_RETENTION}d); deferred $DEFERRED_ICL (no verified NAS copy yet); errors=$ICL_ERRS"
fi

# ---- prune corrupt markers older than 14d (forensics only) -----------------
find "$LOCAL_DIR" -maxdepth 1 -name 'karios-*.dump.corrupt' -mtime +14 -delete || true

_log "done — latest: $(ls -1t "$LOCAL_DIR"/karios-*.dump | head -1 | xargs -I{} basename {})"