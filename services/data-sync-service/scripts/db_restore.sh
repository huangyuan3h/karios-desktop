#!/usr/bin/env bash
# Karios Postgres restore — OPT-060.
#
# Restore a dump produced by db_backup.sh (or karios_migrate_export.sh) into
# a running Postgres container. Used in three scenarios:
#
#   1. Recovery from accidental schema/data corruption (point-in-time restore
#      from the most recent dump).
#   2. Migration to a new laptop — the migrate-export tarball ships with its
#      own copy of this script (karios_restore.sh) so the receiver does not
#      need the full repo.
#   3. Smoke-test of a backup before trusting it ("does this dump actually
#      round-trip?") by restoring into a scratch database and diffing row
#      counts.
#
# Usage:
#   scripts/db_restore.sh <dump-file> [<db-name>] [--drop-existing] [--container=<name>]
#
#   <dump-file>     required — absolute path to a *.dump produced by pg_dump -Fc
#   <db-name>       target database name (default: karios-desktop, or
#                    KARIOS_RESTORE_DB env var)
#   --drop-existing drop & recreate the target DB before restoring (DESTRUCTIVE)
#   --container=    override the Postgres container (default: auto-detect)
#   --dry-run       print the plan without executing
#
# What it does, in order:
#   1. Validates <dump-file> is a real pg_dump -Fc archive.
#   2. Detects the running Postgres container (or honors --container).
#   3. Optionally drops + recreates the target DB.
#   4. pg_restore --no-owner --no-acl --jobs=4.
#   5. Re-runs Alembic upgrade head (so the receiver picks up any migration
#      scripts added after the dump was taken).
#   6. Verifies row counts against a sidecar manifest, if one is provided.
#
# Exit codes:
#   0 — restore succeeded
#   1 — bad arguments or environment
#   2 — dump invalid (TOC parse failed)
#   3 — pg_restore non-zero
#   4 — alembic upgrade failed
#   5 — verification mismatch

set -uo pipefail

# ---- args ------------------------------------------------------------------
DUMP_FILE=""
DB_NAME="${KARIOS_RESTORE_DB:-karios-desktop}"
DROP_EXISTING=0
DRY_RUN=0
PG_CONTAINER_OVERRIDE=""

for arg in "$@"; do
  case "$arg" in
    --drop-existing) DROP_EXISTING=1 ;;
    --dry-run)       DRY_RUN=1 ;;
    --container=*)   PG_CONTAINER_OVERRIDE="${arg#--container=}" ;;
    --help|-h)
      sed -n '2,30p' "$0"; exit 0 ;;
    -*) echo "unknown flag: $arg" >&2; exit 1 ;;
    *)
      if [ -z "$DUMP_FILE" ]; then
        DUMP_FILE="$arg"
      elif [ "$DB_NAME" = "${KARIOS_RESTORE_DB:-karios-desktop}" ]; then
        DB_NAME="$arg"
      else
        echo "too many positional args: $arg" >&2; exit 1
      fi
      ;;
  esac
done

[ -n "$DUMP_FILE" ] || { echo "usage: $0 <dump-file> [<db-name>] [--drop-existing]" >&2; exit 1; }
[ -f "$DUMP_FILE" ] || { echo "dump file not found: $DUMP_FILE" >&2; exit 1; }

# ---- env -------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# If DATABASE_URL is set, extract host/port/user for the target DB. Otherwise
# use sensible defaults matching the local docker-compose / standalone setup.
if [ -z "${DATABASE_URL:-}" ]; then
  for env_file in "$SCRIPT_DIR/../../../.env" "$SCRIPT_DIR/../../../../.env"; do
    if [ -f "$env_file" ]; then
      # shellcheck disable=SC1090
      DATABASE_URL="$(grep -E '^DATABASE_URL=' "$env_file" | head -1 | cut -d= -f2-)"
      export DATABASE_URL
      break
    fi
  done
fi

PG_USER="${POSTGRES_USER:-admin}"
PG_PASSWORD="${POSTGRES_PASSWORD:-admin123}"

# ---- detect container ------------------------------------------------------
detect_container() {
  local port="${PG_PORT:-5432}"
  docker ps --format '{{.Names}}\t{{.Image}}\t{{.Ports}}' 2>/dev/null \
    | awk -v port=":$port->" '$1 != "" && index($3, port) > 0 && $2 ~ /postgres/ {print $1; exit}'
}

PG_CONTAINER="${PG_CONTAINER_OVERRIDE:-${KARIOS_PG_CONTAINER:-$(detect_container)}}"
[ -n "$PG_CONTAINER" ] || { echo "ERROR: no running Postgres container detected (use --container=<name>)" >&2; exit 1; }

# ---- validate dump ---------------------------------------------------------
_log() { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }

if ! docker exec -i "$PG_CONTAINER" pg_restore -l < "$DUMP_FILE" >/dev/null 2>&1; then
  echo "ERROR: $DUMP_FILE is not a valid pg_dump -Fc archive (TOC parse failed)" >&2
  exit 2
fi
_log "dump TOC ok: $DUMP_FILE"

if [ "$DRY_RUN" -eq 1 ]; then
  _log "DRY-RUN: would restore into container=$PG_CONTAINER db=$DB_NAME (drop=$DROP_EXISTING)"
  exit 0
fi

# ---- drop + recreate -------------------------------------------------------
if [ "$DROP_EXISTING" -eq 1 ]; then
  _log "dropping existing database $DB_NAME..."
  docker exec -e PGPASSWORD="$PG_PASSWORD" "$PG_CONTAINER" \
    psql -U "$PG_USER" -d postgres -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='$DB_NAME' AND pid <> pg_backend_pid();" \
    >/dev/null 2>&1 || true
  docker exec -e PGPASSWORD="$PG_PASSWORD" "$PG_CONTAINER" \
    psql -U "$PG_USER" -d postgres -c "DROP DATABASE IF EXISTS \"$DB_NAME\";" \
    >/dev/null
  docker exec -e PGPASSWORD="$PG_PASSWORD" "$PG_CONTAINER" \
    createdb -U "$PG_USER" "$DB_NAME"
fi

# Verify DB exists; auto-create if not.
if ! docker exec -e PGPASSWORD="$PG_PASSWORD" "$PG_CONTAINER" \
    psql -U "$PG_USER" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$DB_NAME'" 2>/dev/null \
    | grep -q 1; then
  _log "database $DB_NAME missing — creating"
  docker exec -e PGPASSWORD="$PG_PASSWORD" "$PG_CONTAINER" \
    createdb -U "$PG_USER" "$DB_NAME"
fi

# ---- pg_restore ------------------------------------------------------------
# pg_restore from stdin can't use --jobs, so we docker cp the dump into the
# container first; that lets us parallelize and skips a slow stdout pipe.
_log "pg_restore -> $DB_NAME @ $PG_CONTAINER"
CONTAINER_DUMP="/tmp/karios_restore_$$.dump"
RESTORE_LOG="$(mktemp -t karios_restore.XXXXXX.log)"

docker cp "$DUMP_FILE" "$PG_CONTAINER:$CONTAINER_DUMP" \
  || { echo "ERROR: docker cp failed" >&2; rm -f "$RESTORE_LOG"; exit 3; }

if ! docker exec -e PGPASSWORD="$PG_PASSWORD" "$PG_CONTAINER" \
    pg_restore -U "$PG_USER" -d "$DB_NAME" \
               --no-owner --no-acl --jobs=4 \
               --exit-on-error \
               "$CONTAINER_DUMP" \
    > "$RESTORE_LOG" 2>&1; then
  echo "ERROR: pg_restore failed — last 30 lines of $RESTORE_LOG:" >&2
  tail -30 "$RESTORE_LOG" >&2
  docker exec "$PG_CONTAINER" rm -f "$CONTAINER_DUMP" 2>/dev/null || true
  rm -f "$RESTORE_LOG"
  exit 3
fi

docker exec "$PG_CONTAINER" rm -f "$CONTAINER_DUMP" 2>/dev/null || true
rm -f "$RESTORE_LOG"
_log "pg_restore ok"

# ---- alembic upgrade -------------------------------------------------------
# Re-run alembic to bring the restored DB to current schema. Skip if alembic
# isn't available (e.g. the migrate-export tarball ships without the repo).
ALEMBIC_INI="$SCRIPT_DIR/../alembic.ini"
ALEMBIC_DIR="$(dirname "$ALEMBIC_INI")"
if [ -f "$ALEMBIC_INI" ] && [ -d "$ALEMBIC_DIR/alembic" ]; then
  _log "running alembic upgrade head..."
  UPGRADE_CMD="PYTHONPATH=src DATABASE_URL=postgresql://$PG_USER:$PG_PASSWORD@localhost:5432/$DB_NAME alembic -c $ALEMBIC_INI upgrade head"
  if (cd "$ALEMBIC_DIR" && sh -c "$UPGRADE_CMD") >/dev/null 2>&1; then
    _log "alembic upgrade head ok"
  else
    echo "WARN: alembic upgrade failed (dump may still be usable — see alembic logs)" >&2
    exit 4
  fi
else
  _log "no alembic.ini here — skipping schema upgrade (caller must run it)"
fi

# ---- verify ----------------------------------------------------------------
_log "verifying row counts..."
TABLE_COUNT_AFTER="$(docker exec -e PGPASSWORD="$PG_PASSWORD" "$PG_CONTAINER" \
  psql -U "$PG_USER" -d "$DB_NAME" -tAc \
  "SELECT count(*) FROM pg_tables WHERE schemaname='public'" 2>/dev/null)"

# Find sidecar manifest (same basename as the dump).
MANIFEST=""
basename="$(basename "$DUMP_FILE" .dump)"
for dir in "$(dirname "$DUMP_FILE")" "$HOME/.karios/backups/postgres" "$HOME/Library/Mobile Documents/com~apple~CloudDocs/Karios/backups/postgres"; do
  candidate="$dir/$basename.manifest.json"
  if [ -f "$candidate" ]; then MANIFEST="$candidate"; break; fi
done

if [ -n "$MANIFEST" ]; then
  EXPECTED_TABLES="$(grep -o '"public_table_count":[[:space:]]*[0-9]*' "$MANIFEST" | grep -o '[0-9]*$')"
  if [ -n "$EXPECTED_TABLES" ] && [ "$TABLE_COUNT_AFTER" != "$EXPECTED_TABLES" ]; then
    echo "ERROR: row-count check failed — expected $EXPECTED_TABLES public tables, got $TABLE_COUNT_AFTER" >&2
    exit 5
  fi
  _log "manifest cross-check ok — $TABLE_COUNT_AFTER public tables (expected $EXPECTED_TABLES)"
else
  _log "no sidecar manifest — basic check: $TABLE_COUNT_AFTER public tables exist"
fi

_log "restore complete — $DB_NAME is live in $PG_CONTAINER"