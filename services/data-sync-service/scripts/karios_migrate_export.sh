#!/usr/bin/env bash
# Karios migrate-export — OPT-060.
#
# Build a single self-contained tarball that lets you stand up a fresh Karios
# database on a new Mac (or any host) with one command:
#
#   $ tar -xzf karios-migrate-20260804-195239.tar.gz
#   $ ./karios_restore.sh                       # uses bundled dump + restore
#
# What's inside:
#   karios-migrate-<ts>/
#   ├── README.txt                  quick start on the new machine
#   ├── karios_restore.sh           copy of scripts/db_restore.sh (no repo needed)
#   ├── karios-<ts>.dump            pg_dump -Fc of the database
#   ├── karios-<ts>.manifest.json   manifest: pg version, table count, sizes
#   ├── env.template                sanitized .env (real values redacted)
#   └── checksums.sha256            sha256 of dump + manifest for tamper detection
#
# Output: written to ./build/karios-migrate-<ts>.tar.gz (override with --out).
#
# Usage:
#   scripts/karios_migrate_export.sh
#   scripts/karios_migrate_export.sh --out ~/Desktop/karios-migrate.tar.gz
#   scripts/karios_migrate_export.sh --include-env   # include a redacted env
#
# Exit codes:
#   0 — success
#   1 — prerequisites missing
#   2 — dump build failed
#   3 — tarball build failed

set -uo pipefail

# ---- args ------------------------------------------------------------------
OUT_PATH=""
INCLUDE_ENV=0
while [ $# -gt 0 ]; do
  arg="$1"; shift
  case "$arg" in
    --out=*) OUT_PATH="${arg#--out=}" ;;
    --out)   OUT_PATH="${1:-}"; shift ;;
    --include-env) INCLUDE_ENV=1 ;;
    --help|-h)
      sed -n '2,32p' "$0"; exit 0 ;;
    *) echo "unknown arg: $arg" >&2; exit 1 ;;
  esac
done

# ---- config ----------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Pull DATABASE_URL from .env (if not already exported) so db_backup.sh
# picks it up and the bundle README can show a masked source URL.
if [ -z "${DATABASE_URL:-}" ] && [ -f "$ROOT/.env" ]; then
  DATABASE_URL="$(grep -E '^DATABASE_URL=' "$ROOT/.env" | head -1 | cut -d= -f2-)"
  export DATABASE_URL
fi

TS="$(date +%Y%m%d-%H%M%S)"
STAGE_DIR="$(mktemp -d -t karios-migrate.XXXXXX)"
PACK_DIR="$STAGE_DIR/karios-migrate-$TS"
DUMP_NAME="karios-$TS.dump"
MANIFEST_NAME="karios-$TS.manifest.json"

OUT_PATH="${OUT_PATH:-$ROOT/build/karios-migrate-$TS.tar.gz}"

mkdir -p "$PACK_DIR" "$(dirname "$OUT_PATH")"

_log() { printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"; }

# ---- first, build a fresh dump via db_backup.sh's logic --------------------
# We call db_backup.sh with --force --dry-run style: actually just call the
# real backup script into our staging dir.
_log "running db_backup.sh --force to obtain a fresh, verified dump..."
KARIOS_BACKUP_LOCAL_DIR="$STAGE_DIR/raw" KARIOS_BACKUP_ICLOUD_DIR= \
  bash "$SCRIPT_DIR/db_backup.sh" --force \
    2>&1 | sed 's/^/  backup: /'

FRESH_DUMP="$(ls -1t "$STAGE_DIR/raw"/karios-*.dump 2>/dev/null | head -1)"
FRESH_MANIFEST="$(ls -1t "$STAGE_DIR/raw"/karios-*.manifest.json 2>/dev/null | head -1)"
[ -n "$FRESH_DUMP" ] || { echo "ERROR: db_backup.sh produced no dump" >&2; exit 2; }

cp "$FRESH_DUMP" "$PACK_DIR/$DUMP_NAME"
[ -n "$FRESH_MANIFEST" ] && cp "$FRESH_MANIFEST" "$PACK_DIR/$MANIFEST_NAME"

# ---- env template ----------------------------------------------------------
# Build a sanitized .env so the receiver can `cp env.template .env` and only
# fill in the secret keys. We strip values for any var whose name looks like
# a credential. To err on the side of safety, we also redacted the DATABASE_URL
# (which contains the password) — receivers must re-set it to match their
# local Postgres.
if [ -f "$ROOT/.env" ]; then
  _log "building sanitized env template..."
  {
    echo "# Karios migrate template — generated $TS"
    echo "# Copy to .env on the new host and fill in the redacted values."
    echo "# DATABASE_URL must be re-pointed at the new host's Postgres."
    echo
    while IFS='=' read -r key value; do
      case "$key" in
        ''|\#*) echo "$key$value" >> "$PACK_DIR/env.template" ;;
        # Anything ending in KEY/TOKEN/SECRET/PASSWORD/URL is treated as a
        # secret by default. Override with explicit allowlist below if needed.
        DATABASE_URL|POSTGRES_URL|POSTGRES_PASSWORD|\
        *_KEY|*_TOKEN|*_SECRET|*_PASSWORD|*_URL|\
        PGADMIN_*|TELEGRAM_*)
          echo "$key=__REDACTED__"
          ;;
        *)
          echo "$key=$value"
          ;;
      esac
    done < "$ROOT/.env"
  } > "$PACK_DIR/env.template"
elif [ "$INCLUDE_ENV" -eq 1 ]; then
  echo "WARN: --include-env requested but $ROOT/.env not found" >&2
fi

# ---- restore script (bundled, no-repo variant) -----------------------------
# Copy db_restore.sh verbatim — the script is self-contained and works in any
# directory as long as DATABASE_URL is set. The receiver invokes it as
# `./karios_restore.sh <dump-file> [<db-name>] [--drop-existing]` from the
# unpacked bundle directory.
_log "bundling standalone restore script..."
cp "$SCRIPT_DIR/db_restore.sh" "$PACK_DIR/karios_restore.sh"
chmod +x "$PACK_DIR/karios_restore.sh"

# ---- checksums -------------------------------------------------------------
_log "writing checksums..."
(cd "$PACK_DIR" && shasum -a 256 "$DUMP_NAME" "${MANIFEST_NAME:-}" 2>/dev/null) \
  > "$PACK_DIR/checksums.sha256"

# ---- README ----------------------------------------------------------------
cat > "$PACK_DIR/README.txt" <<EOF
Karios migration bundle
=======================
Generated:       $TS
Source DB:       $(echo "${DATABASE_URL:-}" | sed -E 's#://[^@]+@#://***@#')
Postgres:        $(grep -o '"pg_version": *"[^"]*"' "$PACK_DIR/$MANIFEST_NAME" 2>/dev/null | head -1 | sed 's/.*: *"//;s/"$//' || echo unknown)
Public tables:   $(grep -o '"public_table_count": *[0-9]*' "$PACK_DIR/$MANIFEST_NAME" 2>/dev/null | grep -o '[0-9]*$' || echo unknown)
Dump size:       $(du -h "$PACK_DIR/$DUMP_NAME" 2>/dev/null | awk '{print $1}')

How to restore on a new Mac
----------------------------

  1. Have a Postgres container running (any name). If you don't:
       docker run -d --name postgres-db \\
         -e POSTGRES_USER=admin -e POSTGRES_PASSWORD=admin123 \\
         -e POSTGRES_DB=karios-desktop \\
         -p 5432:5432 postgres:16-alpine

  2. From this directory:
       ./karios_restore.sh karios-$TS.dump            # creates karios-desktop
       # or to drop & recreate an existing one:
       ./karios_restore.sh karios-$TS.dump --drop-existing

  3. Copy env.template to your data-sync-service .env and fill in the
     redacted keys (TU_SHARE_API_KEY, etc.).

That's it — the dump contains all schemas + data.
EOF

# ---- pack ------------------------------------------------------------------
_log "packing $OUT_PATH..."
if ! tar -C "$STAGE_DIR" -czf "$OUT_PATH" "karios-migrate-$TS"; then
  echo "ERROR: tar failed" >&2
  rm -rf "$STAGE_DIR"
  exit 3
fi

PACK_SIZE="$(du -h "$OUT_PATH" 2>/dev/null | awk '{print $1}')"
_log "tarball ready: $OUT_PATH ($PACK_SIZE)"

rm -rf "$STAGE_DIR"
_log "staging cleaned"