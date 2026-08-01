#!/usr/bin/env bash
# Graceful shutdown for the Karios Docker stack.
#
# Usage:
#   scripts/docker-down.sh
#   scripts/docker-down.sh --volumes   # also drop Postgres + pgAdmin data (DESTRUCTIVE)
#
# Default: keeps all volumes (postgres data, pgadmin data) so the next start
# preserves state. Use --volumes only when you want a fresh database.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

VOLUMES=0
while [ $# -gt 0 ]; do
    case "$1" in
        --volumes)
            VOLUMES=1
            shift
            ;;
        --help|-h)
            cat <<'EOF'
Usage: docker-down.sh [--volumes]

Stops the Karios Docker stack.

Options:
  --volumes   Also remove Postgres + pgAdmin data volumes (DESTRUCTIVE).
EOF
            exit 0
            ;;
        *)
            echo "error: unknown argument '$1' (try --help)" >&2
            exit 64
            ;;
    esac
done

ARGS=(down --remove-orphans)
if [ "$VOLUMES" = "1" ]; then
    ARGS+=(--volumes)
    echo "[docker-down] WARNING: removing all data volumes."
fi

echo "[docker-down] stopping services..."
# Use the `${arr[@]+...}` form so an empty ARGS doesn't trigger `set -u`.
docker compose ${ARGS[@]+"${ARGS[@]}"}

echo "[docker-down] done."