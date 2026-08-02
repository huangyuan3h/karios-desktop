#!/usr/bin/env bash
# Status report for the Karios Docker stack.
#
# Prints:
#   - container status (docker compose ps)
#   - health endpoint results
#   - last 20 lines of any unhealthy container
#
# Usage:
#   scripts/docker-status.sh
#   scripts/docker-status.sh --logs            # include last 50 lines of every service

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT"

LOGS=0
while [ $# -gt 0 ]; do
    case "$1" in
        --logs)
            LOGS=1
            shift
            ;;
        --help|-h)
            cat <<'EOF'
Usage: docker-status.sh [--logs]

Reports container status, health endpoint results, and (with --logs) recent log lines.
EOF
            exit 0
            ;;
        *)
            echo "error: unknown argument '$1' (try --help)" >&2
            exit 64
            ;;
    esac
done

echo "=== Containers ==="
docker compose ps --format 'table {{.Name}}\t{{.Status}}\t{{.Ports}}'

echo
echo "=== Health endpoints ==="
check() {
    local label="$1"
    local url="$2"
    if curl --silent --fail --max-time 3 "$url" >/dev/null 2>&1; then
        printf "  %-20s %s\n" "$label" "OK"
    else
        printf "  %-20s %s\n" "$label" "FAIL ($url)"
    fi
}
check "data-sync /healthz" "http://127.0.0.1:4330/healthz"
check "ai-service /healthz" "http://127.0.0.1:4310/healthz"
check "desktop-ui /healthz" "http://127.0.0.1:8080/healthz"
check "rsshub /" "http://127.0.0.1:1200/"
check "pgadmin /" "http://127.0.0.1:8081/"

if [ "$LOGS" = "1" ]; then
    echo
    echo "=== Recent logs ==="
    docker compose logs --tail=50 --no-color
fi