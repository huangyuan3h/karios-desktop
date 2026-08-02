#!/usr/bin/env bash
# Start a Cloudflare Quick Tunnel to expose Karios /v1/* on the public internet.
#
# This is the zero-config option. Cloudflare issues a random *.trycloudflare.com
# URL each time the tunnel starts. Use this for: testing, demos, short-lived
# access while traveling. NOT for production — see setup-named-tunnel.sh for that.
#
# Usage:
#   scripts/start-quick-tunnel.sh                  # uses default port 4310
#   scripts/start-quick-tunnel.sh --port 3000      # custom local port
#   PORT=3000 scripts/start-quick-tunnel.sh        # same thing via env var
#
# Requirements:
#   - cloudflared installed (brew install cloudflared / see setup doc)
#   - Karios FastAPI running on the chosen port (default 4310)
#
# The script forwards Ctrl-C / SIGTERM to cloudflared. The URL is printed to
# stdout by cloudflared itself; this script just makes sure prerequisites are
# met and exits non-zero with a clear message when they aren't.

set -euo pipefail

# ---- arg parsing -----------------------------------------------------------

PORT="${PORT:-4310}"
while [ $# -gt 0 ]; do
    case "$1" in
        --port)
            PORT="$2"
            shift 2
            ;;
        -h|--help)
            cat <<EOF
Usage: $0 [--port N]

Starts a Cloudflare Quick Tunnel to http://127.0.0.1:<PORT> (default 4310).
Outputs a *.trycloudflare.com URL that anyone on the internet can use to reach
your local Karios /v1/* API.

This is for development and demos only. For a stable URL on your own domain
use setup-named-tunnel.sh.
EOF
            exit 0
            ;;
        *)
            echo "error: unknown argument '$1' (try --help)" >&2
            exit 64
            ;;
    esac
done

# ---- preflight -------------------------------------------------------------

if ! command -v cloudflared >/dev/null 2>&1; then
    cat >&2 <<EOF
error: cloudflared is not installed.

Install one of:
  macOS (Homebrew):   brew install cloudflared
  macOS (manual):     https://github.com/cloudflare/cloudflared/releases
                      (download cloudflared-darwin-arm64.tgz, untar, mv to /usr/local/bin)

After installing, re-run this script.
EOF
    exit 1
fi

# Check the local port is actually serving something.
if ! command -v curl >/dev/null 2>&1; then
    echo "error: curl is required (to verify the local port is listening)" >&2
    exit 1
fi

if ! curl --silent --fail --max-time 2 "http://127.0.0.1:${PORT}/v1/version" >/dev/null 2>&1; then
    cat >&2 <<EOF
error: nothing is listening on http://127.0.0.1:${PORT} (or /v1/version failed).

Make sure Karios FastAPI is running:
  cd services/data-sync-service
  PYTHONPATH=src uvicorn data_sync_service.main:app --host 127.0.0.1 --port ${PORT}
EOF
    exit 1
fi

# ---- run -------------------------------------------------------------------

echo "Karios /v1/version reachable on 127.0.0.1:${PORT} ✓"
echo "Starting Cloudflare Quick Tunnel (Ctrl-C to stop)..."
echo
echo "Once cloudflared prints the *.trycloudflare.com URL, test with:"
echo "  curl https://<your-url>.trycloudflare.com/v1/version"
echo

# Forward signals so Ctrl-C kills cloudflared cleanly.
exec cloudflared tunnel --no-autoupdate --url "http://127.0.0.1:${PORT}"
