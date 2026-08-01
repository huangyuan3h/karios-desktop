#!/usr/bin/env bash
# Set up a named Cloudflare Tunnel for Karios (production / stable URL).
#
# A named tunnel gives you a stable URL on YOUR domain (e.g. api.example.com)
# instead of the random *.trycloudflare.com a quick tunnel gives. It also
# survives restarts and runs unattended via launchd on macOS.
#
# This script is a 3-step skeleton — each step is a separate command because
# some require browser interaction (logging into Cloudflare). Read the
# companion doc first: docs/designs/cloudflare-tunnel-setup.md
#
# Usage:
#   scripts/setup-named-tunnel.sh --help
#
# Requirements (do these BEFORE running the steps):
#   1. brew install cloudflared
#   2. A Cloudflare account (free tier is enough)
#   3. A domain on Cloudflare (free DNS included)
#   4. Karios running locally on PORT (default 4310)

set -euo pipefail

# ---- arg parsing -----------------------------------------------------------

case "${1:-}" in
    -h|--help|"")
        cat <<EOF
Usage: $0

This script PRINTS the 3 commands you need to run, in order. It does NOT run
them for you — step 1 opens a browser, step 2 needs a domain.

  Step 1:  cloudflared tunnel login
           (opens a browser, you pick a domain on Cloudflare)
  Step 2:  cloudflared tunnel create karios
           (creates a tunnel named 'karios', saves credentials to
            ~/.cloudflared/<UUID>.json — keep this file secret)
  Step 3a: cloudflared tunnel route dns karios api.example.com
           (creates a CNAME api.example.com -> <UUID>.cfargotunnel.com)
  Step 3b: cloudflared tunnel run karios
           (starts the tunnel; bind it to a config file for production)

For 'Step 3b' to bind Karios correctly, drop a ~/.cloudflared/config.yml
like this (see the full template in the setup doc):

  tunnel: karios
  credentials-file: /Users/<you>/.cloudflared/<UUID>.json
  ingress:
    - hostname: api.example.com
      service: http://127.0.0.1:4310
    - service: http_status:404

After all 3 steps, test with:
  curl https://api.example.com/v1/version

For unattended start at login on macOS, install as a launchd service:
  sudo cloudflared service install
EOF
        exit 0
        ;;
    *)
        echo "error: unknown argument '$1' (try --help)" >&2
        exit 64
        ;;
esac
