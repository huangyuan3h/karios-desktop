#!/usr/bin/env bash
set -euo pipefail

# Build sidecar binaries for macOS (Apple Silicon / Intel).
# Outputs are placed into `apps/desktop-ui/src-tauri/sidecars/` with Tauri v2 naming:
#   <name>-<target_triple>
#
# Notes:
# - This script assumes you already installed the required packagers:
#   - ai-service: `bun` (compile to a single binary)
#   - data-sync-service: `pyinstaller` (python)
# - You may need to adjust commands based on your environment.

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SIDECARS_DIR="$ROOT/apps/desktop-ui/src-tauri/sidecars"
mkdir -p "$SIDECARS_DIR"

# Detect target triple based on machine arch.
ARCH="$(uname -m)"
case "$ARCH" in
  arm64) TARGET_TRIPLE="aarch64-apple-darwin" ;;
  x86_64) TARGET_TRIPLE="x86_64-apple-darwin" ;;
  *) echo "Unsupported arch: $ARCH" >&2; exit 1 ;;
esac

echo "Target: $TARGET_TRIPLE"

AI_OUT="$SIDECARS_DIR/karios-ai-service-$TARGET_TRIPLE"
DATA_SYNC_OUT="$SIDECARS_DIR/karios-data-sync-service-$TARGET_TRIPLE"

echo "==> Building ai-service sidecar -> $AI_OUT"
if ! command -v bun >/dev/null 2>&1; then
  echo "bun is required to compile ai-service into a single binary. Install bun first." >&2
  exit 2
fi
cd "$ROOT/apps/ai-service"
pnpm -s build
# Bun embeds its runtime, producing a self-contained executable.
bun build dist/index.js --compile --outfile "$AI_OUT"
chmod +x "$AI_OUT"

echo "==> Building data-sync-service sidecar -> $DATA_SYNC_OUT"
# Example using PyInstaller:
cd "$ROOT/services/data-sync-service"
# Ensure deps are present (uv.lock is committed).
uv sync --project .

# Run PyInstaller WITH the project environment so runtime deps (uvicorn/fastapi/etc) are bundled.
# PyInstaller is not pinned in uv.lock by default, so we install it transiently via `--with`.
uv run --with pyinstaller pyinstaller \
  --clean \
  --noconfirm \
  --onefile \
  server_entry.py \
  --name "karios-data-sync-service-$TARGET_TRIPLE" \
  --distpath "$SIDECARS_DIR"

chmod +x "$DATA_SYNC_OUT"

echo "==> Sidecars built:"
ls -al "$AI_OUT" "$DATA_SYNC_OUT"
echo "Done."


