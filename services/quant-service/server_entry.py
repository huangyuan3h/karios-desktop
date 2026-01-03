"""
Packaging-friendly entrypoint for quant-service.

- English-only code/comments (project convention).
- Uses uvicorn programmatic API so bundlers (e.g. PyInstaller) have a stable entry.
"""

from __future__ import annotations

import os

import uvicorn

from main import load_config


def main() -> None:
    cfg = load_config()
    # Prefer env vars for overrides (Tauri sidecar sets them).
    host = os.getenv("HOST", cfg.host)
    port = int(os.getenv("PORT", str(cfg.port)))
    uvicorn.run("main:app", host=host, port=port, log_level="info")


if __name__ == "__main__":
    main()


