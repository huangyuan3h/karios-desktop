from __future__ import annotations

from datetime import datetime, timezone


def foo() -> dict:
    return {
        "message": "foo ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
