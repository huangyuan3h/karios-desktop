"""Process-local guard so ensure_table DDL runs at most once per table key."""

from __future__ import annotations

import threading
from collections.abc import Callable

_ENSURED: set[str] = set()
_LOCK = threading.Lock()


def ensure_once(table_key: str, fn: Callable[[], None]) -> None:
    with _LOCK:
        if table_key in _ENSURED:
            return
        fn()
        _ENSURED.add(table_key)


def reset_ensured_for_tests() -> None:
    """Clear guard state between tests."""
    with _LOCK:
        _ENSURED.clear()
