"""Process-local guard so ensure_table DDL runs at most once per table key."""

from __future__ import annotations

from collections.abc import Callable

_ENSURED: set[str] = set()


def ensure_once(table_key: str, fn: Callable[[], None]) -> None:
    if table_key in _ENSURED:
        return
    fn()
    _ENSURED.add(table_key)


def reset_ensured_for_tests() -> None:
    """Clear guard state between tests."""
    _ENSURED.clear()
