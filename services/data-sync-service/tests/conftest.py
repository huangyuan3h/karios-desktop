"""Shared pytest fixtures and Postgres skip hooks for data-sync-service."""

from __future__ import annotations

import pytest

from db_helpers import postgres_available


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "requires_postgres: integration test that needs a reachable Postgres",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if postgres_available():
        return
    skip = pytest.mark.skip(
        reason="Postgres not available (set DATABASE_URL or start docker-compose postgres)",
    )
    for item in items:
        if "requires_postgres" in item.keywords:
            item.add_marker(skip)
