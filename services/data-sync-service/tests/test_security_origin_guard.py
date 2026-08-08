"""H10: local-origin guard for state-changing requests.

Verifies LocalOriginGuardMiddleware behavior against the real app:
- POST from a malicious (non-local) Origin → 403, body not executed
- POST without Origin (curl / desktop client) → passes through
- POST with a local Origin (localhost / 127.0.0.1 / tauri://) → passes through
- GET with a malicious Origin → passes through (read-only is CORS-protected)
"""

from __future__ import annotations

import pytest

from fastapi.testclient import TestClient

from data_sync_service.main import app

client = TestClient(app)


def _non_local_post_origin(origin: str) -> int:
    resp = client.post(
        "/api/tv/screeners/sync-dummy-local-origin-guard",
        json={},
        headers={"Origin": origin},
    )
    return resp.status_code


@pytest.mark.parametrize(
    "origin",
    ["http://evil.example.com", "https://malware.test", "http://192.168.1.50:8080"],
)
def test_post_from_non_local_origin_rejected(origin: str) -> None:
    assert _non_local_post_origin(origin) == 403


@pytest.mark.parametrize(
    "origin",
    [
        "http://localhost:3000",
        "http://localhost:4173",
        "http://127.0.0.1:4330",
        "http://[::1]:4330",
        "tauri://localhost",
        "karios-desktop://localhost",
    ],
)
def test_post_from_local_origin_allowed(origin: str) -> None:
    assert _non_local_post_origin(origin) != 403


def test_post_without_origin_allowed() -> None:
    resp = client.post(
        "/api/tv/screeners/sync-dummy-local-origin-guard",
        json={},
    )
    assert resp.status_code != 403


def test_get_with_malicious_origin_is_read_only() -> None:
    resp = client.get(
        "/healthz",
        headers={"Origin": "http://evil.example.com"},
    )
    assert resp.status_code == 200
