"""OPT-045 Phase A tests: the 4 stable discovery endpoints + auth behavior.

These tests do not need Postgres (none of the 4 endpoints touches the DB), so
they run under the default test mode without ``@pytest.mark.requires_postgres``.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient  # type: ignore[import-not-found]

from data_sync_service.main import app  # type: ignore[import-not-found]


client = TestClient(app)


# ---------------------------------------------------------------------------
# /v1/version
# ---------------------------------------------------------------------------


def test_version_returns_200() -> None:
    resp = client.get("/v1/version")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("application/json")


def test_version_shape() -> None:
    payload: dict[str, Any] = client.get("/v1/version").json()
    assert set(payload.keys()) == {"version", "min_compatible", "released_at"}
    # SemVer-ish: MAJOR.MINOR.PATCH
    parts = payload["version"].split(".")
    assert len(parts) == 3
    assert all(p.isdigit() for p in parts)
    # released_at must be ISO-8601
    assert "T" in payload["released_at"]


def test_version_min_compatible_not_greater_than_current() -> None:
    payload = client.get("/v1/version").json()
    # Phase A only ships 0.x; min_compatible == version by construction.
    assert payload["min_compatible"] == payload["version"]


# ---------------------------------------------------------------------------
# /v1/schema
# ---------------------------------------------------------------------------


def test_schema_returns_200() -> None:
    resp = client.get("/v1/schema")
    assert resp.status_code == 200


def test_schema_is_openapi_3_or_3_1() -> None:
    spec = client.get("/v1/schema").json()
    # FastAPI emits 3.1.x; accept either 3.0.x or 3.1.x to stay future-proof.
    assert spec.get("openapi", "").startswith("3."), spec.get("openapi")
    assert "paths" in spec
    assert "info" in spec


def test_schema_contains_discovery_paths() -> None:
    """The schema must list the 4 discovery endpoints so a fresh client can
    discover the API by reading the spec it just downloaded."""
    spec = client.get("/v1/schema").json()
    paths = spec["paths"]
    for required in ("/v1/version", "/v1/schema", "/v1/errors", "/v1/changelog"):
        assert required in paths, f"missing path: {required}"
        assert "get" in paths[required], f"no GET on {required}"


def test_schema_contains_existing_routes() -> None:
    """Sanity check: existing /healthz and watchlist routes are still in the
    schema (Phase A must not regress the existing surface)."""
    spec = client.get("/v1/schema").json()
    paths = spec["paths"]
    assert "/healthz" in paths
    assert "/watchlist/registry" in paths


# ---------------------------------------------------------------------------
# /v1/errors
# ---------------------------------------------------------------------------


def test_errors_returns_200() -> None:
    resp = client.get("/v1/errors")
    assert resp.status_code == 200


def test_errors_shape() -> None:
    payload = client.get("/v1/errors").json()
    assert set(payload.keys()) == {"version", "codes"}
    assert isinstance(payload["codes"], list)
    assert len(payload["codes"]) >= 1
    first = payload["codes"][0]
    assert set(first.keys()) >= {
        "code",
        "http_status",
        "meaning",
        "recovery_hint",
        "since",
    }
    assert "deprecated_since" in first  # may be null


def test_errors_every_code_has_recovery_hint() -> None:
    """Invariant for OPT-045 / api-contract.md: a code without recovery_hint
    is useless to an LLM caller."""
    payload = client.get("/v1/errors").json()
    for c in payload["codes"]:
        assert c["code"], "empty code"
        assert c["recovery_hint"], f"missing recovery_hint on {c['code']}"
        assert c["meaning"], f"missing meaning on {c['code']}"
        assert 100 <= int(c["http_status"]) <= 599, f"bad http_status on {c['code']}"


# ---------------------------------------------------------------------------
# /v1/changelog
# ---------------------------------------------------------------------------


def test_changelog_returns_200_with_no_args() -> None:
    resp = client.get("/v1/changelog")
    assert resp.status_code == 200
    body = resp.json()
    assert body["since"] is None
    assert body["changes"] == []


def test_changelog_echoes_since() -> None:
    resp = client.get("/v1/changelog?since=0.0.5")
    assert resp.status_code == 200
    body = resp.json()
    assert body["since"] == "0.0.5"
    assert isinstance(body["changes"], list)


# ---------------------------------------------------------------------------
# Auth behavior: discovery endpoints MUST stay reachable without a key,
# because that's the whole point — an AI assistant has to discover the API
# before it can present a key. Business endpoints get key enforcement in
# Phase B.
# ---------------------------------------------------------------------------


def test_discovery_unauthenticated_by_default() -> None:
    """No KARIOS_API_KEYS env → no key required, no Authorization header sent.
    4 endpoints must all return 200."""
    for path in ("/v1/version", "/v1/schema", "/v1/errors", "/v1/changelog"):
        resp = client.get(path)
        assert resp.status_code == 200, f"{path} unexpectedly required auth"


def test_discovery_unaffected_by_invalid_auth_header() -> None:
    """Even with a malformed Authorization header the discovery endpoints must
    not 401 (auth is opt-in and only enforced by routers that opt in)."""
    for path in ("/v1/version", "/v1/schema", "/v1/errors", "/v1/changelog"):
        resp = client.get(path, headers={"Authorization": "Bearer garbage"})
        assert resp.status_code == 200, f"{path} leaked auth state"


# ---------------------------------------------------------------------------
# require_api_key dependency: when keys are configured, missing/malformed
# keys → 401. We test the dependency in isolation by directly invoking it.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_require_api_key_disabled_is_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    """When KARIOS_API_KEYS is empty the dependency must accept any request."""
    from data_sync_service.api.auth import require_api_key

    # Re-bind settings cache to a known empty-keys state.
    from data_sync_service import config

    config.get_settings.cache_clear()
    monkeypatch.setenv("KARIOS_API_KEYS", "")
    config.get_settings.cache_clear()

    # No header at all — must not raise.
    await require_api_key(authorization=None)


@pytest.mark.asyncio
async def test_require_api_key_rejects_when_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When KARIOS_API_KEYS is set, missing / malformed / wrong key → 401."""
    from fastapi import HTTPException

    from data_sync_service import config
    from data_sync_service.api.auth import require_api_key

    monkeypatch.setenv("KARIOS_API_KEYS", "right-key")
    config.get_settings.cache_clear()

    with pytest.raises(HTTPException) as exc_missing:
        await require_api_key(authorization=None)
    assert exc_missing.value.status_code == 401

    with pytest.raises(HTTPException) as exc_malformed:
        await require_api_key(authorization="not-bearer")
    assert exc_malformed.value.status_code == 401

    with pytest.raises(HTTPException) as exc_wrong:
        await require_api_key(authorization="Bearer wrong-key")
    assert exc_wrong.value.status_code == 401


@pytest.mark.asyncio
async def test_require_api_key_accepts_correct_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from data_sync_service import config
    from data_sync_service.api.auth import require_api_key

    monkeypatch.setenv("KARIOS_API_KEYS", "alpha,beta")
    config.get_settings.cache_clear()

    # Must not raise.
    await require_api_key(authorization="Bearer alpha")
    await require_api_key(authorization="Bearer beta")
