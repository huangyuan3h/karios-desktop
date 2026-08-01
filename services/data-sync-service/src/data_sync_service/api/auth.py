"""API Key authentication (OPT-045).

Opt-in: when ``KARIOS_API_KEYS`` is empty (default), all requests are allowed so
existing internal frontends keep working without changes. When the env var is set
to a comma-separated list of keys, every request to a router that depends on
:func:`require_api_key` must carry a matching ``Authorization: Bearer <key>``
header.

The 4 stable discovery endpoints (``/v1/version``, ``/v1/schema``,
``/v1/errors``, ``/v1/changelog``) intentionally do **not** depend on this — an
external AI assistant must be able to call ``/v1/schema`` before it has been
issued a key.
"""

from __future__ import annotations

from fastapi import Header, HTTPException, status  # type: ignore[import-not-found]

from ..config import get_settings

_UNSET = "__unset__"  # sentinel: distinguish "header missing" from "header empty"


def api_key_auth_enabled() -> bool:
    """True iff ``KARIOS_API_KEYS`` is configured (non-empty)."""
    return bool(get_settings().karios_api_keys)


async def require_api_key(
    authorization: str | None = Header(default=None, include_in_schema=False),
) -> None:
    """FastAPI dependency: enforce API Key when auth is enabled.

    When ``KARIOS_API_KEYS`` is empty this is a no-op. Otherwise the request must
    carry ``Authorization: Bearer <key>`` with a key present in the configured
    allow-list. Missing / malformed / unknown key → 401.
    """
    settings = get_settings()
    valid_keys = settings.karios_api_keys
    if not valid_keys:
        return  # auth disabled — keep zero-friction for local dev / first run

    if not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Authorization header",
            headers={"WWW-Authenticate": "Bearer"},
        )
    parts = authorization.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer" or not parts[1].strip():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization header must be 'Bearer <key>'",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = parts[1].strip()
    if token not in valid_keys:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
            headers={"WWW-Authenticate": "Bearer"},
        )
