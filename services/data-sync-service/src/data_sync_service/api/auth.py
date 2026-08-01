"""API Key authentication (OPT-045) + header parsing helper (OPT-051).

Opt-in: when ``KARIOS_API_KEYS`` is empty (default), all requests are allowed so
existing internal frontends keep working without changes. When the env var is
set (legacy format ``"secret1,secret2"`` or new ``"label:secret:rpm:rph:rpd"``
format), every request to a router that depends on :func:`require_api_key`
must carry a matching ``Authorization: Bearer <key>`` header.

The 4 stable discovery endpoints (``/v1/version``, ``/v1/schema``,
``/v1/errors``, ``/v1/changelog``) intentionally do **not** depend on this — an
external AI assistant must be able to call ``/v1/schema`` before it has been
issued a key.

OPT-051 added quota enforcement on top of auth: see ``api/key_quota.py`` for
the per-key sliding-window tracker and ``enforce_quota`` dependency.
``require_api_key`` is preserved for backwards compatibility; new routers
should use ``enforce_quota`` so they can read the matched ``ApiKey`` from the
request and surface quota usage.
"""

from __future__ import annotations

from fastapi import Header, HTTPException, Request, status  # type: ignore[import-not-found]


def parse_authorization_header(request: Request) -> str:
    """Extract the bearer secret from ``Authorization: Bearer <key>``.

    Raises 401 with ``WWW-Authenticate: Bearer`` if the header is missing,
    malformed, or the token is empty. Returns the stripped secret string on
    success. This is a pure helper (no dependency-injection magic) so it can
    be reused by both ``require_api_key`` and ``key_quota.enforce_quota``.
    """
    authorization = request.headers.get("authorization")
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
    return parts[1].strip()


async def require_api_key(
    authorization: str | None = Header(default=None, include_in_schema=False),
) -> None:
    """FastAPI dependency: enforce API Key when auth is enabled.

    Kept as a router-level dependency (``dependencies=[Depends(require_api_key)]``)
    for backwards compatibility with OPT-045 routers that don't need to know
    which key matched. New routers that want quota enforcement should use
    ``api.key_quota.enforce_quota`` instead.
    """
    from ..config import get_settings  # local import → avoid cycle on app import

    valid_keys = get_settings().karios_api_keys
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