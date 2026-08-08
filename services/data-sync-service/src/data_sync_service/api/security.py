"""Local-origin guard for state-changing requests (H10).

The desktop backend binds 127.0.0.1 and the internal /api/* routers are
unauthenticated by design (local-only). Any local webpage could otherwise
trigger a state-changing POST against these routes (CSRF-style). This
middleware rejects non-idempotent requests whose Origin is not a local
origin:

- http(s)://localhost:<port> / 127.0.0.1 / [::1]
- tauri://<host> (Tauri v2 webview)
- karios-desktop://<host> (Tauri v1 custom protocol)

Requests without an Origin header (curl, desktop fetch with credentials
offload, uvicorn CLI) pass through untouched. /v1/* routes keep their own
Bearer enforcement; this is defense-in-depth for the internal surface.
"""

from __future__ import annotations

from urllib.parse import urlparse

ALLOWED_SCHEMES = {"http", "https", "tauri", "karios-desktop"}
LOCAL_HOSTS = {"localhost", "127.0.0.1", "[::1]", "::1"}

IDEMPOTENT_METHODS = {"GET", "HEAD", "OPTIONS", "TRACE"}


def _is_local_origin(origin: str) -> bool:
    try:
        parsed = urlparse(origin)
    except ValueError:
        return False
    if parsed.scheme in {"tauri", "karios-desktop"}:
        return True
    if parsed.scheme not in {"http", "https"}:
        return False
    host = parsed.hostname or ""
    if host.startswith("["):
        host = host.strip("[]")
    return host in LOCAL_HOSTS


class LocalOriginGuardMiddleware:
    """Reject state-changing requests from non-local web origins."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        method = scope.get("method", "GET")
        headers = dict(scope.get("headers", []))
        origin = headers.get(b"origin")
        if (
            method.upper() not in IDEMPOTENT_METHODS
            and origin
            and not _is_local_origin(origin.decode("latin-1", "replace"))
        ):
            response = b'{"detail":"Non-local origin rejected"}'
            await send(
                {
                    "type": "http.response.start",
                    "status": 403,
                    "headers": [
                        (b"content-type", b"application/json"),
                        (b"content-length", str(len(response)).encode()),
                    ],
                }
            )
            await send({"type": "http.response.body", "body": response})
            return
        return await self.app(scope, receive, send)
