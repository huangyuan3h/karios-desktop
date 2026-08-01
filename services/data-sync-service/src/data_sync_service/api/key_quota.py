"""API Key model + per-key rate-quota (OPT-051 / §12 #5).

Design (see ``docs/designs/api-contract.md`` and the upcoming
``docs/api/openapi.md``):

- KARIOS_API_KEYS env var is the single source of truth for **which keys are
  valid**. Format upgrade from OPT-045:
    old (still supported): ``"sk-abc,sk-xyz"`` — flat list, no per-key quota.
    new:                 ``"label:secret:rpm:rph:rpd,..."`` — per-key quota,
                          label = human-readable (e.g. "frontend", "external-ai"),
                          rpm/rph/rpd = 0 means unlimited.
- Quota is enforced as three independent sliding windows per key:
    rpm = last 60s, rph = last 3600s, rpd = last 86400s.
- Quota state lives **in memory** in the FastAPI process. It is reset on
  restart, which is acceptable because:
    1. Karios is a single-process FastAPI app (one worker) on a homelab box.
    2. Restarts are infrequent and a 60s grace is fine.
    3. Postgres-backed quotas would couple every /v1/* request to a DB write
       (unacceptable IO tax for the AI-assistant traffic profile).
- Keys are **opt-in**: an empty KARIOS_API_KEYS keeps the OPT-045 behaviour
  (no auth, no quota — every request allowed).
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Iterable

from fastapi import HTTPException, Request, status  # type: ignore[import-not-found]


# ---------------------------------------------------------------------------
# ApiKey model + env-var parser
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ApiKey:
    """One configured API key.

    ``label`` is human-readable (shown in `/v1/quota` response and in error
    logs); ``secret`` is the bearer token the caller must present. ``rpm``,
    ``rph``, ``rpd`` are independent request ceilings; 0 means unlimited.

    The frozen dataclass is hashable by (label, secret) so the in-memory
    state map can key off it.
    """

    label: str
    secret: str
    rpm: int
    rph: int
    rpd: int
    enabled: bool = True

    def has_quota(self) -> bool:
        return self.rpm > 0 or self.rph > 0 or self.rpd > 0


class ApiKeyParseError(ValueError):
    """Raised when ``KARIOS_API_KEYS`` is malformed."""


def _parse_one(raw: str) -> ApiKey:
    """Parse one comma-delimited entry.

    Old format (no colon): treat the whole string as a secret with no quota.
    New format (>=1 colon): ``label:secret[:rpm[:rph[:rpd]]]``.
    """
    raw = raw.strip()
    if not raw:
        raise ApiKeyParseError("empty entry in KARIOS_API_KEYS")
    parts = raw.split(":")
    if len(parts) == 1:
        return ApiKey(label=f"key-{raw[:4]}", secret=parts[0], rpm=0, rph=0, rpd=0)

    if len(parts) < 2 or len(parts) > 5:
        raise ApiKeyParseError(
            f"entry must have 1, 2, 3, 4, or 5 colon-separated fields, got {len(parts)}: {raw!r}"
        )

    label = parts[0].strip()
    secret = parts[1].strip()
    if not label:
        raise ApiKeyParseError(f"empty label in entry: {raw!r}")
    if not secret:
        raise ApiKeyParseError(f"empty secret in entry: {raw!r}")

    rpm = 0
    rph = 0
    rpd = 0
    if len(parts) >= 3 and parts[2].strip():
        try:
            rpm = int(parts[2])
        except ValueError as exc:
            raise ApiKeyParseError(f"rpm must be int, got {parts[2]!r}") from exc
        if rpm < 0:
            raise ApiKeyParseError(f"rpm must be >= 0, got {rpm}")
    if len(parts) >= 4 and parts[3].strip():
        try:
            rph = int(parts[3])
        except ValueError as exc:
            raise ApiKeyParseError(f"rph must be int, got {parts[3]!r}") from exc
        if rph < 0:
            raise ApiKeyParseError(f"rph must be >= 0, got {rph}")
    if len(parts) >= 5 and parts[4].strip():
        try:
            rpd = int(parts[4])
        except ValueError as exc:
            raise ApiKeyParseError(f"rpd must be int, got {parts[4]!r}") from exc
        if rpd < 0:
            raise ApiKeyParseError(f"rpd must be >= 0, got {rpd}")

    return ApiKey(label=label, secret=secret, rpm=rpm, rph=rph, rpd=rpd)


def parse_api_keys(raw: str | Iterable[str]) -> list[ApiKey]:
    """Parse the full KARIOS_API_KEYS string into a list of ApiKey.

    Labels must be unique; secrets must be unique. Duplicates are rejected
    because silent dedupe masks config typos and makes quota accounting
    ambiguous.
    """
    if isinstance(raw, str):
        raw = raw.strip()
        if not raw:
            return []
        entries = [chunk.strip() for chunk in raw.split(",") if chunk.strip()]
    else:
        entries = [chunk.strip() for chunk in raw if chunk.strip()]

    keys = [_parse_one(e) for e in entries]
    seen_labels: set[str] = set()
    seen_secrets: set[str] = set()
    for k in keys:
        if k.label in seen_labels:
            raise ApiKeyParseError(f"duplicate label {k.label!r} in KARIOS_API_KEYS")
        if k.secret in seen_secrets:
            raise ApiKeyParseError(f"duplicate secret under label {k.label!r}")
        seen_labels.add(k.label)
        seen_secrets.add(k.secret)
    return keys


def keys_from_env(env_var: str | None = None) -> list[ApiKey]:
    """Read and parse ``KARIOS_API_KEYS`` from the environment."""
    if env_var is None:
        env_var = os.getenv("KARIOS_API_KEYS", "")
    return parse_api_keys(env_var)


# ---------------------------------------------------------------------------
# In-memory sliding-window quota tracker
# ---------------------------------------------------------------------------


@dataclass
class _Window:
    """Sliding-window counter.

    Stores monotonic-clock timestamps for each request that consumed one
    quota slot. ``try_acquire`` prunes anything older than the window and
    then checks whether the remaining count is below the cap.
    """

    max_count: int
    window_seconds: int
    timestamps: list[float] = field(default_factory=list)

    def try_acquire(self, now: float | None = None) -> tuple[bool, int, int]:
        """Atomically check + record a hit.

        Returns (allowed, used_after, reset_in_seconds).
        ``reset_in_seconds`` = how many seconds until the OLDEST timestamp in
        the current window falls out, i.e. when one slot frees up.
        """
        if self.max_count <= 0:
            return True, 0, 0
        if now is None:
            now = time.monotonic()
        cutoff = now - self.window_seconds
        # prune
        self.timestamps = [t for t in self.timestamps if t > cutoff]
        if len(self.timestamps) >= self.max_count:
            oldest = self.timestamps[0]
            reset_in = max(1, int(self.window_seconds - (now - oldest)) + 1)
            return False, len(self.timestamps), reset_in
        self.timestamps.append(now)
        return True, len(self.timestamps), self.window_seconds


class _KeyState:
    """Per-key mutable quota state."""

    __slots__ = ("rpm_window", "rph_window", "rpd_window")

    def __init__(self, key: ApiKey) -> None:
        self.rpm_window = _Window(key.rpm, 60)
        self.rph_window = _Window(key.rph, 3600)
        self.rpd_window = _Window(key.rpd, 86400)


class QuotaTracker:
    """Process-wide in-memory quota tracker.

    Single instance per process; reset on restart. Keys with all-zero
    quotas short-circuit and never touch the window state.
    """

    def __init__(self) -> None:
        self._states: dict[str, _KeyState] = {}

    def _state_for(self, key: ApiKey) -> _KeyState:
        st = self._states.get(key.secret)
        if st is None:
            st = _KeyState(key)
            self._states[key.secret] = st
        return st

    def check_and_record(self, key: ApiKey) -> None:
        """Raise 429 if any of the key's windows is full; otherwise record.

        Order checked: rpm (most likely to fire) → rph → rpd. Even windows
        with limit=0 short-circuit so we don't waste CPU on a 0-capacity
        check.
        """
        if not key.has_quota():
            return
        st = self._state_for(key)
        for window in (st.rpm_window, st.rph_window, st.rpd_window):
            allowed, used, reset_in = window.try_acquire()
            if not allowed:
                limit = window.max_count
                window_name = (
                    "rpm" if window is st.rpm_window
                    else "rph" if window is st.rph_window
                    else "rpd"
                )
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail=(
                        f"Quota exceeded for key {key.label!r}: "
                        f"{used}/{limit} requests in last {window.window_seconds}s "
                        f"(window={window_name})."
                    ),
                    headers={
                        "Retry-After": str(reset_in),
                        "X-RateLimit-Limit": str(limit),
                        "X-RateLimit-Remaining": "0",
                        "X-RateLimit-Reset": str(reset_in),
                    },
                )

    def usage(self, key: ApiKey) -> dict[str, dict[str, int]]:
        """Snapshot of current usage (used / limit / window size) for /v1/quota.

        Windows with limit=0 (unlimited) are omitted because reporting
        "0/unlimited" is not actionable for a caller deciding whether to
        back off.
        """
        if not key.has_quota():
            return {}
        st = self._state_for(key)
        now = time.monotonic()
        snap: dict[str, dict[str, int]] = {}
        for name, window in (
            ("rpm", st.rpm_window),
            ("rph", st.rph_window),
            ("rpd", st.rpd_window),
        ):
            if window.max_count <= 0:
                continue  # unlimited — no quota to report
            window.timestamps = [t for t in window.timestamps if t > now - window.window_seconds]
            snap[name] = {
                "used": len(window.timestamps),
                "limit": window.max_count,
                "window_seconds": window.window_seconds,
            }
        return snap

    def reset(self) -> None:
        """Drop all state (test helper)."""
        self._states.clear()


# Process-wide singleton. Tests that need isolation should call reset().
quota_tracker = QuotaTracker()


# ---------------------------------------------------------------------------
# FastAPI dependency
# ---------------------------------------------------------------------------


class AuthenticatedKey:
    """Tiny request-scoped object that endpoints can `Depends(get_authenticated_key)`.

    Holds the matched ApiKey so /v1/quota can read its usage.
    """

    __slots__ = ("key",)

    def __init__(self, key: ApiKey) -> None:
        self.key = key


async def enforce_quota(request: Request) -> AuthenticatedKey:
    """Resolve + enforce auth + quota. Return AuthenticatedKey for the route.

    Behaviour:
    - If no keys configured: short-circuit (auth disabled, same as OPT-045).
    - If a key is configured: header must be present, must parse as
      ``Bearer <secret>``, secret must match a configured ApiKey. Then
      enforce per-window quota, raising 429 on overflow.
    """
    from .auth import parse_authorization_header  # local import → avoid cycle

    keys: list[ApiKey] = request.app.state.api_keys  # type: ignore[attr-defined]
    if not keys:
        # Auth disabled — return a sentinel with an unlimited key. Routes that
        # call `Depends(enforce_quota)` should also tolerate `key=None`.
        return AuthenticatedKey(ApiKey(label="anonymous", secret="", rpm=0, rph=0, rpd=0))

    secret = parse_authorization_header(request)
    matched = next((k for k in keys if k.secret == secret), None)
    if matched is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
            headers={"WWW-Authenticate": "Bearer"},
        )
    quota_tracker.check_and_record(matched)
    return AuthenticatedKey(matched)