"""OPT-051: /v1/quota — current API-key usage snapshot.

Why this endpoint exists:
- An external AI assistant cannot guess how much of its budget it has burned.
- Without /v1/quota the caller would have to wait until 429 to learn the
  limit, which is a poor integration experience.
- Returning the **matched** key's usage (not the global usage) prevents one
  key from probing another's budget.

No admin endpoint (/v1/admin/keys) ships in v1 — admin auth (who can list
*all* keys?) is a separate design decision. Use log scraping + manual config
edits for now.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Depends  # type: ignore[import-not-found]
from pydantic import BaseModel, Field  # type: ignore[-import-not-found]

from ..api.key_quota import AuthenticatedKey, enforce_quota, quota_tracker

router = APIRouter(
    prefix="/v1",
    tags=["v1:quota"],
    dependencies=[Depends(enforce_quota)],
)


class QuotaWindowSnapshot(BaseModel):
    """One window's used / limit / window size."""

    used: int = Field(description="Number of requests consumed in the current sliding window.")
    limit: int = Field(description="Configured cap for this window; 0 means unlimited.")
    window_seconds: int = Field(description="Size of the sliding window in seconds (60/3600/86400).")


class QuotaResponse(BaseModel):
    """Snapshot of the *current* key's quota state."""

    key_label: str = Field(
        description=(
            "Human-readable label of the matched API key (e.g. 'frontend', "
            "'external-ai'). Anonymous when KARIOS_API_KEYS is unset."
        )
    )
    auth_enabled: bool = Field(
        description="True iff KARIOS_API_KEYS is configured. When false, all windows are empty."
    )
    windows: dict[str, QuotaWindowSnapshot] = Field(
        description=(
            "Map of window name to its usage. Names: 'rpm' (per-minute), "
            "'rph' (per-hour), 'rpd' (per-day). A window with limit=0 is "
            "omitted by the quota tracker so it won't appear here."
        )
    )
    as_of: str = Field(description="ISO-8601 UTC timestamp of the snapshot.")


@router.get(
    "/quota",
    response_model=QuotaResponse,
    summary="Get current API-key quota usage",
    description=(
        "Returns the matched API key's current quota usage (used / limit / "
        "window size) for every configured window. When KARIOS_API_KEYS is "
        "unset this endpoint returns an empty `windows` map and "
        "`auth_enabled: false`. Callers should NOT poll this endpoint "
        "frequently — it is informational, not a counter reset."
    ),
)
def get_quota(auth: AuthenticatedKey = Depends(enforce_quota)) -> QuotaResponse:
    """Return the matched key's quota usage snapshot."""
    key = auth.key
    raw = quota_tracker.usage(key)
    windows = {
        name: QuotaWindowSnapshot(used=s["used"], limit=s["limit"], window_seconds=s["window_seconds"])
        for name, s in raw.items()
    }
    return QuotaResponse(
        key_label=key.label,
        auth_enabled=bool(key.has_quota() or key.label != "anonymous"),
        windows=windows,
        as_of=datetime.now(UTC).isoformat(),
    )


def _all_keys_admin_view() -> list[dict[str, Any]]:
    """Internal helper: list every configured key + its current usage.

    Not exposed as a route — admin auth is a future design decision. Kept here
    so tests + scripts can introspect configuration without re-parsing the
    env var.
    """
    from ..api.key_quota import keys_from_env

    keys = keys_from_env()
    return [
        {
            "label": k.label,
            "rpm": k.rpm,
            "rph": k.rph,
            "rpd": k.rpd,
            "enabled": k.enabled,
            "usage": quota_tracker.usage(k),
        }
        for k in keys
    ]