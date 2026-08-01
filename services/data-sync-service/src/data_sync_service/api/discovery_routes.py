"""OPT-045 Phase A: 4 stable discovery endpoints for external AI assistants.

These four paths are contract-level — once published they cannot be renamed
without bumping KARIOS_API_VERSION MAJOR. See docs/designs/api-contract.md.

| Endpoint            | Purpose                                     | Auth |
|---------------------|---------------------------------------------|------|
| GET /v1/version     | API version + min_compatible + released_at  | no   |
| GET /v1/schema      | OpenAPI 3.1 JSON (full, auto-generated)     | no   |
| GET /v1/errors      | Error code dictionary with recovery hints   | no   |
| GET /v1/changelog   | Interface changelog (Phase A: empty stub)   | no   |

The "no auth" column is intentional: an external AI assistant must be able to
call /v1/schema *before* it has been issued a key. Business endpoints that
should require a key live under a separate router (Phase B).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Request  # type: ignore[import-not-found]
from pydantic import BaseModel, Field  # type: ignore[import-not-found]

from ..config import get_settings

router = APIRouter(prefix="/v1", tags=["v1:discovery"])


# ---------------------------------------------------------------------------
# Pydantic schemas (the *descriptions* are the contract — they are surfaced
# verbatim in /v1/schema and read by LLMs to understand the API).
# ---------------------------------------------------------------------------


class VersionResponse(BaseModel):
    """Current Karios /v1/* API version.

    An external AI assistant MUST call this on startup and compare to its
    cached version. A MAJOR bump (defined in docs/designs/api-contract.md) means
    the assistant must re-bootstrap its client before further calls will work.
    """

    version: str = Field(
        ...,
        description=(
            "Current /v1/* API version in MAJOR.MINOR.PATCH form. Bump rules: "
            "MAJOR = field removed / renamed / endpoint removed; MINOR = new "
            "endpoint or new optional field; PATCH = description / default / "
            "error-code wording changes only. See docs/designs/api-contract.md."
        ),
        examples=["0.1.0"],
    )
    min_compatible: str = Field(
        ...,
        description=(
            "Oldest /v1/* version still supported by this build. A client on a "
            "version below this MUST refuse to start and ask the user to "
            "update the Karios integration."
        ),
        examples=["0.1.0"],
    )
    released_at: str = Field(
        ...,
        description=(
            "ISO-8601 UTC timestamp of when the current version was published. "
            "Use this to compute staleness of the cached /v1/schema."
        ),
    )


class ErrorCodeEntry(BaseModel):
    """One row of the /v1/errors dictionary.

    Error codes are stable contracts: once published, the `code` value never
    changes meaning. New codes can be added; old codes are deprecated but
    continue to be returned by their emitters until a MAJOR bump removes them.
    """

    code: str = Field(
        ...,
        description=(
            "Stable machine-readable error code (UPPER_SNAKE_CASE). Never "
            "reuse for a different meaning; never change the meaning of an "
            "existing code."
        ),
        examples=["SLEEVE_CAP_BLOCK"],
    )
    http_status: int = Field(
        ...,
        description="HTTP status returned to the client when this code is raised.",
        examples=[422],
    )
    meaning: str = Field(
        ...,
        description=(
            "Human-readable explanation of when this code is raised. One "
            "sentence. Avoid jargon; this is the first place an LLM looks when "
            "diagnosing a failure."
        ),
    )
    recovery_hint: str = Field(
        ...,
        description=(
            "Concrete, actionable fix the caller can try. Phrase as an "
            "imperative ('set X to ...', 'retry with ...', 'check ...'). This "
            "is what an LLM uses to auto-correct."
        ),
    )
    since: str = Field(
        ...,
        description="API version in which this code first appeared.",
        examples=["0.1.0"],
    )
    deprecated_since: str | None = Field(
        default=None,
        description=(
            "API version in which this code was marked deprecated. null means "
            "still active. The code continues to be returned; clients should "
            "migrate but will not break."
        ),
    )


class ErrorsResponse(BaseModel):
    """Response of GET /v1/errors."""

    version: str = Field(
        ...,
        description="Karios /v1/* version this dictionary corresponds to.",
    )
    codes: list[ErrorCodeEntry] = Field(
        ...,
        description=(
            "All known error codes in the current /v1/* surface. New codes "
            "may appear in MINOR versions; codes are never removed in-place."
        ),
    )


class ChangelogEntry(BaseModel):
    """One change row in the /v1/changelog feed."""

    version: str = Field(
        ...,
        description="Karios /v1/* version that introduced this change.",
    )
    kind: str = Field(
        ...,
        description=(
            "Change category. 'added' | 'changed' | 'deprecated' | 'removed' | "
            "'fixed'. 'removed' only appears in MAJOR versions."
        ),
    )
    target: str = Field(
        ...,
        description=(
            "Path or symbol affected. Format: 'GET /v1/market/snapshot' for "
            "endpoints, 'WatchlistItem.positionPct' for fields, "
            "'SLEEVE_CAP_BLOCK' for error codes."
        ),
    )
    summary: str = Field(
        ...,
        description=(
            "One-sentence description of the change. Must be specific enough "
            "that an LLM can write migration code from this alone."
        ),
    )


class ChangelogResponse(BaseModel):
    """Response of GET /v1/changelog."""

    since: str | None = Field(
        default=None,
        description=(
            "Echo of the `since` query parameter, or null if omitted. 'all' "
            "returns the full history."
        ),
    )
    changes: list[ChangelogEntry] = Field(
        default_factory=list,
        description=(
            "Chronological list (oldest first) of interface changes since the "
            "requested version. Empty list in Phase A — populated in Phase C "
            "from a structured CHANGELOG file generated by the version-bump "
            "script."
        ),
    )


# ---------------------------------------------------------------------------
# Seed error dictionary (Phase A; new codes can be appended in MINOR releases).
# Codes here mirror the watchlist Execution-Gate Why codes documented in
# docs/modules/README.md. They are surfaced verbatim via /v1/errors.
# ---------------------------------------------------------------------------
_SEED_ERROR_CODES: list[dict[str, Any]] = [
    {
        "code": "SLEEVE_CAP_BLOCK",
        "http_status": 422,
        "meaning": (
            "Refused a BUY/ADD because the satellite-sleeve positionPct sum "
            "has reached the upper bound of Gate.positionRangeHint."
        ),
        "recovery_hint": (
            "Either trim existing positions so the sum drops below the hint "
            "upper bound, or raise the hint in user settings. The endpoint "
            "does not modify positions."
        ),
        "since": "0.1.0",
        "deprecated_since": None,
    },
    {
        "code": "SECTOR_CONC_BLOCK",
        "http_status": 422,
        "meaning": (
            "Refused a BUY/ADD because the target Eastmoney industry already "
            "has ≥30% of the satellite sleeve allocated to it."
        ),
        "recovery_hint": (
            "Pick a different industry or trim current sector exposure. Do "
            "not retry the same symbol."
        ),
        "since": "0.1.0",
        "deprecated_since": None,
    },
    {
        "code": "ENTRY_BELOW_STOP",
        "http_status": 422,
        "meaning": (
            "Refused a BUY because the suggested Entry_Trigger (buyZoneHigh) "
            "is at or below HardStop (stopLossPrice). The trade would breach "
            "the stop on entry."
        ),
        "recovery_hint": (
            "Wait for the buyZoneHigh to move above stopLossPrice, or widen "
            "the score thresholds. Likely the trend has already broken; check "
            "TrendOK first."
        ),
        "since": "0.1.0",
        "deprecated_since": None,
    },
]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/version", response_model=VersionResponse, summary="Get current /v1/* API version")
def get_version() -> VersionResponse:
    """Return the running Karios /v1/* API version.

    This is the FIRST endpoint every external AI assistant should call. The
    value of ``version`` is governed by the ``KARIOS_API_VERSION`` env var
    (default ``0.1.0``). Bumping it is a contract change; see
    ``docs/designs/api-contract.md``.
    """
    settings = get_settings()
    return VersionResponse(
        version=settings.karios_api_version,
        min_compatible=settings.karios_api_version,  # Phase A: 0.x → only itself is compatible
        released_at=datetime.now(timezone.utc).isoformat(),
    )


@router.get(
    "/schema",
    summary="Get OpenAPI 3.1 JSON for the full Karios app",
    response_model=None,
    responses={
        200: {
            "description": "OpenAPI 3.1 document, auto-generated by FastAPI from Pydantic models + route signatures.",
            "content": {"application/json": {}},
        }
    },
)
def get_schema(request: Request) -> dict:
    """Return the live OpenAPI 3.1 document for the whole Karios app.

    The body is whatever FastAPI generates from the registered routers
    (including this discovery router itself, so a client that just learned
    ``/v1/schema`` can immediately discover the same paths). This is the
    single source of truth for "what does the API look like right now" — no
    hand-written schema, no drift.
    """
    return request.app.openapi()


@router.get("/errors", response_model=ErrorsResponse, summary="Get the /v1/* error-code dictionary")
def get_errors() -> ErrorsResponse:
    """Return the dictionary of error codes the /v1/* surface can emit.

    Each entry includes a ``recovery_hint`` phrased as an imperative so an
    LLM-based caller can auto-correct without further human input.
    """
    settings = get_settings()
    return ErrorsResponse(
        version=settings.karios_api_version,
        codes=[ErrorCodeEntry(**c) for c in _SEED_ERROR_CODES],
    )


@router.get(
    "/changelog",
    response_model=ChangelogResponse,
    summary="Get the /v1/* interface changelog since a given version (Phase A: always empty)",
)
def get_changelog(since: str | None = None) -> ChangelogResponse:
    """Return the chronological list of /v1/* interface changes.

    Phase A returns an empty list; Phase C will populate this from a structured
    ``API_CHANGELOG.md`` written by the version-bump script. The endpoint
    contract (path, query param, response shape) is final now so external
    clients can wire it up immediately.
    """
    return ChangelogResponse(since=since, changes=[])
