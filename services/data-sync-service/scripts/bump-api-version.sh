#!/usr/bin/env bash
# Bump Karios /v1/* API version (OPT-047 Phase C).
#
# Usage:
#   scripts/bump-api-version.sh <major|minor|patch> "<one-line message>"
#
# What it does:
#   1. Verifies git working tree is clean (don't lose work).
#   2. Reads the current version from .env (KARIOS_API_VERSION) or config.py default.
#   3. Bumps the requested component.
#   4. Prints a unified diff for the user to review (does NOT auto-commit).
#
# Companion steps (manual, after the user reviews the diff):
#   - Update docs/api/CHANGELOG.md with a "## <NEW_VERSION> — <DATE>" entry,
#     listing every interface change (added / changed / deprecated / removed / fixed).
#   - If new error codes were added, append them to api/discovery_routes.py
#     `_SEED_ERROR_CODES` and update docs/api/errors.md.
#   - Run `pytest tests/test_discovery_endpoints.py tests/test_v1_business_endpoints.py tests/test_v1_explain_endpoint.py --no-cov`.
#   - Commit with message: `chore(api): bump to vX.Y.Z — <one-line message>`.

set -euo pipefail

if [ $# -ne 2 ]; then
    cat <<EOF >&2
Usage: $0 <major|minor|patch> "<one-line message>"

  major  — breaking: field removed / renamed / endpoint removed
  minor  — additive: new endpoint / new optional field
  patch  — non-breaking: description / default / error-code wording

Example:
  $0 minor "add /v1/explain/{symbol} context pack"
EOF
    exit 64
fi

BUMP_KIND="$1"
BUMP_MSG="$2"

case "$BUMP_KIND" in
    major|minor|patch) ;;
    *) echo "error: kind must be major|minor|patch (got '$BUMP_KIND')" >&2; exit 64 ;;
esac

# 1. clean tree
if ! git diff --quiet HEAD 2>/dev/null; then
    echo "error: git working tree is not clean. Commit or stash first." >&2
    exit 1
fi

# 2. read current version
ENV_FILE="$(git rev-parse --show-toplevel)/.env"
CURRENT="0.1.0"
if [ -f "$ENV_FILE" ] && grep -q '^KARIOS_API_VERSION=' "$ENV_FILE"; then
    CURRENT=$(grep '^KARIOS_API_VERSION=' "$ENV_FILE" | head -1 | cut -d= -f2 | tr -d '"' | tr -d ' ')
fi
echo "current: $CURRENT"

# 3. bump
IFS='.' read -r MAJOR MINOR PATCH <<EOF
$CURRENT
EOF

case "$BUMP_KIND" in
    major) MAJOR=$((MAJOR + 1)); MINOR=0; PATCH=0 ;;
    minor) MINOR=$((MINOR + 1)); PATCH=0 ;;
    patch) PATCH=$((PATCH + 1)) ;;
esac

NEW="$MAJOR.$MINOR.$PATCH"
echo "new:     $NEW"
echo "message: $BUMP_MSG"
echo

# 4. write to .env (create or update KARIOS_API_VERSION)
if [ -f "$ENV_FILE" ]; then
    if grep -q '^KARIOS_API_VERSION=' "$ENV_FILE"; then
        sed -i.bak "s/^KARIOS_API_VERSION=.*/KARIOS_API_VERSION=$NEW/" "$ENV_FILE"
        rm -f "$ENV_FILE.bak"
    else
        printf '\nKARIOS_API_VERSION=%s\n' "$NEW" >> "$ENV_FILE"
    fi
else
    printf 'KARIOS_API_VERSION=%s\n' "$NEW" > "$ENV_FILE"
fi

# 5. print diff + reminder
cat <<EOF

============================================================
Bumped KARIOS_API_VERSION: $CURRENT -> $NEW  ($BUMP_KIND)
Message: $BUMP_MSG
============================================================

Next steps (DO NOT auto-commit — review first):
  1. Edit docs/api/CHANGELOG.md — add a "## $NEW — \$(date +%Y-%m-%d)" section.
  2. If you added new error codes:
       - Append to api/discovery_routes.py:_SEED_ERROR_CODES
       - Append to docs/api/errors.md
  3. Run:
       pytest tests/test_discovery_endpoints.py \\
              tests/test_v1_business_endpoints.py \\
              tests/test_v1_explain_endpoint.py --no-cov
  4. Commit:
       git add . && git commit -m "chore(api): bump to v$NEW — $BUMP_MSG"
EOF
