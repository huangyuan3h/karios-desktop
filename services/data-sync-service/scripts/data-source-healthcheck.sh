#!/usr/bin/env bash
# Lightweight data-source health check (OPT-050).
#
# Goal: catch misconfiguration BEFORE the next cron run fails silently.
# Does NOT make any external API calls (would waste rate-limit budget).
# Verifies: env vars are set, importable packages, local services up.
#
# Usage:
#   scripts/data-source-healthcheck.sh
#
# Exit codes:
#   0 — all green
#   1 — at least one required check failed (Karios at risk)
#   2 — at least one optional check failed (degraded but functional)

set -uo pipefail

# ---- color helpers (skip if no tty) -----------------------------------------
if [ -t 1 ]; then
    RED=$'\033[0;31m'
    YELLOW=$'\033[0;33m'
    GREEN=$'\033[0;32m'
    NC=$'\033[0m'
else
    RED=""; YELLOW=""; GREEN=""; NC=""
fi

ok=0
warn=0
fail=0

# ---- helpers ---------------------------------------------------------------

_env() {
    # 1 = required, 0 = optional
    local required="$1" name="$2" var="$3"
    local val="${!var:-}"
    if [ -n "$val" ]; then
        printf "  ${GREEN}✓${NC} %-32s %s = %s\n" "$name" "$var" "${val:0:8}***"
    elif [ "$required" -eq 1 ]; then
        printf "  ${RED}✗${NC} %-32s %s NOT SET (REQUIRED)\n" "$name" "$var"
        fail=$((fail + 1))
    else
        printf "  ${YELLOW}!${NC} %-32s %s not set (optional — feature disabled)\n" "$name" "$var"
        warn=$((warn + 1))
    fi
}

_import() {
    local name="$1" module="$2"
    if python -c "import $module" >/dev/null 2>&1; then
        printf "  ${GREEN}✓${NC} %-32s import %s\n" "$name" "$module"
    else
        printf "  ${RED}✗${NC} %-32s import %s FAILED\n" "$name" "$module"
        fail=$((fail + 1))
    fi
}

_local_port() {
    local name="$1" port="$2"
    if (echo > "/dev/tcp/127.0.0.1/${port}") >/dev/null 2>&1; then
        printf "  ${GREEN}✓${NC} %-32s localhost:%s listening\n" "$name" "$port"
    else
        printf "  ${YELLOW}!${NC} %-32s localhost:%s NOT listening\n" "$name" "$port"
        warn=$((warn + 1))
    fi
}

# ---- checks ---------------------------------------------------------------

echo "=== Required env vars ==="
_env 1 "Tushare Pro"         TU_SHARE_API_KEY
_env 1 "Postgres URL"        DATABASE_URL
_env 0 "Karios API version"  KARIOS_API_VERSION
_env 0 "Karios API keys"     KARIOS_API_KEYS
_env 0 "AI service base URL" AI_SERVICE_BASE_URL
_env 0 "RSSHub auto-start"   KARIOS_AUTO_START_RSSHUB
_env 0 "OpenAI key"          OPENAI_API_KEY
_env 0 "Google AI key"       GOOGLE_GENERATIVE_AI_API_KEY

echo
echo "=== Python imports ==="
# We run from the data-sync-service directory so `import data_sync_service...` works.
_import "psycopg"               psycopg
_import "fastapi"               fastapi
_import "pydantic"              pydantic
_import "akshare"               akshare
_import "pandas"                pandas
_import "apscheduler"           apscheduler
# yfinance + tushare are heavy — only check if available; not strictly required.
python -c "import tushare" >/dev/null 2>&1 \
    && printf "  ${GREEN}✓${NC} %-32s import tushare\n" "tushare" \
    || printf "  ${YELLOW}!${NC} %-32s import tushare (optional, Tushare cron will fail)\n" "tushare"
python -c "import yfinance" >/dev/null 2>&1 \
    && printf "  ${GREEN}✓${NC} %-32s import yfinance\n" "yfinance" \
    || printf "  ${YELLOW}!${NC} %-32s import yfinance (optional, HK backup will fail)\n" "yfinance"

echo
echo "=== Local services ==="
_local_port "Postgres"        5432
_local_port "Karios FastAPI"  4310
_local_port "AI service"      4310
_local_port "RSSHub"          1200

echo
echo "=== Summary ==="
if [ "$fail" -eq 0 ] && [ "$warn" -eq 0 ]; then
    printf "${GREEN}all green${NC} (%d ok)\n" "$ok"
    exit 0
elif [ "$fail" -eq 0 ]; then
    printf "${YELLOW}degraded${NC} (%d ok, %d warnings — Karios still works)\n" "$ok" "$warn"
    exit 2
else
    printf "${RED}failing${NC} (%d ok, %d warnings, %d failures — Karios at risk)\n" "$ok" "$warn" "$fail"
    exit 1
fi
