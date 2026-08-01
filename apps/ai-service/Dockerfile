# syntax=docker/dockerfile:1.7
# AI Service — Hono + tsc (Node 22 + pnpm)
#
# Build context: REPO ROOT (because pnpm workspaces use a single root
# pnpm-lock.yaml at the repo root; per-app lockfiles don't exist).
# The root .dockerignore + this service's own .dockerignore keep image small.
#
# Why node:22-alpine (single-stage): no separate runtime needed — node is the runtime.

FROM node:22-alpine AS runtime

ENV NODE_ENV=production \
    PORT=4310 \
    HOST=0.0.0.0 \
    NPM_CONFIG_UPDATE_NOTIFIER=false

# wget: HEALTHCHECK below. tini: SIGTERM forwarding.
RUN apk add --no-cache wget tini

WORKDIR /repo

# Non-root user (uid/gid 1001 to match data-sync).
RUN addgroup -S -g 1001 karios \
    && adduser -S -u 1001 -G karios -h /app -s /sbin/nologin karios

# Install pnpm (pinned version to match repo).
RUN corepack enable \
    && corepack prepare pnpm@9.14.2 --activate \
    && pnpm config set store-dir /home/karios/.pnpm-store

# Copy ONLY what's needed for pnpm install to compute the dep tree.
# Layer caching: this layer is invalidated only when these files change.
COPY --chown=karios:karios package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY --chown=karios:karios apps/ai-service/package.json ./apps/ai-service/package.json
COPY --chown=karios:karios packages/shared/package.json ./packages/shared/package.json
COPY --chown=karios:karios apps/desktop-ui/package.json ./apps/desktop-ui/package.json
COPY --chown=karios:karios services/data-sync-service/package.json ./services/data-sync-service/package.json

# Install dependencies (frozen lockfile — must exist at repo root).
# --prod=false: include devDependencies (typescript, tsx) needed for `pnpm run build`,
# even though NODE_ENV=production is set globally below.
RUN --mount=type=cache,target=/home/karios/.pnpm-store,uid=1001,gid=1001 \
    pnpm install --frozen-lockfile --prod=false --filter ./apps/ai-service...

# Copy source + build.
# `apps/ai-service` includes its own tsconfig.json — no need to COPY it separately.
COPY --chown=karios:karios apps/ai-service ./apps/ai-service

# shared package source (transpiled by tsx/esbuild at runtime via workspace symlink).
COPY --chown=karios:karios packages/shared ./packages/shared

RUN pnpm --filter ./apps/ai-service run build

# Drop build-time noise; runtime needs only dist/ + node_modules/.
# We rely on pnpm's workspace symlink (node_modules/.pnpm/...) — copying the
# whole /repo is heavier than necessary, but keeps the workspace symlinks intact.
# If image size becomes a concern, switch to a separate runtime stage and
# `pnpm deploy --prod` (out of scope for OPT-056).

USER karios

EXPOSE 4310

# /healthz returns {ok:true} if process is up. AI provider keys validated separately.
HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=5 \
    CMD wget --quiet --tries=1 --spider http://127.0.0.1:4310/healthz \
        || exit 1

ENTRYPOINT ["/sbin/tini", "--"]
WORKDIR /repo/apps/ai-service
CMD ["node", "dist/index.js"]