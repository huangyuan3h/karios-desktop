# syntax=docker/dockerfile:1.7
# AI Service — Hono + tsc (Node 22 + pnpm)
# Single-stage build; pnpm install in /app, tsc compile to dist/, run with node.
#
# Build context: apps/ai-service (package.json is at root).
# Note: ai-service does NOT depend on @karios/shared — pure Hono + Vercel AI SDK.

FROM node:22-alpine AS runtime

ENV NODE_ENV=production \
    PORT=4310 \
    HOST=0.0.0.0 \
    NPM_CONFIG_UPDATE_NOTIFIER=false

# wget: HEALTHCHECK below. tini: SIGTERM forwarding.
RUN apk add --no-cache wget tini

WORKDIR /app

# Non-root user.
RUN addgroup -S -g 1001 karios \
    && adduser -S -u 1001 -G karios -h /app -s /sbin/nologin karios

# Install pnpm (pinned version to match repo).
RUN corepack enable \
    && corepack prepare pnpm@9.14.2 --activate \
    && pnpm config set store-dir /home/karios/.pnpm-store

# Copy manifests first for layer cache.
COPY --chown=karios:karios package.json pnpm-lock.yaml* ./

# Install dependencies (frozen lockfile when available).
RUN --mount=type=cache,target=/home/karios/.pnpm-store,uid=1001,gid=1001 \
    pnpm install --frozen-lockfile --prod=false

# Copy source + build.
COPY --chown=karios:karios tsconfig.json ./
COPY --chown=karios:karios src ./src

RUN pnpm run build

USER karios

EXPOSE 4310

# /healthz returns {ok:true} if process is up. AI provider keys validated separately.
HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=5 \
    CMD wget --quiet --tries=1 --spider http://127.0.0.1:4310/healthz \
        || exit 1

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["node", "dist/index.js"]