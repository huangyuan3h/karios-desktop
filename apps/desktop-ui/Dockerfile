# syntax=docker/dockerfile:1.7
# Desktop UI — Next.js static export → nginx:alpine
#
# Two stages:
#   build:   pnpm install + next build → out/
#   runtime: nginx:alpine serving out/, with /api/ reverse-proxy to data-sync
#
# Build context: REPO ROOT (because desktop-ui depends on @karios/shared which is a
# workspace package at packages/shared). The repo-level .dockerignore filters out
# everything that isn't needed for the build.
#
# Build arguments (set in docker-compose.yml):
#   NEXT_PUBLIC_DATA_SYNC_BASE_URL — browser-visible URL of data-sync service
#   NEXT_PUBLIC_AI_BASE_URL       — browser-visible URL of ai-service
#
# Why baked at build time: Next.js inlines NEXT_PUBLIC_* into the static bundle.

# ---------- Stage 1: build ----------
FROM node:22-alpine AS build

ENV NODE_ENV=production \
    NEXT_TELEMETRY_DISABLED=1 \
    NPM_CONFIG_UPDATE_NOTIFIER=false

# pnpm (pinned to match repo).
RUN corepack enable \
    && corepack prepare pnpm@9.14.2 --activate

WORKDIR /repo

# Copy root + workspace manifests first (layer cache).
COPY package.json pnpm-lock.yaml* pnpm-workspace.yaml turbo.json ./

# Copy all workspace package.json files (pnpm needs them before install).
COPY apps/desktop-ui/package.json ./apps/desktop-ui/package.json
COPY apps/ai-service/package.json ./apps/ai-service/package.json
COPY packages/shared/package.json ./packages/shared/package.json
COPY services/data-sync-service/package.json ./services/data-sync-service/package.json 2>/dev/null || true

# Copy shared source (transpiled by Next transpilePackages).
COPY packages/shared ./packages/shared

# Copy desktop-ui source + config.
COPY apps/desktop-ui ./apps/desktop-ui

# Install (frozen lockfile). Filter to just desktop-ui so we don't pull in ai-service deps.
RUN --mount=type=cache,target=/root/.local/share/pnpm/store \
    pnpm install --frozen-lockfile --filter ./apps/desktop-ui...

# Build static export into out/.
ARG NEXT_PUBLIC_DATA_SYNC_BASE_URL=http://127.0.0.1:4330
ARG NEXT_PUBLIC_AI_BASE_URL=http://127.0.0.1:4310
ENV NEXT_PUBLIC_DATA_SYNC_BASE_URL=$NEXT_PUBLIC_DATA_SYNC_BASE_URL \
    NEXT_PUBLIC_AI_BASE_URL=$NEXT_PUBLIC_AI_BASE_URL

WORKDIR /repo/apps/desktop-ui
RUN pnpm run build

# ---------- Stage 2: runtime ----------
FROM nginx:1.27-alpine AS runtime

# Drop default config; copy ours.
RUN rm /etc/nginx/conf.d/default.conf

COPY --from=build /repo/apps/desktop-ui/out /usr/share/nginx/html
COPY apps/desktop-ui/nginx.conf /etc/nginx/conf.d/default.conf

# Non-root nginx (nginx:alpine ships as root; we keep that for simplicity but use a
# dedicated unprivileged user for the static content).
RUN chown -R nginx:nginx /usr/share/nginx/html /var/cache/nginx /var/log/nginx \
    && touch /var/run/nginx.pid \
    && chown nginx:nginx /var/run/nginx.pid

USER nginx

EXPOSE 8080

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD wget --quiet --tries=1 --spider http://127.0.0.1:8080/ \
        || exit 1

# nginx entrypoint inherited from base image.