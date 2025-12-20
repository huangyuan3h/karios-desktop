# Karios Desktop

Family investment analyzer desktop app (AI-first).

## Repo structure

- `apps/desktop-ui`: Next.js UI (embedded in Tauri WebView)
- `apps/ai-service`: Node/TypeScript AI service (Vercel AI SDK)
- `services/quant-service`: Python quant/data service (uv-managed)
- `packages/shared`: shared schemas/types
- `docs`: architecture and requirements

## Local development

### Prerequisites

- Node.js (LTS recommended)
- pnpm
- Python 3.13+
- uv
- Rust toolchain (for Tauri). Rust >= 1.83 is required.

### Run everything (dev)

```bash
pnpm dev
```

## Docs

See `docs/architecture-and-requirements.md`.


