import dotenv from 'dotenv';
import { existsSync } from 'node:fs';
import path from 'node:path';

// Local .env (apps/ai-service/.env) is the primary source — same as `dotenv/config`.
dotenv.config();
// Repo root .env fills in keys that only live there (GEMINI_API_KEY, proxy vars).
// dotenv never overrides already-set variables, so local values always win.
const rootEnv = path.resolve(process.cwd(), '..', '..', '.env');
if (existsSync(rootEnv)) dotenv.config({ path: rootEnv });

import { serve } from '@hono/node-server';

import { app } from './app';

process.on('unhandledRejection', (reason) => {
  // Prevent process crash / hard connection close; log for debugging.
  console.error('unhandledRejection:', reason);
});

process.on('uncaughtException', (err) => {
  // A stateful service cannot safely continue after an uncaught exception —
  // log and exit so the Tauri sidecar supervisor restarts a fresh process
  // instead of serving from a corrupted state.
  console.error('uncaughtException:', err);
  process.exit(1);
});

const rawPort = process.env.PORT ?? '4310';
const port = Number(rawPort);
if (!Number.isInteger(port) || port <= 0 || port > 65535) {
  console.error(`Invalid PORT env: ${JSON.stringify(rawPort)}`);
  process.exit(1);
}

serve({ fetch: app.fetch, port }, (info) => {
  console.log(`AI service listening on http://127.0.0.1:${info.port}`);
});

