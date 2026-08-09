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
  // Prevent hard close without response; keep process alive for local dev.
  console.error('uncaughtException:', err);
});

const port = Number(process.env.PORT ?? 4310);

serve({ fetch: app.fetch, port }, (info) => {
  console.log(`AI service listening on http://127.0.0.1:${info.port}`);
});

