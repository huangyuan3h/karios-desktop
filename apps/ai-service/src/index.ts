import 'dotenv/config';

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

