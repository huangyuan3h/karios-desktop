import { describe, expect, it } from 'vitest';
import { app } from './app';

describe('app health check', () => {
  it('GET /healthz returns ok', async () => {
    const res = await app.request('/healthz');
    expect(res.status).toBe(200);
    const json = await res.json();
    expect(json).toEqual({ ok: true });
  });
});

describe('app error handling', () => {
  it('returns 404 for unknown routes', async () => {
    const res = await app.request('/unknown-route');
    expect(res.status).toBe(404);
  });
});
