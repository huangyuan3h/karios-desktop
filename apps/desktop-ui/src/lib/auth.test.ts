import { afterEach, describe, expect, it, vi } from 'vitest';

import { clearGatewayKey, getGatewayKey, installFetchAuth, setGatewayKey, UNAUTHORIZED_EVENT } from './auth';

describe('auth (Family Hub Phase 0)', () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    clearGatewayKey();
  });

  it('stores and reads the gateway key', () => {
    expect(getGatewayKey()).toBeNull();
    setGatewayKey('sekrit');
    expect(getGatewayKey()).toBe('sekrit');
  });

  it('attaches X-Karios-Key to Karios API requests', async () => {
    setGatewayKey('sekrit');
    const captured: Record<string, unknown> = {};
    vi.stubGlobal(
      'fetch',
      vi.fn((input: RequestInfo | URL, init?: RequestInit) => {
        captured.url = String(input);
        captured.headers = new Headers(init?.headers);
        return Promise.resolve(new Response('{}', { status: 200 }));
      }),
    );
    installFetchAuth();

    await window.fetch('https://karios.it-t.xyz/healthz');
    expect((captured.headers as Headers).get('X-Karios-Key')).toBe('sekrit');

    // Non-Karios URL must NOT carry the header.
    await window.fetch('https://example.com/foo');
    expect((captured.headers as Headers).get('X-Karios-Key')).toBeNull();
  });

  it('clears the key and broadcasts on 401', async () => {
    setGatewayKey('bad');
    vi.stubGlobal(
      'fetch',
      vi.fn(() => Promise.resolve(new Response('', { status: 401 }))),
    );
    installFetchAuth();
    const listener = vi.fn();
    window.addEventListener(UNAUTHORIZED_EVENT, listener);

    await window.fetch('http://127.0.0.1:4330/api/x');
    expect(getGatewayKey()).toBeNull();
    expect(listener).toHaveBeenCalledTimes(1);
  });
});
