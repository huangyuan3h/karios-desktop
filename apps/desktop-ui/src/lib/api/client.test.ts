import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
  apiDeleteJson,
  apiGetJson,
  apiPostJson,
  apiPutJson,
} from './client';

const BASE = 'http://127.0.0.1:4330';

beforeEach(() => {
  vi.stubGlobal('fetch', vi.fn());
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('apiGetJson', () => {
  it('returns parsed JSON on success', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({
      ok: true,
      status: 200,
      statusText: 'OK',
      text: async () => JSON.stringify({ ok: true }),
    } as Response);

    const out = await apiGetJson<{ ok: boolean }>('/healthz');
    expect(out.ok).toBe(true);
    expect(fetch).toHaveBeenCalledWith(`${BASE}/healthz`, expect.objectContaining({ method: 'GET' }));
  });

  it('uses custom baseUrl', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({
      ok: true,
      status: 200,
      statusText: 'OK',
      text: async () => '{}',
    } as Response);

    await apiGetJson('/x', { baseUrl: 'http://ai.local' });
    expect(fetch).toHaveBeenCalledWith('http://ai.local/x', expect.any(Object));
  });

  it('throws on error status', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({
      ok: false,
      status: 500,
      statusText: 'Error',
      text: async () => 'boom',
    } as Response);

    await expect(apiGetJson('/fail')).rejects.toThrow('500 Error: boom');
  });

  it('returns empty object for empty body', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({
      ok: true,
      status: 204,
      statusText: 'No Content',
      text: async () => '',
    } as Response);

    const out = await apiGetJson<Record<string, never>>('/empty');
    expect(out).toEqual({});
  });
});

describe('apiPostJson', () => {
  it('posts JSON body', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({
      ok: true,
      status: 200,
      statusText: 'OK',
      text: async () => '{"count":1}',
    } as Response);

    const out = await apiPostJson<{ count: number }>('/watchlist/registry', { items: [] });
    expect(out.count).toBe(1);
    expect(fetch).toHaveBeenCalledWith(
      `${BASE}/watchlist/registry`,
      expect.objectContaining({
        method: 'POST',
        body: JSON.stringify({ items: [] }),
      }),
    );
  });
});

describe('apiPutJson and apiDeleteJson', () => {
  it('sends PUT', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({
      ok: true,
      status: 200,
      statusText: 'OK',
      text: async () => '{}',
    } as Response);

    await apiPutJson('/journal/1', { title: 'x' });
    expect(fetch).toHaveBeenCalledWith(
      `${BASE}/journal/1`,
      expect.objectContaining({ method: 'PUT' }),
    );
  });

  it('sends DELETE', async () => {
    vi.mocked(fetch).mockResolvedValueOnce({
      ok: true,
      status: 200,
      statusText: 'OK',
      text: async () => '{}',
    } as Response);

    await apiDeleteJson('/journal/1');
    expect(fetch).toHaveBeenCalledWith(
      `${BASE}/journal/1`,
      expect.objectContaining({ method: 'DELETE' }),
    );
  });
});
