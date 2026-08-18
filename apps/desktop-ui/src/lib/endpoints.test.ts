import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest';
import { AI_BASE_URL, DATA_SYNC_BASE_URL } from './endpoints';

describe('endpoints', () => {
  it('exports AI_BASE_URL with default value', () => {
    expect(AI_BASE_URL).toBeDefined();
    expect(typeof AI_BASE_URL).toBe('string');
  });

  it('exports DATA_SYNC_BASE_URL with default value', () => {
    expect(DATA_SYNC_BASE_URL).toBeDefined();
    expect(typeof DATA_SYNC_BASE_URL).toBe('string');
  });

  it('uses environment variable when set', async () => {
    const originalEnv = process.env.NEXT_PUBLIC_AI_BASE_URL;
    process.env.NEXT_PUBLIC_AI_BASE_URL = 'http://custom:8000';
    vi.resetModules();

    const { AI_BASE_URL: customUrl } = await import('./endpoints');
    expect(customUrl).toBe('http://custom:8000');

    process.env.NEXT_PUBLIC_AI_BASE_URL = originalEnv;
    vi.resetModules();
  });
});

describe('endpoints — tunnel origin (Family Hub Phase 0)', () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('uses the single tunnel host when served from it', async () => {
    vi.stubGlobal('window', { location: { hostname: 'karios.it-t.xyz' } });
    vi.resetModules();
    const { AI_BASE_URL: ai, DATA_SYNC_BASE_URL: ds } = await import('./endpoints');
    expect(ds).toBe('https://karios.it-t.xyz');
    expect(ai).toBe('https://karios.it-t.xyz/ai');
  });

  it('keeps local defaults on localhost', async () => {
    vi.stubGlobal('window', { location: { hostname: 'localhost' } });
    vi.resetModules();
    const { DATA_SYNC_BASE_URL: ds } = await import('./endpoints');
    expect(ds).toBe('http://127.0.0.1:4330');
  });
});
