import '@testing-library/jest-dom/vitest';

// Minimal ResizeObserver stub — recharts ResponsiveContainer needs it in jsdom.
class ResizeObserverStub implements ResizeObserver {
  callback: ResizeObserverCallback;
  constructor(callback: ResizeObserverCallback) {
    this.callback = callback;
  }
  observe(): void {}
  unobserve(): void {}
  disconnect(): void {}
}

if (typeof globalThis.ResizeObserver === 'undefined') {
  globalThis.ResizeObserver = ResizeObserverStub as unknown as typeof ResizeObserver;
}

// Node's undici fetch rejects on jsdom's cross-realm AbortSignal ("Expected
// signal ... to be an instance of AbortSignal") AFTER the test finished, which
// vitest reports as an unhandled error. Tests that exercise fetch stub it
// themselves (vi.stubGlobal / vi.spyOn); this default keeps accidental
// unmocked calls from surfacing as suite-level noise.
globalThis.fetch = (async () =>
  new Response(JSON.stringify({ ok: false, error: 'fetch not mocked in tests' }), {
    status: 503,
    headers: { 'content-type': 'application/json' },
  })) as unknown as typeof fetch;
