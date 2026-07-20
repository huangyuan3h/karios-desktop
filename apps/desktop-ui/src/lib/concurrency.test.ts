import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { mapWithConcurrency } from './concurrency';

describe('mapWithConcurrency', () => {
  it('limits peak concurrency', async () => {
    let inFlight = 0;
    let peak = 0;
    const items = Array.from({ length: 10 }, (_, i) => i);

    await mapWithConcurrency(items, 3, async (n) => {
      inFlight += 1;
      peak = Math.max(peak, inFlight);
      await new Promise((r) => setTimeout(r, 10));
      inFlight -= 1;
      return n * 2;
    });

    expect(peak).toBeLessThanOrEqual(3);
    expect(peak).toBeGreaterThan(1);
  });

  it('preserves result order', async () => {
    const out = await mapWithConcurrency(['a', 'b', 'c'], 2, async (s) => s.toUpperCase());
    expect(out).toEqual(['A', 'B', 'C']);
  });
});
