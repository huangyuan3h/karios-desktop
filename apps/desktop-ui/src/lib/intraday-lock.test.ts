import { beforeEach, describe, expect, it } from 'vitest';

import {
  clearIntradaySnapshotCache,
  resolveStableActionPrice,
} from './intraday-lock';

function sh(iso: string): Date {
  return new Date(iso);
}

const base = {
  symbol: 'CN:600000',
  realtimePrice: 10.0,
  trendClose: 9.8,
  now: sh('2026-08-12T14:00:00+08:00'),
  tradingTime: true,
};

beforeEach(() => {
  clearIntradaySnapshotCache();
});

describe('resolveStableActionPrice (OPT-098 lock · freeze from 14:00)', () => {
  it('lunch break uses morning close (stable)', () => {
    const out = resolveStableActionPrice({
      ...base,
      now: sh('2026-08-12T12:30:00+08:00'),
      realtimePrice: 9.7, // would be stale anyway; must NOT be used
    });
    expect(out).toBe(9.8);
  });

  it('14:00–15:00 freezes the first 2pm quote per symbol', () => {
    const first = resolveStableActionPrice({
      ...base,
      now: sh('2026-08-12T14:00:00+08:00'),
      realtimePrice: 10.05,
    });
    expect(first).toBe(10.05);
    // Later price moves (even crossing a stop line) do NOT change the action price.
    const later = resolveStableActionPrice({
      ...base,
      now: sh('2026-08-12T14:40:00+08:00'),
      realtimePrice: 9.4,
    });
    expect(later).toBe(10.05);
  });

  it('13:00–14:00 keeps realtime (alerts still live before the freeze)', () => {
    const out = resolveStableActionPrice({
      ...base,
      now: sh('2026-08-12T13:30:00+08:00'),
      realtimePrice: 10.3,
    });
    expect(out).toBe(10.3);
  });

  it('snapshot cache resets the next day', () => {
    resolveStableActionPrice({
      ...base,
      now: sh('2026-08-12T14:05:00+08:00'),
      realtimePrice: 10.0,
    });
    const nextDay = resolveStableActionPrice({
      ...base,
      now: sh('2026-08-13T14:05:00+08:00'),
      realtimePrice: 10.5,
    });
    expect(nextDay).toBe(10.5);
  });

  it('falls back to trendClose when no quote seen yet in the window', () => {
    const out = resolveStableActionPrice({
      ...base,
      realtimePrice: null,
      now: sh('2026-08-12T14:10:00+08:00'),
    });
    expect(out).toBe(9.8);
  });

  it('outside the window uses realtime as usual', () => {
    const before = resolveStableActionPrice({
      ...base,
      now: sh('2026-08-12T11:00:00+08:00'),
      realtimePrice: 10.2,
    });
    expect(before).toBe(10.2);
    const after = resolveStableActionPrice({
      ...base,
      now: sh('2026-08-12T15:10:00+08:00'),
      realtimePrice: 9.5,
    });
    expect(after).toBe(9.5);
  });

  it('non-trading time (night/weekend) keeps realtime behaviour', () => {
    const out = resolveStableActionPrice({
      ...base,
      tradingTime: false,
      now: sh('2026-08-12T14:00:00+08:00'),
      realtimePrice: 9.9,
    });
    expect(out).toBe(9.9);
  });

  it('HK freeze extends to 16:00 close (CN stays 15:00)', () => {
    const first = resolveStableActionPrice({
      ...base,
      symbol: 'HK:02099',
      now: sh('2026-08-12T14:00:00+08:00'),
      realtimePrice: 10.0,
    });
    expect(first).toBe(10.0);
    const at1530 = resolveStableActionPrice({
      ...base,
      symbol: 'HK:02099',
      now: sh('2026-08-12T15:30:00+08:00'),
      realtimePrice: 9.4,
    });
    expect(at1530).toBe(10.0); // still frozen at the 14:00 snapshot
    const afterHkClose = resolveStableActionPrice({
      ...base,
      symbol: 'HK:02099',
      now: sh('2026-08-12T16:05:00+08:00'),
      realtimePrice: 9.4,
    });
    expect(afterHkClose).toBe(9.4); // unfrozen after 16:00
  });
});
