import { describe, expect, it } from 'vitest';

import {
  SatelliteLivePanelResponseSchema,
  SatelliteSignalsResponseSchema,
} from './satelliteSignals';

describe('SatelliteSignalsResponseSchema', () => {
  it('parses a full signal panel payload', () => {
    const out = SatelliteSignalsResponseSchema.parse({
      ok: true,
      signals: {
        ok: true,
        date: '2026-09-15',
        decisionAvailable: true,
        gateOpen: true,
        breadth1430: 0.62,
        gapCount: 23,
        bucketSize: 7,
        poolSize: 5,
        ranked: [
          {
            ts: '000978.SZ',
            gapPct: 5.2,
            amp1430Pct: 1.1,
            px1430: 12.34,
            ampRank: 1,
            inBucket: true,
            skipReason: null,
            skipKind: null,
            fillable: true,
            unfillableReason: null,
            wouldFill: true,
          },
        ],
      },
    });
    expect(out.signals?.ranked?.[0]?.ts).toBe('000978.SZ');
    expect(out.signals?.ranked?.[0]?.wouldFill).toBe(true);
  });

  it('parses an unavailable panel payload', () => {
    const out = SatelliteSignalsResponseSchema.parse({
      ok: true,
      signals: { ok: true, date: '2026-09-16', decisionAvailable: false, reason: 'no daily row' },
    });
    expect(out.signals?.decisionAvailable).toBe(false);
  });
});

describe('SatelliteLivePanelResponseSchema', () => {
  it('parses the persisted 14:30 snapshot', () => {
    const out = SatelliteLivePanelResponseSchema.parse({
      ok: true,
      panel: {
        tradeDate: '2026-09-17',
        generatedAt: '2026-09-17T14:30:12+08:00',
        decisionAvailable: true,
        gateOpen: false,
        breadth1430: 0.28,
        gapCount: 34,
        bucketSize: 11,
        poolSize: 11,
        coverage: 0.91,
        quoted: 7289,
        universe: 8023,
        wouldFill: [],
        ranked: [
          {
            ts: '002128.SZ',
            ampRank: 3,
            inBucket: true,
            gapPct: 3.04,
            amp1430Pct: 2.92,
            px1430: 27.7,
            skipReason: null,
            fillable: true,
            wouldFill: false,
          },
        ],
        basis: 'live',
      },
    });
    expect(out.panel?.tradeDate).toBe('2026-09-17');
    expect(out.panel?.gateOpen).toBe(false);
    expect(out.panel?.ranked?.[0]?.ts).toBe('002128.SZ');
  });

  it('accepts a missing snapshot (card shows "待 14:30 判定")', () => {
    const out = SatelliteLivePanelResponseSchema.parse({ ok: true, panel: null });
    expect(out.panel).toBeNull();
  });
});
