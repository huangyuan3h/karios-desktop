import { describe, expect, it } from 'vitest';

import {
  TWIN_STAR_CLIP4,
  TwinStarActionResponseSchema,
  TwinStarClip4Schema,
  parseTwinStarAction,
  type TwinStarAction,
} from './twinStar';

const GOLDEN: TwinStarAction = {
  ok: true,
  core: {
    pick: 'OIL',
    symbol: 'ETF:513350',
    label: '持有原油 ETF',
    action: 'HOLD',
    active: true,
  },
  sat: {
    asOf: '2026-09-02',
    gateOpen: true,
    breadth: 0.588,
    gapCount: 111,
    candidates: [{ ts: '000712.SZ', name: '锦江投资', amp: 1, gapPct: 5, close: 10 }],
    blocked: [],
    alternates: [],
    snapshotMissing: false,
    snapshotStale: false,
    coreTargetPct: 50,
    satTargetPct: 50,
    book: {
      asOf: '2026-09-01',
      holdings: [],
      exitsDue: [],
      body: 3,
      liveHoldings: [],
      liveExitsDue: [],
      liveHeld: 0,
      liveFreeSlots: 4,
      engineHeld: 0,
    },
  },
  clip4: {
    maxPos: 4,
    slotOfSleeve: 0.25,
    satSlotNavPct: 12.5,
    body: 3,
    protectStopPct: 0,
  },
};

describe('TWIN_STAR_CLIP4', () => {
  it('locks 4 × 12.5% NAV (50/50 sleeve)', () => {
    expect(TWIN_STAR_CLIP4.maxPos).toBe(4);
    expect(TWIN_STAR_CLIP4.slotOfSleeve).toBe(0.25);
    expect(TWIN_STAR_CLIP4.satSlotNavPct).toBe(12.5);
    expect(TWIN_STAR_CLIP4.maxPos * TWIN_STAR_CLIP4.satSlotNavPct).toBe(
      TWIN_STAR_CLIP4.satSleevePct,
    );
    expect(TWIN_STAR_CLIP4.body).toBe(3);
    expect(TWIN_STAR_CLIP4.protectStopPct).toBe(0);
  });
});

describe('TwinStarActionResponseSchema', () => {
  it('accepts a live Watchlist action payload', () => {
    const parsed = parseTwinStarAction(GOLDEN);
    expect(parsed.clip4.satSlotNavPct).toBe(12.5);
    expect(parsed.sat.coreTargetPct).toBe(50);
    expect(parsed.sat.candidates?.[0]?.ts).toBe('000712.SZ');
    expect(parsed.sat.book?.liveFreeSlots).toBe(4);
  });

  it('accepts idle 100/0 core/sat split', () => {
    const idle = {
      ...GOLDEN,
      sat: { ...GOLDEN.sat, gateOpen: false, coreTargetPct: 100 as const, satTargetPct: 0 as const },
    };
    expect(TwinStarActionResponseSchema.parse(idle).sat.coreTargetPct).toBe(100);
  });

  it('rejects S-3 10% slot sizing on clip4', () => {
    expect(() =>
      TwinStarClip4Schema.parse({
        ...GOLDEN.clip4,
        maxPos: 10,
        slotOfSleeve: 0.1,
        satSlotNavPct: 10,
      }),
    ).toThrow();
  });

  it('rejects a missing clip4 block', () => {
    const { clip4: droppedClip4, ...rest } = GOLDEN;
    expect(droppedClip4).toBeDefined();
    expect(() => TwinStarActionResponseSchema.parse(rest)).toThrow();
  });

  it('rejects coreTargetPct that is not 50 or 100', () => {
    expect(() =>
      TwinStarActionResponseSchema.parse({
        ...GOLDEN,
        sat: { ...GOLDEN.sat, coreTargetPct: 70, satTargetPct: 30 },
      }),
    ).toThrow();
  });
});
