import { describe, expect, it } from 'vitest';

import { SatellitePaperResponseSchema } from './satellitePaper';

describe('SatellitePaperResponseSchema', () => {
  it('parses a full forward paper book', () => {
    const out = SatellitePaperResponseSchema.parse({
      ok: true,
      paper: {
        ok: true,
        start: '2026-09-18',
        end: '2026-09-19',
        inception: '2026-09-18',
        decisionAvailable: true,
        prereq: { closedCount: 1, target: 20, met: false },
        stats: {
          closedCount: 1,
          openCount: 2,
          winCount: 1,
          winRate: 1,
          avgNetPnlPct: 4.7,
          bestNetPnlPct: 4.7,
          worstNetPnlPct: 4.7,
          paperPct: 3.2,
          paperMaxDdPct: 1.1,
          avgHeldDays: 3,
          closeReasons: { body_exit: 1 },
        },
        closed: [
          {
            ts: '000978.SZ',
            entryDate: '2026-09-18',
            exitDate: '2026-09-22',
            entryPxSrc: 'bar_1430',
            exitPxSrc: 'bar_1430',
            grossPnlPct: 5.0,
            netPnlPct: 4.7,
            heldDays: 3,
            closeReason: 'body_exit',
            ampPct: 1.1,
            ampRank: 2,
          },
        ],
        openLegs: [
          {
            ts: '002128.SZ',
            entryDate: '2026-09-19',
            entryPrice: 27.7,
            close: 28.1,
            heldDays: 1,
            daysLeft: 2,
            exitDue: '2026-09-23',
            pnlPct: 1.44,
          },
        ],
      },
    });
    expect(out.paper?.prereq.closedCount).toBe(1);
    expect(out.paper?.closed[0]?.closeReason).toBe('body_exit');
    expect(out.paper?.openLegs[0]?.ts).toBe('002128.SZ');
  });

  it('parses an empty / unavailable book', () => {
    const out = SatellitePaperResponseSchema.parse({
      ok: true,
      paper: {
        ok: true,
        start: '2026-09-18',
        end: '2026-09-18',
        inception: '2026-09-18',
        decisionAvailable: false,
        reason: 'replay failed',
        prereq: { closedCount: 0, target: 20, met: false },
        stats: {
          closedCount: 0,
          openCount: 0,
          winCount: 0,
          winRate: null,
          avgNetPnlPct: null,
          bestNetPnlPct: null,
          worstNetPnlPct: null,
          paperPct: null,
          paperMaxDdPct: null,
          avgHeldDays: null,
          closeReasons: {},
        },
        closed: [],
        openLegs: [],
      },
    });
    expect(out.paper?.decisionAvailable).toBe(false);
    expect(out.paper?.stats.winRate).toBeNull();
  });
});
