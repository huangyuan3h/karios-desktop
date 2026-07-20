import { describe, expect, it } from 'vitest';

import {
  ExecutionSnapshotIngestRequestSchema,
  ExecutionSnapshotSourceSchema,
} from './executionJournal';

describe('executionJournal schemas', () => {
  it('accepts ingest request shape', () => {
    const parsed = ExecutionSnapshotIngestRequestSchema.parse({
      source: 'manual',
      tradeDate: '2026-07-18',
      gate: {
        mode: 'ATTACK',
        allowNewEntries: true,
        marketRegime: 'Strong',
        indexLight: 'green',
        reasons: [],
      },
      cards: [
        {
          symbol: 'CN:600000',
          action: 'BUY',
          trailArmed: false,
          why: 'MAINLINE_5D_TOP3',
          positionPct: null,
        },
      ],
    });
    expect(parsed.source).toBe('manual');
    expect(parsed.cards[0]?.action).toBe('BUY');
  });

  it('rejects unknown source', () => {
    expect(() => ExecutionSnapshotSourceSchema.parse('cron')).toThrow();
  });
});
