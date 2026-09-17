import { describe, expect, it } from 'vitest';

import { HarborH2ShadowReportSchema } from './harborH2Shadow';

const row = {
  date: '2026-09-16',
  navLive: 1.02,
  navH2: 1.01,
  peakLive: 1.02,
  peakH2: 1.01,
  dayLivePct: 2.0,
  dayH2Pct: 1.0,
  spreadPt: -1.0,
  ddLivePct: 0,
  ddH2Pct: 0,
  mddGapPt: 0,
  pickLive: 'OIL',
  pickH2: 'GOLD',
  actionH2: 'rotate',
  idlePct: 100,
  status: 'watch',
};

const report = {
  ok: true,
  inception: '2026-09-16',
  generatedAt: '2026-09-16T10:35:00+00:00',
  rows: [row],
  latest: row,
  appended: 1,
  thresholds: {
    spreadWatchPt: -1.0,
    spreadRollbackPt: -2.0,
    mddGapWatchPt: 0.5,
    mddGapRollbackPt: 1.0,
  },
  note: 'shadow',
};

describe('HarborH2ShadowReportSchema', () => {
  it('parses a shadow ledger report', () => {
    const out = HarborH2ShadowReportSchema.parse(report);
    expect(out.latest.status).toBe('watch');
  });

  it('rejects an unknown status', () => {
    expect(() =>
      HarborH2ShadowReportSchema.parse({
        ...report,
        latest: { ...row, status: 'live' },
      }),
    ).toThrow();
  });
});
