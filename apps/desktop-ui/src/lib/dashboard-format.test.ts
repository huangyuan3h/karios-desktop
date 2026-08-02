import { describe, expect, it } from 'vitest';

import {
  fmtAmountCn,
  fmtSignedAmountCn,
  formatExecutionGateMarkdown,
  formatSrvIndexLine,
  mdTable,
} from './dashboard-format';

describe('fmtAmountCn', () => {
  it('formats 亿 and 万', () => {
    expect(fmtAmountCn(150_000_000)).toBe('1.50亿');
    expect(fmtAmountCn(25_000)).toBe('2.5万');
    expect(fmtAmountCn(500)).toBe('500');
  });

  it('returns dash for invalid input', () => {
    expect(fmtAmountCn(null)).toBe('—');
    expect(fmtAmountCn('')).toBe('—');
  });
});

describe('fmtSignedAmountCn', () => {
  it('prefixes positive and negative amounts', () => {
    expect(fmtSignedAmountCn(5_230_000_000)).toBe('+52.30亿');
    expect(fmtSignedAmountCn(-1_240_000_000)).toBe('-12.40亿');
    expect(fmtSignedAmountCn(null)).toBe('—');
  });
});

describe('formatSrvIndexLine', () => {
  it('formats level and overlap count', () => {
    expect(
      formatSrvIndexLine({ level: 'Extreme_High', overlapCount: 0 }),
    ).toBe('SRV 轮动指数: 极高（3D重叠 = 0）');
    expect(formatSrvIndexLine(null)).toBe('SRV 轮动指数: —');
  });
});

describe('formatExecutionGateMarkdown', () => {
  it('renders gate fields for downstream AI', () => {
    const md = formatExecutionGateMarkdown({
      mode: 'HOLD_ONLY',
      allowNewEntries: false,
      marketRegime: 'Diverging',
      indexLight: 'yellow',
      srvLevel: 'Extreme_High',
      srvOverlapCount: 1,
      downCount: 2800,
      reasons: ['SRV_EXTREME_HIGH'],
      positionRangeHint: '30%',
      satelliteNote: '禁止开新仓；仅管理退出与吊灯',
    });
    expect(md).toContain('## Execution Gate');
    expect(md).toContain('- mode: HOLD_ONLY');
    expect(md).toContain('- allowNewEntries: false');
    expect(md).toContain('srvLevel: Extreme_High (overlap=1)');
    expect(md).toContain('reasons: [SRV_EXTREME_HIGH]');
  });
});

describe('mdTable', () => {
  it('builds markdown table with header separator', () => {
    const md = mdTable(['A', 'B'], [[1, 2]]);
    expect(md).toContain('| A | B |');
    expect(md).toContain('| --- | --- |');
    expect(md).toContain('| 1 | 2 |');
  });
});
