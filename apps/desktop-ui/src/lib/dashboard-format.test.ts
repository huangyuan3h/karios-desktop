import { describe, expect, it } from 'vitest';

import {
  buildIndexTrafficSummary,
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

describe('buildIndexTrafficSummary', () => {
  const cn = (name: string, signal: string) => ({ name, signal });

  it('is Strong only when all three CN lights are green', () => {
    const out = buildIndexTrafficSummary([
      cn('上证指数', 'green'),
      cn('创业板指', 'deep_green'),
      cn('中证500', 'green'),
      cn('恒生指数', 'red'),
    ]);
    expect(out.title).toContain('Strong');
  });

  it('is Diverging when some (not all) CN lights are green', () => {
    const out = buildIndexTrafficSummary([
      cn('上证指数', 'green'),
      cn('创业板指', 'red'),
      cn('中证500', 'green'),
    ]);
    expect(out.title).toContain('Diverging');
  });

  it('is Weak when no CN light is green', () => {
    const out = buildIndexTrafficSummary([
      cn('上证指数', 'red'),
      cn('创业板指', 'yellow'),
      cn('中证500', 'red'),
      cn('恒生指数', 'green'),
    ]);
    expect(out.title).toContain('Weak');
  });
});

describe('formatExecutionGateMarkdown hkGate', () => {
  it('appends HK gate fields when present', () => {
    const md = formatExecutionGateMarkdown({
      mode: 'HOLD_ONLY',
      allowNewEntries: false,
      marketRegime: 'Diverging',
      indexLight: 'red',
      reasons: ['REGIME_DIVERGING'],
      positionRangeHint: '30%',
      hkGate: {
        mode: 'ATTACK',
        allowNewEntries: true,
        marketRegime: 'Strong',
        indexLight: 'green',
        reasons: ['REGIME_STRONG'],
        positionRangeHint: '50%-60%',
      },
    });
    expect(md).toContain('- hkGate.mode: ATTACK');
    expect(md).toContain('- hkGate.allowNewEntries: true');
    expect(md).toContain('- hkGate.positionRangeHint: 50%-60%');
  });
});
