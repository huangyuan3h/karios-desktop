import { describe, expect, it } from 'vitest';

import type { ExecutionDecisionChange } from '@karios/shared';

import {
  formatCondOrderDraftMarkdown,
  formatSinceLastCopyMarkdown,
} from './copy-ai-brief';

describe('formatSinceLastCopyMarkdown', () => {
  it('notes missing prior marker and lists action/mode changes', () => {
    const changes: ExecutionDecisionChange[] = [
      {
        id: '1',
        field: 'action',
        scope: 'symbol',
        symbol: 'CN:1',
        oldValue: 'BUY',
        newValue: 'WATCH',
        changedAt: '2026-07-18T02:00:00Z',
      },
      {
        id: '2',
        field: 'positionPct',
        scope: 'symbol',
        symbol: 'CN:1',
        oldValue: '10',
        newValue: '12',
        changedAt: '2026-07-18T02:01:00Z',
      },
    ];
    const md = formatSinceLastCopyMarkdown(changes, { lastAt: null });
    expect(md).toContain('## Since last copy');
    expect(md).toContain('no prior copy marker');
    expect(md).toContain('action');
    expect(md).not.toContain('positionPct');
  });

  it('includes since timestamp when provided', () => {
    const md = formatSinceLastCopyMarkdown([], {
      lastAt: '2026-07-18T01:00:00.000Z',
    });
    expect(md).toContain('since: 2026-07-18T01:00:00.000Z');
    expect(md).toContain('- None');
  });
});

describe('formatCondOrderDraftMarkdown', () => {
  it('lists EXIT before BUY with suggest size', () => {
    const md = formatCondOrderDraftMarkdown(
      [
        {
          symbol: 'CN:600002',
          action: 'BUY',
          why: 'MAINLINE_OK',
          suggestAddPct: 5,
          suggestSizeNote: 'clip',
        },
        {
          symbol: 'CN:600001',
          action: 'EXIT',
          why: 'EXIT_NOW',
          trigger: 11.2,
        },
      ],
      { allowNewEntries: true },
    );
    expect(md).toContain('## Cond order draft');
    expect(md).toContain('改单 CN:600001 卖出/清仓条件 @ Exit_Stop=11.2');
    expect(md).toContain('挂买 CN:600002 条件买入 +5.0% (clip)');
    const exitIdx = md.indexOf('CN:600001');
    const buyIdx = md.indexOf('CN:600002');
    expect(exitIdx).toBeLessThan(buyIdx);
  });

  it('prices TRIGGER_HIT EXIT at limit-down Order_Price not Exit_Stop', () => {
    const md = formatCondOrderDraftMarkdown(
      [
        {
          symbol: 'CN:002821',
          action: 'EXIT',
          why: 'TRIGGER_HIT',
          exitStop: 161.991,
        },
      ],
      {
        allowNewEntries: false,
        quotes: {
          'CN:002821': { preClose: 179.99, name: '凯莱英' },
        },
      },
    );
    expect(md).toContain('Trigger=161.991');
    expect(md).toContain('Order_Price=跌停价(161.99)');
    expect(md).not.toMatch(/TRIGGER_HIT[\s\S]*@ Exit_Stop=161\.991/);
  });

  it('does not fall back to Exit_Stop when preClose missing', () => {
    const md = formatCondOrderDraftMarkdown(
      [
        {
          symbol: 'CN:002821',
          action: 'EXIT',
          why: 'TRIGGER_HIT',
          exitStop: 161.991,
        },
      ],
      { allowNewEntries: false },
    );
    expect(md).toContain('Order_Price=LIMIT_DOWN(missing preClose)');
    expect(md).toContain('missing preClose — Order_Price not set to Exit_Stop');
    expect(md).not.toContain('@ Exit_Stop=161.991');
  });

  it('blocks buy lines when allowNewEntries is false', () => {
    const md = formatCondOrderDraftMarkdown(
      [{ symbol: 'CN:1', action: 'BUY', why: 'MAINLINE_OK', suggestAddPct: 5 }],
      { allowNewEntries: false },
    );
    expect(md).toContain('Gate blocks new entries');
    expect(md).not.toContain('- 挂买 ');
  });

  it('allows DEFENSIVE_SLEEVE_ALLOW buys when allowNewEntries is false', () => {
    const md = formatCondOrderDraftMarkdown(
      [
        { symbol: 'CN:601857', action: 'BUY', why: 'DEFENSIVE_SLEEVE_ALLOW', suggestAddPct: 5 },
        { symbol: 'CN:1', action: 'BUY', why: 'MAINLINE_OK', suggestAddPct: 5 },
      ],
      { allowNewEntries: false },
    );
    expect(md).toContain('挂买 CN:601857');
    expect(md).toContain('DEFENSIVE_SLEEVE_ALLOW');
    expect(md).toContain('Gate blocks new entries');
    expect(md).not.toContain('挂买 CN:1');
  });

  it('prefixes Queue for Next Open when tradingTime is false', () => {
    const md = formatCondOrderDraftMarkdown(
      [
        {
          symbol: 'CN:002821',
          action: 'EXIT',
          why: 'EXIT_NOW',
          exitStop: 42.1,
        },
      ],
      { allowNewEntries: false, tradingTime: false, phase: 'Weekend' },
    );
    expect(md).toContain('phase: Weekend — orders queue for next open');
    expect(md).toContain(
      '[Queue for Next Open] 改单 CN:002821 卖出/清仓条件 @ Exit_Stop=42.1',
    );
  });

  it('skips T1_LOCK sell drafts and notes the skip', () => {
    const md = formatCondOrderDraftMarkdown(
      [
        {
          symbol: 'CN:002821',
          action: 'HOLD',
          why: 'T1_LOCK',
          exitStop: 42.1,
        },
        {
          symbol: 'CN:600000',
          action: 'EXIT',
          why: 'TRIGGER_HIT',
          exitStop: 10,
        },
      ],
      {
        allowNewEntries: false,
        quotes: { 'CN:600000': { preClose: 11, name: '浦发银行' } },
      },
    );
    expect(md).toContain('T1_LOCK — skipped sell/exit drafts for 1 same-day buy(s)');
    expect(md).not.toContain('CN:002821');
    expect(md).toContain('改单 CN:600000 卖出/清仓条件 Trigger=10 Order_Price=跌停价(9.9)');
  });

  it('skips ENTRY_DATE_MISSING sell drafts fail-closed', () => {
    const md = formatCondOrderDraftMarkdown(
      [
        {
          symbol: 'CN:002821',
          action: 'HOLD',
          why: 'ENTRY_DATE_MISSING',
          exitStop: 42.1,
        },
      ],
      { allowNewEntries: false },
    );
    expect(md).toContain('ENTRY_DATE_MISSING — skipped sell/exit drafts for 1');
    expect(md).toContain('fail-closed');
    expect(md).not.toContain('改单 CN:002821');
  });
});
