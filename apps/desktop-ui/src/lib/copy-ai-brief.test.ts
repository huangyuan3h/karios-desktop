import { describe, expect, it } from 'vitest';

import type { ExecutionDecisionChange } from '@karios/shared';

import {
  formatAiCopyInstructionHeader,
  formatCondOrderDraftMarkdown,
  formatSinceLastCopyMarkdown,
} from './copy-ai-brief';

describe('formatAiCopyInstructionHeader', () => {
  it('asks for ops table then market brief and coaching', () => {
    const md = formatAiCopyInstructionHeader();
    expect(md).toContain('## AI instructions (embedded)');
    expect(md).toContain('操作表');
    expect(md).toContain('市场简报');
    expect(md).toContain('Suggest%');
    expect(md).toContain('*_BLOCK');
    expect(md).not.toContain('只输出四块');
  });
});

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
    expect(md).toContain('改单 CN:600001 卖出/清仓条件 @ Trigger=11.2');
    expect(md).toContain('挂买 CN:600002 条件买入 +5.0% (clip)');
    const exitIdx = md.indexOf('CN:600001');
    const buyIdx = md.indexOf('CN:600002');
    expect(exitIdx).toBeLessThan(buyIdx);
  });

  it('blocks buy lines when allowNewEntries is false', () => {
    const md = formatCondOrderDraftMarkdown(
      [{ symbol: 'CN:1', action: 'BUY', why: 'MAINLINE_OK', suggestAddPct: 5 }],
      { allowNewEntries: false },
    );
    expect(md).toContain('Gate blocks new entries');
    expect(md).not.toContain('- 挂买 ');
  });
});
