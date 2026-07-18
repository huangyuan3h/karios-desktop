import { describe, expect, it } from 'vitest';

import type { ExecutionDecisionChange, ExecutionGate } from '@karios/shared';

import {
  buildExecAttentionQueue,
  formatDecisionChangeLine,
  formatExecAttentionMarkdown,
  resolveAttentionCards,
} from './exec-attention';

const attackGate: ExecutionGate = {
  mode: 'ATTACK',
  allowNewEntries: true,
  marketRegime: 'Strong',
  indexLight: 'green',
  srvLevel: 'Stable',
  srvOverlapCount: 3,
  downCount: 1000,
  reasons: ['REGIME_STRONG'],
  positionRangeHint: '50%-60%',
  satelliteNote: 'ok',
};

const holdGate: ExecutionGate = {
  ...attackGate,
  mode: 'HOLD_ONLY',
  allowNewEntries: false,
  marketRegime: 'Diverging',
  reasons: ['REGIME_DIVERGING'],
};

describe('resolveAttentionCards', () => {
  it('prefers live over snapshot', () => {
    const live = [{ symbol: 'CN:1', action: 'EXIT', why: 'EXIT_NOW' }];
    const snap = [{ symbol: 'CN:2', action: 'TRIM', why: 'GATE_DEFEND' }];
    expect(resolveAttentionCards({ liveCards: live, snapshotCards: snap })).toEqual({
      cards: live,
      source: 'live',
    });
  });

  it('falls back to snapshot when live is null', () => {
    const snap = [{ symbol: 'CN:2', action: 'TRIM', why: 'GATE_DEFEND' }];
    expect(resolveAttentionCards({ liveCards: null, snapshotCards: snap })).toEqual({
      cards: snap,
      source: 'snapshot',
    });
  });

  it('returns none when both empty', () => {
    expect(resolveAttentionCards({ liveCards: null, snapshotCards: [] })).toEqual({
      cards: [],
      source: 'none',
    });
  });
});

describe('buildExecAttentionQueue', () => {
  it('buckets EXIT/TRIM/BUY and sorts by symbol', () => {
    const q = buildExecAttentionQueue({
      gate: attackGate,
      watchlistItems: [],
      cards: [
        { symbol: 'CN:600002', action: 'BUY', why: 'MAINLINE_OK' },
        { symbol: 'CN:600001', action: 'EXIT', why: 'EXIT_NOW' },
        { symbol: 'CN:600003', action: 'TRIM', why: 'GATE_DEFEND' },
        { symbol: 'CN:600000', action: 'EXIT', why: 'TRIGGER_HIT' },
        { symbol: 'CN:600004', action: 'ADD', why: 'MAINLINE_5D_TOP3' },
        { symbol: 'CN:600005', action: 'HOLD', why: 'HOLD' },
      ],
      changes: [],
    });
    expect(q.exits.map((x) => x.symbol)).toEqual(['CN:600000', 'CN:600001']);
    expect(q.trims.map((x) => x.symbol)).toEqual(['CN:600003']);
    expect(q.fires.map((x) => x.symbol)).toEqual(['CN:600002', 'CN:600004']);
    expect(q.fireBlockedByGate).toBe(false);
  });

  it('blocks fires when allowNewEntries is false', () => {
    const q = buildExecAttentionQueue({
      gate: holdGate,
      watchlistItems: [],
      cards: [
        { symbol: 'CN:600000', action: 'BUY', why: 'MAINLINE_OK' },
        { symbol: 'CN:600001', action: 'EXIT', why: 'EXIT_NOW' },
      ],
      changes: [],
    });
    expect(q.fires).toEqual([]);
    expect(q.fireBlockedByGate).toBe(true);
    expect(q.exits).toHaveLength(1);
  });

  it('computes sleeve label and missing size from watchlist', () => {
    const q = buildExecAttentionQueue({
      gate: attackGate,
      watchlistItems: [
        { symbol: 'CN:1', positionPct: 20 },
        { symbol: 'CN:2', positionPct: 25 },
        { symbol: 'CN:3', costPrice: 10 },
      ],
      cards: [],
      changes: [],
    });
    expect(q.sleeveLabel).toBe('Sleeve 45.0% / 60%');
    expect(q.missingSize).toBe(1);
  });

  it('keeps at most 3 action/mode key changes', () => {
    const changes: ExecutionDecisionChange[] = [
      {
        id: '1',
        field: 'action',
        scope: 'symbol',
        symbol: 'CN:1',
        oldValue: 'BUY',
        newValue: 'WATCH',
        changedAt: '2026-07-18T01:00:00Z',
      },
      {
        id: '2',
        field: 'positionPct',
        scope: 'symbol',
        symbol: 'CN:1',
        oldValue: '10',
        newValue: '12',
        changedAt: '2026-07-18T01:01:00Z',
      },
      {
        id: '3',
        field: 'mode',
        scope: 'gate',
        oldValue: 'ATTACK',
        newValue: 'HOLD_ONLY',
        changedAt: '2026-07-18T01:02:00Z',
      },
      {
        id: '4',
        field: 'action',
        scope: 'symbol',
        symbol: 'CN:2',
        oldValue: 'HOLD',
        newValue: 'TRIM',
        changedAt: '2026-07-18T01:03:00Z',
      },
      {
        id: '5',
        field: 'action',
        scope: 'symbol',
        symbol: 'CN:3',
        oldValue: 'WATCH',
        newValue: 'BUY',
        changedAt: '2026-07-18T01:04:00Z',
      },
    ];
    const q = buildExecAttentionQueue({
      gate: attackGate,
      watchlistItems: [],
      cards: [],
      changes,
    });
    expect(q.keyChanges).toHaveLength(3);
    expect(q.keyChanges.map((x) => x.id)).toEqual(['1', '3', '4']);
    expect(q.keyChanges[0].line).toContain('action');
    expect(formatDecisionChangeLine(changes[2])).toContain('Gate mode');
  });

  it('formats attention markdown for Copy all', () => {
    const q = buildExecAttentionQueue({
      gate: attackGate,
      watchlistItems: [{ symbol: 'CN:1', positionPct: 20 }],
      cards: [
        { symbol: 'CN:600000', action: 'EXIT', why: 'EXIT_NOW' },
        { symbol: 'CN:600001', action: 'BUY', why: 'MAINLINE_OK' },
      ],
      changes: [],
    });
    const md = formatExecAttentionMarkdown(q, { source: 'live' });
    expect(md).toContain('## Exec Attention');
    expect(md).toContain('- source: live');
    expect(md).toContain('Sleeve 20.0% / 60%');
    expect(md).toContain('### Must act');
    expect(md).toContain('CN:600000  EXIT  EXIT_NOW');
    expect(md).toContain('### Fire');
    expect(md).toContain('CN:600001  BUY  MAINLINE_OK');
  });
});
