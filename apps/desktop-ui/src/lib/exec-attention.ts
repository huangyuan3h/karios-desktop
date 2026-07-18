import type { ExecutionDecisionChange, ExecutionGate } from '@karios/shared';

import { fmtDateTime } from '@/lib/dashboard-format';
import {
  buildSleeveExposurePct,
  countHeldMissingPositionPct,
  formatSleeveBudgetLabel,
  type PositionLike,
} from '@/lib/execution-action';

export type ExecAttentionLine = {
  symbol: string;
  action: string;
  why: string | null;
};

export type ExecAttentionQueue = {
  sleeveLabel: string;
  missingSize: number;
  exits: ExecAttentionLine[];
  trims: ExecAttentionLine[];
  fires: ExecAttentionLine[];
  fireBlockedByGate: boolean;
  keyChanges: Array<{ id: string; line: string }>;
};

export function formatDecisionChangeLine(c: ExecutionDecisionChange): string {
  const t = c.changedAt ? fmtDateTime(c.changedAt) : '—';
  if (c.scope === 'gate') {
    return `${t}  Gate ${c.field}: ${c.oldValue ?? '—'} → ${c.newValue ?? '—'}`;
  }
  const sym = c.symbol ?? '—';
  return `${t}  ${sym}  ${c.field}: ${c.oldValue ?? '—'} → ${c.newValue ?? '—'}`;
}

function bySymbol(a: ExecAttentionLine, b: ExecAttentionLine): number {
  return a.symbol.localeCompare(b.symbol);
}

function toLine(c: { symbol: string; action: string; why?: string | null }): ExecAttentionLine {
  return {
    symbol: c.symbol,
    action: String(c.action),
    why: c.why == null || c.why === '' ? null : String(c.why),
  };
}

/**
 * Build a 5-minute attention queue from live gate/watchlist + latest journal snapshot.
 */
export function buildExecAttentionQueue(opts: {
  gate: ExecutionGate | null;
  watchlistItems: PositionLike[];
  cards: Array<{ symbol: string; action: string; why?: string | null }>;
  changes: ExecutionDecisionChange[];
}): ExecAttentionQueue {
  const { gate, watchlistItems, cards, changes } = opts;
  const allowNew = gate?.allowNewEntries === true;
  const exits: ExecAttentionLine[] = [];
  const trims: ExecAttentionLine[] = [];
  const fireCandidates: ExecAttentionLine[] = [];

  for (const c of cards) {
    const action = String(c.action || '').toUpperCase();
    if (action === 'EXIT') exits.push(toLine(c));
    else if (action === 'TRIM') trims.push(toLine(c));
    else if (action === 'BUY' || action === 'ADD') fireCandidates.push(toLine(c));
  }

  exits.sort(bySymbol);
  trims.sort(bySymbol);
  fireCandidates.sort(bySymbol);

  const fireBlockedByGate = !allowNew;
  const fires = allowNew ? fireCandidates : [];

  const keyChanges = changes
    .filter((c) => c.field === 'action' || c.field === 'mode')
    .slice(0, 3)
    .map((c) => ({ id: c.id, line: formatDecisionChangeLine(c) }));

  return {
    sleeveLabel: formatSleeveBudgetLabel(
      buildSleeveExposurePct(watchlistItems),
      gate?.positionRangeHint,
    ),
    missingSize: countHeldMissingPositionPct(watchlistItems),
    exits,
    trims,
    fires,
    fireBlockedByGate,
    keyChanges,
  };
}
