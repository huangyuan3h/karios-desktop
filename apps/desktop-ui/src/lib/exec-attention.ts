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

export type AttentionCardsSource = 'live' | 'snapshot' | 'none';

/** Prefer live Action cards; fall back to journal snapshot. */
export function resolveAttentionCards(opts: {
  liveCards: Array<{ symbol: string; action: string; why?: string | null }> | null | undefined;
  snapshotCards: Array<{ symbol: string; action: string; why?: string | null }>;
}): {
  cards: Array<{ symbol: string; action: string; why?: string | null }>;
  source: AttentionCardsSource;
} {
  if (opts.liveCards != null) {
    return { cards: opts.liveCards, source: 'live' };
  }
  if (opts.snapshotCards.length > 0) {
    return { cards: opts.snapshotCards, source: 'snapshot' };
  }
  return { cards: [], source: 'none' };
}

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

/** Markdown block for Copy all (same shape as Dashboard Exec Attention). */
export function formatExecAttentionMarkdown(
  queue: ExecAttentionQueue,
  opts?: { heading?: string; source?: AttentionCardsSource },
): string {
  const heading = opts?.heading ?? '##';
  const lines: string[] = [];
  lines.push(`${heading} Exec Attention`);
  if (opts?.source && opts.source !== 'none') {
    lines.push(`- source: ${opts.source}`);
  }
  lines.push(`- ${queue.sleeveLabel}`);
  if (queue.missingSize > 0) {
    lines.push(
      `- note: ${queue.missingSize} held missing positionPct (sector/sleeve caps fail-open)`,
    );
  }
  lines.push('');
  lines.push(`${heading}# Must act`);
  const mustAct = [...queue.exits, ...queue.trims];
  if (!mustAct.length) {
    lines.push('- None');
  } else {
    for (const x of mustAct) {
      lines.push(`- ${x.symbol}  ${x.action}  ${x.why ?? '—'}`);
    }
  }
  lines.push('');
  lines.push(`${heading}# Fire`);
  if (queue.fireBlockedByGate) {
    lines.push('- Gate blocks new entries');
  } else if (!queue.fires.length) {
    lines.push('- None');
  } else {
    for (const x of queue.fires) {
      lines.push(`- ${x.symbol}  ${x.action}  ${x.why ?? '—'}`);
    }
  }
  if (queue.keyChanges.length) {
    lines.push('');
    lines.push(`${heading}# Key changes`);
    for (const c of queue.keyChanges) {
      lines.push(`- ${c.line}`);
    }
  }
  lines.push('');
  return lines.join('\n');
}
