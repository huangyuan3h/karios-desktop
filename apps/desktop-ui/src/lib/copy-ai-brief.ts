import type { ExecutionDecisionChange } from '@karios/shared';

import { formatDecisionChangeLine } from '@/lib/exec-attention';

export const COPY_ALL_LAST_AT_KEY = 'karios.copyAll.lastAt.v1';

export function readLastCopyAt(): string | null {
  if (typeof window === 'undefined') return null;
  try {
    const raw = window.localStorage.getItem(COPY_ALL_LAST_AT_KEY);
    const s = String(raw || '').trim();
    return s || null;
  } catch {
    return null;
  }
}

export function writeLastCopyAt(iso: string): void {
  if (typeof window === 'undefined') return;
  const s = String(iso || '').trim();
  if (!s) return;
  try {
    window.localStorage.setItem(COPY_ALL_LAST_AT_KEY, s);
  } catch {
    // ignore quota
  }
}

export function formatSinceLastCopyMarkdown(
  changes: ExecutionDecisionChange[],
  opts: { lastAt: string | null; heading?: string },
): string {
  const heading = opts.heading ?? '##';
  const lines: string[] = [];
  lines.push(`${heading} Since last copy`);
  if (opts.lastAt) {
    lines.push(`- since: ${opts.lastAt}`);
  } else {
    lines.push('- note: no prior copy marker');
  }

  const list = changes
    .filter((c) => c.field === 'action' || c.field === 'mode')
    .slice(0, 15);
  if (!list.length) {
    lines.push('- None');
  } else {
    for (const c of list) {
      lines.push(`- ${formatDecisionChangeLine(c)}`);
    }
  }
  lines.push('');
  return lines.join('\n');
}

export type CondOrderCard = {
  symbol: string;
  action: string;
  why?: string | null;
  trigger?: number | null;
  exitStop?: number | null;
  entryTrigger?: number | null;
  suggestAddPct?: number | null;
  suggestSizeNote?: string | null;
};

function fmtPrice(v: number | null | undefined): string {
  return typeof v === 'number' && Number.isFinite(v) ? String(v) : '—';
}

function fmtSuggest(card: CondOrderCard): string {
  if (typeof card.suggestAddPct === 'number' && Number.isFinite(card.suggestAddPct)) {
    const note = card.suggestSizeNote ? ` (${card.suggestSizeNote})` : '';
    return `+${card.suggestAddPct.toFixed(1)}%${note}`;
  }
  return 'size=TBD';
}

function resolveExitStop(card: CondOrderCard): number | null {
  if (typeof card.exitStop === 'number' && Number.isFinite(card.exitStop)) return card.exitStop;
  if (typeof card.trigger === 'number' && Number.isFinite(card.trigger)) return card.trigger;
  return null;
}

/**
 * Conditional-order draft from Action cards (not broker API).
 * EXIT/TRIM first, then BUY/ADD when allowNewEntries.
 * When tradingTime=false, lines are prefixed [Queue for Next Open].
 */
export function formatCondOrderDraftMarkdown(
  cards: CondOrderCard[],
  opts?: {
    heading?: string;
    allowNewEntries?: boolean;
    tradingTime?: boolean;
    phase?: string | null;
  },
): string {
  const heading = opts?.heading ?? '##';
  const allowNew = opts?.allowNewEntries === true;
  const isClosed = opts?.tradingTime === false;
  const queuePrefix = isClosed ? '[Queue for Next Open] ' : '';
  const lines: string[] = [];
  lines.push(`${heading} Cond order draft`);
  lines.push(
    '- note: 非 BUY/ADD 的监控票若曾挂买入条件单则撤销；勿在回复中逐条罗列全部 WATCH',
  );
  if (isClosed) {
    const phase = opts?.phase?.trim() || 'closed';
    lines.push(`- note: phase: ${phase} — orders queue for next open`);
  }

  const exits: CondOrderCard[] = [];
  const trims: CondOrderCard[] = [];
  const buys: CondOrderCard[] = [];
  for (const c of cards) {
    const action = String(c.action || '').toUpperCase();
    if (action === 'EXIT') exits.push(c);
    else if (action === 'TRIM') trims.push(c);
    else if (action === 'BUY' || action === 'ADD') buys.push(c);
  }

  const bySym = (a: CondOrderCard, b: CondOrderCard) => a.symbol.localeCompare(b.symbol);
  exits.sort(bySym);
  trims.sort(bySym);
  buys.sort(bySym);

  let wrote = false;
  for (const c of [...exits, ...trims]) {
    const kind = String(c.action).toUpperCase() === 'EXIT' ? '卖出/清仓条件' : '减仓条件';
    lines.push(
      `- ${queuePrefix}改单 ${c.symbol} ${kind} @ Exit_Stop=${fmtPrice(resolveExitStop(c))}  Why=${c.why ?? '—'}`,
    );
    wrote = true;
  }

  if (allowNew) {
    for (const c of buys) {
      lines.push(
        `- ${queuePrefix}挂买 ${c.symbol} 条件买入 ${fmtSuggest(c)}  Why=${c.why ?? '—'}`,
      );
      wrote = true;
    }
  } else if (buys.length) {
    lines.push('- note: Gate blocks new entries — 勿挂新买入条件单');
    wrote = true;
  }

  if (!wrote) {
    lines.push('- None');
  }
  lines.push('');
  return lines.join('\n');
}
