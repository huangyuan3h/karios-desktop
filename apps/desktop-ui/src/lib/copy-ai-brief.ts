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

/** Embedded brief so busy users need little/no free-text prompt. */
export function formatAiCopyInstructionHeader(heading = '##'): string {
  const lines = [
    `${heading} AI instructions (embedded)`,
    '- 服从权威层级：Gate → Attention → Cond order draft → Positions；解释层不得推翻合同。',
    '- 只输出四块：改单清单 / 撤单清单 / 维持不动 / 禁止（勿长篇复盘）。',
    '- 数量用 Suggest%；止损/退出价用 Trigger（有 Trail 优先 Trail）；不得平反 *_BLOCK / *_FADE。',
    '- 条件单导向：把建议落成可挂/可改/可撤的价格与仓位；无 Trigger 时标明缺失。',
    '- 用户可能只补一句近况；缺失口头上下文时按 Markdown 合同执行并标注假设。',
    '',
  ];
  return lines.join('\n');
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
  suggestAddPct?: number | null;
  suggestSizeNote?: string | null;
};

function fmtTrigger(trigger: number | null | undefined): string {
  return typeof trigger === 'number' && Number.isFinite(trigger) ? String(trigger) : '—';
}

function fmtSuggest(card: CondOrderCard): string {
  if (typeof card.suggestAddPct === 'number' && Number.isFinite(card.suggestAddPct)) {
    const note = card.suggestSizeNote ? ` (${card.suggestSizeNote})` : '';
    return `+${card.suggestAddPct.toFixed(1)}%${note}`;
  }
  return 'size=TBD';
}

/**
 * Conditional-order draft from Action cards (not broker API).
 * EXIT/TRIM first, then BUY/ADD when allowNewEntries.
 */
export function formatCondOrderDraftMarkdown(
  cards: CondOrderCard[],
  opts?: { heading?: string; allowNewEntries?: boolean },
): string {
  const heading = opts?.heading ?? '##';
  const allowNew = opts?.allowNewEntries === true;
  const lines: string[] = [];
  lines.push(`${heading} Cond order draft`);
  lines.push('- note: WATCH/HOLD/*_BLOCK：撤销对应买入条件单');

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
      `- 改单 ${c.symbol} ${kind} @ Trigger=${fmtTrigger(c.trigger)}  Why=${c.why ?? '—'}`,
    );
    wrote = true;
  }

  if (allowNew) {
    for (const c of buys) {
      lines.push(
        `- 挂买 ${c.symbol} 条件买入 ${fmtSuggest(c)}  Why=${c.why ?? '—'}`,
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
