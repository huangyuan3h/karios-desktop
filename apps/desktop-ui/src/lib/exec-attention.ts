import type { ExecutionDecisionChange, ExecutionGate } from '@karios/shared';

import { fmtDateTime } from '@/lib/dashboard-format';
import {
  buildSleeveExposureByMarket,
  buildSleeveExposurePct,
  countHeldMissingPositionPct,
  marketOfSymbol,
  parsePositionRangeHintMaxPct,
  type PositionLike,
} from '@/lib/execution-action';

export type ExecAttentionLine = {
  symbol: string;
  action: string;
  why: string | null;
  hint?: string | null;
  suggestAddPct?: number | null;
  suggestSizeNote?: string | null;
};

export type ExecAttentionQueue = {
  sleeveLabel: string;
  missingSize: number;
  exits: ExecAttentionLine[];
  trims: ExecAttentionLine[];
  fires: ExecAttentionLine[];
  fireBlockedByGate: boolean;
  /** CN gate closed but CN fire candidates exist (per-market messaging). */
  cnGateBlocked: boolean;
  /** HK gate allows new entries (hkGate ATTACK) — HK fires may fire. */
  hkGateOpen: boolean;
  keyChanges: Array<{ id: string; line: string }>;
};

export type AttentionCardsSource = 'live' | 'snapshot' | 'none';

const ACTION_LABEL: Record<string, string> = {
  EXIT: '卖出',
  TRIM: '减仓',
  BUY: '买入',
  ADD: '加仓',
  HOLD: '持有',
  WATCH: '观望',
};

export function translateAction(action: string): string {
  return ACTION_LABEL[action] ?? action;
}

const WHY_LABEL: Record<string, string> = {
  EXIT_NOW: '强制卖出',
  TRIGGER_HIT: '止损触发',
  HARD_STOP_HIT: '硬止损触发',
  TRAIL_STOP_TRIM: '移动止盈减半',
  ETF_FALLBACK_TRIM: '回撤止损减半',
  WARN_REDUCE_HALF: '减半警告',
  GATE_DEFEND: '防守模式',
  MAINLINE_FADE: '主线退潮',
  SECTOR_OUTFLOW_BLOCK: '板块资金流出',
  MISSING_INDUSTRY: '行业缺失',
  T1_LOCK: 'T+1锁定',
  ENTRY_DATE_MISSING: '建仓日缺失',
  SIZE_CAP_BLOCK: '仓位上限',
  GATE_BLOCK_NEW: 'Gate禁止开仓',
  HOLD: '持有',
  INTRADAY_SURGE_BLOCK: '盘中涨幅过大',
  GAP_UP_WEAK_BLOCK: '跳空弱势阻断',
  SECTOR_CONC_BLOCK: '板块集中度超限',
  SLEEVE_CAP_BLOCK: '组合仓位超限',
  MAINLINE_DATA_UNAVAILABLE: '主线数据未就绪',
  NOT_MAINLINE: '非主线行业',
  TIME_LOCK_WEAK_REGIME: '弱势时段锁定',
  MARKET_CLOSING_LOCK: '尾盘锁定',
  DEFENSE_SECTOR_BLOCK: '防御板块',
  ENTRY_BELOW_STOP: '买入价低于止损',
  MAINLINE_OK: '主线确认',
  MAINLINE_5D_TOP3: '5日净流入Top3',
  MAINLINE_MOMENTUM: '动量主线',
  MOMENTUM_SURGE_ALLOW: '动量突破允许',
  DEFENSIVE_SLEEVE_ALLOW: '防守仓允许',
  WATCH: '观望',
  TREND_RECOVERING: '趋势恢复中',
  ALPHA_S_WATCH: 'Alpha S级关注',
  PURGE_GC: '垃圾清理',
};

export function translateWhy(why: string | null | undefined): string {
  if (!why) return '—';
  return WHY_LABEL[why] ?? why;
}

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
  const field = c.field === 'action' ? '操作' : c.field === 'why' ? '原因' : c.field;
  const oldVal = c.field === 'action' ? translateAction(c.oldValue ?? '') : c.field === 'why' ? translateWhy(c.oldValue) : (c.oldValue ?? '—');
  const newVal = c.field === 'action' ? translateAction(c.newValue ?? '') : c.field === 'why' ? translateWhy(c.newValue) : (c.newValue ?? '—');
  return `${t}  ${sym}  ${field}: ${oldVal} → ${newVal}`;
}

function bySymbol(a: ExecAttentionLine, b: ExecAttentionLine): number {
  return a.symbol.localeCompare(b.symbol);
}

function toLine(c: {
  symbol: string;
  action: string;
  why?: string | null;
  suggestAddPct?: number | null;
  suggestSizeNote?: string | null;
}): ExecAttentionLine {
  return {
    symbol: c.symbol,
    action: String(c.action),
    why: c.why == null || c.why === '' ? null : String(c.why),
    suggestAddPct:
      typeof c.suggestAddPct === 'number' && Number.isFinite(c.suggestAddPct)
        ? c.suggestAddPct
        : null,
    suggestSizeNote: c.suggestSizeNote == null ? null : String(c.suggestSizeNote),
  };
}

const WARN_REASON_LABEL: Record<string, string> = {
  'trend_structure_break:ema5_below_ema20': 'EMA5跌破EMA20',
  'trend_structure_break:close_below_ema20': '收盘跌破EMA20',
  'momentum_exhaustion:hist_shrink3_flip_negative_and_volume_dry': 'MACD三日缩量转负+量能萎缩',
  'momentum_warning:hist_shrinking_and_volume_dry': 'MACD柱连续收缩+量能萎缩',
  'momentum_warning:hist_shrinking': 'MACD柱连续收缩',
  'momentum_warning:hist_shrinking_volume_unknown': 'MACD柱连续收缩（量未知）',
};

export function translateWarnReason(reason: string): string {
  return WARN_REASON_LABEL[reason] ?? reason;
}

/**
 * Trim reasons from TrendOK stop-loss parts (warn_reasons). Lets "Must act"
 * show why a held position gets a reduce-half warning, not just the label.
 */
function warnReasonHint(
  item: PositionLike,
  why: string | null,
): string | null {
  if (why !== 'WARN_REDUCE_HALF') return null;
  const parts = (item as any)?.trendok?.stopLossParts as Record<string, unknown> | null | undefined;
  const reasons = Array.isArray(parts?.warn_reasons) ? parts.warn_reasons : [];
  if (!reasons.length) return null;
  return reasons.map((r) => translateWarnReason(String(r))).join('；');
}

export function formatAttentionFireLine(x: ExecAttentionLine): string {
  const size =
    typeof x.suggestAddPct === 'number' && Number.isFinite(x.suggestAddPct)
      ? `  +${x.suggestAddPct.toFixed(1)}%${x.suggestSizeNote ? ` (${x.suggestSizeNote})` : ''}`
      : '';
  return `${x.symbol}  ${translateAction(x.action)}${size}  ${translateWhy(x.why)}`;
}

/**
 * Build a 5-minute attention queue from live gate/watchlist + latest journal snapshot.
 */
export function buildExecAttentionQueue(opts: {
  gate: ExecutionGate | null;
  watchlistItems: PositionLike[];
  cards: Array<{
    symbol: string;
    action: string;
    why?: string | null;
    suggestAddPct?: number | null;
    suggestSizeNote?: string | null;
  }>;
  changes: ExecutionDecisionChange[];
}): ExecAttentionQueue {
  const { gate, watchlistItems, cards, changes } = opts;
  const allowNew = gate?.allowNewEntries === true;
  const hkOpen = gate?.hkGate?.allowNewEntries === true;
  const exits: ExecAttentionLine[] = [];
  const trims: ExecAttentionLine[] = [];
  const fireCandidates: ExecAttentionLine[] = [];

  const trimHintBySymbol = new Map<string, string>();
  for (const it of watchlistItems) {
    const hint = warnReasonHint(it, 'WARN_REDUCE_HALF');
    if (hint) trimHintBySymbol.set(String(it?.symbol ?? ''), hint);
  }

  for (const c of cards) {
    const action = String(c.action || '').toUpperCase();
    if (action === 'EXIT') exits.push(toLine(c));
    else if (action === 'TRIM') {
      const line = toLine(c);
      if (line.why === 'WARN_REDUCE_HALF' && trimHintBySymbol.has(line.symbol)) {
        line.hint = trimHintBySymbol.get(line.symbol) ?? null;
      }
      trims.push(line);
    }
    else if (action === 'BUY' || action === 'ADD') fireCandidates.push(toLine(c));
  }

  exits.sort(bySymbol);
  trims.sort(bySymbol);
  fireCandidates.sort(bySymbol);

  // Per-market fire gating: HK symbols are evaluated against hkGate (when the
  // row cards carry it); CN/ETF symbols against the CN gate.
  const isHkSym = (s: string) => marketOfSymbol(s) === 'hk';
  const cnFires = fireCandidates.filter((f) => !isHkSym(f.symbol));
  const hkFires = fireCandidates.filter((f) => isHkSym(f.symbol));
  const cnFiresShown = allowNew
    ? cnFires
    : cnFires.filter((x) => String(x.why || '') === 'DEFENSIVE_SLEEVE_ALLOW');
  const fires = [...cnFiresShown, ...(hkOpen ? hkFires : [])];
  const cnGateBlocked = !allowNew;
  const hkGateOpen = hkOpen;

  const keyChanges = changes
    .filter((c) => c.field === 'action' || c.field === 'mode')
    .slice(0, 3)
    .map((c) => ({ id: c.id, line: formatDecisionChangeLine(c) }));

  const capLabel = (v: number | null) => (v == null ? '—' : `${v}%`);
  const sleeveByMarket = buildSleeveExposureByMarket(watchlistItems);
  const cnCap = parsePositionRangeHintMaxPct(gate?.positionRangeHint);
  const hkCap = parsePositionRangeHintMaxPct(gate?.hkGate?.positionRangeHint ?? null);

  return {
    sleeveLabel:
      `卫星仓 ${buildSleeveExposurePct(watchlistItems).toFixed(1)}%` +
      ` = A股 ${sleeveByMarket.cn.toFixed(1)}% + ETF ${sleeveByMarket.etf.toFixed(1)}%` +
      ` + 港股 ${sleeveByMarket.hk.toFixed(1)}%（CN≤${capLabel(cnCap)} / HK≤${capLabel(hkCap)}）`,
    missingSize: countHeldMissingPositionPct(watchlistItems),
    exits,
    trims,
    fires,
    fireBlockedByGate: !allowNew && fires.length === 0,
    cnGateBlocked,
    hkGateOpen,
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
      lines.push(
        `- ${x.symbol}  ${translateAction(x.action)}  ${translateWhy(x.why)}${x.hint ? `（${x.hint}）` : ''}`,
      );
    }
  }
  lines.push('');
  lines.push(`${heading}# Fire`);
  if (queue.cnGateBlocked) {
    lines.push('- CN Gate blocks new entries');
  }
  if (queue.hkGateOpen) {
    lines.push('- HK gate open（hkGate 允许开仓/加仓）');
  }
  for (const x of queue.fires) {
    lines.push(`- ${formatAttentionFireLine(x)}`);
  }
  if (!queue.cnGateBlocked && !queue.hkGateOpen && !queue.fires.length) {
    lines.push('- None');
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
