import { buildDashboardHotIndustryPicks } from '@/lib/hot-industry-picks';

export const BREADTH_PANIC_DOWN_THRESHOLD = 3000;
export const DASHBOARD_CARD_ORDER_KEY = 'karios.dashboard.cardOrder.v0';

export function loadCardOrder(): string[] | null {
  try {
    const raw = window.localStorage.getItem(DASHBOARD_CARD_ORDER_KEY);
    if (!raw) return null;
    const arr = JSON.parse(raw) as unknown;
    return Array.isArray(arr) ? arr.filter((x) => typeof x === 'string') : null;
  } catch {
    return null;
  }
}

export function saveCardOrder(ids: string[]) {
  try {
    window.localStorage.setItem(DASHBOARD_CARD_ORDER_KEY, JSON.stringify(ids));
  } catch {
    // ignore
  }
}

export function fmtDateTime(x: string | null | undefined) {
  if (!x) return '—';
  const d = new Date(x);
  return Number.isNaN(d.getTime()) ? x : d.toLocaleString();
}

export function parseNum(x: unknown): number | null {
  const s = String(x ?? '').trim();
  if (!s) return null;
  const n = Number(s.replaceAll(',', ''));
  return Number.isFinite(n) ? n : null;
}

export function fmtAmountCn(x: unknown): string {
  const n = parseNum(x);
  if (n == null) return '—';
  const abs = Math.abs(n);
  if (abs >= 1e8) return `${(n / 1e8).toFixed(2)}亿`;
  if (abs >= 1e4) return `${(n / 1e4).toFixed(1)}万`;
  return `${n.toFixed(0)}`;
}

export function fmtSignedAmountCn(x: unknown): string {
  const n = parseNum(x);
  if (n == null) return '—';
  const body = fmtAmountCn(n);
  if (body === '—') return body;
  if (n > 0) return `+${body}`;
  if (n < 0) return `-${body.replace(/^-/, '')}`;
  return body;
}

export type SrvIndexLike = {
  level?: string | null;
  overlapCount?: number | null;
  overlapSectors?: string[] | null;
  labelZh?: string | null;
};

export function formatSrvIndexLine(srv: SrvIndexLike | null | undefined): string {
  const level = String(srv?.level ?? '').trim();
  const overlap = srv?.overlapCount;
  if (!level || typeof overlap !== 'number' || !Number.isFinite(overlap)) {
    return 'SRV_Index (Sector Rotation): —';
  }
  return `SRV_Index (Sector Rotation): ${level} (3D Overlap = ${overlap})`;
}

export function srvIndexBadgeClass(level: string | null | undefined): string {
  const lv = String(level ?? '').trim();
  if (lv === 'Stable') {
    return 'border-green-500/30 bg-green-500/10 text-green-700';
  }
  if (lv === 'Elevated') {
    return 'border-amber-500/30 bg-amber-500/10 text-amber-700';
  }
  if (lv === 'Extreme_High') {
    return 'border-red-600/40 bg-red-600/15 text-red-700';
  }
  return 'border-[var(--k-border)] bg-[var(--k-surface-2)] text-[var(--k-muted)]';
}

export type ExecutionGateLike = {
  mode?: string | null;
  allowNewEntries?: boolean | null;
  marketRegime?: string | null;
  indexLight?: string | null;
  srvLevel?: string | null;
  srvOverlapCount?: number | null;
  downCount?: number | null;
  riskMode?: string | null;
  reasons?: string[] | null;
  positionRangeHint?: string | null;
  satelliteNote?: string | null;
};

export function executionGateBadgeClass(mode: string | null | undefined): string {
  const m = String(mode || '').trim();
  if (m === 'ATTACK') {
    return 'border-emerald-600/40 bg-emerald-600/15 text-emerald-800 dark:text-emerald-200';
  }
  if (m === 'HOLD_ONLY') {
    return 'border-amber-500/40 bg-amber-500/15 text-amber-800 dark:text-amber-200';
  }
  if (m === 'DEFEND') {
    return 'border-red-600/40 bg-red-600/15 text-red-800 dark:text-red-200';
  }
  return 'border-[var(--k-border)] bg-[var(--k-surface-2)] text-[var(--k-muted)]';
}

/** Markdown block for Copy all / sentiment export (downstream AI contract). */
export function formatExecutionGateMarkdown(
  gate: ExecutionGateLike | null | undefined,
  heading = '##',
): string {
  const lines: string[] = [];
  lines.push(`${heading} Execution Gate`);
  if (!gate || !gate.mode) {
    lines.push('- mode: —');
    lines.push('');
    return lines.join('\n');
  }
  const overlap =
    typeof gate.srvOverlapCount === 'number' && Number.isFinite(gate.srvOverlapCount)
      ? gate.srvOverlapCount
      : null;
  const srvLevel = gate.srvLevel ? String(gate.srvLevel) : '—';
  const srvLine =
    overlap != null ? `${srvLevel} (overlap=${overlap})` : srvLevel;
  const reasons = Array.isArray(gate.reasons)
    ? gate.reasons.map((x) => String(x)).filter(Boolean)
    : [];
  lines.push(`- mode: ${String(gate.mode)}`);
  lines.push(`- allowNewEntries: ${gate.allowNewEntries === true}`);
  lines.push(`- marketRegime: ${String(gate.marketRegime ?? '—')}`);
  lines.push(`- indexLight: ${String(gate.indexLight ?? '—')}`);
  lines.push(`- srvLevel: ${srvLine}`);
  lines.push(
    `- downCount: ${
      typeof gate.downCount === 'number' && Number.isFinite(gate.downCount)
        ? gate.downCount
        : '—'
    }`,
  );
  if (gate.riskMode) lines.push(`- riskMode: ${String(gate.riskMode)}`);
  lines.push(`- reasons: [${reasons.join(', ')}]`);
  if (gate.positionRangeHint) {
    lines.push(`- positionRangeHint: ${String(gate.positionRangeHint)}`);
  }
  if (gate.satelliteNote) {
    lines.push(`- satelliteNote: ${String(gate.satelliteNote)}`);
  }
  lines.push('');
  return lines.join('\n');
}

export function escapeMarkdownCell(x: unknown): string {
  const s0 = String(x ?? '');
  const s1 = s0.replaceAll('\r\n', '\n').replaceAll('\r', '\n').replaceAll('\n', '<br/>');
  return s1.replaceAll('|', '\\|');
}

export function mdRow(cells: unknown[]): string {
  return `| ${cells.map(escapeMarkdownCell).join(' | ')} |`;
}

export function mdTable(headers: string[], rows: unknown[][]): string {
  const out: string[] = [];
  out.push(mdRow(headers));
  out.push(mdRow(headers.map(() => '---')));
  for (const r of rows) out.push(mdRow(r));
  return out.join('\n');
}

export function mdNum(v: number | null | undefined, digits = 2): string {
  if (typeof v !== 'number' || !Number.isFinite(v)) return '—';
  return v.toFixed(digits);
}

export function mdScore(v: number | null | undefined): string {
  if (typeof v !== 'number' || !Number.isFinite(v)) return '—';
  return String(Math.round(v));
}

export function mdPrice(v: number | null | undefined): string {
  if (typeof v !== 'number' || !Number.isFinite(v)) return '—';
  return v.toFixed(2);
}

export function mdLines(items: string[]): string {
  return items.filter((x) => String(x || '').trim()).join('\n');
}

/**
 * Collapse consecutive dates whose Top5 industry signature is identical.
 * Used by both the UI card and copy-all-markdown so they stay consistent.
 */
export function dedupeShownDates(
  rawShownDates: string[],
  topByDateMap: Record<string, string[]>,
): { dedupedDates: string[]; collapsed: number } {
  const dedupedDates: string[] = [];
  let prevSig = '';
  let collapsed = 0;
  for (const d of rawShownDates) {
    const sig = (topByDateMap[d] || []).slice(0, 5).join('|');
    if (sig && sig === prevSig) {
      collapsed += 1;
      continue;
    }
    dedupedDates.push(d);
    prevSig = sig;
  }
  return { dedupedDates, collapsed };
}

/** Build a {date -> top industry names[]} map from the dashboard topByDate array. */
export function buildTopByDateMap(summary: unknown): Record<string, string[]> {
  const root =
    summary && typeof summary === 'object' ? (summary as Record<string, unknown>) : null;
  const industryFundFlow =
    root?.industryFundFlow && typeof root.industryFundFlow === 'object'
      ? (root.industryFundFlow as Record<string, unknown>)
      : null;
  const arr: unknown[] = Array.isArray(industryFundFlow?.topByDate)
    ? industryFundFlow.topByDate
    : [];
  const map: Record<string, string[]> = {};
  for (const it of arr) {
    const row = it && typeof it === 'object' ? (it as Record<string, unknown>) : null;
    const d = String(row?.date ?? '');
    const top = Array.isArray(row?.top)
      ? row.top.map((x: unknown) => String(x ?? ''))
      : [];
    if (d) map[d] = top;
  }
  return map;
}

function signalRank(x: string): number {
  if (x === 'green' || x === 'light_green' || x === 'deep_green') return 3;
  if (x === 'yellow') return 2;
  if (x === 'red') return 1;
  return 0;
}

export function buildIndexTrafficSummary(indexSignals: unknown[]): { title: string; detail: string } {
  const items = Array.isArray(indexSignals) ? indexSignals : [];
  if (items.length < 2) {
    return {
      title: '⚠️ 当前行情：弱势 (Weak)',
      detail: '缺少完整指数信号，保持防守。',
    };
  }
  const byName = new Map(
    items.map((x: unknown) => {
      const row = x && typeof x === 'object' ? (x as Record<string, unknown>) : null;
      return [String(row?.name ?? row?.tsCode ?? ''), String(row?.signal ?? '')] as const;
    }),
  );
  const first = items[0] && typeof items[0] === 'object' ? (items[0] as Record<string, unknown>) : null;
  const second = items[1] && typeof items[1] === 'object' ? (items[1] as Record<string, unknown>) : null;
  const sse = byName.get('上证指数') || String(first?.signal ?? '');
  const cyb = byName.get('创业板指') || String(second?.signal ?? '');
  const g1 = sse === 'green' || sse === 'light_green' || sse === 'deep_green';
  const g2 = cyb === 'green' || cyb === 'light_green' || cyb === 'deep_green';

  if (g1 && g2) {
    return {
      title: '✅ 当前行情：强势 (Strong)',
      detail: '双绿确认，顺势为主，控制仓位与回撤。',
    };
  }

  if (g1 || g2) {
    const r1 = signalRank(sse);
    const r2 = signalRank(cyb);
    const bias = r1 === r2 ? '分化' : r1 > r2 ? '主强创弱' : '创强主弱';
    return {
      title: '⚠️ 当前行情：震荡/分化 (Diverging)',
      detail: `震荡分化（${bias}），严禁追高，仅限防守型回踩；买入仅用反弹买入策略单。`,
    };
  }

  return {
    title: '⚠️ 当前行情：弱势 (Weak)',
    detail: '非绿环境，防守为主，严格控制风险；买入仅用反弹买入策略单。',
  };
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function buildHotIndustriesMarkdown(s: any | null, heading = '##'): string {
  const asOfDate = String(s?.industryFundFlow?.asOfDate ?? s?.asOfDate ?? '').trim();
  const picks = buildDashboardHotIndustryPicks(s);
  const lines: string[] = [];
  lines.push(`${heading} Hot industries workflow`);
  if (asOfDate) lines.push(`- asOfDate: ${asOfDate}`);
  lines.push(
    '- Rule V4.0: prioritize "momentum breakout" (今日净流入>20亿 且 排名提升>10名); fallback to daily top ∩ strong 5D ranking.',
  );
  lines.push(
    '- Momentum breakout sectors are often the first day of a new mainline, more explosive than sectors already in 5D ranking.',
  );
  lines.push(
    '- Action: only stocks from these 3 sectors and passing technical checks should be added to Watchlist.',
  );
  lines.push('');

  const headers = ['#', 'Industry', '1D rank', '5D rank', '1D net', '5D sum', 'RankΔ', 'Signal'];
  const rows: unknown[][] = picks
    .slice(0, 3)
    .map((p, idx) => [
      idx + 1,
      p.industryName || '—',
      typeof p.dailyRank === 'number' ? `#${p.dailyRank}` : '—',
      typeof p.fiveDayRank === 'number' ? `#${p.fiveDayRank}` : '—',
      fmtAmountCn(p.netInflow ?? null),
      fmtAmountCn(p.sum5d ?? null),
      typeof p.rankChange === 'number'
        ? p.rankChange > 0
          ? `+${p.rankChange}`
          : String(p.rankChange)
        : '—',
      p.momentumSignal ? '🚀 MOMENTUM' : '—',
    ]);
  if (!rows.length) rows.push([1, '—', '—', '—', '—', '—', '—', '—']);
  lines.push(mdTable(headers, rows));
  lines.push('');
  return lines.join('\n').trim() + '\n';
}
