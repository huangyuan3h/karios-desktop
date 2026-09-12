import type { FundFlowRow } from '@/lib/queries/backtest';

export type FlowSpan = '6m' | '1y' | '3y';

export type FlowRun = { start: number; end: number };

export function flowSpanWindow(
  span: FlowSpan,
  now: Date = new Date(),
): { start: string; end: string } {
  const end = now.toISOString().slice(0, 10);
  const days = span === '6m' ? 183 : span === '1y' ? 365 : 1095;
  const start = new Date(now.getTime() - days * 86_400_000).toISOString().slice(0, 10);
  return { start, end };
}

/** Inclusive start, exclusive end index runs where gateOn (国家队撤退态) holds. */
export function gateOnRuns(rows: FundFlowRow[]): FlowRun[] {
  const runs: FlowRun[] = [];
  let start = -1;
  for (let i = 0; i < rows.length; i += 1) {
    if (rows[i].gateOn) {
      if (start < 0) start = i;
    } else if (start >= 0) {
      runs.push({ start, end: i });
      start = -1;
    }
  }
  if (start >= 0) runs.push({ start, end: rows.length });
  return runs;
}

/** Last row carrying an ETF share level (latest published day). */
export function flowLatest(rows: FundFlowRow[]): FundFlowRow | undefined {
  for (let i = rows.length - 1; i >= 0; i -= 1) {
    if (rows[i].etfShareYi != null || rows[i].marginTrillion != null) return rows[i];
  }
  return undefined;
}

/** Evenly spaced tick dates for a shared X axis. */
export function flowTickDates(rows: FundFlowRow[], maxTicks = 7): string[] {
  const n = rows.length;
  if (n <= 1) return rows.map((r) => r.date);
  const step = Math.max(1, Math.ceil(n / maxTicks));
  return rows.filter((_, i) => i % step === 0).map((r) => r.date);
}

/** 国家队+两融+北向 20日累计 net flow total (亿元, 1:1:1); null when all three missing. */
export function flowTotalDaily20(row: FundFlowRow): number | null {
  const parts = [row.natD20Yi, row.marginD20Yi, row.northD20Yi];
  if (parts.every((p) => p == null)) return null;
  return Math.round(parts.reduce<number>((s, p) => s + (p ?? 0), 0) * 10) / 10;
}

export function fmtSigned(v: number | null | undefined, digits = 2, unit = ''): string {
  if (v == null || !Number.isFinite(v)) return '—';
  return `${v > 0 ? '+' : ''}${v.toFixed(digits)}${unit}`;
}
