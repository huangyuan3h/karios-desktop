import { chunk } from '@/lib/chunk';

import { apiGetJson } from './client';
import type { TrendOkResult } from './types';

const inflight = new Map<string, Promise<TrendOkResult[]>>();

function trendOkRequestKey(symbols: string[], realtime: boolean): string {
  const sorted = [...symbols].map((s) => s.trim().toUpperCase()).filter(Boolean).sort();
  return `${realtime ? 'rt1' : 'rt0'}:${sorted.join(',')}`;
}

async function fetchTrendOkBatch(
  symbols: string[],
  options: { realtime: boolean },
): Promise<TrendOkResult[]> {
  if (!symbols.length) return [];
  const sp = new URLSearchParams();
  sp.set('realtime', options.realtime ? 'true' : 'false');
  for (const sym of symbols) sp.append('symbols', sym);
  return apiGetJson<TrendOkResult[]>(`/market/stocks/trendok?${sp.toString()}`);
}

/** @internal Test helper */
export function resetTrendOkInflightForTests(): void {
  inflight.clear();
}

export async function fetchTrendOkMap(
  symbols: string[],
  options: { realtime: boolean },
): Promise<Map<string, TrendOkResult>> {
  const trendMap = new Map<string, TrendOkResult>();
  const syms = symbols.map((s) => s.trim().toUpperCase()).filter(Boolean);
  if (!syms.length) return trendMap;

  const key = trendOkRequestKey(syms, options.realtime);
  let request = inflight.get(key);
  if (!request) {
    const parts = chunk(syms, 200);
    request = Promise.all(
      parts.map((batch) => fetchTrendOkBatch(batch, { realtime: options.realtime })),
    ).then((batches) => batches.flat());
    inflight.set(key, request);
    void request.finally(() => {
      if (inflight.get(key) === request) inflight.delete(key);
    });
  }

  const flat = await request;
  for (const item of flat) {
    if (item?.symbol) trendMap.set(String(item.symbol).toUpperCase(), item);
  }
  return trendMap;
}
