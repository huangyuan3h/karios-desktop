'use client';

import { useQuery } from '@tanstack/react-query';

import { apiGetJson } from '@/lib/api/client';

export type WeeklyPlanResponse = {
  ok: boolean;
  plan?: { brief_date?: string; brief_type?: string; markdown?: string } | null;
};

export type WeeklyPlanGenerateResponse = {
  ok: boolean;
  plan?: string | null;
  provider?: string | null;
  error?: string | null;
};

export function useWeeklyPlanQuery(enabled = true) {
  return useQuery({
    queryKey: ['weekly-plan'],
    queryFn: () => apiGetJson<WeeklyPlanResponse>('/api/backtest/weekly-plan'),
    staleTime: 60_000,
    enabled,
  });
}

/** Ask the decision agent to produce next week's action plan (ai-service). */
export async function generateWeeklyPlan(): Promise<WeeklyPlanGenerateResponse> {
  const { AI_BASE_URL } = await import('@/lib/endpoints');
  const res = await fetch(`${AI_BASE_URL}/weekly-plan`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({}),
    signal: AbortSignal.timeout(120_000),
  });
  const txt = await res.text().catch(() => '');
  if (!res.ok) {
    throw new Error(`${res.status} ${res.statusText}${txt ? `: ${txt}` : ''}`);
  }
  return (txt ? JSON.parse(txt) : { ok: false }) as WeeklyPlanGenerateResponse;
}
