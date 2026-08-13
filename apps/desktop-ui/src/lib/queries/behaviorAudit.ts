import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';

import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';

/** OPT-106: real book (registry) vs S-3 backtest "should hold" audit. */
export interface BehaviorAuditItem {
  auditDate: string;
  market: string;
  expected: number;
  actual: number;
  extra: number;
  missing: number;
  extraList?: Array<{
    symbol: string;
    name?: string;
    costPrice?: number | null;
    entryDate?: string;
    kind?: 'exited' | 'never_entered';
  }>;
  missingList?: Array<{ symbol: string; entry?: string | null; score?: number | null }>;
}

export const behaviorAuditKey = 'behavior-audit';

async function fetchLatestAudit(): Promise<BehaviorAuditItem[]> {
  const res = await fetch(
    `${DATA_SYNC_BASE_URL}/api/backtest/behavior-audit/latest?limit=2`,
    { cache: 'no-store' },
  );
  if (!res.ok) {
    throw new Error(`behavior-audit latest failed: ${res.status}`);
  }
  const data = (await res.json()) as { ok: boolean; items: BehaviorAuditItem[] };
  return data.items ?? [];
}

async function refreshAudit(): Promise<{ reconDate: string }> {
  const res = await fetch(`${DATA_SYNC_BASE_URL}/api/backtest/behavior-audit/refresh`, {
    method: 'POST',
    cache: 'no-store',
  });
  if (!res.ok) {
    throw new Error(`behavior-audit refresh failed: ${res.status}`);
  }
  return (await res.json()) as { reconDate: string };
}

/** Latest behavior-audit rows (read-only; refresh via useRefreshBehaviorAudit). */
export function useBehaviorAuditQuery() {
  return useQuery({
    queryKey: [behaviorAuditKey],
    queryFn: fetchLatestAudit,
    refetchInterval: 5 * 60 * 1000,
    retry: 1,
  });
}

/** Trigger a fresh audit (engine simulate — takes a few minutes). */
export function useRefreshBehaviorAudit() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: refreshAudit,
    onSuccess: () => {
      setTimeout(() => {
        void qc.invalidateQueries({ queryKey: [behaviorAuditKey] });
      }, 1000);
    },
  });
}
