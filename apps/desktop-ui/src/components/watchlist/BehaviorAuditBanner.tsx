import * as React from 'react';

import { Button } from '@/components/ui/button';
import { useBehaviorAuditQuery, useRefreshBehaviorAudit } from '@/lib/queries/behaviorAudit';

/**
 * OPT-106: real-book vs S-3 backtest behavior audit banner.
 *
 * Shows any holding the backtest would NOT hold (买了不该买 / 该卖没卖) and
 * any backtest holding the user skipped (该持没买) — so behavior that
 * deviates from the backtested rule set is surfaced right on the watchlist.
 */
export function BehaviorAuditBanner() {
  const auditQuery = useBehaviorAuditQuery();
  const refresh = useRefreshBehaviorAudit();
  const [refreshing, setRefreshing] = React.useState(false);

  const rows = auditQuery.data ?? [];
  const extraRows = rows.flatMap((r) =>
    (r.extraList ?? []).map((e) => ({ ...e, market: r.market, auditDate: r.auditDate })),
  );
  const missingRows = rows.flatMap((r) =>
    (r.missingList ?? []).map((m) => ({ ...m, market: r.market, auditDate: r.auditDate })),
  );

  const latestDate = rows[0]?.auditDate;

  const onRefresh = () => {
    setRefreshing(true);
    void refresh.mutateAsync(undefined, {
      onSettled: () => setRefreshing(false),
    });
  };

  if (!extraRows.length && !missingRows.length) {
    // Silent when there's nothing to flag — but still allow a manual refresh.
    return latestDate ? (
      <div className="mb-4 flex items-center gap-2 text-[12px] text-[var(--k-muted)]">
        <span>✅ 行为对账（{latestDate}）：持仓与 S-3 回测口径一致</span>
        <Button variant="ghost" size="sm" className="h-6 px-2 text-[11px]" onClick={onRefresh} disabled={refreshing || refresh.isPending}>
          {refreshing ? '回测模拟中（约3-4分钟）…' : '刷新对账'}
        </Button>
      </div>
    ) : (
      <div className="mb-4 flex items-center gap-2 text-[12px] text-[var(--k-muted)]">
        <span>行为对账：暂无数据（第一次需手动刷新，回测模拟约 3-4 分钟）</span>
        <Button variant="ghost" size="sm" className="h-6 px-2 text-[11px]" onClick={onRefresh} disabled={refreshing || refresh.isPending}>
          {refreshing ? '回测模拟中…' : '开始对账'}
        </Button>
      </div>
    );
  }

  return (
    <div className="mb-4 rounded-lg border border-amber-500/40 bg-amber-500/10 px-4 py-3 text-sm">
      <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
        <span className="font-medium text-amber-800 dark:text-amber-200">
          ⚠ 行为与 S-3 回测不一致（{latestDate ?? '—'}）
        </span>
        <Button variant="ghost" size="sm" className="ml-auto h-6 px-2 text-[11px]" onClick={onRefresh} disabled={refreshing || refresh.isPending}>
          {refreshing ? '回测模拟中（约3-4分钟）…' : '刷新对账'}
        </Button>
      </div>
      {extraRows.length ? (
        <div className="mt-2 space-y-1 text-[12px]">
          {extraRows.map((e) => (
            <div key={`${e.auditDate}-${e.symbol}`} className="flex flex-wrap gap-x-2">
              <span className={e.kind === 'exited' ? 'text-red-700 dark:text-red-300' : 'text-orange-700 dark:text-orange-300'}>
                {e.kind === 'exited' ? '🔴 该卖没卖' : '🟠 买了不该买'}
              </span>
              <span className="font-mono">{e.symbol}</span>
              {e.name ? <span>{e.name}</span> : null}
              <span className="text-[var(--k-muted)]">
                {e.costPrice != null ? `成本 ${e.costPrice}` : ''}
                {e.entryDate ? ` · ${e.entryDate} 买入` : ''} · 回测口径不持有
              </span>
            </div>
          ))}
        </div>
      ) : null}
      {missingRows.length ? (
        <div className="mt-1.5 space-y-0.5 text-[12px] text-sky-700 dark:text-sky-300">
          {missingRows.map((m) => (
            <div key={`${m.auditDate}-${m.symbol}`}>
              🔵 该持没买：<span className="font-mono">{m.symbol}</span>
              {m.score != null ? `（score ${m.score}）` : ''} · 回测今日应持有
            </div>
          ))}
        </div>
      ) : null}
    </div>
  );
}
