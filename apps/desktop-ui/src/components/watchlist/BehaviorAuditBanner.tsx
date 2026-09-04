import * as React from 'react';
import { useQuery } from '@tanstack/react-query';

import { Button } from '@/components/ui/button';
import { useBehaviorAuditQuery, useRefreshBehaviorAudit } from '@/lib/queries/behaviorAudit';
import { fetchPortfolioHealth, isMarketGateClosed } from '@/lib/queries/portfolioHealth';

/**
 * OPT-106: real-book vs S-3 backtest behavior audit banner.
 *
 * Shows any holding the backtest would NOT hold (买了不该买 / 该卖没卖) and
 * any backtest holding the user skipped (该持没买) — so behavior that
 * deviates from the backtested rule set is surfaced right on the watchlist.
 *
 * 2026-08-14: when a market's S-3 gate is CLOSED today (闸门关闭 — regime
 * Weak / panic cooldown / circuit breaker, so new entries are impossible),
 * that market's 该持没买 suggestions are hidden — only actionable exits
 * (该卖没卖) and held-but-unapproved rows (买了不该买) stay visible.
 *
 * OPT-140: satellite-leg holdings are split out of the S-3 comparison
 * backend-side — they render here as a neutral 🛰 卫星腿 panel (vs the
 * twin-star engine book), never as 买了不该买.
 */
export function BehaviorAuditBanner() {
  const auditQuery = useBehaviorAuditQuery();
  const healthQuery = useQuery({
    queryKey: ['portfolio-health'],
    queryFn: ({ signal }) => fetchPortfolioHealth(undefined, signal),
    refetchInterval: 5 * 60_000,
  });
  const refresh = useRefreshBehaviorAudit();
  const [refreshing, setRefreshing] = React.useState(false);

  const rows = auditQuery.data ?? [];
  const extraRows = rows.flatMap((r) =>
    (r.extraList ?? []).map((e) => ({ ...e, market: r.market, auditDate: r.auditDate })),
  );
  const missingRows = rows.flatMap((r) =>
    (r.missingList ?? []).map((m) => ({ ...m, market: r.market, auditDate: r.auditDate })),
  );

  // Per-market "cannot buy today" (shared with PortfolioHealthCard's 闸门关闭 badge).
  const blockedMarkets = React.useMemo(() => {
    const s = new Set<string>();
    const cn = healthQuery.data;
    if (isMarketGateClosed(cn)) s.add('CN');
    if (isMarketGateClosed(cn?.hkHealth)) s.add('HK');
    return s;
  }, [healthQuery.data]);

  const hiddenMissing = missingRows.filter((m) => blockedMarkets.has(m.market));
  const visibleMissing = missingRows.filter((m) => !blockedMarkets.has(m.market));
  const latestDate = rows[0]?.auditDate;

  // OPT-140 satellite leg (vs twin-star engine book — info, never a warning).
  const satExtraRows = rows.flatMap((r) =>
    (r.satExtraList ?? []).map((e) => ({ ...e, market: r.market, auditDate: r.auditDate })),
  );
  const satMissingRows = rows.flatMap((r) =>
    (r.satMissingList ?? []).map((m) => ({ ...m, market: r.market, auditDate: r.auditDate })),
  );
  const satExpected = rows.reduce((n, r) => n + (r.satExpected ?? 0), 0);
  const satActual = rows.reduce((n, r) => n + (r.actualSat ?? 0), 0);
  const hasSatData = satExpected > 0 || satActual > 0 || satExtraRows.length > 0 || satMissingRows.length > 0;

  const satPanel = hasSatData ? (
    <div className="mt-2 rounded-md border border-sky-500/30 bg-sky-500/5 px-3 py-2 text-[12px]">
      <div className="font-medium text-sky-800 dark:text-sky-200">
        🛰 卫星腿（引擎账本对照）：实持 {satActual} / 引擎应持 {satExpected}
        {satExtraRows.length === 0 && satMissingRows.length === 0 ? ' · 一致' : ''}
      </div>
      {satExtraRows.length ? (
        <div className="mt-1 space-y-0.5 text-sky-700 dark:text-sky-300">
          {satExtraRows.map((e) => (
            <div key={`${e.auditDate}-${e.symbol}`}>
              账外持有：<span className="font-mono">{e.symbol}</span>
              {e.name ? <span> {e.name}</span> : null}
              <span className="text-[var(--k-muted)]"> · 引擎账本无（对照双子星，非 S-3 口径）</span>
            </div>
          ))}
        </div>
      ) : null}
      {satMissingRows.length ? (
        <div className="mt-1 space-y-0.5 text-sky-700 dark:text-sky-300">
          {satMissingRows.map((m) => (
            <div key={`${m.auditDate}-${m.symbol}`}>
              引擎应持未持有：<span className="font-mono">{m.symbol}</span>
            </div>
          ))}
        </div>
      ) : null}
    </div>
  ) : null;

  const onRefresh = () => {
    setRefreshing(true);
    void refresh.mutateAsync(undefined, {
      onSettled: () => setRefreshing(false),
    });
  };

  if (!extraRows.length && !visibleMissing.length && !hiddenMissing.length) {
    // Silent when there's nothing to flag — but still allow a manual refresh.
    return latestDate ? (
      <div className="mb-4">
        <div className="flex items-center gap-2 text-[12px] text-[var(--k-muted)]">
          <span>✅ 行为对账（{latestDate}）：持仓与 S-3 回测口径一致</span>
          <Button variant="ghost" size="sm" className="h-6 px-2 text-[11px]" onClick={onRefresh} disabled={refreshing || refresh.isPending}>
            {refreshing ? '回测模拟中（约3-4分钟）…' : '刷新对账'}
          </Button>
        </div>
        {satPanel}
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

  if (!extraRows.length && !visibleMissing.length) {
    // Everything flagged is a buy suggestion — but the gate is closed, so
    // nothing is actionable today; stay quiet and say so.
    return (
      <div className="mb-4">
        <div className="flex items-center gap-2 text-[12px] text-[var(--k-muted)]">
          <span>
            ✅ 行为对账（{latestDate ?? '—'}）：无待操作提醒
            {hiddenMissing.length ? ` — 闸门关闭 · 今日不可买入，已隐藏 ${hiddenMissing.length} 条该持没买` : ''}
          </span>
          <Button variant="ghost" size="sm" className="h-6 px-2 text-[11px]" onClick={onRefresh} disabled={refreshing || refresh.isPending}>
            {refreshing ? '回测模拟中（约3-4分钟）…' : '刷新对账'}
          </Button>
        </div>
        {satPanel}
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
      {visibleMissing.length ? (
        <div className="mt-1.5 space-y-0.5 text-[12px] text-sky-700 dark:text-sky-300">
          {visibleMissing.map((m) => (
            <div key={`${m.auditDate}-${m.symbol}`}>
              🔵 该持没买：<span className="font-mono">{m.symbol}</span>
              {m.score != null ? `（score ${m.score}）` : ''} · 回测今日应持有
            </div>
          ))}
        </div>
      ) : null}
      {hiddenMissing.length ? (
        <div className="mt-1.5 text-[11px] text-[var(--k-muted)]">
          🔒 闸门关闭 · 今日不可买入——已隐藏 {hiddenMissing.length} 条该持没买
        </div>
      ) : null}
      {satPanel}
    </div>
  );
}
