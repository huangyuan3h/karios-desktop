'use client';
import * as React from 'react';
import { useQuery } from '@tanstack/react-query';
import { Button } from '@/components/ui/button';
import { useBehaviorAuditQuery, useRefreshBehaviorAudit } from '@/lib/queries/behaviorAudit';
import { fetchPortfolioHealth } from '@/lib/queries/portfolioHealth';

export function TodayActionCard() {
  const auditQuery = useBehaviorAuditQuery();
  const healthQuery = useQuery({
    queryKey: ['portfolio-health'],
    queryFn: ({ signal }) => fetchPortfolioHealth(undefined, signal),
    refetchInterval: 5 * 60_000,
  });
  const refresh = useRefreshBehaviorAudit();
  const [refreshing, setRefreshing] = React.useState(false);

  const rows = auditQuery.data ?? [];
  const extraRows = rows.flatMap((r) => (r.extraList ?? []).map((e) => ({ ...e, market: r.market })));
  const missingRows = rows.flatMap((r) => (r.missingList ?? []).map((m) => ({ ...m, market: r.market })));

  // holdings from portfolio health for HOLD detection
  const holdings: Array<{ symbol: string; name?: string; positionPct?: number }> =
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    ((healthQuery.data as any)?.holdings as Array<any>) ?? [];
  const multi = (healthQuery.data as unknown as { multiAssetSleeve?: { pick?: { symbol?: string; key?: string; mom60?: number }; action?: string; label?: string; message?: string } } | undefined)?.multiAssetSleeve;

  // classify HOLD: holdings that are not in extraRows
  const extraSet = new Set(extraRows.map((e) => e.symbol));
  const holdRows = holdings.filter((h) => !extraSet.has(h.symbol));

  // sleeve as buy/hold candidate when missing is empty but sleeve says BUY
  const sleeveBuy = multi && (multi.action === 'BUY' || multi.action === 'ROTATE') ? multi : null;
  const sleeveHold = multi && multi.action === 'HOLD' ? multi : null;

  // merge missing + sleeve buy into "今日买入"
  const buyList: Array<{ symbol: string; name?: string; reason: string }> = [
    ...missingRows.map((m) => ({
      symbol: m.symbol,
      name: (m as unknown as { name?: string }).name,
      reason: `回测应持有${(m as unknown as { score?: number }).score != null ? ` score ${(m as unknown as { score?: number }).score}` : ''}`,
    })),
    ...(sleeveBuy
      ? [
          {
            symbol: sleeveBuy.pick?.symbol ?? 'ETF',
            name: sleeveBuy.pick?.key ?? '',
            reason: `${sleeveBuy.label ?? sleeveBuy.action} mom60 ${sleeveBuy.pick?.mom60 ?? ''}%`,
          },
        ]
      : []),
  ];
  // dedup buy by symbol
  const buyDedup = Array.from(new Map(buyList.map((b) => [b.symbol, b])).values());

  const onRefresh = () => {
    setRefreshing(true);
    void refresh.mutateAsync(undefined, { onSettled: () => setRefreshing(false) });
  };

  const hasAction = buyDedup.length || extraRows.length || holdRows.length || sleeveHold;

  // compact inline layout (single row, not 3 large boxes)
  const buyChips = buyDedup.slice(0, 8);
  const sellChips = extraRows.slice(0, 8);
  const holdChips = holdRows.slice(0, 8);
  return (
    <div className="mb-4 rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-3 py-2.5">
      <div className="flex items-center gap-2">
        <span className="text-[12px] font-semibold">今日操作</span>
        <span className="text-[11px] text-[var(--k-muted)]">
          {buyDedup.length ? `${buyDedup.length}买` : '无买'} · {extraRows.length ? `${extraRows.length}卖` : '无卖'} · {holdChips.length + (sleeveHold ? 1 : 0)}持有
        </span>
        <Button variant="ghost" size="sm" className="ml-auto h-6 px-2 text-[11px]" onClick={onRefresh} disabled={refreshing || refresh.isPending}>
          {refreshing ? '回测中…' : '刷新对账'}
        </Button>
      </div>
      {!hasAction ? (
        <div className="mt-1.5 text-xs text-[var(--k-muted)]">与回测一致 · 空闲按单轨持有 {multi?.pick?.symbol ?? 'REPO'}</div>
      ) : (
        <div className="mt-2 flex flex-col gap-1.5 text-xs">
          <div className="flex flex-wrap items-center gap-1.5">
            <span className="shrink-0 rounded bg-emerald-500/10 px-1.5 py-0.5 text-[11px] font-medium text-emerald-700">买</span>
            {buyChips.length ? (
              buyChips.map((b) => (
                <span key={b.symbol} className="rounded border border-emerald-500/20 bg-emerald-500/5 px-1.5 py-0.5 font-mono text-[11px]" title={b.reason}>
                  {b.symbol}
                </span>
              ))
            ) : (
              <span className="text-[11px] text-[var(--k-muted)]">—</span>
            )}
            {buyDedup.length > 8 ? <span className="text-[11px] text-[var(--k-muted)]">+{buyDedup.length - 8}</span> : null}
          </div>
          <div className="flex flex-wrap items-center gap-1.5">
            <span className="shrink-0 rounded bg-red-500/10 px-1.5 py-0.5 text-[11px] font-medium text-red-700">卖</span>
            {sellChips.length ? (
              sellChips.map((e) => (
                <span key={e.symbol} className="rounded border border-red-500/20 bg-red-500/5 px-1.5 py-0.5 font-mono text-[11px]">
                  {e.symbol}
                </span>
              ))
            ) : (
              <span className="text-[11px] text-[var(--k-muted)]">—</span>
            )}
          </div>
          <div className="flex flex-wrap items-center gap-1.5">
            <span className="shrink-0 rounded bg-[var(--k-surface-2)] px-1.5 py-0.5 text-[11px] font-medium">持有</span>
            {holdChips.length
              ? holdChips.map((h) => (
                  <span key={h.symbol} className="rounded border border-[var(--k-border)] bg-[var(--k-surface-2)] px-1.5 py-0.5 font-mono text-[11px]">
                    {h.symbol}
                  </span>
                ))
              : null}
            {sleeveHold ? (
              <span className="rounded border border-sky-500/20 bg-sky-500/5 px-1.5 py-0.5 font-mono text-[11px] text-sky-700" title={`mom60 ${sleeveHold.pick?.mom60}%`}>
                {sleeveHold.pick?.symbol}持有
              </span>
            ) : null}
            {!holdChips.length && !sleeveHold ? <span className="text-[11px] text-[var(--k-muted)]">—</span> : null}
            {holdRows.length > 8 ? <span className="text-[11px] text-[var(--k-muted)]">+{holdRows.length - 8}</span> : null}
          </div>
        </div>
      )}
    </div>
  );
}
