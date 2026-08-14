'use client';

import * as React from 'react';
import { useQuery } from '@tanstack/react-query';

import { fetchPortfolioHealth, isMarketGateClosed } from '@/lib/queries/portfolioHealth';
import { GateBadge, MobileCard, MobileSection, PriceText, StatusPill } from '../primitives';

/**
 * 执行 tab — market gates + 2pm buy list + things to sell.
 * Mobile Redesign 2027 (docs/designs/mobile-redesign-2027.md §5.1).
 */
export function ExecutionTab() {
  const health = useQuery({
    queryKey: ['portfolio-health'],
    queryFn: ({ signal }) => fetchPortfolioHealth(undefined, signal),
    refetchInterval: 5 * 60_000,
  });

  const cn = health.data;
  const hk = cn?.hkHealth ?? null;
  const cnGate = cn == null ? null : isMarketGateClosed(cn);
  const hkGate = hk == null ? null : isMarketGateClosed(hk);
  const candidates = cn?.s3Candidates ?? [];
  const holdings = [...(cn?.holdings ?? []), ...(hk?.holdings ?? [])];
  const exitHoldings = holdings.filter((h) => h.action === 'EXIT');

  if (health.isLoading && !health.data) {
    return (
      <div className="space-y-3">
        <div className="m-shimmer h-16" />
        <div className="m-shimmer h-24" />
        <div className="m-shimmer h-24" />
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {/* Market gates */}
      <MobileSection title="市场闸门">
        <MobileCard className="p-3">
          <div className="flex items-center justify-between">
            <div className="flex gap-1.5">
              {cn ? <GateBadge market="A股" open={!cnGate} /> : null}
              {hk ? <GateBadge market="港股" open={!hkGate} /> : null}
            </div>
            <div className="text-[var(--m-text-sm)] text-[var(--k-muted)]">
              {cn?.regime ?? '—'} · 强度 {cn?.strength ?? '—'}
            </div>
          </div>
          {cn?.tradeDate ? (
            <div className="mt-2 text-[var(--m-text-xs)] text-[var(--k-muted)]">数据日期 {cn.tradeDate}</div>
          ) : null}
        </MobileCard>
        {cn?.sentiment || cn?.panicCooldown?.active ? (
          <div className="rounded-[var(--m-radius-md)] border border-[var(--k-warn)]/30 bg-[var(--k-warn)]/5 px-3 py-2 text-[var(--m-text-sm)] text-[var(--k-warn)]">
            {cn?.sentiment ? `市场情绪 ${cn.sentiment}` : ''}
            {cn?.panicCooldown?.active ? ` · 恐慌冷却至 ${cn.panicCooldown.cooldownEndDate ?? '—'}` : ''}
          </div>
        ) : null}
      </MobileSection>

      {/* Buy list (2pm) */}
      <MobileSection title={`下午 2 点买入清单${candidates.length ? `（${candidates.length}）` : ''}`}>
        {candidates.length ? (
          <div className="space-y-2">
            {candidates.map((c) => (
              <MobileCard key={c.symbol ?? c.ts_code} className="p-3">
                <div className="flex items-center justify-between gap-2">
                  <div className="min-w-0">
                    <div className="truncate text-[var(--m-text-base)] font-semibold">
                      {c.name ?? c.symbol}
                    </div>
                    <div className="mt-0.5 truncate font-mono text-[var(--m-text-xs)] text-[var(--k-muted)]">
                      {c.symbol}
                      {c.industry ? ` · ${c.industry}` : ''}
                    </div>
                  </div>
                  <div className="shrink-0 text-right">
                    <div className="font-mono text-[var(--m-text-sm)] tabular-nums">score {c.score ?? '—'}</div>
                    {c.alphaEvents?.[0]?.grade ? (
                      <StatusPill tone="open">{c.alphaEvents[0].grade}</StatusPill>
                    ) : null}
                  </div>
                </div>
              </MobileCard>
            ))}
          </div>
        ) : (
          <MobileCard className="px-3 py-4 text-center text-[var(--m-text-sm)] text-[var(--k-muted)]">
            {cnGate ? '闸门关闭 · 今日不买' : '今日无候选'}
          </MobileCard>
        )}
      </MobileSection>

      {/* Sell list */}
      {exitHoldings.length ? (
        <MobileSection title={`需要卖出（${exitHoldings.length}）`}>
          <div className="space-y-2">
            {exitHoldings.map((h) => (
              <MobileCard key={h.symbol} className="border-[var(--k-danger)]/40 bg-[var(--k-danger)]/5 p-3">
                <div className="flex items-center justify-between gap-2">
                  <div className="min-w-0">
                    <div className="truncate text-[var(--m-text-base)] font-semibold">
                      {h.name ?? h.symbol}
                    </div>
                    <div className="mt-0.5 truncate font-mono text-[var(--m-text-xs)] text-[var(--k-muted)]">
                      {h.symbol} · 已持 {h.holdingDays ?? '—'} 天
                    </div>
                  </div>
                  <div className="shrink-0">
                    <StatusPill tone="danger">退出</StatusPill>
                  </div>
                </div>
                <div className="mt-2 grid grid-cols-3 gap-1.5 text-[var(--m-text-xs)]">
                  <div className="rounded-[var(--m-radius-sm)] bg-[var(--k-surface-2)] px-2 py-1">
                    <div className="text-[var(--k-muted)]">止损线</div>
                    <div className="font-mono tabular-nums">{h.stopLossLine ?? '—'}</div>
                  </div>
                  <div className="rounded-[var(--m-radius-sm)] bg-[var(--k-surface-2)] px-2 py-1">
                    <div className="text-[var(--k-muted)]">盈亏</div>
                    <PriceText value={h.pnlPct ?? 0} prefix={h.pnlPct == null ? '—' : ''} />
                  </div>
                  <div className="rounded-[var(--m-radius-sm)] bg-[var(--k-surface-2)] px-2 py-1">
                    <div className="text-[var(--k-muted)]">到期</div>
                    <div className="font-mono tabular-nums">{h.expireDate ?? '—'}</div>
                  </div>
                </div>
                {h.realtimeAlert ? (
                  <div className="mt-2 text-[var(--m-text-sm)] text-[var(--k-warn)]">⚠ {h.realtimeAlert}</div>
                ) : null}
                {h.reason ? <div className="mt-1 text-[var(--m-text-xs)] text-[var(--k-muted)]">{h.reason}</div> : null}
              </MobileCard>
            ))}
          </div>
        </MobileSection>
      ) : null}
    </div>
  );
}
