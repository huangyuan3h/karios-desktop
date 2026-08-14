'use client';

import * as React from 'react';
import { useQuery } from '@tanstack/react-query';

import { fetchPortfolioHealth, type PortfolioHolding } from '@/lib/queries/portfolioHealth';
import { MobileCard, MobileSection, PriceText, StatusPill } from '../primitives';

/** 持仓 tab — every position as a card. Mobile Redesign 2027 §5.1. */
function HoldingRow({ h, market }: { h: PortfolioHolding; market: string }) {
  return (
    <MobileCard className="p-3">
      <div className="flex items-center justify-between gap-2">
        <div className="min-w-0">
          <div className="truncate text-[var(--m-text-base)] font-semibold">{h.name ?? h.symbol}</div>
          <div className="mt-0.5 truncate font-mono text-[var(--m-text-xs)] text-[var(--k-muted)]">
            {h.symbol} · {market} · 已持 {h.holdingDays ?? '—'} 天
          </div>
        </div>
        <div className="shrink-0 text-right">
          <PriceText value={h.pnlPct ?? 0} />
          <div className="mt-0.5">
            {h.action === 'EXIT' ? (
              <StatusPill tone="danger">退出</StatusPill>
            ) : (
              <StatusPill tone="open">持有</StatusPill>
            )}
          </div>
        </div>
      </div>
      <div className="mt-2 grid grid-cols-3 gap-1.5 text-[var(--m-text-xs)]">
        <div className="rounded-[var(--m-radius-sm)] bg-[var(--k-surface-2)] px-2 py-1">
          <div className="text-[var(--k-muted)]">止损线</div>
          <div className="font-mono tabular-nums">{h.stopLossLine ?? '—'}</div>
        </div>
        <div className="rounded-[var(--m-radius-sm)] bg-[var(--k-surface-2)] px-2 py-1">
          <div className="text-[var(--k-muted)]">移动线</div>
          <div className="font-mono tabular-nums">{h.trailingLine ?? '—'}</div>
        </div>
        <div className="rounded-[var(--m-radius-sm)] bg-[var(--k-surface-2)] px-2 py-1">
          <div className="text-[var(--k-muted)]">到期</div>
          <div className="font-mono tabular-nums">{h.expireDate ?? '—'}</div>
        </div>
      </div>
      {h.realtimeAlert ? (
        <div className="mt-2 text-[var(--m-text-sm)] text-[var(--k-warn)]">⚠ {h.realtimeAlert}</div>
      ) : null}
    </MobileCard>
  );
}

export function HoldingsTab() {
  const health = useQuery({
    queryKey: ['portfolio-health'],
    queryFn: ({ signal }) => fetchPortfolioHealth(undefined, signal),
    refetchInterval: 5 * 60_000,
  });

  const cn = health.data;
  const rows = [
    ...(cn?.holdings ?? []).map((h) => ({ h, m: 'A股' as const })),
    ...(cn?.hkHealth?.holdings ?? []).map((h) => ({ h, m: '港股' as const })),
  ];

  if (health.isLoading && !health.data) {
    return (
      <div className="space-y-3">
        <div className="m-shimmer h-20" />
        <div className="m-shimmer h-20" />
        <div className="m-shimmer h-20" />
      </div>
    );
  }

  return (
    <MobileSection title={`全部持仓（${rows.length}）`}>
      {rows.length ? (
        <div className="space-y-2">
          {rows.map(({ h, m }) => (
            <HoldingRow key={h.symbol} h={h} market={m} />
          ))}
        </div>
      ) : (
        <MobileCard className="px-3 py-8 text-center text-[var(--m-text-sm)] text-[var(--k-muted)]">
          暂无持仓
        </MobileCard>
      )}
    </MobileSection>
  );
}
