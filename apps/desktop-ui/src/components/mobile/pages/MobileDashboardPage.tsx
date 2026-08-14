'use client';

import * as React from 'react';
import { useQuery } from '@tanstack/react-query';

import { fetchPortfolioHealth, isMarketGateClosed } from '@/lib/queries/portfolioHealth';
import { useNewsItemsQuery } from '@/lib/queries/news';
import { useIndustryFundFlowQuery } from '@/lib/queries/industryFlow';
import { GateBadge, MobileCard, MobileSection, PriceText, StatusPill } from '../primitives';

/** Dashboard (mobile) — gates + news pulse + top industry inflow. §5.2 高频. */
export function MobileDashboardPage() {
  const health = useQuery({
    queryKey: ['portfolio-health'],
    queryFn: ({ signal }) => fetchPortfolioHealth(undefined, signal),
    refetchInterval: 5 * 60_000,
  });
  const news = useNewsItemsQuery(24, 5);
  const flow = useIndustryFundFlowQuery(10, 200);

  const cn = health.data;
  const cnGate = cn == null ? null : isMarketGateClosed(cn);
  const hk = cn?.hkHealth ?? null;
  const hkGate = hk == null ? null : isMarketGateClosed(hk);

  const topIn = [...(flow.data?.top ?? [])].sort((a, b) => b.netInflow - a.netInflow).slice(0, 5);

  return (
    <div className="space-y-4">
      <MobileSection title="今日状态">
        <MobileCard className="p-3">
          <div className="flex items-center justify-between">
            <div className="flex gap-1.5">
              {cn ? <GateBadge market="A股" open={!cnGate} /> : null}
              {hk ? <GateBadge market="港股" open={!hkGate} /> : null}
            </div>
            <div className="flex gap-2 text-[var(--m-text-sm)] text-[var(--k-muted)]">
              {cn ? <StatusPill tone={cnGate ? 'closed' : 'open'}>{cn.regime ?? '—'}</StatusPill> : null}
              <StatusPill tone="neutral">强度 {cn?.strength ?? '—'}</StatusPill>
            </div>
          </div>
          <div className="mt-2 flex justify-between text-[var(--m-text-xs)] text-[var(--k-muted)]">
            <span>买入候选 {cn?.s3Candidates?.length ?? 0} 个</span>
            <span>持仓 {cn?.holdings?.length ?? 0} + {(hk?.holdings?.length ?? 0) ? `${hk?.holdings?.length ?? 0} 港股` : ''}</span>
            <span>数据 {cn?.tradeDate ?? '—'}</span>
          </div>
        </MobileCard>
        {cn?.sentiment || cn?.panicCooldown?.active ? (
          <MobileCard className="border-[var(--k-warn)]/40 bg-[var(--k-warn)]/5 p-3 text-[var(--m-text-sm)] text-[var(--k-warn)]">
            {cn?.sentiment ? `市场情绪 ${cn.sentiment}` : ''}
            {cn?.panicCooldown?.active ? ` · 恐慌冷却至 ${cn.panicCooldown.cooldownEndDate ?? '—'}` : ''}
          </MobileCard>
        ) : null}
      </MobileSection>

      <MobileSection title="行业资金流 Top 5">
        {topIn.length ? (
          <div className="space-y-2">
            {topIn.map((r, i) => (
              <MobileCard key={r.industryCode} className="p-3">
                <div className="flex items-center justify-between gap-2">
                  <div className="min-w-0">
                    <div className="truncate text-[var(--m-text-base)] font-semibold">
                      {i + 1}. {r.industryName}
                    </div>
                    <div className="mt-0.5 text-[var(--m-text-xs)] text-[var(--k-muted)]">
                      10 日累计 {r.sum10d.toFixed(1)} 亿
                    </div>
                  </div>
                  <div className="shrink-0 text-right">
                    <PriceText value={r.netInflow} prefix="+" />
                    <div className="mt-0.5 text-right text-[var(--m-text-xs)] text-[var(--k-muted)]">亿元</div>
                  </div>
                </div>
              </MobileCard>
            ))}
          </div>
        ) : (
          <MobileCard className="px-3 py-6 text-center text-[var(--m-text-sm)] text-[var(--k-muted)]">
            暂无行业资金流数据
          </MobileCard>
        )}
      </MobileSection>

      <MobileSection title="最新要闻">
        {news.data?.items.length ? (
          <div className="space-y-2">
            {news.data.items.slice(0, 5).map((n) => (
              <MobileCard key={n.id} className="p-3">
                <div className="truncate text-[var(--m-text-base)] font-medium">{n.title}</div>
                {n.aiSummary ? (
                  <div className="mt-1 line-clamp-2 text-[var(--m-text-sm)] text-[var(--k-muted)]">
                    {n.aiSummary}
                  </div>
                ) : null}
                {n.eventType ? <StatusPill tone="neutral">{n.eventType}</StatusPill> : null}
              </MobileCard>
            ))}
          </div>
        ) : (
          <MobileCard className="px-3 py-6 text-center text-[var(--m-text-sm)] text-[var(--k-muted)]">
            暂无新闻
          </MobileCard>
        )}
      </MobileSection>
    </div>
  );
}
