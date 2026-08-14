'use client';

import * as React from 'react';
import { useQuery } from '@tanstack/react-query';

import { fetchPortfolioHealth, isMarketGateClosed } from '@/lib/queries/portfolioHealth';
import { GateBadge } from './primitives';
import { ExecutionTab } from './tabs/ExecutionTab';
import { HoldingsTab } from './tabs/HoldingsTab';
import { ReconcileTab } from './tabs/ReconcileTab';

/**
 * Mobile-first shell (Mobile Redesign 2027 · docs/designs/mobile-redesign-2027.md).
 *
 * Phone UI is fully isolated from the desktop: bottom tabs (执行/持仓/对账) plus
 * a 更多 tab that reaches every feature through mobile-native pages — never the
 * desktop components. Data comes from the same lib/queries/* as the desktop.
 */

type MobileTab = '执行' | '持仓' | '对账' | '更多';

/** Every mobile page, lazy-loaded on demand (keeps the first screen light). */
const MobileDashboard = React.lazy(() => import('./pages/MobileDashboardPage').then((m) => ({ default: m.MobileDashboardPage })));
const MobileWatchlist = React.lazy(() => import('./pages/MobileWatchlistPage').then((m) => ({ default: m.MobileWatchlistPage })));
const MobileMarket = React.lazy(() => import('./pages/MobileMarketPage').then((m) => ({ default: m.MobileMarketPage })));
const MobileNews = React.lazy(() => import('./pages/MobileNewsPage').then((m) => ({ default: m.MobileNewsPage })));
const MobileIndustryFlow = React.lazy(() => import('./pages/MobileIndustryFlowPage').then((m) => ({ default: m.MobileIndustryFlowPage })));
const MobileAlpha = React.lazy(() => import('./pages/MobileAlphaPage').then((m) => ({ default: m.MobileAlphaPage })));
const MobileDecision = React.lazy(() => import('./pages/MobileDecisionPage').then((m) => ({ default: m.MobileDecisionPage })));
const MobileBacktest = React.lazy(() => import('./pages/MobileBacktestPage').then((m) => ({ default: m.MobileBacktestPage })));
const MobileIndex = React.lazy(() => import('./pages/MobileIndexPage').then((m) => ({ default: m.MobileIndexPage })));
const MobileBroker = React.lazy(() => import('./pages/MobileBrokerPage').then((m) => ({ default: m.MobileBrokerPage })));
const MobileJournal = React.lazy(() => import('./pages/MobileJournalPage').then((m) => ({ default: m.MobileJournalPage })));
const MobileScheduler = React.lazy(() => import('./pages/MobileSchedulerPage').then((m) => ({ default: m.MobileSchedulerPage })));
const MobileWebhook = React.lazy(() => import('./pages/MobileWebhookPage').then((m) => ({ default: m.MobileWebhookPage })));
const MobileSettings = React.lazy(() => import('./pages/MobileSettingsPage').then((m) => ({ default: m.MobileSettingsPage })));

const PAGES: Record<string, { label: string; Comp: React.ComponentType }> = {
  dashboard: { label: 'Dashboard', Comp: MobileDashboard },
  watchlist: { label: 'Watchlist', Comp: MobileWatchlist },
  market: { label: 'Market', Comp: MobileMarket },
  news: { label: 'News', Comp: MobileNews },
  industryFlow: { label: '行业资金流', Comp: MobileIndustryFlow },
  alpha: { label: 'Alpha 雷达', Comp: MobileAlpha },
  decision: { label: '决策 Agent', Comp: MobileDecision },
  backtest: { label: '回测', Comp: MobileBacktest },
  index: { label: '指数', Comp: MobileIndex },
  broker: { label: 'Broker 账户', Comp: MobileBroker },
  journal: { label: '交易日志', Comp: MobileJournal },
  scheduler: { label: '任务调度', Comp: MobileScheduler },
  webhook: { label: 'Webhook', Comp: MobileWebhook },
  settings: { label: '设置', Comp: MobileSettings },
};

export function MobileShell() {
  const [tab, setTab] = React.useState<MobileTab>('执行');
  const [pageId, setPageId] = React.useState<string | null>(null);
  const health = useQuery({
    queryKey: ['portfolio-health'],
    queryFn: ({ signal }) => fetchPortfolioHealth(undefined, signal),
    refetchInterval: 5 * 60_000,
  });

  const cn = health.data;
  const hk = cn?.hkHealth ?? null;
  const cnGate = cn == null ? null : isMarketGateClosed(cn);
  const hkGate = hk == null ? null : isMarketGateClosed(hk);

  const openPage = (id: string) => {
    setPageId(id);
    setTab('更多');
  };

  const current = pageId ? PAGES[pageId] : null;

  return (
    <div className="flex h-dvh w-full flex-col bg-[var(--k-bg)] text-[var(--k-text)]">
      {/* Header */}
      <header className="flex h-[var(--m-header-h)] shrink-0 items-center justify-between border-b border-[var(--k-border)] bg-[var(--k-surface)] px-4">
        <div className="flex min-w-0 items-center gap-2">
          {pageId ? (
            <button
              type="button"
              onClick={() => setPageId(null)}
              className="shrink-0 rounded-md px-1.5 py-1 text-[var(--m-text-sm)] text-[var(--k-accent)] active:bg-[var(--k-surface-2)]"
            >
              ‹ 返回
            </button>
          ) : null}
          <div className="truncate text-[var(--m-text-lg)] font-bold">
            {pageId ? current?.label ?? '页面' : 'Karios'}
          </div>
        </div>
        {!pageId ? (
          <div className="flex shrink-0 items-center gap-1.5">
            {cn ? <GateBadge market="A股" open={!cnGate} /> : null}
            {hk ? <GateBadge market="港股" open={!hkGate} /> : null}
          </div>
        ) : null}
      </header>

      {/* Content */}
      <main className="flex-1 overflow-y-auto p-[var(--m-content-pad)] pb-[calc(var(--m-content-pad)+env(safe-area-inset-bottom))]">
        {pageId ? (
          <React.Suspense
            fallback={
              <div className="space-y-3">
                <div className="m-shimmer h-16" />
                <div className="m-shimmer h-24" />
              </div>
            }
          >
            {current ? <current.Comp /> : null}
          </React.Suspense>
        ) : tab === '执行' ? (
          <ExecutionTab />
        ) : tab === '持仓' ? (
          <HoldingsTab />
        ) : tab === '对账' ? (
          <ReconcileTab />
        ) : (
          <div className="grid grid-cols-2 gap-2">
            {Object.entries(PAGES).map(([id, def]) => (
              <button
                key={id}
                type="button"
                onClick={() => openPage(id)}
                className="rounded-[var(--m-radius-md)] border border-[var(--k-border)] bg-[var(--k-surface)] px-3 py-4 text-left text-[var(--m-text-sm)] font-medium active:bg-[var(--k-surface-2)]"
              >
                {def.label}
              </button>
            ))}
          </div>
        )}
      </main>

      {/* Bottom tabs */}
      <nav
        className="flex shrink-0 border-t border-[var(--k-border)] bg-[var(--k-surface)] pb-[env(safe-area-inset-bottom)]"
        style={{ minHeight: 'var(--m-tabbar-h)' }}
      >
        {(['执行', '持仓', '对账', '更多'] as const).map((t) => (
          <button
            key={t}
            type="button"
            onClick={() => {
              setTab(t);
              if (t !== '更多') setPageId(null);
            }}
            className={`flex-1 text-[var(--m-text-sm)] font-medium ${
              tab === t ? 'text-[var(--k-accent)]' : 'text-[var(--k-muted)]'
            }`}
          >
            {t}
          </button>
        ))}
      </nav>
    </div>
  );
}
