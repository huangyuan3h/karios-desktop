'use client';

import * as React from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  ArrowLeft,
  BarChart3,
  Bot,
  CalendarClock,
  ClipboardList,
  Factory,
  FlaskConical,
  LayoutGrid,
  Menu,
  Newspaper,
  Radar,
  Scale,
  Settings,
  Star,
  TrendingUp,
  Wallet,
  Webhook,
  X,
} from 'lucide-react';

import { fetchPortfolioHealth, isMarketGateClosed } from '@/lib/queries/portfolioHealth';
import { GateBadge } from './primitives';
import { ReconcileTab } from './tabs/ReconcileTab';

/**
 * Mobile-first shell (Mobile Redesign 2027 · docs/designs/mobile-redesign-2027.md).
 *
 * IA v2 (2026-08-14 user feedback): the bottom bar holds the THREE things that
 * matter — Dashboard / Watchlist / Agent. Everything else lives behind the
 * animated 更多 panel in the header. Phone UI stays fully isolated from desktop.
 */

type MobileTab = 'dashboard' | 'watchlist' | 'agent';
type PageDef = { id: string; label: string; icon: typeof LayoutGrid; Comp: React.ComponentType };

const MobileDashboard = React.lazy(() =>
  import('./pages/MobileDashboardPage').then((m) => ({ default: m.MobileDashboardPage })),
);
const MobileWatchlist = React.lazy(() =>
  import('./pages/MobileWatchlistPage').then((m) => ({ default: m.MobileWatchlistPage })),
);
const MobileDecision = React.lazy(() =>
  import('./pages/MobileDecisionPage').then((m) => ({ default: m.MobileDecisionPage })),
);
const MobileMarket = React.lazy(() =>
  import('./pages/MobileMarketPage').then((m) => ({ default: m.MobileMarketPage })),
);
const MobileNews = React.lazy(() =>
  import('./pages/MobileNewsPage').then((m) => ({ default: m.MobileNewsPage })),
);
const MobileIndustryFlow = React.lazy(() =>
  import('./pages/MobileIndustryFlowPage').then((m) => ({ default: m.MobileIndustryFlowPage })),
);
const MobileAlpha = React.lazy(() =>
  import('./pages/MobileAlphaPage').then((m) => ({ default: m.MobileAlphaPage })),
);
const MobileBacktest = React.lazy(() =>
  import('./pages/MobileBacktestPage').then((m) => ({ default: m.MobileBacktestPage })),
);
const MobileIndex = React.lazy(() =>
  import('./pages/MobileIndexPage').then((m) => ({ default: m.MobileIndexPage })),
);
const MobileBroker = React.lazy(() =>
  import('./pages/MobileBrokerPage').then((m) => ({ default: m.MobileBrokerPage })),
);
const MobileJournal = React.lazy(() =>
  import('./pages/MobileJournalPage').then((m) => ({ default: m.MobileJournalPage })),
);
const MobileScheduler = React.lazy(() =>
  import('./pages/MobileSchedulerPage').then((m) => ({ default: m.MobileSchedulerPage })),
);
const MobileWebhook = React.lazy(() =>
  import('./pages/MobileWebhookPage').then((m) => ({ default: m.MobileWebhookPage })),
);
const MobileSettings = React.lazy(() =>
  import('./pages/MobileSettingsPage').then((m) => ({ default: m.MobileSettingsPage })),
);

const TABS: { id: MobileTab; label: string; icon: typeof LayoutGrid; Comp: React.ComponentType }[] = [
  { id: 'dashboard', label: '首页', icon: LayoutGrid, Comp: MobileDashboard },
  { id: 'watchlist', label: '自选', icon: Star, Comp: MobileWatchlist },
  { id: 'agent', label: 'Agent', icon: Bot, Comp: MobileDecision },
];

/** Everything else — reachable from the animated 更多 panel in the header. */
const MORE_SECTIONS: { label: string; items: PageDef[] }[] = [
  {
    label: '行情研究',
    items: [
      { id: 'market', label: 'Market', icon: TrendingUp, Comp: MobileMarket },
      { id: 'news', label: 'News', icon: Newspaper, Comp: MobileNews },
      { id: 'industryFlow', label: '行业资金流', icon: Factory, Comp: MobileIndustryFlow },
      { id: 'alpha', label: 'Alpha 雷达', icon: Radar, Comp: MobileAlpha },
      { id: 'index', label: '指数', icon: BarChart3, Comp: MobileIndex },
      { id: 'backtest', label: '回测', icon: FlaskConical, Comp: MobileBacktest },
    ],
  },
  {
    label: '复盘',
    items: [
      { id: 'reconcile', label: '行为对账', icon: Scale, Comp: ReconcileTab },
      { id: 'journal', label: '交易日志', icon: ClipboardList, Comp: MobileJournal },
      { id: 'broker', label: 'Broker 账户', icon: Wallet, Comp: MobileBroker },
    ],
  },
  {
    label: '系统',
    items: [
      { id: 'scheduler', label: '任务调度', icon: CalendarClock, Comp: MobileScheduler },
      { id: 'webhook', label: 'Webhook', icon: Webhook, Comp: MobileWebhook },
      { id: 'settings', label: '设置', icon: Settings, Comp: MobileSettings },
    ],
  },
];

export function MobileShell() {
  const [tab, setTab] = React.useState<MobileTab>('dashboard');
  const [pageId, setPageId] = React.useState<string | null>(null);
  const [moreOpen, setMoreOpen] = React.useState(false);
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
    setMoreOpen(false);
  };

  const currentDef: PageDef | null = (() => {
    if (!pageId) return null;
    for (const s of MORE_SECTIONS) {
      const hit = s.items.find((p) => p.id === pageId);
      if (hit) return hit;
    }
    return null;
  })();

  const activeTab = TABS.find((t) => t.id === tab);

  return (
    <div className="flex h-dvh w-full flex-col bg-[var(--k-bg)] text-[var(--k-text)]">
      {/* Header */}
      <header className="relative z-20 flex h-[var(--m-header-h)] shrink-0 items-center justify-between border-b border-[var(--k-border)] bg-[var(--k-surface)] px-3">
        <div className="flex min-w-0 items-center gap-2">
          {pageId ? (
            <button
              type="button"
              onClick={() => setPageId(null)}
              aria-label="返回"
              className="flex h-8 w-8 shrink-0 items-center justify-center rounded-full text-[var(--k-accent)] active:bg-[var(--k-surface-2)]"
            >
              <ArrowLeft size={18} />
            </button>
          ) : (
            <div className="flex items-center gap-2">
              <span
                className="h-2.5 w-2.5 rounded-full"
                style={{ background: 'linear-gradient(135deg, var(--k-accent), #f59e0b)' }}
              />
              <span className="text-[17px] font-bold tracking-tight">Karios</span>
            </div>
          )}
          <div className="truncate text-[17px] font-bold tracking-tight">
            {pageId ? currentDef?.label : activeTab?.label}
          </div>
        </div>

        <div className="flex shrink-0 items-center gap-2">
          {!pageId ? (
            <div className="flex items-center gap-1.5">
              {cn ? <GateBadge market="A股" open={!cnGate} /> : null}
              {hk ? <GateBadge market="港股" open={!hkGate} /> : null}
            </div>
          ) : null}
          {!pageId ? (
            <button
              type="button"
              onClick={() => setMoreOpen((o) => !o)}
              aria-label="更多"
              className={`flex h-9 w-9 items-center justify-center rounded-full transition-colors ${
                moreOpen ? 'bg-[var(--k-accent)]/10 text-[var(--k-accent)]' : 'text-[var(--k-text)] active:bg-[var(--k-surface-2)]'
              }`}
            >
              {moreOpen ? <X size={20} /> : <Menu size={20} />}
            </button>
          ) : null}
        </div>

        {/* More panel — slides down from the header */}
        {moreOpen ? (
          <div className="m-panel-enter absolute left-0 right-0 top-full max-h-[72vh] overflow-y-auto border-b border-[var(--k-border)] bg-[var(--k-surface)] pb-[calc(1rem+env(safe-area-inset-bottom))] shadow-[var(--m-shadow-md)]">
            <div className="space-y-4 px-4 pb-2 pt-3">
              {MORE_SECTIONS.map((sec) => (
                <div key={sec.label}>
                  <div className="px-1 text-[var(--m-text-xs)] font-semibold uppercase tracking-wide text-[var(--k-muted)]">
                    {sec.label}
                  </div>
                  <div className="mt-1.5 grid grid-cols-3 gap-2">
                    {sec.items.map((p) => (
                      <button
                        key={p.id}
                        type="button"
                        onClick={() => openPage(p.id)}
                        className="flex flex-col items-center gap-1.5 rounded-[var(--m-radius-md)] border border-[var(--k-border)] bg-[var(--k-bg)] px-2 py-3 active:bg-[var(--k-surface-2)]"
                      >
                        <p.icon size={18} className="text-[var(--k-accent)]" />
                        <span className="truncate text-[var(--m-text-xs)] font-medium">{p.label}</span>
                      </button>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          </div>
        ) : null}
      </header>

      {/* Content */}
      <main className="m-fade-in flex-1 overflow-y-auto p-[var(--m-content-pad)] pb-[calc(var(--m-content-pad)+env(safe-area-inset-bottom))]">
        {pageId ? (
          <React.Suspense
            fallback={
              <div className="space-y-3">
                <div className="m-shimmer h-16" />
                <div className="m-shimmer h-24" />
              </div>
            }
          >
            {currentDef ? <currentDef.Comp /> : null}
          </React.Suspense>
        ) : (
          <React.Suspense
            fallback={
              <div className="space-y-3">
                <div className="m-shimmer h-16" />
                <div className="m-shimmer h-24" />
              </div>
            }
          >
            {activeTab ? <activeTab.Comp /> : null}
          </React.Suspense>
        )}
      </main>

      {/* Bottom tabs — the three things that matter */}
      <nav
        className="flex shrink-0 border-t border-[var(--k-border)] bg-[var(--k-surface)] pb-[env(safe-area-inset-bottom)]"
        style={{ minHeight: 'var(--m-tabbar-h)' }}
      >
        {TABS.map((t) => (
          <button
            key={t.id}
            type="button"
            onClick={() => {
              setTab(t.id);
              setPageId(null);
            }}
            className={`flex flex-1 flex-col items-center justify-center gap-0.5 ${
              tab === t.id ? 'text-[var(--k-accent)]' : 'text-[var(--k-muted)]'
            }`}
          >
            <t.icon size={20} />
            <span className="text-[11px] font-medium">{t.label}</span>
          </button>
        ))}
      </nav>
    </div>
  );
}
