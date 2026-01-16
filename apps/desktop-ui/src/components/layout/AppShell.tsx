'use client';

import * as React from 'react';
import { Bot } from 'lucide-react';

import { AgentPanel } from '@/components/agent/AgentPanel';
import { SidebarNav } from '@/components/layout/SidebarNav';
import { DashboardPage } from '@/components/pages/DashboardPage';
import { BrokerPage } from '@/components/pages/BrokerPage';
import { IndustryFlowPage } from '@/components/pages/IndustryFlowPage';
import { LeaderStocksPage } from '@/components/pages/LeaderStocksPage';
import { MarketPage } from '@/components/pages/MarketPage';
import { RankPage } from '@/components/pages/RankPage';
import { ScreenerPage } from '@/components/pages/ScreenerPage';
import { SettingsPage } from '@/components/pages/SettingsPage';
import { StrategyPage } from '@/components/pages/StrategyPage';
import { StockPage } from '@/components/pages/StockPage';
import { WatchlistPage } from '@/components/pages/WatchlistPage';
import { GlobalStockSearch } from '@/components/search/GlobalStockSearch';
import { ThemeToggle } from '@/components/theme/ThemeToggle';
import { Button } from '@/components/ui/button';
import { useChatStore } from '@/lib/chat/store';
import { cn } from '@/lib/utils';

export function AppShell() {
  const { state, setAgent } = useChatStore();
  const agentVisible = state.agent.visible;
  const agentMode = state.agent.mode;
  const agentWidth = state.agent.width;
  const agentMaximized = agentVisible && agentMode === 'maximized';

  const [activePage, setActivePage] = React.useState('dashboard');
  const [sidebarCollapsed, setSidebarCollapsed] = React.useState(false);
  const [activeStockSymbol, setActiveStockSymbol] = React.useState<string | null>(null);
  const draggingRef = React.useRef(false);
  const agentVisibleRef = React.useRef(agentVisible);
  const agentModeRef = React.useRef(agentMode);
  const [overlayMounted, setOverlayMounted] = React.useState(false);
  const [overlayEntered, setOverlayEntered] = React.useState(false);

  React.useEffect(() => {
    agentVisibleRef.current = agentVisible;
    agentModeRef.current = agentMode;
    if (!agentVisible) {
      // If the agent panel is hidden while dragging, stop resizing immediately.
      draggingRef.current = false;
    }
  }, [agentVisible, agentMode]);

  React.useEffect(() => {
    function onMove(e: MouseEvent) {
      if (!draggingRef.current) return;
      // Only allow resizing when the agent panel is visible and docked.
      if (!agentVisibleRef.current) return;
      if (agentModeRef.current !== 'docked') return;
      const vw = window.innerWidth;
      const next = Math.min(720, Math.max(320, vw - e.clientX));
      setAgent((prev) => ({ ...prev, width: next }));
    }
    function onUp() {
      draggingRef.current = false;
    }
    window.addEventListener('mousemove', onMove);
    window.addEventListener('mouseup', onUp);
    return () => {
      window.removeEventListener('mousemove', onMove);
      window.removeEventListener('mouseup', onUp);
    };
  }, [setAgent]);

  React.useEffect(() => {
    let raf = 0;
    let t: number | null = null;
    if (agentMaximized) {
      setOverlayMounted(true);
      raf = window.requestAnimationFrame(() => setOverlayEntered(true));
    } else {
      setOverlayEntered(false);
      if (overlayMounted) {
        t = window.setTimeout(() => setOverlayMounted(false), 180);
      }
    }
    return () => {
      if (raf) window.cancelAnimationFrame(raf);
      if (t) window.clearTimeout(t);
    };
  }, [agentMaximized, overlayMounted]);

  return (
    <div className="flex h-screen w-screen bg-[var(--k-bg)] text-[var(--k-text)]">
      <SidebarNav
        activeId={activePage}
        onSelect={setActivePage}
        collapsed={sidebarCollapsed}
        onToggleCollapsed={() => setSidebarCollapsed((v) => !v)}
      />

      <main className="flex flex-1 flex-col">
        <header className="flex items-center border-b border-[var(--k-border)] bg-[var(--k-surface)] px-4 py-3">
          <div className="text-sm font-semibold">
            {activePage === 'dashboard'
              ? 'Dashboard'
              : activePage === 'market'
                ? 'Market'
              : activePage === 'rank'
                ? 'Quant'
              : activePage === 'industryFlow'
                ? 'Industry Flow'
              : activePage === 'watchlist'
                ? 'Watchlist'
              : activePage === 'broker'
                ? 'Broker'
              : activePage === 'strategy'
                ? 'Strategy'
              : activePage === 'leaders'
                ? 'Leaders'
              : activePage === 'stock'
                ? activeStockSymbol ?? 'Stock'
              : activePage === 'screener'
                ? 'Screener'
              : activePage === 'settings'
                ? 'Settings'
                : activePage}
          </div>

          <div className="flex-1" />

          <div className="flex items-center gap-2">
            <GlobalStockSearch
              onSelectSymbol={(symbol) => {
                setActiveStockSymbol(symbol);
                setActivePage('stock');
              }}
            />
            <ThemeToggle />
            <Button
              variant="secondary"
              size="sm"
              className="h-9 w-9 rounded-full p-0"
              onClick={() => setAgent((prev) => ({ ...prev, visible: !prev.visible, mode: 'docked' }))}
              title={agentVisible ? 'Hide agent' : 'Show agent'}
            >
              <Bot className="h-4 w-4" />
            </Button>
            <div className="grid h-9 w-9 place-items-center rounded-full bg-[var(--k-accent)] text-sm font-semibold text-white">
              U
            </div>
          </div>
        </header>

        <div className="flex min-h-0 flex-1">
          <div className="min-w-0 flex-1 overflow-auto">
            {activePage === 'settings' ? (
              <SettingsPage />
            ) : activePage === 'market' ? (
              <MarketPage
                onOpenStock={(symbol) => {
                  setActiveStockSymbol(symbol);
                  setActivePage('stock');
                }}
              />
            ) : activePage === 'broker' ? (
              <BrokerPage />
            ) : activePage === 'rank' ? (
              <RankPage
                onOpenStock={(symbol) => {
                  setActiveStockSymbol(symbol);
                  setActivePage('stock');
                }}
              />
            ) : activePage === 'industryFlow' ? (
              <IndustryFlowPage />
            ) : activePage === 'watchlist' ? (
              <WatchlistPage
                onOpenStock={(symbol) => {
                  setActiveStockSymbol(symbol);
                  setActivePage('stock');
                }}
              />
            ) : activePage === 'strategy' ? (
              <StrategyPage />
            ) : activePage === 'leaders' ? (
              <LeaderStocksPage
                onOpenStock={(symbol) => {
                  setActiveStockSymbol(symbol);
                  setActivePage('stock');
                }}
              />
            ) : activePage === 'stock' && activeStockSymbol ? (
              <StockPage
                symbol={activeStockSymbol}
                onBack={() => setActivePage('market')}
              />
            ) : activePage === 'screener' ? (
              <ScreenerPage />
            ) : (
              <DashboardPage
                onNavigate={(id) => setActivePage(id)}
                onOpenStock={(symbol) => {
                  setActiveStockSymbol(symbol);
                  setActivePage('stock');
                }}
              />
            )}
          </div>

          {agentVisible && agentMode !== 'maximized' ? (
            <div
              className="w-1 cursor-col-resize bg-transparent hover:bg-[var(--k-border)]"
              onMouseDown={() => {
                if (!agentVisible) return;
                if (agentMode !== 'docked') return;
                draggingRef.current = true;
              }}
              role="separator"
              aria-orientation="vertical"
              aria-label="Resize agent panel"
            />
          ) : null}

          {agentVisible && agentMode !== 'maximized' ? (
            <div
              className="shrink-0"
              style={{ width: agentWidth }}
            >
              <div className="h-full border-l border-[var(--k-border)]">
                <AgentPanel />
              </div>
            </div>
          ) : null}
        </div>
      </main>

      {overlayMounted ? (
        <div
          className={cn(
            'fixed inset-0 z-50',
            'bg-black/20 backdrop-blur-[2px]',
            'transition-opacity duration-200',
            overlayEntered ? 'opacity-100' : 'opacity-0',
          )}
          aria-hidden={!overlayEntered}
        >
          <div
            className={cn(
              'absolute inset-0',
              'transition-transform duration-200 will-change-transform',
              overlayEntered ? 'translate-y-0 scale-100' : 'translate-y-2 scale-[0.985]',
            )}
          >
            <div className="h-full w-full bg-[var(--k-bg)]">
              <AgentPanel />
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}


