'use client';

import * as React from 'react';
import { PanelRightClose, PanelRightOpen, Search } from 'lucide-react';

import { AgentPanel } from '@/components/agent/AgentPanel';
import { SidebarNav } from '@/components/layout/SidebarNav';
import { DashboardPage } from '@/components/pages/DashboardPage';
import { Button } from '@/components/ui/button';
import { useChatStore } from '@/lib/chat/store';

export function AppShell() {
  const { state, setAgent } = useChatStore();
  const agentVisible = state.agent.visible;
  const agentMode = state.agent.mode;
  const agentWidth = state.agent.width;

  const [activePage, setActivePage] = React.useState('dashboard');
  const draggingRef = React.useRef(false);

  React.useEffect(() => {
    function onMove(e: MouseEvent) {
      if (!draggingRef.current) return;
      const vw = window.innerWidth;
      const next = Math.min(720, Math.max(320, vw - e.clientX));
      setAgent((prev) => ({ ...prev, width: next, visible: true, mode: 'docked' }));
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

  return (
    <div className="flex h-screen w-screen bg-zinc-50 text-zinc-950 dark:bg-black dark:text-zinc-50">
      <SidebarNav activeId={activePage} onSelect={setActivePage} />

      <main className="flex flex-1 flex-col">
        <header className="flex items-center justify-between border-b border-zinc-200 bg-white px-4 py-3 dark:border-zinc-800 dark:bg-zinc-950">
          <div className="flex items-center gap-3">
            <div className="text-sm font-semibold">
              {activePage === 'dashboard' ? 'Dashboard' : activePage}
            </div>
            <div className="relative hidden md:block">
              <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-zinc-500" />
              <input
                className="h-9 w-[360px] rounded-full border border-zinc-200 bg-white pl-9 pr-3 text-sm outline-none focus:ring-2 focus:ring-zinc-950/10 dark:border-zinc-800 dark:bg-zinc-950 dark:focus:ring-zinc-50/10"
                placeholder="Search stocks / indices..."
              />
            </div>
          </div>

          <div className="flex items-center gap-2">
            <Button
              variant="secondary"
              size="sm"
              className="h-9 w-9 rounded-full p-0"
              onClick={() => setAgent((prev) => ({ ...prev, visible: !prev.visible, mode: 'docked' }))}
              title={agentVisible ? 'Hide agent' : 'Show agent'}
            >
              {agentVisible ? (
                <PanelRightClose className="h-4 w-4" />
              ) : (
                <PanelRightOpen className="h-4 w-4" />
              )}
            </Button>
            <div className="h-9 w-9 rounded-full bg-indigo-600 text-white grid place-items-center text-sm font-semibold">
              U
            </div>
          </div>
        </header>

        <div className="flex min-h-0 flex-1">
          <div className="min-w-0 flex-1 overflow-auto">
            {activePage === 'dashboard' ? <DashboardPage /> : <DashboardPage />}
          </div>

          {agentVisible && agentMode !== 'maximized' ? (
            <div
              className="w-1 cursor-col-resize bg-transparent hover:bg-zinc-200 dark:hover:bg-zinc-800"
              onMouseDown={() => {
                draggingRef.current = true;
              }}
              role="separator"
              aria-orientation="vertical"
              aria-label="Resize agent panel"
            />
          ) : null}

          {agentVisible ? (
            <div
              className={agentMode === 'maximized' ? 'w-full' : 'shrink-0'}
              style={agentMode === 'maximized' ? undefined : { width: agentWidth }}
            >
              <div className="h-full border-l border-zinc-200 dark:border-zinc-800">
                <AgentPanel />
              </div>
            </div>
          ) : null}
        </div>
      </main>
    </div>
  );
}


