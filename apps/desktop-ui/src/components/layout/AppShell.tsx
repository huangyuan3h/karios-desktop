'use client';

import { PanelRightOpen } from 'lucide-react';

import { AgentPanel } from '@/components/agent/AgentPanel';
import { ChatPanel } from '@/components/chat/ChatPanel';
import { ChatSessionList } from '@/components/chat/ChatSessionList';
import { Button } from '@/components/ui/button';
import { useChatStore } from '@/lib/chat/store';

export function AppShell() {
  const { state, setAgent } = useChatStore();
  const agentVisible = state.agent.visible;
  const agentMode = state.agent.mode;

  return (
    <div className="flex h-screen w-screen bg-zinc-50 text-zinc-950 dark:bg-black dark:text-zinc-50">
      <aside className="flex w-[260px] flex-col border-r border-zinc-200 bg-white dark:border-zinc-800 dark:bg-zinc-950">
        <div className="flex items-center gap-2 px-4 py-3">
          <div className="h-6 w-6 rounded-md bg-zinc-950/10 dark:bg-zinc-50/10" />
          <div className="text-sm font-semibold">Karios</div>
        </div>
        <div className="flex-1">
          <ChatSessionList />
        </div>
        <div className="border-t border-zinc-200 px-4 py-3 text-xs text-zinc-500 dark:border-zinc-800 dark:text-zinc-400">
          Local-first • SQLite-only (v0)
        </div>
      </aside>

      <main className="flex flex-1 flex-col">
        <div className="flex items-center justify-between border-b border-zinc-200 bg-white px-3 py-2 dark:border-zinc-800 dark:bg-zinc-950">
          <div className="text-sm font-medium">Workspace</div>
          {!agentVisible ? (
            <Button
              variant="secondary"
              size="sm"
              onClick={() => setAgent((prev) => ({ ...prev, visible: true }))}
              title="Show agent panel"
            >
              <PanelRightOpen className="mr-2 h-4 w-4" />
              Agent
            </Button>
          ) : null}
        </div>

        <div className="flex min-h-0 flex-1">
          {agentMode !== 'maximized' ? (
            <div className="min-w-0 flex-1">
              <ChatPanel />
            </div>
          ) : null}

          <div
            className={
              agentVisible
                ? agentMode === 'maximized'
                  ? 'w-full'
                  : 'w-[360px]'
                : 'w-0'
            }
          >
            <AgentPanel />
          </div>
        </div>
      </main>
    </div>
  );
}


