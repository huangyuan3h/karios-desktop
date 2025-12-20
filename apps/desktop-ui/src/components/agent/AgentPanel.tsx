'use client';

import { Maximize2, Minimize2, PanelRightClose } from 'lucide-react';

import { ChatPanel } from '@/components/chat/ChatPanel';
import { ChatSessionList } from '@/components/chat/ChatSessionList';
import { Button } from '@/components/ui/button';
import { useChatStore } from '@/lib/chat/store';

export function AgentPanel() {
  const { state, setAgent } = useChatStore();
  const { visible, mode } = state.agent;

  if (!visible) {
    return null;
  }

  return (
    <div className="flex h-full flex-col bg-white dark:bg-zinc-950">
      <div className="flex items-center justify-between border-b border-zinc-200 px-3 py-2 dark:border-zinc-800">
        <div className="text-sm font-medium text-zinc-950 dark:text-zinc-50">Kairos AI</div>
        <div className="flex gap-2">
          <Button
            variant="ghost"
            size="sm"
            onClick={() =>
              setAgent((prev) => ({ ...prev, mode: prev.mode === 'docked' ? 'maximized' : 'docked' }))
            }
            title={mode === 'docked' ? 'Maximize' : 'Restore'}
          >
            {mode === 'docked' ? (
              <Maximize2 className="h-4 w-4" />
            ) : (
              <Minimize2 className="h-4 w-4" />
            )}
          </Button>
          <Button
            variant="ghost"
            size="sm"
            onClick={() => setAgent((prev) => ({ ...prev, visible: false, mode: 'docked' }))}
            title="Hide"
          >
            <PanelRightClose className="h-4 w-4" />
          </Button>
        </div>
      </div>

      <div className="min-h-0 flex-1">
        <div className="h-full overflow-hidden">
          <div className="h-full">
            <div className="h-full">
              <div className="h-full">
                <div className="grid h-full grid-rows-[auto_1fr]">
                  <div className="border-b border-zinc-200 dark:border-zinc-800">
                    <ChatSessionList />
                  </div>
                  <ChatPanel />
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}


