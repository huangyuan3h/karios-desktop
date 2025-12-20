'use client';

import { History, Maximize2, Minimize2, PanelRightClose, Plus } from 'lucide-react';

import { ChatPanel } from '@/components/chat/ChatPanel';
import { ChatSessionList } from '@/components/chat/ChatSessionList';
import { SystemPromptEditor } from '@/components/agent/SystemPromptEditor';
import { Button } from '@/components/ui/button';
import { useChatStore } from '@/lib/chat/store';

export function AgentPanel() {
  const { state, setAgent, createEmptySession, activeSession } = useChatStore();
  const { visible, mode, historyOpen } = state.agent;

  if (!visible) {
    return null;
  }

  const headerTitle = activeSession?.title?.trim() ? activeSession.title.trim() : 'Kairos AI';

  return (
    <div className="flex h-full flex-col bg-[var(--k-surface)]">
      <div className="flex items-center justify-between border-b border-[var(--k-border)] px-3 py-2">
        <div className="min-w-0 pr-2 text-sm font-medium" title={headerTitle}>
          <div className="truncate">{headerTitle}</div>
        </div>
        <div className="flex gap-2">
          <Button
            variant="ghost"
            size="sm"
            onClick={() => setAgent((prev) => ({ ...prev, historyOpen: !prev.historyOpen }))}
            title="History"
          >
            <History className="h-4 w-4" />
          </Button>
          <Button
            variant="ghost"
            size="sm"
            onClick={() => {
              createEmptySession();
              setAgent((prev) => ({ ...prev, historyOpen: false }));
            }}
            title="New thread"
          >
            <Plus className="h-4 w-4" />
          </Button>
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
                  <SystemPromptEditor />
                  {historyOpen ? (
                    <div className="min-h-0 flex-1">
                      <ChatSessionList
                        onSelected={() => setAgent((prev) => ({ ...prev, historyOpen: false }))}
                      />
                    </div>
                  ) : (
                    <ChatPanel />
                  )}
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}


