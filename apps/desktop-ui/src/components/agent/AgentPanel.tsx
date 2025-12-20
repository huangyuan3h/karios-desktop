'use client';

import { Maximize2, Minimize2, PanelRightClose, PanelRightOpen } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { useChatStore } from '@/lib/chat/store';

export function AgentPanel() {
  const { state, setAgent } = useChatStore();
  const { visible, mode } = state.agent;

  if (!visible) {
    return (
      <div className="flex h-full items-start justify-end p-2">
        <Button
          variant="secondary"
          size="sm"
          onClick={() => setAgent((prev) => ({ ...prev, visible: true }))}
          title="Show agent panel"
        >
          <PanelRightOpen className="mr-2 h-4 w-4" />
          Agent
        </Button>
      </div>
    );
  }

  return (
    <div className="flex h-full flex-col border-l border-zinc-200 bg-white dark:border-zinc-800 dark:bg-zinc-950">
      <div className="flex items-center justify-between border-b border-zinc-200 px-3 py-2 dark:border-zinc-800">
        <div className="text-sm font-medium text-zinc-950 dark:text-zinc-50">Agent</div>
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

      <div className="flex-1 overflow-auto p-3">
        <div className="rounded-md border border-zinc-200 bg-zinc-50 p-3 text-sm text-zinc-700 dark:border-zinc-800 dark:bg-zinc-900 dark:text-zinc-200">
          <div className="font-medium">Coming next</div>
          <ul className="mt-2 list-disc pl-5 text-sm">
            <li>Context Collector artifacts (links/text/images)</li>
            <li>Tools / MCP actions</li>
            <li>References panel (pin evidence)</li>
          </ul>
        </div>
      </div>
    </div>
  );
}


