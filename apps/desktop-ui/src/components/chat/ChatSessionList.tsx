'use client';

import * as React from 'react';
import { Plus } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { useChatStore } from '@/lib/chat/store';
import { cn } from '@/lib/utils';

export function ChatSessionList({ onSelected }: { onSelected?: () => void }) {
  const { state, activeSession, createEmptySession, setActiveSession } = useChatStore();

  return (
    <div className="flex h-full flex-col">
      <div className="flex items-center justify-between px-3 py-2">
        <div className="text-xs font-semibold uppercase tracking-wide text-zinc-500 dark:text-zinc-400">
          Chats
        </div>
        <Button
          variant="secondary"
          size="sm"
          onClick={createEmptySession}
          className="h-7 px-2"
          title="New chat"
        >
          <Plus className="h-4 w-4" />
        </Button>
      </div>

      <div className="flex-1 overflow-auto px-2 pb-2">
        <div className="flex flex-col gap-1">
          {state.sessions.map((s) => (
            <Button
              key={s.id}
              onClick={() => {
                setActiveSession(s.id);
                onSelected?.();
              }}
              variant="ghost"
              size="sm"
              className={cn(
                'h-auto w-full justify-start px-3 py-2 text-left text-sm',
                activeSession?.id === s.id
                  ? 'bg-[var(--k-surface-2)] text-[var(--k-text)]'
                  : 'text-[var(--k-muted)] hover:bg-[var(--k-surface-2)] hover:text-[var(--k-text)]',
              )}
            >
              <div className="truncate">{s.title}</div>
              <div className="mt-0.5 truncate text-xs text-zinc-500 dark:text-zinc-400">
                {new Date(s.updatedAt).toLocaleString()}
              </div>
            </Button>
          ))}

          {state.sessions.length === 0 ? (
            <div className="px-3 py-2 text-sm text-zinc-500 dark:text-zinc-400">
              No chats yet.
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}


