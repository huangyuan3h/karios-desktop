'use client';

import * as React from 'react';
import { Trash2 } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { useChatStore } from '@/lib/chat/store';
import { cn } from '@/lib/utils';

export function ChatSessionList({ onSelected }: { onSelected?: () => void }) {
  const { state, activeSession, setActiveSession, deleteSession } = useChatStore();

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="flex items-center justify-between px-3 py-2">
        <div className="text-xs font-semibold uppercase tracking-wide text-zinc-500 dark:text-zinc-400">
          Chats
        </div>
      </div>

      <div className="min-h-0 flex-1 overflow-auto px-2 pb-2">
        <div className="flex flex-col gap-1">
          {state.sessions.map((s) => {
            const isActive = activeSession?.id === s.id;
            return (
              <div key={s.id} className="group relative">
                <Button
                  onClick={() => {
                    setActiveSession(s.id);
                    onSelected?.();
                  }}
                  variant="ghost"
                  size="sm"
                  className={cn(
                    'h-auto w-full justify-start px-3 py-2 pr-10 text-left text-sm',
                    isActive
                      ? 'bg-[var(--k-surface-2)] text-[var(--k-text)]'
                      : 'text-[var(--k-muted)] hover:bg-[var(--k-surface-2)] hover:text-[var(--k-text)]',
                  )}
                >
                  <div className="truncate">{s.title}</div>
                  <div className="mt-0.5 truncate text-xs text-zinc-500 dark:text-zinc-400">
                    {new Date(s.updatedAt).toLocaleString()}
                  </div>
                </Button>

                <Button
                  variant="ghost"
                  size="icon"
                  className={cn(
                    'absolute right-1 top-1 h-8 w-8 text-[var(--k-muted)]',
                    'opacity-0 transition-opacity group-hover:opacity-100',
                    isActive ? 'opacity-100' : '',
                  )}
                  title="Delete chat"
                  aria-label="Delete chat"
                  onClick={(e) => {
                    e.preventDefault();
                    e.stopPropagation();
                    if (!confirm('Delete this thread?')) return;
                    deleteSession(s.id);
                  }}
                >
                  <Trash2 className="h-4 w-4" />
                </Button>
              </div>
            );
          })}

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


