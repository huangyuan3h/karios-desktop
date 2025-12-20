'use client';

import * as React from 'react';

import { ChatComposer } from '@/components/chat/ChatComposer';
import { ChatMessageList } from '@/components/chat/ChatMessageList';
import { newId } from '@/lib/id';
import { useChatStore } from '@/lib/chat/store';
import type { ChatAttachment, ChatMessage } from '@/lib/chat/types';

export function ChatPanel() {
  const { activeSession, createSession, appendMessages } = useChatStore();

  React.useEffect(() => {
    if (!activeSession) {
      createSession();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const messages: ChatMessage[] = activeSession?.messages ?? [];

  return (
    <div className="flex h-full flex-col">
      <div className="flex items-center justify-between border-b border-zinc-200 px-4 py-3 dark:border-zinc-800">
        <div className="text-sm font-medium text-zinc-950 dark:text-zinc-50">Chat</div>
        <div className="text-xs text-zinc-500 dark:text-zinc-400">⌘/Ctrl + Enter to send</div>
      </div>

      <div className="flex-1 overflow-auto">
        <ChatMessageList messages={messages} />
      </div>

      <ChatComposer
        onSend={(text: string, attachments: ChatAttachment[]) => {
          if (!activeSession) return;
          appendMessages(activeSession.id, [
            { id: newId(), role: 'user', content: text, createdAt: new Date().toISOString(), attachments },
            {
              id: newId(),
              role: 'assistant',
              content: 'AI service not connected yet. This is a UI placeholder.\n\n- Markdown supported\n- Images stored locally (v0)\n',
              createdAt: new Date().toISOString(),
            },
          ]);
        }}
      />
    </div>
  );
}


