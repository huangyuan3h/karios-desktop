'use client';

import * as React from 'react';

import { ChatComposer } from '@/components/chat/ChatComposer';
import { ChatMessageList, type ChatMessage } from '@/components/chat/ChatMessageList';

function nowId() {
  return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

export function ChatPanel() {
  const [messages, setMessages] = React.useState<ChatMessage[]>([
    {
      id: nowId(),
      role: 'assistant',
      content:
        'Welcome to Karios Desktop. Start by pasting context (links/text) or asking a question about your portfolio.',
    },
  ]);

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
        onSend={(text) => {
          setMessages((prev) => [
            ...prev,
            { id: nowId(), role: 'user', content: text },
            {
              id: nowId(),
              role: 'assistant',
              content: 'AI service not connected yet. This is a UI placeholder.',
            },
          ]);
        }}
      />
    </div>
  );
}


