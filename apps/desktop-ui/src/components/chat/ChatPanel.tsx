'use client';

import * as React from 'react';

import { ChatComposer } from '@/components/chat/ChatComposer';
import { ChatMessageList } from '@/components/chat/ChatMessageList';
import { AI_BASE_URL } from '@/lib/endpoints';
import { newId } from '@/lib/id';
import { useChatStore } from '@/lib/chat/store';
import type { ChatAttachment, ChatMessage } from '@/lib/chat/types';

type ModelMessage = { role: 'system' | 'user' | 'assistant'; content: string };

function toModelMessages(messages: ChatMessage[], systemPrompt: string): ModelMessage[] {
  const out: ModelMessage[] = [];
  const sp = systemPrompt.trim();
  if (sp) out.push({ role: 'system', content: sp });
  for (const m of messages) {
    if (m.role === 'system') continue;
    out.push({ role: m.role, content: m.content });
  }
  return out;
}

export function ChatPanel() {
  const { activeSession, createSession, appendMessages, updateMessageContent, state, renameSession } =
    useChatStore();
  const scrollerRef = React.useRef<HTMLDivElement | null>(null);
  const stickToBottomRef = React.useRef(true);

  React.useEffect(() => {
    if (!activeSession) {
      createSession();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const messages: ChatMessage[] = activeSession?.messages ?? [];
  const lastMessageId = messages[messages.length - 1]?.id ?? '';
  const lastMessageLen = messages[messages.length - 1]?.content?.length ?? 0;

  React.useEffect(() => {
    const el = scrollerRef.current;
    if (!el) return;
    if (!stickToBottomRef.current) return;
    el.scrollTop = el.scrollHeight;
  }, [messages.length, lastMessageId, lastMessageLen]);

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div
        ref={scrollerRef}
        className="min-h-0 flex-1 overflow-auto"
        onScroll={() => {
          const el = scrollerRef.current;
          if (!el) return;
          const distanceToBottom = el.scrollHeight - el.scrollTop - el.clientHeight;
          stickToBottomRef.current = distanceToBottom < 80;
        }}
      >
        <ChatMessageList messages={messages} />
      </div>

      <ChatComposer
        onSend={(text: string, attachments: ChatAttachment[]) => {
          if (!activeSession) return;
          const shouldInferTitle =
            activeSession.title === 'New chat' && messages.every((m) => m.role !== 'user');
          const now = new Date().toISOString();
          const userMessage: ChatMessage = {
            id: newId(),
            role: 'user',
            content: text,
            createdAt: now,
            attachments,
          };
          const assistantId = newId();
          const assistantMessage: ChatMessage = {
            id: assistantId,
            role: 'assistant',
            content: '',
            createdAt: now,
          };

          appendMessages(activeSession.id, [userMessage, assistantMessage]);

          // v0: send text-only messages; attachments are stored locally for now.
          const payload = {
            messages: toModelMessages([...messages, userMessage], state.settings.systemPrompt),
          };

          (async () => {
            try {
              if (shouldInferTitle) {
                try {
                  const t = await fetch(`${AI_BASE_URL}/title`, {
                    method: 'POST',
                    headers: { 'content-type': 'application/json' },
                    body: JSON.stringify({
                      text,
                      systemPrompt: state.settings.systemPrompt,
                    }),
                  });
                  if (t.ok) {
                    const data = (await t.json()) as { title?: string };
                    const title = typeof data.title === 'string' ? data.title.trim() : '';
                    if (title) {
                      renameSession(activeSession.id, title);
                    }
                  }
                } catch {
                  // ignore
                }
              }

              const resp = await fetch(`${AI_BASE_URL}/chat`, {
                method: 'POST',
                headers: { 'content-type': 'application/json' },
                body: JSON.stringify(payload),
              });

              if (!resp.ok || !resp.body) {
                const msg = await resp.text().catch(() => '');
                updateMessageContent(
                  activeSession.id,
                  assistantId,
                  `**Error**: AI service failed (${resp.status}).\n\n${msg}`,
                );
                return;
              }

              const reader = resp.body.getReader();
              const decoder = new TextDecoder();
              let acc = '';
              while (true) {
                const { value, done } = await reader.read();
                if (done) break;
                acc += decoder.decode(value, { stream: true });
                updateMessageContent(activeSession.id, assistantId, acc);
              }
            } catch (err) {
              const message = err instanceof Error ? err.message : String(err);
              updateMessageContent(activeSession.id, assistantId, `**Error**: ${message}`);
            }
          })();
        }}
      />
    </div>
  );
}


