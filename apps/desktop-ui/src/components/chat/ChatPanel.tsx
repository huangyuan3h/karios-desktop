'use client';

import * as React from 'react';

import { ChatComposer } from '@/components/chat/ChatComposer';
import { ChatMessageList } from '@/components/chat/ChatMessageList';
import { AI_BASE_URL, QUANT_BASE_URL } from '@/lib/endpoints';
import { newId } from '@/lib/id';
import { useChatStore } from '@/lib/chat/store';
import type { ChatAttachment, ChatMessage, ChatReference } from '@/lib/chat/types';

type TvSnapshotDetail = {
  id: string;
  screenerId: string;
  capturedAt: string;
  rowCount: number;
  screenTitle: string | null;
  url: string;
  headers: string[];
  rows: Record<string, string>[];
};

type StockBarsDetail = {
  symbol: string;
  market: string;
  ticker: string;
  name: string;
  currency: string;
  bars: Array<{
    date: string;
    open: string;
    high: string;
    low: string;
    close: string;
    volume: string;
    amount: string;
  }>;
};

function pickColumns(headers: string[]) {
  const preferred = [
    'Ticker',
    'Name',
    'Symbol',
    'Price',
    'Change %',
    'Rel Volume',
    'Rel Volume 1W',
    'Market cap',
    'Sector',
    'Analyst Rating',
    'RSI (14)',
  ];
  const set = new Set(headers);
  const picked = preferred.filter((h) => set.has(h));
  const rest = headers.filter((h) => !picked.includes(h));
  return [...picked, ...rest].slice(0, 8);
}

async function buildReferenceBlock(refs: ChatReference[]): Promise<string> {
  let out = '# Reference Context: TradingView Screener Snapshots\n\n';
  for (const ref of refs) {
    if (ref.kind === 'tv') {
      try {
        const resp = await fetch(
          `${QUANT_BASE_URL}/integrations/tradingview/snapshots/${encodeURIComponent(ref.snapshotId)}`,
          { cache: 'no-store' },
        );
        if (!resp.ok) throw new Error('failed to load snapshot');
        const snap = (await resp.json()) as TvSnapshotDetail;
        out += `## TradingView: ${ref.screenerName}\n`;
        out += `- snapshotId: ${ref.snapshotId}\n`;
        out += `- capturedAt: ${ref.capturedAt}\n`;
        out += `- url: ${snap.url}\n`;
        if (snap.screenTitle) out += `- screenTitle: ${snap.screenTitle}\n`;
        const cols = pickColumns(snap.headers);
        out += `- columns: ${cols.join(', ')}\n`;
        const rows = snap.rows.slice(0, 20);
        out += `\nRows (first ${rows.length}):\n`;
        for (const r of rows) {
          const line = cols.map((c) => `${c}=${(r[c] ?? '').replaceAll('\n', ' ')}`).join(' ; ');
          out += `- ${line}\n`;
        }
        out += `\n`;
      } catch (e) {
        out += `## TradingView: ${ref.screenerName}\n`;
        out += `- snapshotId: ${ref.snapshotId}\n`;
        out += `- capturedAt: ${ref.capturedAt}\n`;
        out += `- status: failed to load snapshot\n\n`;
      }
      continue;
    }

    // Stock reference
    try {
      const resp = await fetch(
        `${QUANT_BASE_URL}/market/stocks/${encodeURIComponent(ref.symbol)}/bars?days=${ref.days}`,
        { cache: 'no-store' },
      );
      if (!resp.ok) throw new Error('failed to load stock bars');
      const snap = (await resp.json()) as StockBarsDetail;
      out += `## Stock: ${snap.ticker} ${snap.name}\n`;
      out += `- symbol: ${snap.symbol}\n`;
      out += `- market: ${snap.market}\n`;
      out += `- currency: ${snap.currency}\n`;
      out += `- capturedAt: ${ref.capturedAt}\n`;
      const rows = snap.bars.slice(-50);
      out += `\nBars (last ${rows.length}):\n`;
      for (const b of rows) {
        out += `- ${b.date} O=${b.open} H=${b.high} L=${b.low} C=${b.close} V=${b.volume} A=${b.amount}\n`;
      }
      out += `\n`;
    } catch {
      out += `## Stock: ${ref.ticker} ${ref.name}\n`;
      out += `- symbol: ${ref.symbol}\n`;
      out += `- capturedAt: ${ref.capturedAt}\n`;
      out += `- status: failed to load bars\n\n`;
    }
  }
  return out;
}

export function ChatPanel() {
  const {
    activeSession,
    createSession,
    appendMessages,
    updateMessageContent,
    state,
    renameSession,
    removeReference,
    clearReferences,
  } = useChatStore();
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
        references={state.references}
        onRemoveReference={removeReference}
        onClearReferences={clearReferences}
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

          const sp = state.settings.systemPrompt.trim();
          const baseMessages = [...messages, userMessage]
            .filter((m) => m.role !== 'system')
            .map((m) => ({
              role: m.role,
              content: m.content,
              attachments: m.attachments,
            }));
          const refs = state.references;

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

              const referenceText =
                refs.length > 0 ? await buildReferenceBlock(refs) : '';
              const payload = {
                messages: [
                  ...(sp ? [{ role: 'system' as const, content: sp }] : []),
                  ...(referenceText
                    ? [{ role: 'system' as const, content: referenceText }]
                    : []),
                  ...baseMessages,
                ],
              };

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


