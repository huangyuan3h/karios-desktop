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
  filters: string[];
  url: string;
  headers: string[];
  rows: Record<string, string>[];
};

type BrokerSnapshotDetail = {
  id: string;
  broker: string;
  accountId: string | null;
  capturedAt: string;
  kind: string;
  createdAt: string;
  imagePath: string;
  extracted: Record<string, unknown>;
};

type BrokerAccountState = {
  accountId: string;
  broker: string;
  updatedAt: string;
  overview: Record<string, unknown>;
  positions: Array<Record<string, unknown>>;
  conditionalOrders: Array<Record<string, unknown>>;
  trades: Array<Record<string, unknown>>;
  counts: Record<string, number>;
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

type StockChipsDetail = {
  symbol: string;
  market: string;
  ticker: string;
  name: string;
  currency: string;
  items: Array<{
    date: string;
    profitRatio: string;
    avgCost: string;
    cost90Low: string;
    cost90High: string;
    cost90Conc: string;
    cost70Low: string;
    cost70High: string;
    cost70Conc: string;
  }>;
};

type StockFundFlowDetail = {
  symbol: string;
  market: string;
  ticker: string;
  name: string;
  currency: string;
  items: Array<{
    date: string;
    close: string;
    changePct: string;
    mainNetAmount: string;
    mainNetRatio: string;
    superNetAmount: string;
    superNetRatio: string;
    largeNetAmount: string;
    largeNetRatio: string;
    mediumNetAmount: string;
    mediumNetRatio: string;
    smallNetAmount: string;
    smallNetRatio: string;
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
  let out = '# Reference Context\n\n';
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
        if (Array.isArray(snap.filters) && snap.filters.length) {
          out += `- filters: ${snap.filters.join(' | ')}\n`;
        }
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

    if (ref.kind === 'broker') {
      try {
        const resp = await fetch(
          `${QUANT_BASE_URL}/broker/${encodeURIComponent(ref.broker)}/snapshots/${encodeURIComponent(ref.snapshotId)}`,
          { cache: 'no-store' },
        );
        if (!resp.ok) throw new Error('failed to load broker snapshot');
        const snap = (await resp.json()) as BrokerSnapshotDetail;

        out += `## Broker: ${ref.accountTitle}\n`;
        out += `- broker: ${snap.broker}\n`;
        if (snap.accountId) out += `- accountId: ${snap.accountId}\n`;
        out += `- kind: ${snap.kind}\n`;
        out += `- capturedAt: ${snap.capturedAt}\n`;
        out += `- snapshotId: ${ref.snapshotId}\n`;
        const extracted = snap.extracted || {};
        const kind = String((extracted as any).kind || snap.kind || 'unknown');
        const data = (extracted as any).data || {};

        if (kind === 'account_overview') {
          out += `\nAccount overview:\n`;
          for (const k of [
            'currency',
            'totalAssets',
            'securitiesValue',
            'cashAvailable',
            'withdrawable',
            'pnlTotal',
            'pnlToday',
            'accountIdMasked',
          ]) {
            if (data && typeof data === 'object' && (data as any)[k] != null) {
              out += `- ${k}: ${(data as any)[k]}\n`;
            }
          }
          out += `\n`;
        } else if (kind === 'positions' && Array.isArray((data as any).positions)) {
          const rows = ((data as any).positions as any[]).slice(0, 30);
          out += `\nPositions (first ${rows.length}):\n`;
          for (const p of rows) {
            const ticker = String(p.ticker ?? '');
            const name = String(p.name ?? '');
            const qty = String(p.qtyHeld ?? p.qty ?? '');
            const price = String(p.price ?? '');
            const cost = String(p.cost ?? '');
            const pnl = String(p.pnl ?? '');
            const pnlPct = String(p.pnlPct ?? '');
            out += `- ${ticker} ${name} qty=${qty} price=${price} cost=${cost} pnl=${pnl} pnlPct=${pnlPct}\n`;
          }
          out += `\n`;
        } else if (kind === 'conditional_orders' && Array.isArray((data as any).orders)) {
          const rows = ((data as any).orders as any[]).slice(0, 30);
          out += `\nConditional orders (first ${rows.length}):\n`;
          for (const o of rows) {
            out += `- ${String(o.ticker ?? '')} ${String(o.name ?? '')} side=${String(o.side ?? '')} `
              + `trigger=${String(o.triggerCondition ?? '')} ${String(o.triggerValue ?? '')} `
              + `qty=${String(o.qty ?? '')} status=${String(o.status ?? '')} validUntil=${String(o.validUntil ?? '')}\n`;
          }
          out += `\n`;
        } else {
          out += `\nExtracted JSON:\n`;
          out += `${JSON.stringify(extracted, null, 2)}\n\n`;
        }
      } catch {
        out += `## Broker: ${ref.accountTitle}\n`;
        out += `- snapshotId: ${ref.snapshotId}\n`;
        out += `- status: failed to load snapshot\n\n`;
      }
      continue;
    }

    if (ref.kind === 'brokerState') {
      try {
        const resp = await fetch(
          `${QUANT_BASE_URL}/broker/${encodeURIComponent(ref.broker)}/accounts/${encodeURIComponent(ref.accountId)}/state`,
          { cache: 'no-store' },
        );
        if (!resp.ok) throw new Error('failed to load broker account state');
        const st = (await resp.json()) as BrokerAccountState;
        out += `## Broker account state: ${ref.accountTitle}\n`;
        out += `- broker: ${st.broker}\n`;
        out += `- accountId: ${st.accountId}\n`;
        out += `- updatedAt: ${st.updatedAt}\n`;
        const counts = st.counts || {};
        out += `- counts: positions=${counts.positions ?? st.positions?.length ?? 0}, conditionalOrders=${counts.conditionalOrders ?? st.conditionalOrders?.length ?? 0}, trades=${counts.trades ?? st.trades?.length ?? 0}\n`;

        const ov = st.overview || {};
        if (Object.keys(ov).length) {
          out += `\nAccount overview:\n`;
          for (const k of Object.keys(ov)) {
            out += `- ${k}: ${String((ov as any)[k])}\n`;
          }
        }

        const ps = (st.positions || []).slice(0, 40);
        if (ps.length) {
          out += `\nPositions (first ${ps.length}):\n`;
          for (const p of ps) {
            out += `- ${String((p as any).ticker ?? '')} ${String((p as any).name ?? '')} qty=${String((p as any).qtyHeld ?? (p as any).qty ?? '')} price=${String((p as any).price ?? '')} cost=${String((p as any).cost ?? '')} pnl=${String((p as any).pnl ?? '')} pnlPct=${String((p as any).pnlPct ?? '')}\n`;
          }
        }

        const os = (st.conditionalOrders || []).slice(0, 40);
        if (os.length) {
          out += `\nConditional orders (first ${os.length}):\n`;
          for (const o of os) {
            out += `- ${String((o as any).ticker ?? '')} ${String((o as any).name ?? '')} side=${String((o as any).side ?? '')} trigger=${String((o as any).triggerCondition ?? '')} ${String((o as any).triggerValue ?? '')} qty=${String((o as any).qty ?? '')} status=${String((o as any).status ?? '')} validUntil=${String((o as any).validUntil ?? '')}\n`;
          }
        }

        const ts = (st.trades || []).slice(0, 60);
        if (ts.length) {
          out += `\nTrades (first ${ts.length}):\n`;
          for (const t of ts) {
            out += `- ${String((t as any).time ?? (t as any).date ?? '')} ${String((t as any).side ?? '')} ${String((t as any).ticker ?? '')} ${String((t as any).name ?? '')} qty=${String((t as any).qty ?? '')} price=${String((t as any).price ?? '')}\n`;
          }
        }

        out += `\n`;
      } catch {
        out += `## Broker account state: ${ref.accountTitle}\n`;
        out += `- status: failed to load state\n\n`;
      }
      continue;
    }

    // Stock reference
    try {
      const [barsResp, chipsResp, ffResp] = await Promise.all([
        fetch(
          `${QUANT_BASE_URL}/market/stocks/${encodeURIComponent(ref.symbol)}/bars?days=${ref.barsDays}`,
          { cache: 'no-store' },
        ),
        fetch(
          `${QUANT_BASE_URL}/market/stocks/${encodeURIComponent(ref.symbol)}/chips?days=${ref.chipsDays}`,
          { cache: 'no-store' },
        ).catch(() => null as any),
        fetch(
          `${QUANT_BASE_URL}/market/stocks/${encodeURIComponent(ref.symbol)}/fund-flow?days=${ref.fundFlowDays}`,
          { cache: 'no-store' },
        ).catch(() => null as any),
      ]);

      if (!barsResp.ok) throw new Error('failed to load stock bars');
      const snap = (await barsResp.json()) as StockBarsDetail;
      out += `## Stock: ${snap.ticker} ${snap.name}\n`;
      out += `- symbol: ${snap.symbol}\n`;
      out += `- market: ${snap.market}\n`;
      out += `- currency: ${snap.currency}\n`;
      out += `- capturedAt: ${ref.capturedAt}\n`;

      const bars = snap.bars.slice(-ref.barsDays);
      out += `\nBars (last ${bars.length}):\n`;
      for (const b of bars) {
        out += `- ${b.date} O=${b.open} H=${b.high} L=${b.low} C=${b.close} V=${b.volume} A=${b.amount}\n`;
      }

      if (chipsResp && chipsResp.ok) {
        const chips = (await chipsResp.json()) as StockChipsDetail;
        const items = (chips.items || []).slice(-ref.chipsDays);
        if (items.length) {
          out += `\nChips (last ${items.length}):\n`;
          for (const it of items) {
            out += `- ${it.date} profitRatio=${it.profitRatio} avgCost=${it.avgCost} `
              + `70%=[${it.cost70Low},${it.cost70High}] conc70=${it.cost70Conc} `
              + `90%=[${it.cost90Low},${it.cost90High}] conc90=${it.cost90Conc}\n`;
          }
        }
      }

      if (ffResp && ffResp.ok) {
        const ff = (await ffResp.json()) as StockFundFlowDetail;
        const items = (ff.items || []).slice(-ref.fundFlowDays);
        if (items.length) {
          out += `\nFund flow (last ${items.length}):\n`;
          for (const it of items) {
            out += `- ${it.date} close=${it.close} chg=${it.changePct}% `
              + `main=${it.mainNetAmount}(${it.mainNetRatio}%) `
              + `super=${it.superNetAmount}(${it.superNetRatio}%) `
              + `large=${it.largeNetAmount}(${it.largeNetRatio}%) `
              + `medium=${it.mediumNetAmount}(${it.mediumNetRatio}%) `
              + `small=${it.smallNetAmount}(${it.smallNetRatio}%)\n`;
          }
        }
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


