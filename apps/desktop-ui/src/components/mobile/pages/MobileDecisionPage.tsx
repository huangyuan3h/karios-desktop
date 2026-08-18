'use client';

import * as React from 'react';
import { useQuery } from '@tanstack/react-query';
import { FileText, Send, Sparkles } from 'lucide-react';

import { AI_BASE_URL } from '@/lib/endpoints';
import { fetchPortfolioHealth } from '@/lib/queries/portfolioHealth';
import {
  appendDecisionMessage,
  decisionMessagesQueryKey,
  fetchDecisionMessages,
  type DecisionMessage,
} from '@/lib/queries/decision';
import { MobileButton, MobileCard } from '../primitives';

const MAX_WINDOW_MESSAGES = 24;
const THREAD_KEY = 'karios.decision.threadId';

function foldWindow(messages: DecisionMessage[], cap: number): DecisionMessage[] {
  if (messages.length <= cap) return messages;
  const keep = messages.slice(-cap);
  const foldedCount = messages.length - cap;
  return [
    { id: -1, sessionId: messages[0]?.sessionId ?? 0, role: 'user', content: `（略早前 ${foldedCount} 条消息已折叠）`, contextSnapshot: null, createdAt: messages[0]?.createdAt ?? '' },
    ...keep,
  ];
}

/** Current situation → compact markdown the agent can act on. */
async function buildContextMd(): Promise<string> {
  const lines: string[] = [];
  try {
    const h = await fetchPortfolioHealth();
    lines.push(`# 当前快照（${h.tradeDate ?? '—'}）`);
    lines.push(`- 市场 regime: ${h.regime ?? '—'} · 强度 ${h.strength ?? '—'} · 情绪 ${h.sentiment ?? '—'}`);
    if (h.panicCooldown?.active) lines.push(`- ⚠ 恐慌冷却至 ${h.panicCooldown.cooldownEndDate ?? '—'}`);
    const cands = h.s3Candidates ?? [];
    if (cands.length) {
      lines.push(`- 下午2点买入候选(${cands.length}): ${cands.map((c) => `${c.name ?? c.symbol}(score ${c.score ?? '—'})`).join('、')}`);
    }
    const holdings = [
      ...(h.holdings ?? []).map((x) => ({ ...x, m: 'A股' })),
      ...(h.hkHealth?.holdings ?? []).map((x) => ({ ...x, m: '港股' })),
    ];
    if (holdings.length) {
      lines.push(`- 持仓(${holdings.length}): ${holdings.map((x) => `${x.name ?? x.symbol}(${x.m}) pnl ${x.pnlPct ?? '—'}% 止损${x.stopLossLine ?? '—'}${x.action === 'EXIT' ? ' [EXIT]' : ''}`).join('；')}`);
    }
  } catch {
    lines.push('# 当前快照（不可用）');
  }
  return lines.join('\n');
}

const QUICK_QUESTIONS = ['当前市场怎么看？', '买入候选有哪些？', '持仓有什么风险？', '今天适合加仓吗？'];

/** 决策 Agent (mobile) — always-on chat with live context. §5.2 中频. */
export function MobileDecisionPage() {
  const [threadId, setThreadId] = React.useState<number | null>(null);
  const [local, setLocal] = React.useState<DecisionMessage[]>([]);
  const [input, setInput] = React.useState('');
  const [streaming, setStreaming] = React.useState(false);
  const [inserting, setInserting] = React.useState(false);

  React.useEffect(() => {
    const raw = localStorage.getItem(THREAD_KEY);
    if (raw) {
      const n = Number(raw);
      if (Number.isFinite(n) && n > 0) setThreadId(n);
    }
  }, []);

  const query = useQuery({
    queryKey: decisionMessagesQueryKey(threadId ?? -1),
    queryFn: () => fetchDecisionMessages(threadId as number),
    enabled: threadId != null,
  });

  React.useEffect(() => {
    if (query.data) setLocal(query.data);
  }, [query.data]);

  const send = async (text: string) => {
    if (!threadId || !text.trim() || streaming) return;
    const userMsg = await appendDecisionMessage(threadId, {
      role: 'user',
      content: text.trim(),
      contextSnapshot: { window: { count: local.length, cap: MAX_WINDOW_MESSAGES } },
    });
    setLocal((prev) => [...prev, userMsg]);
    setInput('');
    setStreaming(true);
    const assistantId = -Date.now();
    setLocal((prev) => [
      ...prev,
      { id: assistantId, sessionId: threadId, role: 'assistant', content: '', contextSnapshot: null, createdAt: new Date().toISOString() },
    ]);
    try {
      const history = [...local, userMsg];
      const payload = {
        messages: foldWindow(history, MAX_WINDOW_MESSAGES).map((m) => ({
          role: m.role === 'user' ? 'user' : 'assistant',
          content: m.content,
        })),
      };
      const resp = await fetch(`${AI_BASE_URL}/decision`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(payload),
      });
      if (!resp.ok || !resp.body) throw new Error(`chat failed: ${resp.status}`);
      const reader = resp.body.getReader();
      const decoder = new TextDecoder();
      let full = '';
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        full += decoder.decode(value, { stream: true });
        setLocal((prev) => prev.map((m) => (m.id === assistantId ? { ...m, content: full } : m)));
      }
      if (!full.trim()) throw new Error('empty assistant response');
      const saved = await appendDecisionMessage(threadId, { role: 'assistant', content: full });
      setLocal((prev) => prev.map((m) => (m.id === assistantId ? saved : m)));
    } catch (err) {
      setLocal((prev) =>
        prev.map((m) =>
          m.id === assistantId
            ? { ...m, content: `⚠ 调用失败：${err instanceof Error ? err.message : String(err)}` }
            : m,
        ),
      );
    } finally {
      setStreaming(false);
    }
  };

  /** Ask with the live snapshot attached (the "always informed" path). */
  const askWithContext = async (question: string) => {
    if (!threadId || streaming) return;
    const ctx = await buildContextMd();
    await send(`${ctx}\n\n${question}`);
  };

  /** Insert the snapshot into the conversation without asking. */
  const insertContext = async () => {
    if (!threadId || inserting) return;
    setInserting(true);
    try {
      const ctx = await buildContextMd();
      const saved = await appendDecisionMessage(threadId, { role: 'user', content: ctx });
      setLocal((prev) => [...prev, saved]);
    } finally {
      setInserting(false);
    }
  };

  return (
    <div className="space-y-3">
      {/* Quick asks — live context attached */}
      <div className="flex gap-1.5 overflow-x-auto pb-0.5">
        <div className="flex shrink-0 items-center gap-1 pr-1 text-[var(--m-text-xs)] text-[var(--k-muted)]">
          <Sparkles size={13} />
        </div>
        {QUICK_QUESTIONS.map((q) => (
          <button
            key={q}
            type="button"
            onClick={() => void askWithContext(q)}
            disabled={streaming || threadId == null}
            className="shrink-0 rounded-[var(--m-radius-pill)] border border-[var(--k-accent)]/30 bg-[var(--k-accent)]/5 px-2.5 py-1 text-[var(--m-text-xs)] font-medium text-[var(--k-accent)] disabled:opacity-50"
          >
            {q}
          </button>
        ))}
      </div>

      <MobileCard className="flex h-[62vh] flex-col">
        <div className="flex-1 space-y-2 overflow-y-auto p-3">
          {!threadId ? (
            <div className="pt-16 text-center text-[var(--m-text-sm)] text-[var(--k-muted)]">
              无决策会话，请先在桌面端使用一次
            </div>
          ) : local.length === 0 ? (
            <div className="pt-16 text-center text-[var(--m-text-sm)] text-[var(--k-muted)]">
              点上方快捷问题，Agent 会自动带上当前持仓与候选数据
            </div>
          ) : (
            local.map((m) =>
              m.role === 'user' ? (
                <div key={m.id} className="flex justify-end">
                  <div className="max-w-[85%] whitespace-pre-wrap rounded-[var(--m-radius-lg)] bg-[var(--k-accent)] px-3 py-2 text-[var(--m-text-sm)] text-white">
                    {m.content}
                  </div>
                </div>
              ) : (
                <div key={m.id} className="flex justify-start">
                  <div className="max-w-[92%] whitespace-pre-wrap rounded-[var(--m-radius-lg)] bg-[var(--k-surface-2)] px-3 py-2 text-[var(--m-text-sm)] text-[var(--k-text)]">
                    {m.content || '…'}
                  </div>
                </div>
              ),
            )
          )}
        </div>
        <div className="flex items-center gap-2 border-t border-[var(--k-border)] p-3">
          <MobileButton variant="ghost" size="sm" onClick={() => void insertContext()} disabled={inserting || threadId == null} className="!h-10 shrink-0">
            <FileText size={14} /> 数据
          </MobileButton>
          <input
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => e.key === 'Enter' && void send(input)}
            disabled={streaming}
            placeholder={streaming ? '回复中…' : '输入问题'}
            className="h-[var(--m-tap)] min-w-0 flex-1 rounded-[var(--m-radius-md)] border border-[var(--k-border)] bg-[var(--k-surface-2)] px-3 text-[var(--m-text-base)] outline-none focus:border-[var(--k-accent)] disabled:opacity-50"
          />
          <MobileButton onClick={() => void send(input)} disabled={streaming || !input.trim()} className="!h-[var(--m-tap)] shrink-0 px-4">
            <Send size={15} />
          </MobileButton>
        </div>
      </MobileCard>
    </div>
  );
}
