'use client';

import React from 'react';

import { Bot, ClipboardList, Settings2, Trash2 } from 'lucide-react';

import { useQuery, useQueryClient } from '@tanstack/react-query';

import { Button } from '@/components/ui/button';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Textarea } from '@/components/ui/textarea';
import { ChatComposer } from '@/components/chat/ChatComposer';
import { MarkdownMessage } from '@/components/chat/MarkdownMessage';
import { ContextInspector } from '@/components/decision/ContextInspector';
import { AnalysisView } from '@/components/decision/AnalysisView';
import { WeeklyReviewCard } from '@/components/decision/WeeklyReviewCard';
import { AI_BASE_URL, DATA_SYNC_BASE_URL } from '@/lib/endpoints';
import type { ChatMessage } from '@/lib/chat/types';
import { cn } from '@/lib/utils';
import { buildDashboardCopyAllMarkdown, buildCopyDeltaMarkdown } from '@/lib/dashboard-export';
import { fetchDashboardSummaryPartial } from '@/lib/queries/dashboard';
import {
  activeLayerToMarkdown,
  buildDecisionActiveLayer,
  type DecisionActiveLayer,
} from '@/lib/decision-context';

import {
  appendDecisionMessage,
  createDecisionSession,
  decisionMessagesQueryKey,
  decisionSessionsQueryKey,
  decisionSnapshotToMarkdown,
  deleteDecisionMessage,
  fetchDecisionMessages,
  fetchDecisionSnapshot,
  updateDecisionSession,
  type DecisionMessage,
  type DecisionSnapshot,
} from '@/lib/queries/decision';

const MAX_WINDOW_MESSAGES = 24;
const THREAD_STORAGE_KEY = 'karios.decision.threadId';

/** Collapse older messages beyond the window into one-line summaries. */
function foldWindow(messages: DecisionMessage[], cap: number): DecisionMessage[] {
  if (messages.length <= cap) return messages;
  const keep = messages.slice(-cap);
  const folded = messages.slice(0, -cap);
  const summary = folded
    .slice(-6)
    .map(
      (m) =>
        `${m.role === 'user' ? 'user' : 'assistant'}(${m.createdAt.slice(5, 16)}): ${m.content.replace(/\s+/g, ' ').slice(0, 60)}…`,
    )
    .join('\n');
  const foldedMsg: DecisionMessage = {
    id: -1,
    sessionId: messages[0]?.sessionId ?? 0,
    role: 'system',
    content: `[折叠] 窗口之外的更早对话（${folded.length} 条），最近几条摘要：\n${summary}`,
    contextSnapshot: null,
    createdAt: folded[0]?.createdAt ?? new Date().toISOString(),
  };
  return [foldedMsg, ...keep];
}

export function DecisionPage() {
  const queryClient = useQueryClient();
  const [threadId, setThreadId] = React.useState<number | null>(null);
  const [systemPrompt, setSystemPrompt] = React.useState<string>('');
  const [promptLoaded, setPromptLoaded] = React.useState(false);
  const [streaming, setStreaming] = React.useState(false);
  const [inserting, setInserting] = React.useState(false);
  const [localMessages, setLocalMessages] = React.useState<DecisionMessage[]>([]);
  const [toggles, setToggles] = React.useState<Record<string, boolean>>({});
  const [activeLayer, setActiveLayer] = React.useState<DecisionActiveLayer | null>(null);
  const [layerBusy, setLayerBusy] = React.useState(false);
  const [promptEditorOpen, setPromptEditorOpen] = React.useState(false);
  const [promptDraft, setPromptDraft] = React.useState('');
  const scrollRef = React.useRef<HTMLDivElement>(null);
  const initialLayerBuilt = React.useRef(false);
  const layerCache = React.useRef<{ at: number; layer: DecisionActiveLayer } | null>(null);

  const sessionsQuery = useQuery({
    queryKey: decisionSessionsQueryKey(),
    queryFn: async () => {
      const resp = await fetch(`${DATA_SYNC_BASE_URL}/api/decision/sessions`);
      const data = (await resp.json()) as {
        ok: boolean;
        sessions: Array<{ id: number; systemPrompt?: string | null }>;
      };
      return data?.sessions ?? [];
    },
  });

  // Single-thread mode: auto-create the main thread on first visit.
  React.useEffect(() => {
    if (threadId != null) return;
    const saved = Number(window.localStorage.getItem(THREAD_STORAGE_KEY));
    if (saved && Number.isFinite(saved)) {
      setThreadId(saved);
      return;
    }
    let cancelled = false;
    (async () => {
      const session = await createDecisionSession({
        title: '决策主线程',
        systemPrompt: systemPrompt || null,
      });
      if (!cancelled) {
        window.localStorage.setItem(THREAD_STORAGE_KEY, String(session.id));
        setThreadId(session.id);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [threadId, systemPrompt]);

  const messagesQuery = useQuery({
    queryKey: decisionMessagesQueryKey(threadId ?? -1),
    queryFn: () => fetchDecisionMessages(threadId as number),
    enabled: threadId != null,
  });

  const snapshotsQuery = useQuery({
    queryKey: ['decision', 'snapshots'],
    queryFn: async () => {
      const resp = await fetch(`${DATA_SYNC_BASE_URL}/api/decision/snapshots`);
      const data = (await resp.json()) as { ok: boolean; snapshots: Array<DecisionSnapshot> };
      return data?.snapshots ?? [];
    },
  });

  // Load system prompt: session's own prompt, else active preset.
  React.useEffect(() => {
    if (promptLoaded || threadId == null) return;
    let cancelled = false;
    async function load() {
      try {
        const session = sessionsQuery.data?.find((s) => s.id === threadId);
        const fallback = session?.systemPrompt;
        const resp = await fetch(`${DATA_SYNC_BASE_URL}/system-prompts/active`);
        const active = resp.ok ? ((await resp.json()) as { content?: string }) : null;
        const content = fallback?.trim() || active?.content?.trim() || '';
        if (!cancelled) setSystemPrompt(content);
      } catch {
        // ignore
      }
      if (!cancelled) setPromptLoaded(true);
    }
    load();
    return () => {
      cancelled = true;
    };
  }, [promptLoaded, threadId, sessionsQuery.data]);

  React.useEffect(() => {
    if (threadId == null) {
      setLocalMessages([]);
      setActiveLayer(null);
      layerCache.current = null;
      initialLayerBuilt.current = false;
      return;
    }
    setLocalMessages(messagesQuery.data ?? []);
    // Build the active layer exactly once per thread mount; afterwards only
    // explicit user actions (send / refresh) rebuild it. Prevents bursts of
    // API calls on every messages refetch.
    if (!initialLayerBuilt.current) {
      initialLayerBuilt.current = true;
      void buildLayer();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [threadId, messagesQuery.data]);

  React.useEffect(() => {
    scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight });
  }, [localMessages.length]);

  async function buildLayer(forceFresh = false) {
    if (threadId == null) return null;
    // Reuse the live layer for 60s unless forced fresh — sending a follow-up
    // message should not re-fetch every source every time.
    if (!forceFresh && layerCache.current && Date.now() - layerCache.current.at < 60_000) {
      return layerCache.current.layer;
    }
    setLayerBusy(true);
    try {
      const layer = await buildDecisionActiveLayer({
        queryClient,
        forceFresh,
        include: toggles,
      });
      layerCache.current = { at: Date.now(), layer };
      setActiveLayer(layer);
      return layer;
    } catch {
      return null;
    } finally {
      setLayerBusy(false);
    }
  }

  /** Stream one assistant reply for the given history (caller appended the user message). */
  async function streamAssistantReply(history: DecisionMessage[]) {
    if (!threadId) return;
    const layer = await buildLayer(false);
    const layerMd = layer ? activeLayerToMarkdown(layer) : '';
    const messages: Array<{ role: 'system' | 'user' | 'assistant'; content: string }> = [];
    if (systemPrompt.trim()) {
      messages.push({ role: 'system', content: systemPrompt.trim() });
    }
    if (layerMd) {
      messages.push({ role: 'system', content: layerMd });
    }
    for (const m of foldWindow(history, MAX_WINDOW_MESSAGES)) {
      messages.push({
        role: m.role === 'user' ? 'user' : 'assistant',
        content: m.content,
      });
    }
    const payload = { messages };
    const assistantId = -Date.now();
    setLocalMessages((prev) => [...prev, {
      id: assistantId,
      sessionId: threadId,
      role: 'assistant',
      content: '',
      contextSnapshot: null,
      createdAt: new Date().toISOString(),
    }]);
    setStreaming(true);
    try {
      const resp = await fetch(`${AI_BASE_URL}/decision`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(payload),
      });
      if (!resp.ok || !resp.body) {
        throw new Error(`chat failed: ${resp.status}`);
      }
      const reader = resp.body.getReader();
      const decoder = new TextDecoder();
      let full = '';
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        full += decoder.decode(value, { stream: true });
        setLocalMessages((prev) =>
          prev.map((m) => (m.id === assistantId ? { ...m, content: full } : m)),
        );
      }
      if (!full.trim()) {
        throw new Error('empty assistant response');
      }
      const saved = await appendDecisionMessage(threadId, {
        role: 'assistant',
        content: full,
      });
      setLocalMessages((prev) => prev.map((m) => (m.id === assistantId ? saved : m)));
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      setLocalMessages((prev) =>
        prev.map((m) =>
          m.id === assistantId ? { ...m, content: `⚠ 调用失败：${msg}` } : m,
        ),
      );
    } finally {
      setStreaming(false);
    }
  }

  async function handleSend(text: string) {
    if (!threadId || !text.trim()) return;
    const userMsg = await appendDecisionMessage(threadId, {
      role: 'user',
      content: text.trim(),
      contextSnapshot: { window: { count: localMessages.length, cap: MAX_WINDOW_MESSAGES } },
    });
    setLocalMessages((prev) => [...prev, userMsg]);
    await streamAssistantReply([...localMessages, userMsg]);
  }

  /** Insert current data into the conversation: full snapshot as baseline, delta afterwards. */
  async function handleInsertCopyAll() {
    if (!threadId || inserting) return;
    setInserting(true);
    try {
      const summary = await fetchDashboardSummaryPartial({
        includeMacro: true,
        includeSentiment: true,
        includeNews: true,
        includeIndustry: true,
        includeScreeners: true,
      });
      // Force the builder's internal fetchQuery calls to refetch the latest
      // watchlist/screener data (invalidate resets their staleTime to 0),
      // without a global invalidate storm.
      await queryClient.invalidateQueries({ queryKey: ['watchlist'] });
      const newsItems = Array.isArray((summary as { news?: { items?: Array<{ title?: string; relevanceScore?: number }> } }).news?.items)
        ? (summary as { news?: { items?: Array<{ title?: string; relevanceScore?: number }> } }).news?.items ?? []
        : [];
      const newsFallback = newsItems.length
        ? newsItems
            .slice(0, 15)
            .map(
              (it) =>
                `- ${String(it?.title ?? '—').slice(0, 80)}${
                  it?.relevanceScore ? ` (rel=${it.relevanceScore})` : ''
                }`,
            )
            .join('\n')
        : '';
      const hasBaseline = localMessages.some((m) => m.content.startsWith('📋'));
      if (!hasBaseline) {
        // First reference: full snapshot as baseline.
        const text = await buildDashboardCopyAllMarkdown({
          summary,
          newsSummary: null,
          newsFallback: newsFallback || null,
          queryClient,
          mode: 'full',
          forceFresh: false,
        });
        const marker = `📋 引用当前数据快照\n\n${text}`;
        const userMsg = await appendDecisionMessage(threadId, {
          role: 'user',
          content: marker,
          contextSnapshot: { copyAllRef: new Date().toISOString() },
        });
        setLocalMessages((prev) => [...prev, userMsg]);
      } else {
        // Later references: delta only (full tables live in the active layer).
        const deltaMd = await buildCopyDeltaMarkdown({ summary, queryClient });
        const marker = `📈 增量快照（变更与待办）\n\n${deltaMd}`;
        const userMsg = await appendDecisionMessage(threadId, {
          role: 'user',
          content: marker,
          contextSnapshot: { copyDeltaRef: new Date().toISOString() },
        });
        setLocalMessages((prev) => [...prev, userMsg]);
      }
      // 只把数据插入对话，不触发 LLM；由指挥官在下方输入自己的指令触发。
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      setLocalMessages((prev) => [
        ...prev,
        {
          id: -Date.now(),
          sessionId: threadId,
          role: 'system',
          content: `⚠ 数据快照插入失败：${msg}`,
          contextSnapshot: null,
          createdAt: new Date().toISOString(),
        },
      ]);
    } finally {
      setInserting(false);
    }
  }

  async function handleInsertArchive(date: string) {
    if (!threadId) return;
    const snap = await fetchDecisionSnapshot(date);
    if (!snap) return;
    const md = decisionSnapshotToMarkdown(snap);
    await appendDecisionMessage(threadId, {
      role: 'user',
      content: md,
      contextSnapshot: { archiveRef: date },
    });
    await queryClient.invalidateQueries({ queryKey: decisionMessagesQueryKey(threadId) });
  }

  async function handleSavePrompt() {
    if (!threadId) return;
    await updateDecisionSession(threadId, { systemPrompt: promptDraft });
    setSystemPrompt(promptDraft);
    setPromptEditorOpen(false);
  }

  async function handleResetPrompt() {
    if (!threadId) return;
    try {
      const resp = await fetch(`${DATA_SYNC_BASE_URL}/system-prompts/active`);
      const active = resp.ok ? ((await resp.json()) as { content?: string }) : null;
      const content = active?.content?.trim() || '';
      setPromptDraft(content);
      setSystemPrompt(content);
      await updateDecisionSession(threadId, { systemPrompt: content });
    } catch {
      // ignore
    }
    setPromptEditorOpen(false);
  }

  async function handleDeleteMessage(messageId: number) {
    if (!threadId) return;
    if (messageId > 0) {
      await deleteDecisionMessage(threadId, messageId);
    }
    setLocalMessages((prev) => prev.filter((m) => m.id !== messageId));
  }

  const chatMessages: ChatMessage[] = localMessages.filter((m) => m.content).map((m) => ({
    id: String(m.id),
    role: m.role,
    content: m.content,
    createdAt: m.createdAt,
  }));
  const messageCount = chatMessages.length;
  const windowCount = localMessages.filter((m) => m.role === 'user' || m.role === 'assistant').length;

  return (
    <div className="flex h-full min-h-0">
      <div className="flex min-w-0 flex-1 flex-col">
        {/* Header */}
        <div className="flex items-center justify-between border-b border-[var(--k-border)] px-4 py-2.5">
          <div className="flex items-center gap-2">
            <Bot size={15} className="text-[var(--k-accent)]" />
            <span className="text-sm font-semibold">决策 Agent（主线程）</span>
            {activeLayer && (
              <span className="rounded border border-[var(--k-border)] px-1.5 py-0.5 text-[10px] tabular-nums text-[var(--k-muted)]">
                L1 {(activeLayer.totalTokens / 1000).toFixed(1)}k tok
              </span>
            )}
          </div>
          <Button
            variant="ghost"
            size="sm"
            onClick={() => {
              setPromptDraft(systemPrompt);
              setPromptEditorOpen(true);
            }}
          >
            <Settings2 size={13} className="mr-1" />
            系统提示词
          </Button>
        </div>

        {/* Messages */}
        <div ref={scrollRef} className="min-h-0 flex-1 overflow-y-auto px-6">
          <div className="mx-auto flex max-w-3xl flex-col">
            {threadId == null ? (
              <div className="flex flex-col items-center justify-center gap-3 px-8 py-24 text-center">
                <Bot size={36} className="text-[var(--k-muted)]" />
                <p className="max-w-md text-sm leading-6 text-[var(--k-muted)]">
                  正在初始化决策主线程…
                </p>
              </div>
            ) : chatMessages.length === 0 ? (
              <div className="flex flex-col items-center justify-center gap-3 px-8 py-24 text-center">
                <Bot size={36} className="text-[var(--k-muted)]" />
                <p className="max-w-md text-sm leading-6 text-[var(--k-muted)]">
                  决策主线程：所有对话集中在这里，context 由系统自动管理
                  （活跃层实时装配 · 窗口自动折叠 · 18:00 归档）。
                </p>
              </div>
            ) : (
              <div className="flex flex-col gap-3 p-4">
                {chatMessages.map((m, idx) => {
                  const isSnapshot = m.content.startsWith('📋');
                  const isDelta = m.content.startsWith('📈');
                  const isArchive = m.content.startsWith('# 归档引用');
                  if (isSnapshot || isDelta || isArchive) {
                    const body = isSnapshot || isDelta
                      ? m.content.replace(/^[📋📈][^\n]*\n+/, '')
                      : m.content;
                    return (
                      <details
                        key={m.id}
                        open={idx === messageCount - 1 && (isSnapshot || isDelta)}
                        className="group relative rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] px-3 py-2"
                      >
                        <button
                          onClick={() => void handleDeleteMessage(Number(m.id))}
                          className="absolute -top-2 right-2 z-10 rounded-md bg-[var(--k-surface)] p-1 text-red-500 opacity-0 shadow-sm transition-opacity hover:bg-red-500/10 group-hover:opacity-100"
                          title="删除此消息"
                        >
                          <Trash2 size={13} />
                        </button>
                        <summary className="cursor-pointer select-none text-xs font-medium text-[var(--k-muted)] transition-colors hover:text-[var(--k-text)]">
                          {isSnapshot ? '📋 数据快照' : isDelta ? '📈 增量快照' : '📎 归档引用'} ·{' '}
                          {m.createdAt.slice(5, 16)} · 点击展开/折叠
                        </summary>
                        <div className="mt-2 max-h-[32rem] overflow-auto text-sm leading-6">
                          <MarkdownMessage content={body} className="prose-sm" />
                        </div>
                      </details>
                    );
                  }
                  return (
                    <div
                      key={m.id}
                      className={cn(
                        'group relative max-w-[88%] rounded-lg border px-3.5 py-2.5 text-[15px] leading-7',
                        m.role === 'user' &&
                          'ml-auto border-zinc-200 bg-zinc-50 text-zinc-950 dark:border-zinc-800 dark:bg-zinc-900 dark:text-zinc-50',
                        m.role === 'assistant' &&
                          'mr-auto border-zinc-200 bg-white text-zinc-950 dark:border-zinc-800 dark:bg-zinc-950 dark:text-zinc-50',
                        m.role === 'system' &&
                          'mx-auto border-transparent bg-transparent text-zinc-500 dark:text-zinc-400',
                      )}
                    >
                      <button
                        onClick={() => void handleDeleteMessage(Number(m.id))}
                        className="absolute -top-2 right-0 z-10 rounded-md bg-[var(--k-surface)] p-1 text-red-500 opacity-0 shadow-sm transition-opacity hover:bg-red-500/10 group-hover:opacity-100"
                        title="删除此消息"
                      >
                        <Trash2 size={13} />
                      </button>
                      {m.role === 'assistant' ? (
                        <MarkdownMessage content={m.content} className="prose-sm" />
                      ) : (
                        <div className="whitespace-pre-wrap">{m.content}</div>
                      )}
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        </div>

        {/* Actions + Composer */}
        <div className="border-t border-[var(--k-border)] px-6 py-3">
          <div className="mx-auto max-w-3xl">
            <div className="mb-2 flex items-center gap-2">
              <Button
                variant="outline"
                size="sm"
                onClick={handleInsertCopyAll}
                disabled={inserting || streaming || threadId == null}
              >
                <ClipboardList size={13} className="mr-1" />
                {inserting ? '正在生成数据快照…' : '引用当前数据（Copy All）'}
              </Button>
              <span className="text-[11px] leading-4 text-[var(--k-muted)]">
                首次插入全量快照，之后只插入增量变更（完整表格由活跃层每次对话实时注入）；均不自动发送，在下方输入指令后发送
              </span>
            </div>
            <ChatComposer onSend={(t) => void handleSend(t)} disabled={streaming || threadId == null} />
            <p className="mt-1 px-1 text-[11px] text-[var(--k-muted)]">
              {streaming
                ? 'Agent 生成中…'
                : layerBusy
                  ? '正在装配活跃层…'
                  : `系统提示词：${systemPrompt.trim() ? '已加载' : '未设置'}（点击右上角编辑）`}
            </p>
          </div>
        </div>
      </div>

      {/* Context Inspector / Analysis */}
      {threadId != null && (
        <Tabs defaultValue="context" className="flex w-80 shrink-0 flex-col">
          <TabsList className="mx-3 mt-2.5 grid w-auto grid-cols-2">
            <TabsTrigger value="context">Context</TabsTrigger>
            <TabsTrigger value="analysis">分析</TabsTrigger>
          </TabsList>
          <TabsContent value="context" className="min-h-0 flex-1">
            <ContextInspector
              layer={activeLayer}
              toggles={toggles}
              onToggle={(blockId, enabled) => {
                setToggles((prev) => ({ ...prev, [blockId]: enabled }));
              }}
              windowCount={windowCount}
              windowCap={MAX_WINDOW_MESSAGES}
              snapshots={snapshotsQuery.data ?? []}
              refreshing={layerBusy}
              onRefresh={() => void buildLayer(true)}
              onInsertArchive={(s) => void handleInsertArchive(s.snapshotDate)}
            />
          </TabsContent>
          <TabsContent value="analysis" className="min-h-0 flex-1">
            <div className="flex flex-col gap-3 p-3">
              <WeeklyReviewCard />
              <AnalysisView />
            </div>
          </TabsContent>
        </Tabs>
      )}

      {/* System prompt editor */}
      {promptEditorOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-6">
          <div className="w-full max-w-2xl rounded-lg border border-[var(--k-border)] bg-[var(--k-surface)] p-4 shadow-xl">
            <div className="mb-2 flex items-center justify-between">
              <span className="text-sm font-semibold">系统提示词（决策合同）</span>
              <button
                onClick={() => setPromptEditorOpen(false)}
                className="rounded px-2 py-1 text-xs text-[var(--k-muted)] hover:bg-[var(--k-surface-2)]"
              >
                关闭
              </button>
            </div>
            <Textarea
              value={promptDraft}
              onChange={(e) => setPromptDraft(e.target.value)}
              rows={14}
              className="w-full font-mono text-xs leading-5"
            />
            <div className="mt-3 flex items-center justify-between gap-2">
              <Button variant="ghost" size="sm" onClick={handleResetPrompt}>
                重置为 active preset
              </Button>
              <div className="flex gap-2">
                <Button variant="ghost" size="sm" onClick={() => setPromptEditorOpen(false)}>
                  取消
                </Button>
                <Button size="sm" onClick={handleSavePrompt}>
                  保存
                </Button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
