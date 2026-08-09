import { DATA_SYNC_BASE_URL } from '@/lib/endpoints';
import { apiGetJson } from '@/lib/api/client';

export type DecisionSession = {
  id: number;
  title: string | null;
  modelProfile: string | null;
  systemPrompt: string | null;
  createdAt: string;
  lastActiveAt: string;
  messageCount: number;
};

export type DecisionMessage = {
  id: number;
  sessionId: number;
  role: 'user' | 'assistant' | 'system';
  content: string;
  contextSnapshot: Record<string, unknown> | null;
  createdAt: string;
};

export function decisionSessionsQueryKey() {
  return ['decision', 'sessions'] as const;
}

export function decisionMessagesQueryKey(sessionId: number) {
  return ['decision', 'messages', sessionId] as const;
}

export async function fetchDecisionSessions(): Promise<DecisionSession[]> {
  const resp = await apiGetJson<{ ok: boolean; sessions: DecisionSession[] }>(
    '/api/decision/sessions',
  );
  return resp?.sessions ?? [];
}

export async function createDecisionSession(opts?: {
  title?: string | null;
  systemPrompt?: string | null;
  modelProfile?: string | null;
}): Promise<DecisionSession> {
  const resp = await fetch(`${DATA_SYNC_BASE_URL}/api/decision/sessions`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(opts ?? {}),
  });
  if (!resp.ok) throw new Error(`create session failed: ${resp.status}`);
  const data = (await resp.json()) as { ok: boolean; session: DecisionSession };
  return data.session;
}

export async function renameDecisionSession(
  sessionId: number,
  title: string,
): Promise<DecisionSession> {
  return updateDecisionSession(sessionId, { title });
}

export async function updateDecisionSession(
  sessionId: number,
  patch: { title?: string | null; systemPrompt?: string | null },
): Promise<DecisionSession> {
  const resp = await fetch(
    `${DATA_SYNC_BASE_URL}/api/decision/sessions/${sessionId}`,
    {
      method: 'PATCH',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        title: patch.title ?? null,
        system_prompt: patch.systemPrompt ?? null,
      }),
    },
  );
  if (!resp.ok) throw new Error(`update session failed: ${resp.status}`);
  const data = (await resp.json()) as { ok: boolean; session: DecisionSession };
  return data.session;
}

export async function fetchDecisionMessages(sessionId: number): Promise<DecisionMessage[]> {
  const resp = await apiGetJson<{ ok: boolean; messages: DecisionMessage[] }>(
    `/api/decision/sessions/${sessionId}/messages`,
  );
  return resp?.messages ?? [];
}

export type DecisionSnapshot = {
  snapshotDate: string;
  status: string;
  activeLayerRef: {
    watchlistSymbols?: string[];
    bySource?: Record<string, number>;
    count?: number;
    snapshotAt?: string;
  } | null;
  agentExchanges: Array<{ role: string; content: string; createdAt?: string }>;
  outcome: {
    fired?: Array<{ symbol: string | null; field: string | null; newValue: string | null; source: string | null }>;
    paper?: Array<{ symbol: string | null; side: string | null; status: string | null; pnlPct: number | null }>;
  } | null;
};

export async function fetchDecisionSnapshot(date: string): Promise<DecisionSnapshot | null> {
  const resp = await apiGetJson<{ ok: boolean; snapshot: DecisionSnapshot }>(
    `/api/decision/snapshots/${date}`,
  );
  return resp?.snapshot ?? null;
}

export function decisionSnapshotToMarkdown(snap: DecisionSnapshot): string {
  const lines = [`# 归档引用 ${snap.snapshotDate}（${snap.status === 'reviewed' ? '已反馈' : '未反馈'}）`, ''];
  const fired = snap.outcome?.fired ?? [];
  if (fired.length) {
    lines.push(`## 当日开火（${fired.length}）`);
    for (const f of fired.slice(0, 10)) {
      lines.push(`- ${f.symbol ?? '—'} ${f.newValue ?? f.field ?? ''} (${f.source ?? '?'})`);
    }
    lines.push('');
  }
  const paper = snap.outcome?.paper ?? [];
  if (paper.length) {
    lines.push(`## 当日模拟盘（${paper.length}）`);
    for (const p of paper.slice(0, 10)) {
      lines.push(`- ${p.symbol ?? '—'} ${p.side ?? ''} ${p.status ?? ''} pnl=${p.pnlPct ?? '—'}%`);
    }
    lines.push('');
  }
  const exchanges = snap.agentExchanges ?? [];
  if (exchanges.length) {
    lines.push(`## 当日决策对话（${exchanges.length} 条，列最近 6 条）`);
    for (const ex of exchanges.slice(-6)) {
      lines.push(`- **${ex.role}**(${String(ex.createdAt ?? '').slice(0, 16)}): ${String(ex.content ?? '').slice(0, 300)}`);
    }
    lines.push('');
  }
  lines.push('- 以上为历史归档数据，时效性以当前活跃层为准。');
  return lines.join('\n');
}

export async function deleteDecisionMessage(
  sessionId: number,
  messageId: number,
): Promise<boolean> {
  const resp = await fetch(
    `${DATA_SYNC_BASE_URL}/api/decision/sessions/${sessionId}/messages/${messageId}`,
    { method: 'DELETE' },
  );
  if (!resp.ok) return false;
  const data = (await resp.json()) as { ok: boolean };
  return data?.ok === true;
}

export async function appendDecisionMessage(
  sessionId: number,
  msg: {
    role: 'user' | 'assistant';
    content: string;
    contextSnapshot?: Record<string, unknown> | null;
  },
): Promise<DecisionMessage> {
  const resp = await fetch(
    `${DATA_SYNC_BASE_URL}/api/decision/sessions/${sessionId}/messages`,
    {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(msg),
    },
  );
  if (!resp.ok) throw new Error(`append message failed: ${resp.status}`);
  const data = (await resp.json()) as { ok: boolean; message: DecisionMessage };
  return data.message;
}
