import { Hono } from 'hono';
import { streamText, generateText, tool } from 'ai';
import { z } from 'zod';

import { ChatRequestSchema, toModelMessagesFromChatRequest } from '../chat';
import { getResolvedModel, getDecisionModelBundle, generateTextJsonObjectModeOptions, AiModel } from '../model';
import { ThinkingStreamStripper, stripJsonCodeFence } from '../model_thinking';

const DATA_SYNC_BASE_URL = process.env.DATA_SYNC_BASE_URL ?? 'http://127.0.0.1:4330';

async function fetchJson<T>(path: string): Promise<T | null> {
  try {
    const resp = await fetch(`${DATA_SYNC_BASE_URL}${path}`, {
      headers: { accept: 'application/json' },
      signal: AbortSignal.timeout(15_000),
    });
    if (!resp.ok) return null;
    return (await resp.json()) as T;
  } catch {
    return null;
  }
}

async function retrieveSnapshot(date: string): Promise<string> {
  const data = await fetchJson<{
    ok: boolean;
    snapshot?: {
      snapshotDate?: string;
      status?: string;
      outcome?: { fired?: Array<Record<string, unknown>>; paper?: Array<Record<string, unknown>> } | null;
      exchanges?: Array<{ role: string; content: string; createdAt?: string }>;
    };
  }>(`/api/decision/snapshots/${encodeURIComponent(date)}`);
  if (!data?.ok || !data.snapshot) return `归档快照不存在：${date}`;
  const s = data.snapshot;
  const lines = [`# 归档快照 ${s.snapshotDate ?? date}（状态：${s.status ?? 'open'}）`, ''];
  const fired = s.outcome?.fired ?? [];
  if (fired.length) {
    lines.push(`## 当日开火记录（${fired.length}）`);
    for (const f of fired.slice(0, 10)) {
      lines.push(`- ${String(f.symbol ?? '—')} ${String(f.newValue ?? f.field ?? '')} (${String(f.source ?? '?')})`);
    }
    lines.push('');
  }
  const paper = s.outcome?.paper ?? [];
  if (paper.length) {
    lines.push(`## 当日模拟盘（${paper.length}）`);
    for (const p of paper.slice(0, 10)) {
      lines.push(`- ${String(p.symbol ?? '—')} ${String(p.side ?? '')} ${String(p.status ?? '')} pnl=${String(p.pnlPct ?? '—')}%`);
    }
    lines.push('');
  }
  const exchanges = s.exchanges ?? [];
  if (exchanges.length) {
    lines.push(`## 当日决策对话（${exchanges.length} 条，最多列 8 条）`);
    for (const ex of exchanges.slice(-8)) {
      lines.push(`- **${ex.role}**(${String(ex.createdAt ?? '').slice(0, 16)}): ${String(ex.content ?? '').slice(0, 400)}`);
    }
    lines.push('');
  }
  lines.push(`- 提示：以上为历史归档数据，注意与当前活跃层对比时效性。`);
  return lines.join('\n');
}

async function searchArchive(symbol: string): Promise<string> {
  const data = await fetchJson<{
    ok: boolean;
    hits?: Array<{
      date: string;
      status?: string;
      matches?: string[];
      outcome?: { fired?: Array<Record<string, unknown>> } | null;
    }>;
  }>(`/api/decision/archive/search?symbol=${encodeURIComponent(symbol)}`);
  if (!data?.ok || !Array.isArray(data.hits) || data.hits.length === 0) {
    return `归档检索无命中：${symbol}`;
  }
  const lines = [`# 归档检索：${symbol}（${data.hits.length} 天命中）`, ''];
  for (const h of data.hits.slice(0, 10)) {
    const fired = (h.outcome?.fired ?? []).filter((f) => String(f.symbol ?? '').includes(symbol.toUpperCase())).length;
    lines.push(`- **${h.date}**（${h.status ?? 'open'}${fired ? ` · 开火 ${fired}` : ''}）：${String(h.matches?.[0] ?? '').slice(0, 120)}`);
  }
  lines.push('');
  lines.push('- 提示：历史归档为判断记录，胜负统计以 outcome.fired/paper 为准。');
  return lines.join('\n');
}

export const decisionRoutes = new Hono();

/**
 * Extract structured action recommendations from a decision brief.
 * Returns { actions: [{ symbol, action, rationale, confidence }] }.
 */
decisionRoutes.post('/extract-actions', async (c) => {
  const body = await c.req.json().catch(() => null);
  const text = typeof body?.text === 'string' ? body.text : '';
  if (!text.trim()) {
    return c.json({ error: 'text required' }, 400);
  }
  let model: AiModel;
  try {
    model = (await getResolvedModel()).model;
  } catch (err) {
    const message = err instanceof Error ? err.message : 'Invalid AI configuration';
    return c.json({ error: message }, 500);
  }
  try {
    const result = await generateText({
      model,
      temperature: 0.1,
      maxOutputTokens: 900,
      ...generateTextJsonObjectModeOptions(false),
      messages: [
        {
          role: 'system',
          content:
            '你是 Karios 决策简报解析器。从简报中提取所有针对具体股票的操作建议，输出 JSON：' +
            '{"actions":[{"symbol":"CN:600519.SH","action":"BUY|ADD|HOLD|EXIT","rationale":"一句话依据","confidence":0.8}]}。' +
            '规则：1) symbol 必须完整（CN: 前缀 + 6 位代码 + .SH/.SZ，如 CN:600519.SH）；' +
            '2) action 只能是 BUY/ADD/HOLD/EXIT 之一；' +
            '3) 只提取明确对单只股票的建议，含糊的宏观建议不提取；' +
            '4) 没有建议时输出 {"actions":[]}。不要输出任何其他内容。',
        },
        { role: 'user', content: text },
      ],
    });
    const cleaned = stripJsonCodeFence(result.text);
    const parsed = JSON.parse(cleaned);
    type ExtractedAction = {
      symbol?: unknown;
      action?: unknown;
      rationale?: unknown;
      confidence?: unknown;
    };
    const raw: ExtractedAction[] = Array.isArray(parsed?.actions)
      ? (parsed.actions as ExtractedAction[])
      : [];
    const normalized = raw
      .filter((a) => typeof a.symbol === 'string' && typeof a.action === 'string')
      .map((a) => ({
        symbol: String(a.symbol).trim().toUpperCase(),
        action: String(a.action).trim().toUpperCase(),
        rationale: typeof a.rationale === 'string' ? a.rationale.slice(0, 300) : '',
        confidence:
          typeof a.confidence === 'number' && Number.isFinite(a.confidence)
            ? Math.min(1, Math.max(0, a.confidence))
            : null,
      }))
      .filter((a) => /^CN:[0-9]{6}\.(SH|SZ)$/.test(a.symbol))
      .filter((a) => ['BUY', 'ADD', 'HOLD', 'EXIT'].includes(a.action));
    return c.json({ ok: true, actions: normalized });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return c.json({ ok: false, error: `extract failed: ${message}` }, 200);
  }
});

decisionRoutes.post('/', async (c) => {
  const body = await c.req.json().catch(() => null);
  const parsed = ChatRequestSchema.safeParse(body);
  if (!parsed.success) {
    return c.json({ error: 'Invalid request body', issues: parsed.error.issues }, 400);
  }

  const messages = toModelMessagesFromChatRequest(parsed.data);

  let model: AiModel;
  try {
    model = (await getDecisionModelBundle()).model;
  } catch (err) {
    const message = err instanceof Error ? err.message : 'Invalid AI configuration';
    return c.json({ error: message }, 500);
  }

  const archiveTools = {
    retrieve_archive_snapshot: tool({
      description:
        '按日期检索历史决策归档快照（当日决策对话 + 开火记录 + 模拟盘结果）。仅在用户询问历史/上周/某天决策时才调用。',
      inputSchema: z.object({
        date: z.string().describe('YYYY-MM-DD'),
      }),
      execute: async ({ date }: { date: string }) => retrieveSnapshot(date),
    }),
    search_archive_by_symbol: tool({
      description:
        '按股票代码检索归档中哪些天提及该标的及其 outcome（开火/模拟盘）。用户问"历史上对 X 的判断"时调用。',
      inputSchema: z.object({
        symbol: z.string().describe('如 CN:600519.SH 或 600519'),
      }),
      execute: async ({ symbol }: { symbol: string }) => searchArchive(symbol),
    }),
  };

  let result;
  try {
    result = await streamText({
      model,
      messages,
      tools: archiveTools,
    });
  } catch {
    // Fallback: model may not support tool calling — retry without tools.
    result = await streamText({ model, messages });
  }

  const stripper = new ThinkingStreamStripper();
  const cleaned = new ReadableStream<string>({
    async start(controller) {
      try {
        for await (const chunk of result.textStream) {
          const visible = stripper.push(chunk);
          if (visible) controller.enqueue(visible);
        }
        const rest = stripper.flush();
        if (rest) controller.enqueue(rest);
        controller.close();
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        controller.enqueue(`\n[决策模型错误] ${message}`);
        controller.close();
      }
    },
  });

  return new Response(cleaned.pipeThrough(new TextEncoderStream()), {
    headers: { 'Content-Type': 'text/plain; charset=utf-8' },
  });
});
