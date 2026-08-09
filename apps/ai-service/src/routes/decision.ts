import { Hono } from 'hono';
import { streamText, generateText, tool } from 'ai';
import { google } from '@ai-sdk/google';
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

export async function searchArchive(symbol: string): Promise<string> {
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

/** S-3 rules knowledge injected statically so answers stay aligned with the
 * backtest even before the holdings tool is called. */
const S3_RULES_KNOWLEDGE = `【S-3 回测纪律（本系统唯一证据 · 双窗验证 2026-08-09）】
- 市场状态只挡开仓不触发卖出：Weak=空仓观望（不开新仓），Strong/Diverging=进攻（Diverging 满仓）
- 回答"买什么"：先查体检里的今日候选清单，只推荐清单内标的，每票 ~5% 仓位；
  入池门槛 = score>=65 · RS前50% · 行业 5D 净流入 Top3（mainline）· 无恐慌冷却 · regime!=Weak
- 回答"加仓吗"：先查体检里该票的金字塔触发线/是否已加仓——成本涨幅 >= +2.5% 且未加过
  才可加半仓（1/2 袖套），每票至多 1 次；已加过/未到线 = 不加
- 持仓退出只有 4 条规则（命中即卖，未命中绝不因"市场弱"减仓）：
  1) 固定止损：净亏 >= 5% → 卖
  2) 移动止损：从持仓峰值（收盘价口径）回撤 >= 8% → 卖（保护利润）
  3) 60 天持有上限 → 卖
  4) score<0 平仓（等于永不触发）
- 历史证据：2024-25 弱市年（Weak 占 73%）持仓全程持有最终 +80.5%——
  弱市减仓违反回测证明的最优行为；收益来自少数 Strong/Diverging 进攻窗口
- 参数（S-3 定案）：score65 · hold60 · target100（不止盈）· floor0 · trailing-8 ·
  stop-5 · RS前50% · Diverging满仓 · 恐慌冷却3天 · 滑点0.05% · mp20 ·
  金字塔+2.5%加半仓(每票1次) · 持仓>19票时RS最弱先轮出
- 回答持仓/买卖/加仓问题时：先调用 query_s3_holdings_health 获取实时体检，按上述规则给结论；
  不要凭感觉建议，回测证据优先`;

export async function queryHoldingsHealth(): Promise<string> {
  const data = await fetchJson<{
    tradeDate?: string;
    regime?: string | null;
    sentiment?: string | null;
    s3Candidates?: Array<{
      symbol?: string;
      name?: string | null;
      ts_code?: string;
      industry?: string;
      score?: number;
      rs?: number;
      regime?: string | null;
    }> | null;
    panicCooldown?: { lastPanicDate?: string | null; cooldownEndDate?: string | null; active?: boolean } | null;
    s3Rules?: Record<string, unknown>;
    holdings?: Array<{
      symbol?: string;
      name?: string;
      positionPct?: number;
      costPrice?: number;
      lastClose?: number;
      pnlPct?: number;
      drawdownFromPeakPct?: number;
      holdingDays?: number;
      stopLossLine?: number;
      trailingLine?: number;
      pyramidTriggerLine?: number;
      pyramidAdded?: boolean;
      expireDate?: string;
      action?: string;
      reason?: string;
      note?: string;
    }>;
  }>(`/v1/agent/portfolio-health`);
  if (!data) return '持仓体检服务暂不可用（data-sync-service 未响应），请稍后再试。';
  const rules = data.s3Rules ?? {};
  const lines = [
    `# S-3 决策体检（${data.tradeDate ?? '最新'}）`,
    '',
    `- 市场状态：regime=${data.regime ?? '—'} · sentiment=${data.sentiment ?? '—'} · 恐慌冷却=${data.panicCooldown?.active ? `至 ${data.panicCooldown.cooldownEndDate}` : '无'}`,
    `- S-3 规则：止损 -5% · 移动止损 -8% · 60 天上限 · 金字塔 +${rules.pyramidTriggerPct ?? 2.5}% 加半仓（每票至多 1 次，加过的不要再加）`,
    '',
  ];
  const holds = data.holdings ?? [];
  if (holds.length === 0) {
    lines.push('（当前无持仓——未录入成本/仓位的 watchlist 票不算持仓）');
  }
  for (const h of holds) {
    const line = h.action === 'EXIT' ? '🔴 建议退出' : '✅ 持有';
    const name = h.name ?? h.symbol;
    lines.push(
      `- **[${name}](#/stock/${encodeURIComponent(h.symbol ?? '')})**（${h.symbol} · 仓位 ${h.positionPct ?? '—'}%）${line}` +
        `：盈亏 ${h.pnlPct != null ? `${h.pnlPct >= 0 ? '+' : ''}${h.pnlPct}%` : '—'}` +
        ` · 峰值回撤 ${h.drawdownFromPeakPct ?? '—'}%` +
        ` · 已持 ${h.holdingDays ?? '—'} 天` +
        (h.stopLossLine != null ? ` · 止损线 ${h.stopLossLine}` : '') +
        (h.trailingLine != null ? ` · 移动线 ${h.trailingLine}` : '') +
        (h.pyramidTriggerLine != null ? ` · 金字塔触发线 ${h.pyramidTriggerLine}` : '') +
        (h.pyramidAdded ? ' · 已加仓' : ' · 未加仓') +
        (h.expireDate ? ` · 到期 ${h.expireDate}` : '') +
        (h.reason ? ` · 触发：${h.reason}` : '') +
        (h.note ? ` · ${h.note}` : ''),
    );
  }
  const cands = data.s3Candidates ?? [];
  if (cands.length > 0) {
    lines.push('', '## 今日 S-3 开仓候选（买什么参考）');
    for (const c of cands) {
      lines.push(
        `- **[${c.name ?? c.symbol}](#/stock/${encodeURIComponent(c.symbol ?? '')})**（${c.symbol}${c.industry ? ` · ${c.industry}` : ''}）` +
          ` score=${c.score ?? '—'} · RS前50%=${c.rs ?? '—'} · 建议仓位 ~5%`,
      );
    }
    lines.push('- 入池门槛：score≥65 · RS 前 50% · regime≠Weak · 行业在 5D 净流入 Top3 · 无恐慌冷却');
  } else if (data.regime === 'Weak') {
    lines.push('', '- 今日 **无开仓候选**（regime=Weak：S-3 规定空仓观望，不新开仓）');
  } else {
    lines.push('', '- 今日无开仓候选（门槛：score≥65 · RS 前 50% · 行业净流入 Top3 · 无恐慌冷却）');
  }
  lines.push('', '- 结论口径：退出/加仓按 S-3 规则自动判定；市场 Weak 只挡开仓不触发卖出。');
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

  // S-3 backtest knowledge so answers stay aligned with the validated rules.
  const s3SystemMessage = { role: 'system' as const, content: S3_RULES_KNOWLEDGE };

  let model: AiModel;
  try {
    model = (await getDecisionModelBundle()).model;
  } catch (err) {
    const message = err instanceof Error ? err.message : 'Invalid AI configuration';
    return c.json({ error: message }, 500);
  }

  const archiveTools = {
    query_s3_holdings_health: tool({
      description:
        '获取 S-3 决策体检：①每只真实持仓的盈亏/峰值回撤/止损线/移动止损线/金字塔触发线/' +
        '是否已加仓/到期日/持有建议；②今日开仓候选（买什么）；③市场状态（regime/sentiment/' +
        '恐慌冷却）。用户问"该不该减仓/卖/持有/加仓/金字塔/买什么/能不能买/我的仓位"时' +
        '必须先调用此工具，按 S-3 规则回答，不要凭感觉。',
      inputSchema: z.object({}),
      execute: async () => queryHoldingsHealth(),
    }),
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

  // Gemini grounding: real-time web search (free tier does not support it —
  // enable via GEMINI_SEARCH_GROUNDING=1 once on a paid plan) + deep thinking.
  const searchEnabled =
    (process.env.GEMINI_SEARCH_GROUNDING ?? '').trim().length > 0;
  const decisionTools = searchEnabled
    ? { ...archiveTools, google_search: google.tools.googleSearch({ mode: 'MODE_DYNAMIC' }) }
    : archiveTools;
  const geminiProviderOptions = {
    googleGenerativeAI: { thinkingConfig: { thinkingLevel: 'high' as const } },
  };

  let result;
  try {
    result = await streamText({
      model,
      messages: [s3SystemMessage, ...messages],
      tools: decisionTools,
      providerOptions: geminiProviderOptions,
    });
  } catch {
    // Fallback: model may not support tool calling — retry without tools.
    result = await streamText({
      model,
      messages: [s3SystemMessage, ...messages],
      providerOptions: geminiProviderOptions,
    });
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
