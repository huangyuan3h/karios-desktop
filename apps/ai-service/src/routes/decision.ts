import { Hono } from 'hono';
import { streamText, generateText } from 'ai';

import { ChatRequestSchema, toModelMessagesFromChatRequest } from '../chat.js';
import {
  getResolvedModel,
  getDecisionModelBundle,
  generateTextJsonObjectModeOptions,
  type AiModel,
  type ResolvedModelBundle,
} from '../model.js';
import { ThinkingStreamStripper, stripJsonCodeFence } from '../model_thinking.js';

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

  // 2026-08-10: tool calling was replaced with a prefetched live-health
  // context block. ai@5.0.116 ends its step loop right after the tool step
  // (finishReason='tool-calls', empty textStream) for every provider — the
  // UI surfaces that as "empty assistant response". Prefetching keeps the
  // answer live while staying on a plain text generation path that works.
  const healthMd = await queryHoldingsHealth();
  const s3SystemMessage = {
    role: 'system' as const,
    content:
      `${S3_RULES_KNOWLEDGE}\n\n【实时决策体检（已预取，回答持仓/买卖/加仓/买什么问题时以它为事实依据）】\n` +
      `${healthMd}\n\n` +
      '（历史归档查询暂不可用——如需查看上周/历史判断，请到「决策 · 历史快照」面板。）',
  };

  async function collectText(bundle: ResolvedModelBundle): Promise<string> {
    const result = await streamText({
      model: bundle.model,
      messages: [s3SystemMessage, ...messages],
      providerOptions:
        bundle.provider === 'google'
          ? { googleGenerativeAI: { thinkingConfig: { thinkingLevel: 'high' as const } } }
          : undefined,
    });
    const stripper = new ThinkingStreamStripper();
    let full = '';
    for await (const chunk of result.textStream) {
      const visible = stripper.push(chunk);
      if (visible) full += visible;
    }
    const rest = stripper.flush();
    if (rest) full += rest;
    return full;
  }

  let primary: ResolvedModelBundle;
  try {
    primary = await getDecisionModelBundle();
  } catch (err) {
    const message = err instanceof Error ? err.message : 'Invalid AI configuration';
    return c.json({ error: message }, 500);
  }

  let text = '';
  let usedProvider = primary.provider;
  try {
    text = await collectText(primary);
  } catch (err) {
    console.error('decision primary stream failed:', err);
  }

  // Fallback: primary came back empty or errored (e.g. Gemini tool-loop
  // regression) — retry on the resolved profile/env model (openai/ollama).
  if (!text.trim()) {
    try {
      const fb = await getResolvedModel();
      text = await collectText(fb);
      usedProvider = fb.provider;
    } catch (err) {
      console.error('decision fallback stream failed:', err);
    }
  }

  if (!text.trim()) {
    return new Response('决策模型暂不可用（primary 与 fallback 均无输出），请稍后重试。', {
      headers: { 'Content-Type': 'text/plain; charset=utf-8' },
    });
  }

  const prefix = usedProvider === primary.provider ? '' : `[fallback: ${usedProvider}]\n`;
  return new Response(`${prefix}${text}`, {
    headers: { 'Content-Type': 'text/plain; charset=utf-8' },
  });
});
