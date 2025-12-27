import 'dotenv/config';

import { serve } from '@hono/node-server';
import { cors } from 'hono/cors';
import { Hono } from 'hono';
import { generateObject, generateText, streamText } from 'ai';
import { openai } from '@ai-sdk/openai';
import { google } from '@ai-sdk/google';
import { z } from 'zod';

import { ChatRequestSchema, toModelMessagesFromChatRequest } from './chat';
import { tryParseJsonObject } from './json_parse';

type AiModel = Parameters<typeof generateText>[0]['model'];

process.on('unhandledRejection', (reason) => {
  // Prevent process crash / hard connection close; log for debugging.
  console.error('unhandledRejection:', reason);
});

process.on('uncaughtException', (err) => {
  // Prevent hard close without response; keep process alive for local dev.
  console.error('uncaughtException:', err);
});

const TitleRequestSchema = z.object({
  text: z.string().min(1).max(8000),
  systemPrompt: z.string().optional(),
});

const BrokerExtractRequestSchema = z.object({
  imageDataUrl: z.string().min(1),
});

const BrokerExtractResponseSchema = z.object({
  kind: z.enum([
    'account_overview',
    'positions',
    'conditional_orders',
    'trades',
    'settlement_statement',
    'unknown',
  ]),
  broker: z.literal('pingan'),
  extractedAt: z.string(),
  data: z.record(z.any()).optional(),
});

const StrategyDailyRequestSchema = z.object({
  date: z.string().min(1),
  accountId: z.string().min(1),
  accountTitle: z.string().optional(),
  accountPrompt: z.string().optional(),
  context: z.record(z.any()),
});

const StrategyDailyMarkdownResponseSchema = z.object({
  date: z.string(),
  accountId: z.string(),
  accountTitle: z.string(),
  markdown: z.string(),
  model: z.string(),
});

const StrategyCandidateSchema = z.object({
  symbol: z.string(),
  market: z.string(),
  ticker: z.string(),
  name: z.string(),
  score: z.number().min(0).max(100),
  rank: z.number().int().min(1),
  why: z.string(),
});

const StrategyOrderSchema = z.object({
  kind: z.string(),
  side: z.enum(['buy', 'sell']),
  trigger: z.string(),
  qty: z.string(),
  timeInForce: z.string().nullable(),
  notes: z.string().nullable(),
});

const StrategyRecommendationSchema = z.object({
  symbol: z.string(),
  ticker: z.string(),
  name: z.string(),
  thesis: z.string(),
  levels: z.object({
    support: z.array(z.string()),
    resistance: z.array(z.string()),
    invalidations: z.array(z.string()),
  }),
  orders: z.array(StrategyOrderSchema),
  positionSizing: z.string(),
  riskNotes: z.array(z.string()),
});

const StrategyDailyResponseSchema = z.object({
  date: z.string(),
  accountId: z.string(),
  accountTitle: z.string(),
  candidates: z.array(StrategyCandidateSchema).max(5),
  leader: z.object({
    symbol: z.string(),
    reason: z.string(),
  }),
  recommendations: z.array(StrategyRecommendationSchema).max(3),
  riskNotes: z.array(z.string()),
  model: z.string(),
});

// NOTE: JSON parsing helpers live in ./json_parse.ts

function getStrategyFallbackModelId(): string | null {
  const id = (process.env.AI_STRATEGY_FALLBACK_MODEL ?? '').trim();
  return id || null;
}

async function tryRepairStrategyJson({
  model,
  system,
  instruction,
  badText,
}: {
  model: AiModel;
  system: string;
  instruction: string;
  badText: string;
}): Promise<unknown> {
  const repairPrompt =
    instruction +
    '\n\nThe previous output was INVALID JSON. Repair it.\n' +
    'Rules:\n' +
    '- Output MUST be a SINGLE JSON object only.\n' +
    '- Must strictly match the schema.\n' +
    '- Do not add commentary or markdown fences.\n\n' +
    'Invalid JSON:\n' +
    badText;

  const { text } = await generateText({
    model,
    system,
    prompt: repairPrompt,
    temperature: 0,
    maxOutputTokens: 2400,
  });
  return tryParseJsonObject(text);
}

function getModel(modelOverride?: string): AiModel {
  const provider = (process.env.AI_PROVIDER ?? 'openai').toLowerCase();
  const modelId = modelOverride ?? process.env.AI_MODEL;

  if (!modelId) {
    throw new Error('Missing AI_MODEL');
  }

  if (provider === 'google') {
    return google(modelId);
  }

  return openai(modelId);
}

const app = new Hono();
app.use('*', cors());

app.onError((err, c) => {
  console.error('AI service error:', err);
  const message = err instanceof Error ? err.message : String(err);
  const stack =
    process.env.NODE_ENV !== 'production' && err instanceof Error ? (err.stack ?? null) : null;
  return c.json({ error: 'Internal server error', message, stack }, 500);
});

app.get('/healthz', (c) => c.json({ ok: true }));

app.post('/chat', async (c) => {
  const body = await c.req.json().catch(() => null);
  const parsed = ChatRequestSchema.safeParse(body);
  if (!parsed.success) {
    return c.json({ error: 'Invalid request body', issues: parsed.error.issues }, 400);
  }

  const messages = toModelMessagesFromChatRequest(parsed.data);

  let model;
  try {
    model = getModel();
  } catch (err) {
    const message = err instanceof Error ? err.message : 'Invalid AI configuration';
    return c.json({ error: message }, 500);
  }

  const result = await streamText({
    model,
    messages,
  });

  return result.toTextStreamResponse();
});

app.post('/title', async (c) => {
  const body = await c.req.json().catch(() => null);
  const parsed = TitleRequestSchema.safeParse(body);
  if (!parsed.success) {
    return c.json({ error: 'Invalid request body', issues: parsed.error.issues }, 400);
  }

  let model;
  try {
    model = getModel(process.env.AI_TITLE_MODEL);
  } catch (err) {
    const message = err instanceof Error ? err.message : 'Invalid AI configuration';
    return c.json({ error: message }, 500);
  }

  const system = (parsed.data.systemPrompt ?? '').trim();
  const userText = parsed.data.text.trim();

  const prompt =
    'Generate a short, specific conversation title (max 6 words). ' +
    'Use the same language as the user text. ' +
    'Do not wrap in quotes. Return title only.\n\n' +
    `User text:\n${userText}\n`;

  const { text } = await generateText({
    model,
    system: system || undefined,
    prompt,
    maxOutputTokens: 24,
    temperature: 0.2,
  });

  return c.json({ title: text.trim().replace(/^"|"$/g, '') });
});

app.post('/extract/broker/pingan', async (c) => {
  const body = await c.req.json().catch(() => null);
  const parsed = BrokerExtractRequestSchema.safeParse(body);
  if (!parsed.success) {
    return c.json({ error: 'Invalid request body', issues: parsed.error.issues }, 400);
  }

  let model;
  try {
    model = getModel();
  } catch (err) {
    const message = err instanceof Error ? err.message : 'Invalid AI configuration';
    return c.json({ error: message }, 500);
  }

  const system =
    'You are a data extraction engine. ' +
    'Given a screenshot of the Ping An Securities (平安证券) iOS app, extract structured data. ' +
    'Return STRICT JSON only. Do not include markdown fences.';

  const instruction =
    'Classify the screenshot kind and extract fields when possible.\n' +
    '- kind: one of account_overview | positions | conditional_orders | trades | settlement_statement | unknown\n' +
    '- broker: "pingan"\n' +
    '- extractedAt: ISO timestamp\n' +
    '- data: object with extracted fields\n\n' +
    'IMPORTANT: a single screenshot may contain both account overview and holdings table. In that case, set kind="positions" and include overview fields as well.\n' +
    'For account_overview, data may include: currency, totalAssets, securitiesValue, cashAvailable, withdrawable, pnlTotal, pnlToday, accountIdMasked.\n' +
    'For positions, data may include: currency, accountIdMasked, positions: [{ ticker, name, qtyHeld, qtyAvailable, price, cost, pnl, pnlPct, marketValue }].\n' +
    'For conditional_orders, data may include: orders: [{ ticker, name, side, triggerCondition, triggerValue, qty, status, validUntil }].\n\n' +
    'For trades, data may include: trades: [{ time, ticker, name, side, price, qty, amount, fee }]. Use a full timestamp when possible.\n' +
    'For settlement_statement, data may include: date, lines: [{ time, ticker, name, side, price, qty, amount, fee, tax, remark }].\n\n' +
    'Output example:\n' +
    '{"kind":"positions","broker":"pingan","extractedAt":"2025-01-01T00:00:00Z","data":{"currency":"CNY","positions":[...]}}';

  const { text } = await generateText({
    model,
    system,
    messages: [
      {
        role: 'user' as const,
        content: [
          { type: 'text' as const, text: instruction },
          { type: 'file' as const, data: parsed.data.imageDataUrl, mediaType: 'image/*' },
        ],
      },
    ],
    temperature: 0,
    maxOutputTokens: 900,
  });

  try {
    const obj = tryParseJsonObject(text);
    const out = BrokerExtractResponseSchema.safeParse(obj);
    if (!out.success) {
      return c.json(
        {
          kind: 'unknown',
          broker: 'pingan',
          extractedAt: new Date().toISOString(),
          data: { rawText: text },
        },
        200,
      );
    }
    return c.json(out.data);
  } catch {
    return c.json(
      {
        kind: 'unknown',
        broker: 'pingan',
        extractedAt: new Date().toISOString(),
        data: { rawText: text },
      },
      200,
    );
  }
});

app.post('/strategy/daily', async (c) => {
  const body = await c.req.json().catch(() => null);
  const parsed = StrategyDailyRequestSchema.safeParse(body);
  if (!parsed.success) {
    return c.json({ error: 'Invalid request body', issues: parsed.error.issues }, 400);
  }

  let model: AiModel;
  let fallbackModel: AiModel | null = null;
  try {
    model = getModel(process.env.AI_STRATEGY_MODEL);
    const fb = getStrategyFallbackModelId();
    if (fb) fallbackModel = getModel(fb);
  } catch (err) {
    const message = err instanceof Error ? err.message : 'Invalid AI configuration';
    return c.json({ error: message }, 500);
  }

  const modelId = process.env.AI_STRATEGY_MODEL ?? process.env.AI_MODEL ?? '';
  const fallbackModelId = getStrategyFallbackModelId();
  const accountTitle = (parsed.data.accountTitle ?? '').trim() || 'Account';
  const accountPrompt = (parsed.data.accountPrompt ?? '').trim();
  const date = parsed.data.date.trim();

  const system =
    'You are a swing trading strategy engine. ' +
    'You must produce an actionable daily plan using conditional-order style recipes. ' +
    'Focus on right-side trading and maximizing profit, but always define invalidation and risk boundaries. ' +
    'Return a valid JSON object matching the provided schema. No markdown fences.';

  const instruction =
    `Task: Generate a daily trading guide for ${accountTitle} on ${date}.\n` +
    'Constraints:\n' +
    '- Candidate universe: use ONLY the provided TradingView snapshots + the provided stocks list + current holdings.\n' +
    '- Output <= 5 candidates with score 0-100 and rank.\n' +
    '- Pick a single leader (龙头) and explain why.\n' +
    '- Recommend <= 3 symbols (do not exceed 3).\n' +
    '- Orders must be conditional-order style. Provide clear trigger and quantity.\n' +
    '- Always include levels.support/resistance/invalidations arrays (use empty arrays if unknown).\n' +
    '- Always include riskNotes arrays (use empty arrays if none).\n' +
    '- Use the SAME language as the user/account prompt (Chinese is expected).\n\n' +
    (accountPrompt ? `Account prompt:\n${accountPrompt}\n\n` : '') +
    'Context JSON:\n' +
    JSON.stringify(parsed.data.context);

  // Compact JSON template to reduce invalid outputs in text mode.
  const jsonTemplate =
    '{' +
    `"date":"${date}","accountId":"${parsed.data.accountId}","accountTitle":"${accountTitle}",` +
    '"candidates":[{"symbol":"","market":"","ticker":"","name":"","score":0,"rank":1,"why":""}],' +
    '"leader":{"symbol":"","reason":""},' +
    '"recommendations":[{"symbol":"","ticker":"","name":"","thesis":"","levels":{"support":[],"resistance":[],"invalidations":[]},"orders":[{"kind":"","side":"","trigger":"","qty":"","timeInForce":"day","notes":""}],"positionSizing":"","riskNotes":[]}],' +
    '"riskNotes":[],' +
    `"model":"${modelId || 'unknown'}"` +
    '}';

  async function runGenerateObject(m: AiModel): Promise<unknown> {
    const { object } = await generateObject({
      model: m,
      schema: StrategyDailyResponseSchema,
      system,
      prompt: instruction,
      temperature: 0,
      maxOutputTokens: 2400,
    });
    return object;
  }

  async function runGenerateTextJson(m: AiModel): Promise<string> {
    const { text } = await generateText({
      model: m,
      system,
      prompt:
        instruction +
        '\n\nReturn a single JSON object ONLY.\n' +
        'Do not include markdown fences.\n' +
        'Do not include trailing text.\n' +
        'Use this JSON template as a guide (fill with real content, do not add extra keys):\n' +
        jsonTemplate,
      temperature: 0,
      maxOutputTokens: 2400,
    });
    return text;
  }

  const attempts: Array<{ kind: 'object' | 'text'; model: AiModel; modelName: string }> = [
    { kind: 'object', model, modelName: modelId || 'primary' },
  ];
  if (fallbackModel)
    attempts.push({
      kind: 'object',
      model: fallbackModel,
      modelName: fallbackModelId || 'fallback',
    });
  attempts.push({ kind: 'text', model, modelName: modelId || 'primary' });
  if (fallbackModel)
    attempts.push({ kind: 'text', model: fallbackModel, modelName: fallbackModelId || 'fallback' });

  const failures: string[] = [];

  for (const a of attempts) {
    try {
      if (a.kind === 'object') {
        const obj = await runGenerateObject(a.model);
        const ok = StrategyDailyResponseSchema.safeParse(obj);
        if (ok.success) return c.json({ ...ok.data, model: a.modelName || ok.data.model });
        failures.push(`${a.modelName}:${a.kind}: schema validation failed`);
        continue;
      }

      const text = await runGenerateTextJson(a.model);
      try {
        const obj = tryParseJsonObject(text);
        const ok = StrategyDailyResponseSchema.safeParse(obj);
        if (ok.success) return c.json({ ...ok.data, model: a.modelName || ok.data.model });
        failures.push(`${a.modelName}:${a.kind}: schema validation failed`);
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        failures.push(`${a.modelName}:${a.kind}: ${msg}`);
        // One repair pass for invalid JSON.
        try {
          const repaired = await tryRepairStrategyJson({
            model: a.model,
            system,
            instruction,
            badText: text,
          });
          const ok2 = StrategyDailyResponseSchema.safeParse(repaired);
          if (ok2.success) return c.json({ ...ok2.data, model: a.modelName || ok2.data.model });
          failures.push(`${a.modelName}:repair: schema validation failed`);
        } catch (e2) {
          const msg2 = e2 instanceof Error ? e2.message : String(e2);
          failures.push(`${a.modelName}:repair: ${msg2}`);
        }
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      failures.push(`${a.modelName}:${a.kind}: ${msg}`);
    }
  }

  return c.json(
    {
      date,
      accountId: parsed.data.accountId,
      accountTitle,
      candidates: [],
      leader: { symbol: '', reason: '' },
      recommendations: [],
      riskNotes: [
        'Strategy generation failed after multiple attempts.',
        ...failures.slice(0, 6).map((x) => `- ${x}`),
      ],
      model: modelId || 'unknown',
    },
    200,
  );
});

app.post('/strategy/daily-markdown', async (c) => {
  const body = await c.req.json().catch(() => null);
  const parsed = StrategyDailyRequestSchema.safeParse(body);
  if (!parsed.success) {
    return c.json({ error: 'Invalid request body', issues: parsed.error.issues }, 400);
  }

  let model: AiModel;
  let fallbackModel: AiModel | null = null;
  try {
    model = getModel(process.env.AI_STRATEGY_MODEL);
    const fb = getStrategyFallbackModelId();
    if (fb) fallbackModel = getModel(fb);
  } catch (err) {
    const message = err instanceof Error ? err.message : 'Invalid AI configuration';
    return c.json({ error: message }, 500);
  }

  const modelId = process.env.AI_STRATEGY_MODEL ?? process.env.AI_MODEL ?? '';
  const fallbackModelId = getStrategyFallbackModelId();
  const accountTitle = (parsed.data.accountTitle ?? '').trim() || 'Account';
  const accountPrompt = (parsed.data.accountPrompt ?? '').trim();
  const date = parsed.data.date.trim();

  const system =
    'You are a swing trading strategy engine. ' +
    'You must produce an actionable daily plan using conditional-order style recipes. ' +
    'Focus on right-side trading and maximizing profit, but always define invalidation and risk boundaries. ' +
    'Return Markdown only. No JSON.';

  const instruction =
    `Task: Write a daily trading report for ${accountTitle} on ${date}.\n` +
    'Output requirements:\n' +
    '- Return a SINGLE Markdown document.\n' +
    '- Use clear headings and bullet points.\n' +
    '- STRICT Markdown formatting rules:\n' +
    '  - Every heading must start at the beginning of a line (column 0).\n' +
    '  - Each heading MUST be on its own line.\n' +
    '  - Insert a blank line between sections.\n' +
    '  - NEVER put "# ..." and "## ..." on the same line.\n' +
    '- IMPORTANT output style:\n' +
    '  - This must be a COMPLETE, readable report: combine short analysis paragraphs + tables.\n' +
    '  - Prefer TABLES for decisions; keep paragraphs short (2-6 lines). Avoid huge walls of text.\n' +
    '  - Each table MUST be written in valid GFM markdown table syntax, with header row + separator row on separate lines.\n' +
    '  - Do NOT squeeze tables into a single line. Every row must be on its own line.\n' +
    '  - Section 2 (candidates) MUST be a markdown table.\n' +
    '  - Section 3 (holdings) MUST be a markdown table.\n' +
    '  - Section 5 MUST be a markdown table (the final action table).\n' +
    '- The report MUST contain the following sections IN THIS ORDER:\n' +
    '  1) Market/Industry fund flow (资金流向板块)\n' +
    "  2) Today's Top candidates (<= 3)\n" +
    '  3) Holdings plan (现有持仓：哪些止损/持有/减仓/清仓)\n' +
    '  4) Overall execution plan (盘中执行要点)\n' +
    '  5) Ping An conditional-order action table (平安证券条件单风格 总表)\n' +
    '\n' +
    'You MUST follow this template (fill with real content, keep headings exactly):\n' +
    `# ${accountTitle} 日度交易报告（${date}）\n\n` +
    '## 0）结果摘要（只要结论，用表格）\n\n' +
    '| Focus themes | Leader | Risk budget | Max positions | Today stance | Notes |\n' +
    '|---|---|---|---|---|\n' +
    '| TBD | TBD | 单笔≤1% 净值 | ≤3 | 进攻/均衡/防守 | 右侧交易/条件单 |\n\n' +
    '## 1）资金流向板块（行业资金流与轮动判断）\n\n' +
    '（用 3-6 条 bullet，总结：Top流入/Top流出/持续性/对持仓威胁/今日聚焦主题）\n\n' +
    '## 2）Top candidates（≤ 3）\n\n' +
    '评分说明（0-100）：Trend(0-40)+Flow(0-30)+Structure(0-20)+Risk(0-10)。\n\n' +
    '| Rank | Score | Symbol | Name | Current | Why now (1 line) | Key levels (S/R/Invalid) | Plan A (breakout trigger) | Plan B (pullback trigger) | Risk (1 line) |\n' +
    '|---:|---:|---|---|---:|---|---|---|---|---|\n' +
    '| 1 | 0 | TBD | TBD | TBD | TBD | TBD | TBD | TBD | TBD |\n\n' +
    '（用 1-2 段短文说明：为什么它是“龙头/优先级最高”，以及今天不做什么。）\n\n' +
    '## 3）现有持仓：止损 / 持有 / 减仓 / 清仓\n\n' +
    '| Symbol | Name | Qty | Cost | Current | PnL% | Action | Score | StopLoss trigger | Reduce/Exit trigger | Orders (keep/adjust/cancel) | Notes |\n' +
    '|---|---|---:|---:|---:|---:|---|---:|---|---|---|---|\n' +
    '| TBD | TBD | TBD | TBD | TBD | TBD | Hold/Reduce/Exit | 0 | TBD | TBD | TBD | TBD |\n\n' +
    '（用 1 段短文总结：今天优先处理哪一只持仓的风险、哪些仓位可以顺势持有。）\n\n' +
    '## 4）盘中执行要点\n\n' +
    '- 只写 5-8 条“可执行规则”（例如：触发后必须补止损单；未触发不交易；午后复核；收盘撤销等）\n\n' +
    '## 5）平安证券条件单风格（总表）\n\n' +
    '- Section 1 MUST analyze capital rotation using context.industryFundFlow:\n' +
    '  - Identify top inflow industries (1D and 10D sum) and whether inflow is sustained or one-off.\n' +
    '  - Identify top outflow industries and whether it threatens current holdings.\n' +
    '  - Conclude a single "focus industry theme" for today (1-2 themes).\n' +
    '- Section 2: Top candidates <= 3. For each candidate provide concrete analysis:\n' +
    '  - Why now (trend/relative strength/industry flow)\n' +
    '  - Key levels (support/resistance/invalidation)\n' +
    '  - Plan A (breakout) + Plan B (pullback) triggers\n' +
    '  - Risk points\n' +
    '  Use available fields from context.stocks[*]: features, barsTail, chipsTail, fundFlowTail.\n' +
    '- Section 3 MUST cover EACH current position in context.accountState.positions:\n' +
    '  - Decide: Hold / Add / Reduce / Exit\n' +
    '  - Provide a stop-loss trigger (price下穿/到价卖出)\n' +
    '  - If there are existing conditional orders in context.accountState.conditionalOrders, say whether to keep/adjust/cancel.\n' +
    '- Section 5: Provide ONE consolidated action table (merge new opportunities + existing holdings) in Ping An style.\n' +
    '  - Use these columns exactly:\n' +
    '    Priority | Score | Symbol | Name | Current | Action | OrderType | TriggerCondition | TriggerValue | Qty | ValidUntil | Rationale | Risk | Exit\n' +
    '  - OrderType should match Ping An wording (examples): 到价买入/到价卖出/反弹买入/回落卖出\n' +
    '  - TriggerCondition examples: 价格上穿/价格下穿/到价/回落/反弹\n' +
    '  - TriggerValue should be specific if possible; otherwise write TBD.\n' +
    '  - ValidUntil should be a concrete date (e.g. within 3-10 trading days).\n' +
    '- IMPORTANT:\n' +
    '  - If you lack a field (e.g. Current price), write TBD and explain what data is missing.\n' +
    '  - Do NOT exceed 3 candidates.\n' +
    '  - Prefer actionable triggers over vague advice.\n' +
    '- Use the SAME language as the user/account prompt (Chinese is expected).\n\n' +
    (accountPrompt ? `Account prompt:\n${accountPrompt}\n\n` : '') +
    'Context JSON:\n' +
    JSON.stringify(parsed.data.context);

  async function run(m: AiModel): Promise<string> {
    const { text } = await generateText({
      model: m,
      system,
      prompt: instruction,
      temperature: 0,
      maxOutputTokens: 3200,
    });
    return text.trim();
  }

  try {
    const md = await run(model);
    const out = StrategyDailyMarkdownResponseSchema.parse({
      date,
      accountId: parsed.data.accountId,
      accountTitle,
      markdown: md,
      model: modelId || 'unknown',
    });
    return c.json(out);
  } catch (e) {
    if (fallbackModel) {
      try {
        const md = await run(fallbackModel);
        const out = StrategyDailyMarkdownResponseSchema.parse({
          date,
          accountId: parsed.data.accountId,
          accountTitle,
          markdown: md,
          model: fallbackModelId || modelId || 'unknown',
        });
        return c.json(out);
      } catch {
        // fallthrough
      }
    }
    const msg = e instanceof Error ? e.message : String(e);
    return c.json(
      {
        date,
        accountId: parsed.data.accountId,
        accountTitle,
        markdown:
          `# Daily Strategy Report\\n\\n` +
          `- Date: ${date}\\n` +
          `- Account: ${accountTitle}\\n\\n` +
          `## Error\\n\\n` +
          `Strategy generation failed: ${msg}\\n`,
        model: modelId || 'unknown',
      },
      200,
    );
  }
});

const port = Number(process.env.PORT ?? 4310);

serve({ fetch: app.fetch, port }, (info) => {
  console.log(`AI service listening on http://127.0.0.1:${info.port}`);
});
