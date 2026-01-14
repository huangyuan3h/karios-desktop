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

const StrategyCandidatesRowSchema = z.object({
  symbol: z.string(),
  market: z.string(),
  ticker: z.string(),
  name: z.string(),
  score: z.number().min(0).max(100),
  rank: z.number().int().min(1).max(5),
  why: z.string(),
  scoreBreakdown: z
    .object({
      trend: z.number().min(0).max(40),
      flow: z.number().min(0).max(30),
      structure: z.number().min(0).max(20),
      risk: z.number().min(0).max(10),
    })
    .optional(),
});

const StrategyCandidatesResponseSchema = z.object({
  date: z.string(),
  accountId: z.string(),
  accountTitle: z.string(),
  candidates: z.array(StrategyCandidatesRowSchema).max(5),
  leader: z.object({
    symbol: z.string(),
    reason: z.string(),
  }),
  riskNotes: z.array(z.string()).optional(),
  model: z.string(),
});

const StrategyDailyMarkdownResponseSchema = z.object({
  date: z.string(),
  accountId: z.string(),
  accountTitle: z.string(),
  markdown: z.string(),
  model: z.string(),
});

const LeaderDailyRequestSchema = z.object({
  date: z.string().min(1),
  context: z.record(z.any()),
});

const LeaderBuyZoneSchema = z.object({
  low: z.union([z.number(), z.string()]),
  high: z.union([z.number(), z.string()]),
  note: z.string().optional(),
});

const LeaderTriggerSchema = z.object({
  kind: z.enum(['breakout', 'pullback']),
  condition: z.string(),
  value: z.union([z.number(), z.string()]).optional(),
});

const LeaderTargetPriceSchema = z.object({
  primary: z.union([z.number(), z.string()]),
  stretch: z.union([z.number(), z.string()]).optional(),
  note: z.string().optional(),
});

const LeaderPickSchema = z.object({
  symbol: z.string(),
  market: z.string(),
  ticker: z.string(),
  name: z.string(),
  score: z.number().min(0).max(100),
  reason: z.string(),
  whyBullets: z.array(z.string()).min(3).max(6),
  expectedDurationDays: z.number().int().min(1).max(10),
  buyZone: LeaderBuyZoneSchema,
  triggers: z.array(LeaderTriggerSchema).min(1).max(4),
  invalidation: z.string(),
  targetPrice: LeaderTargetPriceSchema,
  probability: z.number().int().min(1).max(5),
  risks: z.array(z.string()).min(2).max(4),
  sourceSignals: z
    .object({
      industries: z.array(z.string()).optional(),
      screeners: z.array(z.string()).optional(),
      notes: z.array(z.string()).optional(),
    })
    .optional(),
  riskPoints: z.array(z.string()).optional(),
});

const LeaderDailyResponseSchema = z.object({
  date: z.string(),
  leaders: z.array(LeaderPickSchema).max(2),
  model: z.string(),
});

const MainlineThemeInputSchema = z.object({
  kind: z.enum(['industry', 'concept']),
  name: z.string().min(1),
  evidence: z.record(z.any()),
});

const MainlineExplainRequestSchema = z.object({
  date: z.string().min(1),
  themes: z.array(MainlineThemeInputSchema).min(1).max(20),
  context: z.record(z.any()).optional(),
});

const MainlineThemeExplainSchema = z.object({
  kind: z.enum(['industry', 'concept']),
  name: z.string(),
  logicScore: z.number().min(0).max(100),
  logicGrade: z.enum(['S', 'A', 'B']).optional(),
  logicSummary: z.string().optional(),
  catalysts: z.array(z.string()).optional(),
});

const MainlineExplainResponseSchema = z.object({
  date: z.string(),
  themes: z.array(MainlineThemeExplainSchema),
  model: z.string(),
});

// --- Quant rank (2D profit) (v0) ---
const QuantRankCandidateInputSchema = z.object({
  symbol: z.string().min(1),
  ticker: z.string().min(1),
  name: z.string().optional(),
  evidence: z.record(z.any()),
});

const QuantRankExplainRequestSchema = z.object({
  asOfTs: z.string().min(1),
  asOfDate: z.string().min(1),
  horizon: z.literal('2d'),
  objective: z.literal('profit_probability'),
  candidates: z.array(QuantRankCandidateInputSchema).min(1).max(30),
  context: z.record(z.any()).optional(),
});

const QuantRankWhyBulletSchema = z.object({
  text: z.string().min(1).max(200),
  evidenceRefs: z.array(z.string().min(1)).min(1).max(4),
});

const QuantRankExplainItemSchema = z.object({
  symbol: z.string().min(1),
  llmScoreAdj: z.number().min(-5).max(5),
  whyBullets: z.array(QuantRankWhyBulletSchema).min(2).max(5),
  riskNotes: z.array(z.string()).max(4).optional(),
});

const QuantRankExplainResponseSchema = z.object({
  asOfTs: z.string(),
  asOfDate: z.string(),
  items: z.array(QuantRankExplainItemSchema),
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

app.post('/strategy/candidates', async (c) => {
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
    'You are a stock selection engine for swing trading. ' +
    'Your ONLY job is to rank candidates and choose a leader using the given context. ' +
    'Return a valid JSON object matching the provided schema. No markdown fences.';

  const instruction =
    `Task: Rank Top 5 candidate assets for ${accountTitle} on ${date}.\n` +
    'Constraints:\n' +
    '- Do NOT require per-stock deep context. Assume it is NOT available.\n' +
    '- Use ONLY: accountState, TradingView latest+history, industryFundFlow, marketSentiment.\n' +
    "- Mainline (主线): if context.mainline.selected exists, you MUST treat it as today's primary focus theme and reflect it in:\n" +
    '  - leader.reason (mention mainline name and whether it is clear)\n' +
    '  - candidate ranking (prefer candidates aligned with mainline when it is clear)\n' +
    '  - If context.mainline.debug.selectedClear is false, describe it as "weak mainline / rotation" and do NOT overfit.\n' +
    '- industryFundFlow format: use context.industryFundFlow.dailyTopInflow (Top5×Date industry names).\n' +
    '- marketSentiment format: use context.marketSentiment.latest (riskMode, upDownRatio, yesterdayLimitUpPremium, failedLimitUpRate).\n' +
    '- If riskMode is "no_new_positions": still output candidates, but you MUST set Today stance to defensive in leader reason and reduce Risk sub-score accordingly.\n' +
    '- Return exactly 1..5 candidates with numeric Score 0-100 and rank 1..5.\n' +
    '- Rank must be consistent with score (higher score => better rank).\n' +
    '- Provide a single leader (龙头) and a short reason.\n' +
    '- Score rubric (0-100): Trend(0-40)+Flow(0-30)+Structure(0-20)+Risk(0-10).\n' +
    '- Fill scoreBreakdown numbers to match the total score.\n' +
    '- Use Chinese.\n\n' +
    (accountPrompt ? `Account prompt:\n${accountPrompt}\n\n` : '') +
    'Context JSON:\n' +
    JSON.stringify(parsed.data.context);

  async function run(m: AiModel): Promise<unknown> {
    const { object } = await generateObject({
      model: m,
      schema: StrategyCandidatesResponseSchema,
      system,
      prompt: instruction,
      temperature: 0,
      maxOutputTokens: 1800,
    });
    return object;
  }

  try {
    const obj = await run(model);
    const out = StrategyCandidatesResponseSchema.parse(obj);
    return c.json({ ...out, model: modelId || out.model });
  } catch (e) {
    if (fallbackModel) {
      try {
        const obj = await run(fallbackModel);
        const out = StrategyCandidatesResponseSchema.parse(obj);
        return c.json({ ...out, model: fallbackModelId || modelId || out.model });
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
        candidates: [],
        leader: { symbol: '', reason: '' },
        riskNotes: [`Candidates generation failed: ${msg}`],
        model: modelId || 'unknown',
      },
      200,
    );
  }
});

app.post('/leader/daily', async (c) => {
  const body = await c.req.json().catch(() => null);
  const parsed = LeaderDailyRequestSchema.safeParse(body);
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
  const date = parsed.data.date.trim();

  const system =
    'You are a leader stock (龙头股) selection engine for CN/HK swing trading. ' +
    'Your ONLY job is to pick up to 2 leaders for today using the provided context. ' +
    'Return a valid JSON object matching the provided schema. No markdown fences.';

  const instruction =
    `Task: Select up to 2 leader stocks for ${date}.\n` +
    'Rules:\n' +
    '- You MUST choose leaders ONLY from context.candidateUniverse symbols.\n' +
    '- Use inputs:\n' +
    '  - context.tradingView.latest (screener latest rows)\n' +
    '  - context.industryFundFlow.dailyTopInflow (Top5×Date industry names)\n' +
    '  - context.market (per-stock summaries if present)\n' +
    '  - context.leaderHistory (last 10 trading days leaders)\n' +
    '- Daily limit: leaders <= 2.\n' +
    '- Prefer NEW leaders from today’s industry themes + screener strength.\n' +
    '- Avoid duplicates: if a symbol was selected recently, only pick again if it is clearly still the leader today.\n' +
    '- Objective: maximize upside (bigger expected move) over the next ~1-3 trading days, accepting lower win-rate.\n' +
    '- score (0-100) MUST represent UpsideScore (higher = larger expected upside / momentum continuation).\n' +
    '- Provide a concise Chinese reason.\n' +
    '- CRITICAL: Provide actionable fields for execution:\n' +
    '  - whyBullets: 3-6 short bullets (each <= 20 Chinese chars), explain why it is worth buying.\n' +
    '  - expectedDurationDays: 1-10 (how long this leader thesis likely lasts).\n' +
    '  - buyZone: a price range {low, high} where fill probability is high.\n' +
    '  - triggers: 1-2 triggers (breakout/pullback), each has condition and optional value.\n' +
    '  - invalidation: ONE clear invalidation rule (price below X / structure breaks).\n' +
    '  - targetPrice: {primary, stretch?} price targets.\n' +
    '  - probability: integer 1-5 (win-rate / success probability), do NOT conflate with UpsideScore.\n' +
    '  - risks: 2-4 key risks.\n' +
    '- If you lack a field (e.g. current price), write a best-effort number based on context.market.barsTail close; otherwise write "TBD" and explain in risks.\n' +
    '- Provide sourceSignals:\n' +
    '  - industries: 1-3 industry names from the matrix\n' +
    '  - screeners: screener names/ids that surfaced it\n' +
    '  - notes: optional short supporting notes\n' +
    '- Provide riskPoints: 2-4 bullets.\n' +
    'Return JSON only.\n\n' +
    'Context JSON:\n' +
    JSON.stringify(parsed.data.context);

  async function run(m: AiModel): Promise<unknown> {
    const { object } = await generateObject({
      model: m,
      schema: LeaderDailyResponseSchema,
      system,
      prompt: instruction,
      temperature: 0,
      maxOutputTokens: 1400,
    });
    return object;
  }

  try {
    const obj = await run(model);
    const out = LeaderDailyResponseSchema.parse(obj);
    return c.json({ ...out, model: modelId || out.model });
  } catch (e) {
    if (fallbackModel) {
      try {
        const obj = await run(fallbackModel);
        const out = LeaderDailyResponseSchema.parse(obj);
        return c.json({ ...out, model: fallbackModelId || modelId || out.model });
      } catch {
        // fallthrough
      }
    }
    const msg = e instanceof Error ? e.message : String(e);
    return c.json(
      {
        date,
        leaders: [],
        model: modelId || 'unknown',
        error: `Leader generation failed: ${msg}`,
      },
      200,
    );
  }
});

app.post('/mainline/explain', async (c) => {
  const body = await c.req.json().catch(() => null);
  const parsed = MainlineExplainRequestSchema.safeParse(body);
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
  const date = parsed.data.date.trim();

  const system =
    'You are a CN market mainline (主线) analysis engine. ' +
    'You must analyze WHY a theme is moving using the provided structured evidence. ' +
    'Return a valid JSON object matching the schema. No markdown fences.';

  const instruction =
    `Task: For each candidate theme on ${date}, assign logicScore(0-100) and logicGrade(S/A/B), and write a concise English logicSummary.\n` +
    'Scoring rubric (approx):\n' +
    '- S (81-100): policy + industry trend both present, with plausible catalysts.\n' +
    '- A (61-80): any 2 of {policy, industry trend, earnings} present.\n' +
    '- B (0-60): single short-term event or weak evidence.\n' +
    'Rules:\n' +
    '- Base ONLY on provided evidence. Do NOT fabricate news or specific policy documents.\n' +
    '- If uncertain, lower the score and say uncertainty explicitly.\n' +
    '- logicSummary must be <= 3 short sentences, English.\n' +
    '- catalysts (optional): 1-3 short bullets, English.\n' +
    'Return JSON only.\n\n' +
    'Input JSON:\n' +
    JSON.stringify(parsed.data);

  async function run(m: AiModel): Promise<unknown> {
    const { object } = await generateObject({
      model: m,
      schema: MainlineExplainResponseSchema,
      system,
      prompt: instruction,
      temperature: 0,
      maxOutputTokens: 1200,
    });
    return object;
  }

  try {
    const obj = await run(model);
    const out = MainlineExplainResponseSchema.parse(obj);
    return c.json({ ...out, model: modelId || out.model });
  } catch (e) {
    if (fallbackModel) {
      try {
        const obj = await run(fallbackModel);
        const out = MainlineExplainResponseSchema.parse(obj);
        return c.json({ ...out, model: fallbackModelId || modelId || out.model });
      } catch {
        // fallthrough
      }
    }
    const msg = e instanceof Error ? e.message : String(e);
    return c.json(
      {
        date,
        themes: parsed.data.themes.map((t) => ({
          kind: t.kind,
          name: t.name,
          logicScore: 50,
          logicGrade: 'B',
          logicSummary: `Mainline analysis failed: ${msg}`,
        })),
        model: modelId || 'unknown',
      },
      200,
    );
  }
});

app.post('/quant/rank/explain', async (c) => {
  const body = await c.req.json().catch(() => null);
  const parsed = QuantRankExplainRequestSchema.safeParse(body);
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

  const { asOfTs, asOfDate } = parsed.data;

  const system =
    'You are a quant ranking engine for CN A-shares. ' +
    'You must base ALL reasoning strictly on the provided structured evidence. ' +
    'Return a valid JSON object matching the schema. No markdown fences.';

  const instruction =
    `Task: Re-rank candidates for a 2-trading-day horizon (buy NOW at asOfTs=${asOfTs}).\n` +
    'Goal: prioritize high probability of profit, while also preferring small stable gains and avoiding tail losses.\n' +
    'Rules:\n' +
    '- You MUST NOT use any external knowledge about the company. Use ONLY evidence.\n' +
    '- For each item, output llmScoreAdj in [-5, +5]. Use small adjustments only.\n' +
    '- whyBullets: 2-5 bullets. Each bullet MUST include 1-4 evidenceRefs strings.\n' +
    '- evidenceRefs must point to existing evidence keys (dot paths like "spot.price", "bars.sma20", "fundFlow.mainNetRatio", "chips.profitRatio", "breakdown.trend").\n' +
    '- Keep text short and actionable (English).\n' +
    'Return JSON only.\n\n' +
    'Input JSON:\n' +
    JSON.stringify(parsed.data);

  async function run(m: AiModel): Promise<unknown> {
    const { object } = await generateObject({
      model: m,
      schema: QuantRankExplainResponseSchema,
      system,
      prompt: instruction,
      temperature: 0,
      maxOutputTokens: 1400,
    });
    return object;
  }

  try {
    const obj = await run(model);
    const out = QuantRankExplainResponseSchema.parse(obj);
    return c.json({ ...out, model: modelId || out.model });
  } catch (e) {
    if (fallbackModel) {
      try {
        const obj = await run(fallbackModel);
        const out = QuantRankExplainResponseSchema.parse(obj);
        return c.json({ ...out, model: fallbackModelId || modelId || out.model });
      } catch {
        // fallthrough
      }
    }
    const msg = e instanceof Error ? e.message : String(e);
    return c.json(
      {
        asOfTs,
        asOfDate,
        // Fail closed: do not override baseline ranking/why if model fails.
        items: [],
        model: modelId || 'unknown',
        error: `Quant rerank failed: ${msg}`,
      },
      200,
    );
  }
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
    '- Return a SINGLE Markdown document with a clean, professional layout.\n' +
    '- STRICT Markdown formatting: Headings at column 0, blank lines between sections, valid GFM tables.\n' +
    '- NO "###" levels. Only use H2 headings exactly as defined below.\n' +
    '- LANGUAGE: Chinese (Simplified).\n\n' +
    '## 1 总览 (Market & Execution)\n\n' +
    '用 1 段短文（<=200字）综合概括：\n' +
    '1. 市场情绪：引用 context.marketSentiment 的 riskMode/ratio 等数据给出结论。\n' +
    '2. 资金流向：总结 context.industryFundFlow.dailyTopInflow 的流入/流出及持续性。\n' +
    '3. 执行军规：给出今日核心操作原则（如：禁止开新仓、只做回踩、严格止损等）。\n\n' +
    '| Focus themes | Leader | Risk Mode | Today Stance | Execution Key |\n' +
    '|---|---|---|---|---|\n' +
    '| 主线/备选主题 | 龙头股 | 从 sentiment 获取 | 进攻/均衡/防守 | 1句话风控准则 |\n\n' +
    '## 2 机会Top3 (Selection)\n\n' +
    '核心描述：基于主线逻辑与评分系统筛选出的最强标的（从 context.stage1.candidates 选取）。\n\n' +
    '| Rank | Score | Symbol | Name | Current | Why now (1行) | Risk (1行) |\n' +
    '|---:|---:|---|---|---:|---|---|\n\n' +
    '## 3 持仓计划 (Holdings)\n\n' +
    '核心描述：对 context.accountState.positions 进行风险检查，明确优先处理的对象与防御边界。\n\n' +
    '| Symbol | Name | PnL% | Action | Score | StopLoss | Orders | Notes |\n' +
    '|---|---|---:|---|---:|---|---|---|\n\n' +
    '## 4 条件单总表 (Operations)\n\n' +
    '核心描述：整合新机会与旧持仓，提供可直接录入平安证券系统的条件单指令明细。\n\n' +
    '| Priority | Symbol | Name | Action | OrderType | TriggerCondition | TriggerValue | Qty | Rationale |\n' +
    '|---|---|---|---|---|---|---|---|---|\n\n' +
    '## 5 总结 (Summary)\n\n' +
    '用 2-3 句话总结今日操作的胜负手关键点，以及盘中需额外警惕的变量。\n\n' +
    'CRITICAL RULES:\n' +
    '1. Section 1 MUST merge fund flow analysis and execution rules into the intro and table.\n' +
    '2. "Actionable" focus: If riskMode is "no_new_positions", Section 2/4/5 must NOT suggest any buy orders.\n' +
    '3. Score calculation (0-100): Trend(40)+Flow(30)+Structure(20)+Risk(10).\n' +
    '4. Each section (except Summary) MUST follow the "Title -> 1-2 sentence description -> Table" sequence.\n' +
    '5. Tables must NOT have leading spaces and must render reliably.\n\n' +
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
