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
  model: any;
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

function getModel(modelOverride?: string) {
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

  let model;
  let fallbackModel: any | null = null;
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

  async function runGenerateObject(m: any): Promise<unknown> {
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

  async function runGenerateTextJson(m: any): Promise<string> {
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

  const attempts: Array<{ kind: 'object' | 'text'; model: any; modelName: string }> = [
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

  let model;
  let fallbackModel: any | null = null;
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
    '- MUST include an "Industry fund flow" section analyzing capital rotation using context.industryFundFlow.\n' +
    '- Top candidates: pick <= 3 (not 5).\n' +
    '- MUST include: (1) new opportunities, (2) existing holdings actions (hold/stop/reduce/exit).\n' +
    '- Provide concrete analysis: why, key levels, risk boundaries.\n' +
    '- Provide a single consolidated action table in Ping An Securities style (平安证券条件单风格).\n' +
    '  The table should include BOTH new opportunities and existing holdings.\n' +
    '  Columns: Symbol | Name | Current | Action | OrderType | Trigger | Qty | ValidUntil | StopLoss | TakeProfit | Notes.\n' +
    '- Use conditional-order phrasing (examples: 到价买入/到价卖出/价格上穿/价格下穿/回落卖出/反弹买入).\n' +
    '- Use the SAME language as the user/account prompt (Chinese is expected).\n\n' +
    (accountPrompt ? `Account prompt:\n${accountPrompt}\n\n` : '') +
    'Context JSON:\n' +
    JSON.stringify(parsed.data.context);

  async function run(m: any): Promise<string> {
    const { text } = await generateText({
      model: m,
      system,
      prompt: instruction,
      temperature: 0.2,
      maxOutputTokens: 2200,
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
