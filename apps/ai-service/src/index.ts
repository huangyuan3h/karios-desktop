import 'dotenv/config';

import { serve } from '@hono/node-server';
import { cors } from 'hono/cors';
import { Hono } from 'hono';
import { generateObject, generateText, streamText } from 'ai';
import { openai } from '@ai-sdk/openai';
import { google } from '@ai-sdk/google';
import { z } from 'zod';

import { ChatRequestSchema, toModelMessagesFromChatRequest } from './chat';

const TitleRequestSchema = z.object({
  text: z.string().min(1).max(8000),
  systemPrompt: z.string().optional(),
});

const BrokerExtractRequestSchema = z.object({
  imageDataUrl: z.string().min(1),
});

const BrokerExtractResponseSchema = z.object({
  kind: z.enum(['account_overview', 'positions', 'conditional_orders', 'trades', 'settlement_statement', 'unknown']),
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

function tryParseJsonObject(text: string): unknown {
  const t = text.trim();
  try {
    return JSON.parse(t);
  } catch {
    // Try to extract the first JSON object block.
    const start = t.indexOf('{');
    const end = t.lastIndexOf('}');
    if (start >= 0 && end > start) {
      const slice = t.slice(start, end + 1);
      return JSON.parse(slice);
    }
    throw new Error('Failed to parse JSON');
  }
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
  try {
    model = getModel(process.env.AI_STRATEGY_MODEL);
  } catch (err) {
    const message = err instanceof Error ? err.message : 'Invalid AI configuration';
    return c.json({ error: message }, 500);
  }

  const modelId = process.env.AI_STRATEGY_MODEL ?? process.env.AI_MODEL ?? '';
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

  // 1) Prefer structured output (generateObject).
  try {
    const { object } = await generateObject({
      model,
      schema: StrategyDailyResponseSchema,
      system,
      prompt: instruction,
      temperature: 0.2,
      maxOutputTokens: 1600,
    });
    return c.json({ ...object, model: modelId || object.model });
  } catch (e) {
    // 2) Fallback: generate text JSON and parse manually (works for models that don't support structured output).
    const msg = e instanceof Error ? e.message : String(e);
    try {
      const { text } = await generateText({
        model,
        system,
        prompt:
          instruction +
          '\n\nIMPORTANT: Return a single JSON object ONLY. Do not include markdown, comments, or trailing text.',
        temperature: 0,
        maxOutputTokens: 1600,
      });
      const obj = tryParseJsonObject(text);
      const parsedObj = StrategyDailyResponseSchema.safeParse(obj);
      if (parsedObj.success) {
        return c.json({ ...parsedObj.data, model: modelId || parsedObj.data.model });
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
            `Strategy generation failed: ${msg}`,
            'Fallback JSON parse/validate failed.',
          ],
          model: modelId || 'unknown',
        },
        200,
      );
    } catch (e2) {
      const msg2 = e2 instanceof Error ? e2.message : String(e2);
      return c.json(
        {
          date,
          accountId: parsed.data.accountId,
          accountTitle,
          candidates: [],
          leader: { symbol: '', reason: '' },
          recommendations: [],
          riskNotes: [`Strategy generation failed: ${msg}`, `Fallback failed: ${msg2}`],
          model: modelId || 'unknown',
        },
        200,
      );
    }
  }
});

const port = Number(process.env.PORT ?? 4310);

serve({ fetch: app.fetch, port }, (info) => {
  console.log(`AI service listening on http://127.0.0.1:${info.port}`);
});
