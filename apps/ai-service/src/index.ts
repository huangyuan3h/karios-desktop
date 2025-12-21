import 'dotenv/config';

import { serve } from '@hono/node-server';
import { cors } from 'hono/cors';
import { Hono } from 'hono';
import { generateText, streamText } from 'ai';
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
  kind: z.enum(['account_overview', 'positions', 'conditional_orders', 'unknown']),
  broker: z.literal('pingan'),
  extractedAt: z.string(),
  data: z.record(z.any()).optional(),
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
    '- kind: one of account_overview | positions | conditional_orders | unknown\n' +
    '- broker: "pingan"\n' +
    '- extractedAt: ISO timestamp\n' +
    '- data: object with extracted fields\n\n' +
    'For account_overview, data may include: currency, totalAssets, securitiesValue, cashAvailable, withdrawable, pnlTotal, pnlToday, accountIdMasked.\n' +
    'For positions, data may include: currency, accountIdMasked, positions: [{ ticker, name, qtyHeld, qtyAvailable, price, cost, pnl, pnlPct, marketValue }].\n' +
    'For conditional_orders, data may include: orders: [{ ticker, name, side, triggerCondition, triggerValue, qty, status, validUntil }].\n\n' +
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

const port = Number(process.env.PORT ?? 4310);

serve({ fetch: app.fetch, port }, (info) => {
  console.log(`AI service listening on http://127.0.0.1:${info.port}`);
});
