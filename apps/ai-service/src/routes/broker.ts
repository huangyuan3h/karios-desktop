import { Hono } from 'hono';
import { generateText } from 'ai';

import { BrokerExtractRequestSchema, BrokerExtractResponseSchema } from '../schemas';
import { getResolvedModel, AiModel } from '../model';
import { tryParseJsonObject } from '../json_parse';

export const brokerRoutes = new Hono();

brokerRoutes.post('/pingan', async (c) => {
  const body = await c.req.json().catch(() => null);
  const parsed = BrokerExtractRequestSchema.safeParse(body);
  if (!parsed.success) {
    return c.json({ error: 'Invalid request body', issues: parsed.error.issues }, 400);
  }

  let model: AiModel;
  let modelId: string;
  try {
    const r = await getResolvedModel();
    model = r.model;
    modelId = r.modelId;
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
          data: { rawText: text, model: modelId },
        },
        200,
      );
    }
    return c.json({ ...out.data, model: modelId });
  } catch {
    return c.json(
      {
        kind: 'unknown',
        broker: 'pingan',
        extractedAt: new Date().toISOString(),
        data: { rawText: text, model: modelId },
      },
      200,
    );
  }
});
