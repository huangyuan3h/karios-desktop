import { Hono } from 'hono';
import { generateObject } from 'ai';

import { QuantRankExplainRequestSchema, QuantRankExplainResponseSchema } from '../schemas';
import { getStrategyPrimaryAndFallbackModels, AiModel } from '../model';

export const quantRoutes = new Hono();

quantRoutes.post('/rank/explain', async (c) => {
  const body = await c.req.json().catch(() => null);
  const parsed = QuantRankExplainRequestSchema.safeParse(body);
  if (!parsed.success) {
    return c.json({ error: 'Invalid request body', issues: parsed.error.issues }, 400);
  }

  let model: AiModel;
  let fallbackModel: AiModel | null = null;
  let modelId = '';
  let fallbackModelId: string | null = null;
  try {
    const r = await getStrategyPrimaryAndFallbackModels();
    model = r.model;
    modelId = r.modelId;
    fallbackModel = r.fallbackModel;
    fallbackModelId = r.fallbackModelId;
  } catch (err) {
    const message = err instanceof Error ? err.message : 'Invalid AI configuration';
    return c.json({ error: message }, 500);
  }

  const fallbackLabel = fallbackModelId || modelId || 'fallback';

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
        return c.json({ ...out, model: fallbackLabel || out.model });
      } catch {
        // fallthrough
      }
    }
    const msg = e instanceof Error ? e.message : String(e);
    return c.json(
      {
        asOfTs,
        asOfDate,
        items: [],
        model: modelId || 'unknown',
        error: `Quant rerank failed: ${msg}`,
      },
      200,
    );
  }
});
