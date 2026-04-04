import { Hono } from 'hono';
import { generateObject } from 'ai';

import { LeaderDailyRequestSchema, LeaderDailyResponseSchema } from '../schemas';
import { buildContextMarkdown } from '../utils';
import { getStrategyPrimaryAndFallbackModels, AiModel } from '../model';

export const leaderRoutes = new Hono();

leaderRoutes.post('/daily', async (c) => {
  const body = await c.req.json().catch(() => null);
  const parsed = LeaderDailyRequestSchema.safeParse(body);
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
    "- Prefer NEW leaders from today's industry themes + screener strength.\n" +
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
    'Context (markdown):\n' +
    buildContextMarkdown(parsed.data.context);

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
