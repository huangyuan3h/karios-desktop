import { Hono } from 'hono';
import { generateObject } from 'ai';

import { MainlineExplainRequestSchema, MainlineExplainResponseSchema } from '../schemas';
import {
  getStrategyPrimaryAndFallbackModels,
  AiModel,
  generateObjectCompatOptions,
} from '../model';

export const mainlineRoutes = new Hono();

mainlineRoutes.post('/explain', async (c) => {
  const body = await c.req.json().catch(() => null);
  const parsed = MainlineExplainRequestSchema.safeParse(body);
  if (!parsed.success) {
    return c.json({ error: 'Invalid request body', issues: parsed.error.issues }, 400);
  }

  let model: AiModel;
  let fallbackModel: AiModel | null = null;
  let modelId = '';
  let fallbackModelId: string | null = null;
  let looseStructuredOutputs = false;
  try {
    const r = await getStrategyPrimaryAndFallbackModels();
    model = r.model;
    modelId = r.modelId;
    fallbackModel = r.fallbackModel;
    fallbackModelId = r.fallbackModelId;
    looseStructuredOutputs = r.looseStructuredOutputs;
  } catch (err) {
    const message = err instanceof Error ? err.message : 'Invalid AI configuration';
    return c.json({ error: message }, 500);
  }

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
      ...generateObjectCompatOptions(looseStructuredOutputs),
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
