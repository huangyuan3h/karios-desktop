import { Hono } from 'hono';
import { generateObject, generateText } from 'ai';

import {
  normalizeAlphaRadarExtract,
  parseAlphaRadarBatchExtract,
} from '../alphaRadarBatchNormalize.js';
import {
  ALPHA_RADAR_V4_JSON_SUFFIX,
  ALPHA_RADAR_V4_SYSTEM_PROMPT,
  buildExtractBatchInstruction,
  buildExtractInstruction,
  buildMapCnInstruction,
  buildMapCnSystemPrompt,
} from '../alphaRadarPrompts.js';
import { tryParseJsonObject } from '../json_parse.js';
import {
  AlphaRadarExtractRequestSchema,
  AlphaRadarExtractResponseSchema,
  AlphaRadarExtractBatchRequestSchema,
  AlphaRadarExtractBatchResponseSchema,
  AlphaRadarMapCnRequestSchema,
  AlphaRadarMapCnResponseSchema,
} from '../schemas.js';
import {
  getStrategyPrimaryAndFallbackModels,
  AiModel,
  generateObjectCompatOptions,
  generateTextJsonObjectModeOptions,
} from '../model.js';

export const alphaRadarRoutes = new Hono();

const CHUNK_SIZE = 12000;
const CHUNK_OVERLAP = 400;

function chunkText(text: string, chunkSize = CHUNK_SIZE, overlap = CHUNK_OVERLAP): string[] {
  const normalized = text.replace(/\r\n/g, '\n').trim();
  if (!normalized) return [];
  if (normalized.length <= chunkSize) return [normalized];
  const chunks: string[] = [];
  let start = 0;
  while (start < normalized.length) {
    const end = Math.min(start + chunkSize, normalized.length);
    chunks.push(normalized.slice(start, end));
    if (end >= normalized.length) break;
    start = Math.max(0, end - overlap);
  }
  return chunks;
}

alphaRadarRoutes.post('/extract', async (c) => {
  const body = await c.req.json().catch(() => null);
  const parsed = AlphaRadarExtractRequestSchema.safeParse(body);
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

  const { text, title, category, sourceUrl } = parsed.data;
  const chunks = chunkText(text);
  const condensed =
    chunks.length <= 1
      ? text
      : chunks
          .map((chunk, idx) => `### Segment ${idx + 1}/${chunks.length}\n${chunk.slice(0, 8000)}`)
          .join('\n\n')
          .slice(0, 48000);

  const instruction = buildExtractInstruction({
    category,
    title,
    sourceUrl,
    text: condensed,
  });

  async function runObject(m: AiModel): Promise<unknown> {
    const { object } = await generateObject({
      model: m,
      schema: AlphaRadarExtractResponseSchema,
      system: ALPHA_RADAR_V4_SYSTEM_PROMPT,
      prompt: instruction,
      temperature: 0,
      // Thinking models may spend tokens on CoT before JSON; keep headroom.
      maxOutputTokens: 4000,
      ...generateObjectCompatOptions(looseStructuredOutputs),
    });
    return object;
  }

  async function runText(m: AiModel): Promise<unknown> {
    const { text } = await generateText({
      model: m,
      system: ALPHA_RADAR_V4_SYSTEM_PROMPT,
      prompt: instruction + ALPHA_RADAR_V4_JSON_SUFFIX,
      temperature: 0,
      maxOutputTokens: 4000,
      ...generateTextJsonObjectModeOptions(looseStructuredOutputs),
    });
    return tryParseJsonObject(text);
  }

  async function attempt(m: AiModel, mid: string) {
    const failures: string[] = [];
    try {
      const obj = await runObject(m);
      const out = AlphaRadarExtractResponseSchema.parse(
        normalizeAlphaRadarExtract(obj, category),
      );
      return { ...out, model: mid || out.model };
    } catch (e) {
      failures.push(e instanceof Error ? e.message : String(e));
    }
    try {
      const obj = await runText(m);
      const out = AlphaRadarExtractResponseSchema.parse(
        normalizeAlphaRadarExtract(obj, category),
      );
      return { ...out, model: mid || out.model };
    } catch (e) {
      failures.push(e instanceof Error ? e.message : String(e));
      throw new Error(failures.join(' | '));
    }
  }

  try {
    return c.json(await attempt(model, modelId));
  } catch (e) {
    if (fallbackModel) {
      try {
        return c.json(await attempt(fallbackModel, fallbackModelId || modelId));
      } catch (fallbackErr) {
        const msg = fallbackErr instanceof Error ? fallbackErr.message : String(fallbackErr);
        return c.json({ error: msg }, 500);
      }
    }
    const msg = e instanceof Error ? e.message : String(e);
    return c.json({ error: msg }, 500);
  }
});

alphaRadarRoutes.post('/extract-batch', async (c) => {
  const body = await c.req.json().catch(() => null);
  const parsed = AlphaRadarExtractBatchRequestSchema.safeParse(body);
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

  const docs = parsed.data.documents;
  const instruction = buildExtractBatchInstruction(docs);

  async function finalize(label: string, raw: unknown, mid: string) {
    const parsedOut = parseAlphaRadarBatchExtract(raw);
    if (parsedOut.success) {
      return { ...parsedOut.data, model: mid };
    }
    throw new Error(`${label}: ${parsedOut.error.message}`);
  }

  async function runObject(m: AiModel, mid: string) {
    const { object } = await generateObject({
      model: m,
      schema: AlphaRadarExtractBatchResponseSchema,
      system: ALPHA_RADAR_V4_SYSTEM_PROMPT,
      prompt: instruction,
      temperature: 0,
      maxOutputTokens: 6000,
      ...generateObjectCompatOptions(looseStructuredOutputs),
    });
    return finalize('generateObject', object, mid);
  }

  async function runText(m: AiModel, mid: string) {
    const { text } = await generateText({
      model: m,
      system: ALPHA_RADAR_V4_SYSTEM_PROMPT,
      prompt: instruction + ALPHA_RADAR_V4_JSON_SUFFIX,
      temperature: 0,
      maxOutputTokens: 6000,
      ...generateTextJsonObjectModeOptions(looseStructuredOutputs),
    });
    return finalize('generateText', tryParseJsonObject(text), mid);
  }

  const failures: string[] = [];

  async function attemptWithModel(m: AiModel, mid: string) {
    try {
      return await runObject(m, mid);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      failures.push(msg);
    }
    try {
      return await runText(m, mid);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      failures.push(msg);
      return null;
    }
  }

  let result = await attemptWithModel(model, modelId);
  if (!result && fallbackModel && fallbackModelId) {
    result = await attemptWithModel(fallbackModel, fallbackModelId);
  }
  if (result) {
    return c.json(result);
  }

  return c.json(
    { error: failures.length ? failures.join(' | ') : 'extract-batch failed' },
    500,
  );
});

alphaRadarRoutes.post('/map-cn', async (c) => {
  const body = await c.req.json().catch(() => null);
  const parsed = AlphaRadarMapCnRequestSchema.safeParse(body);
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

  const { trend, candidates, externalContext, allowKnowledgeFallback, seedSymbols } = parsed.data;
  const knowledgeFallback = Boolean(allowKnowledgeFallback) || candidates.length === 0;
  const seeds = Array.isArray(seedSymbols) ? seedSymbols : [];
  const system = buildMapCnSystemPrompt({
    candidateCount: candidates.length,
    seedSymbols: seeds,
  });
  const instruction = buildMapCnInstruction({
    trend,
    candidates,
    externalContext,
    knowledgeFallback,
    seedSymbols: seeds,
  });

  async function runObject(m: AiModel): Promise<unknown> {
    const { object } = await generateObject({
      model: m,
      schema: AlphaRadarMapCnResponseSchema,
      system,
      prompt: instruction,
      temperature: 0,
      maxOutputTokens: 2000,
      ...generateObjectCompatOptions(looseStructuredOutputs),
    });
    return object;
  }

  async function runText(m: AiModel): Promise<unknown> {
    const { text } = await generateText({
      model: m,
      system,
      prompt:
        instruction +
        '\n\nOutput ONLY one JSON object. No markdown fences. No <think> tags.',
      temperature: 0,
      maxOutputTokens: 2000,
      ...generateTextJsonObjectModeOptions(looseStructuredOutputs),
    });
    return tryParseJsonObject(text);
  }

  async function attempt(m: AiModel, mid: string) {
    const failures: string[] = [];
    try {
      const obj = await runObject(m);
      const out = AlphaRadarMapCnResponseSchema.parse(obj);
      return { ...out, model: mid || out.model };
    } catch (e) {
      failures.push(e instanceof Error ? e.message : String(e));
    }
    try {
      const obj = await runText(m);
      const out = AlphaRadarMapCnResponseSchema.parse(obj);
      return { ...out, model: mid || out.model };
    } catch (e) {
      failures.push(e instanceof Error ? e.message : String(e));
      throw new Error(failures.join(' | '));
    }
  }

  try {
    return c.json(await attempt(model, modelId));
  } catch (e) {
    if (fallbackModel) {
      try {
        return c.json(await attempt(fallbackModel, fallbackModelId || modelId));
      } catch (fallbackErr) {
        const msg = fallbackErr instanceof Error ? fallbackErr.message : String(fallbackErr);
        return c.json({ error: msg }, 500);
      }
    }
    const msg = e instanceof Error ? e.message : String(e);
    return c.json({ error: msg }, 500);
  }
});
