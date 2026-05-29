import { Hono } from 'hono';
import { generateObject, generateText } from 'ai';

import {
  normalizeAlphaRadarExtract,
  parseAlphaRadarBatchExtract,
} from '../alphaRadarBatchNormalize';
import { tryParseJsonObject } from '../json_parse';
import {
  AlphaRadarExtractRequestSchema,
  AlphaRadarExtractResponseSchema,
  AlphaRadarExtractBatchRequestSchema,
  AlphaRadarExtractBatchResponseSchema,
  AlphaRadarMapCnRequestSchema,
  AlphaRadarMapCnResponseSchema,
} from '../schemas';
import {
  getStrategyPrimaryAndFallbackModels,
  AiModel,
  generateObjectCompatOptions,
  generateTextJsonObjectModeOptions,
} from '../model';

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

  const system =
    'You are a top-tier global macro and technology analyst. ' +
    'Extract incremental technology/industry trends from source documents. ' +
    'Ignore routine financial metrics unless they imply a structural tech shift. ' +
    'Base ONLY on provided text. Do NOT fabricate facts, numbers, or policy documents. ' +
    'macro_theme and catalyst_grade are REQUIRED for every trend. ' +
    'Return valid JSON matching the schema. No markdown fences.';

  const instruction =
    `Source category: ${category}\n` +
    `Title: ${title}\n` +
    `URL: ${sourceUrl}\n\n` +
    'Task: Extract up to 3 highest-signal incremental trends.\n' +
    'For each trend return JSON fields:\n' +
    '- macro_theme: standardized English theme bucket (e.g. "Next-Gen Energy", "HBM Supply Chain")\n' +
    '- catalyst_grade: S|A|B|C (S = imminent structural catalyst)\n' +
    '- trend_name: optional display title (English, optionally with Chinese in parentheses); defaults to macro_theme\n' +
    '- catalyst: 1-2 evidence sentences\n' +
    '- global_target: US/global ticker or company if applicable, else "N/A"\n' +
    '- keywords_for_mapping: 2-5 Chinese keywords targeting upstream components/materials (NOT system integrators)\n\n' +
    'Document text:\n' +
    condensed;

  async function run(m: AiModel): Promise<unknown> {
    const { object } = await generateObject({
      model: m,
      schema: AlphaRadarExtractResponseSchema,
      system,
      prompt: instruction,
      temperature: 0,
      maxOutputTokens: 2000,
      ...generateObjectCompatOptions(looseStructuredOutputs),
    });
    return object;
  }

  try {
    const obj = await run(model);
    const out = AlphaRadarExtractResponseSchema.parse(normalizeAlphaRadarExtract(obj));
    return c.json({ ...out, model: modelId || out.model });
  } catch (e) {
    if (fallbackModel) {
      try {
        const obj = await run(fallbackModel);
        const out = AlphaRadarExtractResponseSchema.parse(normalizeAlphaRadarExtract(obj));
        return c.json({ ...out, model: fallbackModelId || out.model });
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
  const digest = docs
    .map(
      (d, idx) =>
        `### Item ${idx}\n` +
        `DocumentId: ${d.documentId}\n` +
        `Category: ${d.category}\n` +
        `Title: ${d.title}\n` +
        `URL: ${d.url}\n` +
        `Summary: ${(d.summary || '').trim() || '(none)'}\n`,
    )
    .join('\n');

  const system =
    'You are a top-tier global semiconductor and AI infrastructure analyst. ' +
    'Read a batch of headlines/summaries and extract the highest-signal incremental industry trends. ' +
    'Prioritize: semiconductors, AI datacenter, HBM/memory, advanced packaging, optical modules, ' +
    'hyperscaler capex, earnings call transcripts, and supply-chain shifts. ' +
    'Deprioritize pure biomedical, consumer lifestyle, crypto, and politics unless directly tied to chip supply. ' +
    'Merge duplicate themes across items. Base ONLY on provided text. ' +
    'macro_theme and catalyst_grade are REQUIRED for every trend. ' +
    'Return valid JSON. No markdown fences.';

  const instruction =
    `Batch size: ${docs.length} items\n\n` +
    'Task: Extract up to 8 distinct trends across ALL items (deduplicate similar themes).\n' +
    'Return JSON: {"trends":[{"macro_theme","catalyst_grade","catalyst","global_target","keywords_for_mapping","source_index"}]}\n' +
    'macro_theme = standardized English theme bucket (e.g. "Next-Gen Energy").\n' +
    'catalyst_grade = S|A|B|C (S = imminent structural catalyst).\n' +
    'source_index = 0-based item number.\n' +
    'keywords_for_mapping = 2-5 Chinese strings targeting upstream components/materials (NOT system integrators).\n\n' +
    digest;

  const jsonSuffix =
    '\n\nOutput ONLY one JSON object with key "trends" (array). No markdown fences.';

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
      system,
      prompt: instruction,
      temperature: 0,
      maxOutputTokens: 3500,
      ...generateObjectCompatOptions(looseStructuredOutputs),
    });
    return finalize('generateObject', object, mid);
  }

  async function runText(m: AiModel, mid: string) {
    const { text } = await generateText({
      model: m,
      system,
      prompt: instruction + jsonSuffix,
      temperature: 0,
      maxOutputTokens: 3500,
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

  const { trend, candidates, externalContext, allowKnowledgeFallback } = parsed.data;
  const knowledgeFallback = Boolean(allowKnowledgeFallback) || candidates.length === 0;
  const system =
    'You map global technology trends to pure-play A-share leaders. ' +
    (candidates.length > 0
      ? 'Pick at most 2 symbols ONLY from the candidate list unless external context strongly supports a listed name. '
      : 'No local candidates were found. You MAY suggest up to 2 well-known A-share leaders using your knowledge, but confidence must be <= 0.45 and rationale must say manual review required. ') +
    'Symbol format must be CN:xxxxxx (6-digit ticker). ' +
    'Prefer industry purity over size. Mark low confidence when evidence is weak. ' +
    'When mapping A-share leaders, AVOID system integrators / OEM assemblers (e.g. Inspur 浪潮, Sugon 中科曙光, generic server brands). ' +
    'Drill down one supply-chain layer to pure-play component/material leaders, such as: ' +
    'liquid cooling (Envicool 英维克), CPO / optical modules (Zhongji Innolight 中际旭创), ' +
    'advanced packaging materials, high-frequency high-speed PCB. ' +
    'Prefer highest industry purity and direct revenue exposure to the trend. ' +
    'If candidates include both integrators and component suppliers, always pick suppliers. ' +
    'Return valid JSON. No markdown fences.';

  const instruction =
    'Trend JSON:\n' +
    JSON.stringify(trend) +
    '\n\nCandidate A-shares:\n' +
    (candidates.length ? JSON.stringify(candidates) : '(empty — use cautious knowledge fallback if allowed)') +
    (externalContext ? `\n\nExternal search context:\n${externalContext}` : '') +
    (knowledgeFallback
      ? '\n\nIf candidates are empty, return best-effort CN: symbols with confidence <= 0.45.'
      : '') +
    '\n\nReturn cnSymbols (max 2) with symbol (CN:xxxxxx), name, confidence (0-1), rationale (Chinese). ' +
    'Rationale must explain why component/material suppliers were chosen over system integrators when applicable.';

  async function run(m: AiModel): Promise<unknown> {
    const { object } = await generateObject({
      model: m,
      schema: AlphaRadarMapCnResponseSchema,
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
    const out = AlphaRadarMapCnResponseSchema.parse(obj);
    return c.json({ ...out, model: modelId || out.model });
  } catch (e) {
    if (fallbackModel) {
      try {
        const obj = await run(fallbackModel);
        const out = AlphaRadarMapCnResponseSchema.parse(obj);
        return c.json({ ...out, model: fallbackModelId || out.model });
      } catch (fallbackErr) {
        const msg = fallbackErr instanceof Error ? fallbackErr.message : String(fallbackErr);
        return c.json({ error: msg }, 500);
      }
    }
    const msg = e instanceof Error ? e.message : String(e);
    return c.json({ error: msg }, 500);
  }
});
