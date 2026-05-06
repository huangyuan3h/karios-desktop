import { Hono } from 'hono';
import { generateObject, generateText } from 'ai';
import { z } from 'zod';

import {
  StrategyDailyRequestSchema,
  StrategyDailyResponseSchema,
  StrategyCandidatesResponseSchema,
  StrategyDailyMarkdownResponseSchema,
} from '../schemas';
import { buildContextMarkdown, buildPromptDebug, asTrimmedString } from '../utils';
import {
  getStrategyPrimaryAndFallbackModels,
  AiModel,
  generateObjectCompatOptions,
} from '../model';
import { tryParseJsonObject } from '../json_parse';

export const strategyRoutes = new Hono();

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

strategyRoutes.post('/daily', async (c) => {
  const body = await c.req.json().catch(() => null);
  const parsed = StrategyDailyRequestSchema.safeParse(body);
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
    '- Candidate universe: use ONLY the provided TradingView snapshots + the provided stocks list + current holdings + watchlist.\n' +
    '- Output <= 5 candidates with score 0-100 and rank.\n' +
    '- Pick a single leader (龙头) and explain why.\n' +
    '- Recommend <= 3 symbols (do not exceed 3).\n' +
    '- Orders must be conditional-order style. Provide clear trigger and quantity.\n' +
    '- Always include levels.support/resistance/invalidations arrays (use empty arrays if unknown).\n' +
    '- Always include riskNotes arrays (use empty arrays if none).\n' +
    '- watchlist: if context.watchlist.items exists, it is user-curated and includes fields like trendOk/score/stopLoss/buyAction; use it to prioritize.\n' +
    '- Use the SAME language as the user/account prompt (Chinese is expected).\n\n' +
    (accountPrompt ? `Account prompt:\n${accountPrompt}\n\n` : '') +
    'Context (markdown):\n' +
    buildContextMarkdown(parsed.data.context);

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
      ...generateObjectCompatOptions(looseStructuredOutputs),
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
      modelName: fallbackModelId || modelId || 'fallback',
    });
  attempts.push({ kind: 'text', model, modelName: modelId || 'primary' });
  if (fallbackModel)
    attempts.push({
      kind: 'text',
      model: fallbackModel,
      modelName: fallbackModelId || modelId || 'fallback',
    });

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

strategyRoutes.post('/candidates', async (c) => {
  const body = await c.req.json().catch(() => null);
  const parsed = StrategyDailyRequestSchema.safeParse(body);
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
    '- If context.watchlist.items exists, you MAY use it as additional candidate hints (user-curated list with trendOk/score).\n' +
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
    'Context (markdown):\n' +
    buildContextMarkdown(parsed.data.context);

  const promptDebug = buildPromptDebug({
    system,
    promptText: instruction,
    context: parsed.data.context,
  });

  async function run(m: AiModel): Promise<unknown> {
    const { object } = await generateObject({
      model: m,
      schema: StrategyCandidatesResponseSchema,
      system,
      prompt: instruction,
      temperature: 0,
      maxOutputTokens: 1800,
      ...generateObjectCompatOptions(looseStructuredOutputs),
    });
    return object;
  }

  try {
    const obj = await run(model);
    const out = StrategyCandidatesResponseSchema.parse(obj);
    return c.json({ ...out, model: modelId || out.model, promptDebug });
  } catch (e) {
    if (fallbackModel) {
      try {
        const obj = await run(fallbackModel);
        const out = StrategyCandidatesResponseSchema.parse(obj);
        return c.json({ ...out, model: fallbackModelId || modelId || out.model, promptDebug });
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
        promptDebug,
      },
      200,
    );
  }
});

strategyRoutes.post('/daily-markdown', async (c) => {
  const body = await c.req.json().catch(() => null);
  const parsed = StrategyDailyRequestSchema.safeParse(body);
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
  const accountTitle = (parsed.data.accountTitle ?? '').trim() || 'Account';
  const accountPrompt = (parsed.data.accountPrompt ?? '').trim();

  const system =
    'You are a swing trading strategy engine. ' +
    'You must produce an actionable daily plan using conditional-order style recipes. ' +
    'Focus on right-side trading and maximizing profit, but always define invalidation and risk boundaries. ' +
    'Return Markdown only. No JSON.';

  const instruction =
    `Task: Write a daily trading report for ${accountTitle} on ${date}.\n` +
    'Output requirements:\n' +
    '- Return a SINGLE Markdown document.\n' +
    '- LANGUAGE: Chinese (Simplified).\n' +
    '- Use ONLY these H2 headings EXACTLY (no extra headings, no "###"):\n' +
    '  - "## 1 总览"\n' +
    '  - "## 2 机会Top3"\n' +
    '  - "## 3 持仓计划"\n' +
    '  - "## 4 条件单总表"\n' +
    '  - "## 5 总结"\n' +
    '- SECTION ORDER rule (MUST follow, for each section):\n' +
    '  - Heading line\n' +
    '  - TABLE immediately (no prose before the table)\n' +
    '  - Then 1 short paragraph (<=150字) or 3-5 bullets (no tables)\n' +
    '- STRICT TABLE rules (MUST follow):\n' +
    '  - Tables MUST be valid GFM markdown tables.\n' +
    '  - Each table row MUST be on its own line.\n' +
    '  - No leading spaces before any "|" table line.\n' +
    '  - NEVER use "|" in normal paragraphs/bullets. Pipes are only allowed inside tables.\n' +
    '  - Table cells MUST be single-line text (no "\\n"). If you need multiple points, use ";" within the cell.\n' +
    '  - If data is missing, write "—" (do NOT omit columns).\n\n' +
    '## 1 总览\n\n' +
    '| Focus themes | Leader | Sentiment | Stance | Execution Key |\n' +
    '|---|---|---|---|---|\n' +
    '| 主线名称 | 龙头股 | 情绪定性 | 进攻/均衡/防守 | 一句话风控准则 |\n\n' +
    '在表格下面用 1 段短文（<=150字）概括：主线+情绪+行业资金结论，并给出今日唯一硬准则（必须可执行，拒绝虚词）。\n\n' +
    '## 2 机会Top3\n\n' +
    '| Rank | Score | Symbol | Name | Current | Why | Risk |\n' +
    '|---:|---:|---|---|---:|---|---|\n' +
    '| 1 | 0 | CN:000000 | 示例 | — | — | — |\n' +
    '| 2 | 0 | CN:000000 | 示例 | — | — | — |\n' +
    '| 3 | 0 | CN:000000 | 示例 | — | — | — |\n\n' +
    '在表格下面写 1 句话：这 3 个机会的共同结构特征（必须具体，禁止套话）。\n\n' +
    '## 3 持仓计划\n\n' +
    '| Symbol | Name | PnL% | Action | Score | StopLoss | Orders | Notes |\n' +
    '|---|---|---:|---|---:|---|---|---|\n' +
    '| CN:000000 | 示例 | — | Hold/Reduce/Exit | 0 | — | — | — |\n\n' +
    '在表格下面写 1 句话：当前持仓风险集中在哪（只说最关键的 1 个点）。\n\n' +
    '## 4 条件单总表\n\n' +
    '| Priority | Symbol | Name | Action | OrderType | TriggerCondition | TriggerValue | Qty | Rationale |\n' +
    '|---:|---|---|---|---|---|---:|---:|---|\n' +
    '| 1 | CN:000000 | 示例 | Buy/Sell | 到价买入/到价卖出 | 价格上穿/价格下穿/到价 | 0 | 0 | — |\n\n' +
    '在表格下面写 3-5 条 bullet：录入顺序与撤单规则（每条必须可执行）。\n\n' +
    '## 5 总结\n\n' +
    '用 2 句话点明：今日胜负手 + 盘中最该盯的 1 个变量。\n\n' +
    'CRITICAL RULES:\n' +
    '1. Data-grounded: Use ONLY provided Context JSON. No external knowledge.\n' +
    '2. NO JARGON/TRUISMS: Delete sentences like "优先选择量价强...". Use concrete data-backed statements.\n' +
    '3. TABLE STABILITY: Every table must have correct header + separator. Section 2 must have EXACTLY 3 data rows.\n' +
    '4. ACTIONABLE ONLY: If context.marketSentiment.latest implies "no new positions", then Section 4 must NOT include any Buy actions.\n' +
    '4b. If context.watchlist.items exists, it is user watchlist with trendOk/score/stopLoss/buyAction; use it to improve Top3 and Holding plans.\n' +
    '5. Avoid internal variable names (riskMode/ratio/premium/failedRate). Translate them into trader language.\n\n' +
    (accountPrompt ? `Account prompt:\n${accountPrompt}\n\n` : '') +
    'Context (markdown):\n' +
    buildContextMarkdown(parsed.data.context);

  const promptDebug = buildPromptDebug({
    system,
    promptText: instruction,
    context: parsed.data.context,
  });

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
    return c.json({ ...out, promptDebug });
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
        return c.json({ ...out, promptDebug });
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
          `# Daily Strategy Report\n\n` +
          `- Date: ${date}\n` +
          `- Account: ${accountTitle}\n\n` +
          `## Error\n\n` +
          `Strategy generation failed: ${msg}\n`,
        model: modelId || 'unknown',
        promptDebug,
      },
      200,
    );
  }
});
