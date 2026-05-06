import { Hono } from 'hono';
import { generateObject, generateText } from 'ai';

import {
  InvestmentDailyReportRequestSchema,
  InvestmentDailyReportResponseSchema,
} from '../schemas';
import { parseInvestmentDailyReportAfterNormalize } from '../investmentDailyReportNormalize';
import { tryParseJsonObject } from '../json_parse';
import {
  getResolvedModel,
  AiModel,
  generateObjectCompatOptions,
  generateTextJsonObjectModeOptions,
} from '../model';

export const reportRoutes = new Hono();

reportRoutes.post('/investment-daily', async (c) => {
  const body = await c.req.json().catch(() => null);
  const parsed = InvestmentDailyReportRequestSchema.safeParse(body);
  if (!parsed.success) {
    return c.json({ error: 'Invalid request body', issues: parsed.error.issues }, 400);
  }

  let model: AiModel;
  let modelId: string;
  let looseStructuredOutputs = false;
  try {
    const r = await getResolvedModel();
    model = r.model;
    modelId = r.modelId;
    looseStructuredOutputs = r.looseStructuredOutputs;
  } catch (err) {
    const message = err instanceof Error ? err.message : 'Invalid AI configuration';
    return c.json({ error: message }, 500);
  }

  const markdown = parsed.data.markdown;

  const system = `你是中国 A 股市场的资深投研撰稿人。用户会提供从「Dashboard」导出的 Markdown（行业五日净流入/流出表、热点行业工作流、市场环境摘要、指数红绿灯与情绪表、宏观指数、新闻摘要、筛选器、自选等）。
你必须只依据 Markdown 中的事实写作：不得编造未出现的股票代码/名称、行业金额、指数点位。可引用表中具体行业名与资金数字。信息不足时明确写「依据不足」。

语言与去重（必须遵守）：
- 全文使用**简体中文**撰写；可保留常见英文缩写（如 MA、ETF、risk 档位名 hot/normal/caution），但不要整段英文论述。
- **禁止在不同字段重复同一观点**：「红绿灯/情绪」段落只写信号灯、广度、成交额与仓位建议；「主线与资金流向」只写 5D 行业净流入/流出与热点矩阵、与主线的关系，不要再复述红绿灯结论；「热点书面分析」只写工作流规则与观测行业逻辑，不要再次给出泛泛的仓位口号。
- 「市场环境要点提炼」用 4–8 条 Markdown 列表（每条以 "- " 开头），**压缩提炼**即可，不要整段照抄 Markdown 里「市场环境摘要」原文句子。

输出必须严格匹配 JSON schema：
- trafficLightPositionAndSentiment：在读者已看到指数红绿灯与情绪表的前提下，写 2–4 段中文解读：仓位建议、风险状态（hot/normal/caution 等）、与前一两个交易日的对比、操作纪律。语气专业、可执行。
- marketEnvironmentHighlights：把「市场环境摘要」压缩为 4–8 条要点，每条一行，以 "- " 开头（Markdown 列表），只保留对 A 股与资产配置最关键的信息（指数、汇率、商品、外盘对风险偏好影响等）。
- hotIndustriesFormalAnalysis：针对「Hot industries workflow」表格与规则，写一段正式书面分析（不少于 300 字）：动量突破 vs 5D 强势的含义、当前入选行业的资金与排名逻辑、对主线持续性的判断、与 Watchlist 动作纪律的衔接。
- capitalFlowAndMainline：综合「5D net inflow / outflow」与 Top5×Date 热点矩阵，写 3–6 段中文：主线与支线、流入/流出对立面、与热点行业工作流的一致性或不一致、短期风险。可分段落或小标题（纯文本）。
- topStocks：恰好 3 条，从筛选器与自选中择优；symbol 与 Markdown 一致；name 为中文简称且须与表中 Name 列或自选 Name 一致；rationale 每条 3–6 句，须引用分数/趋势/行业等已有字段。
- topNews：恰好 5 条；title 精简；summary 每条 2–3 句中文。`;

  const prompt = `以下为用户 Dashboard Markdown（可能已截断，文末会有说明）：

${markdown}

请生成投资要点日报 JSON（六段文字 + 3 只股票 + 5 条新闻）。`;

  const jsonOnlySuffix =
    '\n\n---\n' +
    '输出要求：只输出**一个** JSON 对象本体；不要使用 markdown 代码围栏；不要在 JSON 前后添加任何说明文字。\n' +
    '键名必须完全一致：trafficLightPositionAndSentiment, marketEnvironmentHighlights, hotIndustriesFormalAnalysis, capitalFlowAndMainline, topStocks, topNews。\n' +
    'topStocks 为长度 3 的数组，元素含 symbol, name, rationale；topNews 为长度 5 的数组，元素含 title, summary。';

  const failures: string[] = [];

  const tryFinalize = (label: string, raw: unknown) => {
    const fin = parseInvestmentDailyReportAfterNormalize(raw);
    if (fin.success) return fin.data;
    failures.push(`${label}: ${fin.error.message}`);
    return null;
  };

  try {
    try {
      const { object } = await generateObject({
        model,
        schema: InvestmentDailyReportResponseSchema,
        system,
        prompt,
        temperature: 0,
        maxOutputTokens: 8192,
        ...generateObjectCompatOptions(looseStructuredOutputs),
      });
      const ok = tryFinalize('generateObject', object);
      if (ok) return c.json({ ...ok, model: modelId });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      failures.push(`generateObject: ${msg}`);
    }

    const { text } = await generateText({
      model,
      system,
      prompt: prompt + jsonOnlySuffix,
      temperature: 0,
      maxOutputTokens: 8192,
      ...generateTextJsonObjectModeOptions(looseStructuredOutputs),
    });
    try {
      const obj = tryParseJsonObject(text);
      const ok = tryFinalize('generateText+json', obj);
      if (ok) return c.json({ ...ok, model: modelId });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      failures.push(`generateText+json: ${msg}`);
    }

    return c.json(
      {
        error: failures.length ? failures.join(' | ') : 'Investment daily report generation failed',
      },
      500,
    );
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return c.json({ error: msg }, 500);
  }
});
