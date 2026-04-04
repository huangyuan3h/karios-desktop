import { Hono } from 'hono';
import { generateText } from 'ai';

import { NewsSummaryRequestSchema } from '../schemas';
import { getResolvedModel, AiModel } from '../model';

export const newsRoutes = new Hono();

newsRoutes.post('/summary', async (c) => {
  const body = await c.req.json().catch(() => null);
  const parsed = NewsSummaryRequestSchema.safeParse(body);
  if (!parsed.success) {
    return c.json({ error: 'Invalid request body', issues: parsed.error.issues }, 400);
  }

  const items = parsed.data.items || [];
  const hours = parsed.data.hours || 24;

  if (!items.length) {
    return c.json({ summary: '', itemsCount: 0 });
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

  const newsTitles = items
    .slice(0, 50)
    .map((item, idx) => `${idx + 1}. ${item.title}`)
    .join('\n');

  const system = `你是一个财经新闻分析师。你的任务是从过去${hours}小时的新闻中，总结出与财经、股票市场相关的关键信息。

输出格式要求：
1. 使用数字列表格式（1. 2. 3. ...）列出要点
2. 每个要点用一句话概括，简洁明了
3. 总字数控制在 300-400 字
4. 只挑选最重要的、与财经/股票相关的新闻
5. 按重要性排序，最重要的放在前面
6. 如果没有重要财经新闻，简要说明即可
7. 使用简洁专业的中文表达`;

  const prompt = `以下是过去${hours}小时的新闻标题：

${newsTitles}

请用数字列表格式（1. 2. 3. ...）总结其中的财经/股票相关重要信息（300-400字）：`;

  try {
    const { text } = await generateText({
      model,
      prompt: `${system}\n\n${prompt}`,
      temperature: 0,
      maxOutputTokens: 500,
    });
    return c.json({
      summary: text.trim(),
      itemsCount: items.length,
      model: modelId,
    });
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return c.json({ error: msg }, 500);
  }
});
