import { Hono } from 'hono';
import { streamText } from 'ai';

import {
  getResolvedModel,
  getDecisionModelBundle,
  type ResolvedModelBundle,
} from '../model.js';
import { ThinkingStreamStripper } from '../model_thinking.js';

const DATA_SYNC_BASE_URL = process.env.DATA_SYNC_BASE_URL ?? 'http://127.0.0.1:4330';

async function fetchJson<T>(path: string, timeoutMs = 20_000): Promise<T | null> {
  try {
    const resp = await fetch(`${DATA_SYNC_BASE_URL}${path}`, {
      headers: { accept: 'application/json' },
      signal: AbortSignal.timeout(timeoutMs),
    });
    if (!resp.ok) return null;
    return (await resp.json()) as T;
  } catch {
    return null;
  }
}

export const weeklyPlanRoutes = new Hono();

const SYSTEM_PROMPT = `你是 Karios 的交易策略复核官（H2 周度自动驾驶复盘）。
输入：本周决策质量报告 + 实时持仓体检 + 回测 vs paper 对账 + 滚动 OOS 预警 + S-3 回测纪律。

任务：产出「下周行动计划」，只依据输入数据（数字就是证据），格式为 markdown：

## 下周行动计划
### 买入（候选/加仓）
- CN:xxxx / HK:xxxx · 仓位 X% · 理由（引用 score/RS/regime/回测缺票）
### 卖出/减仓
- symbol · 理由（触发哪条规则：止损/移动线/到期/EXIT）
### 条件单（券商端下发）
- symbol · 止损价 / 移动价 / 到期日
### 观察项（不操作，盯盘）
- 事件 · 触发什么条件才行动

铁律：
1. 没有数据支撑的票不要写（缺票清单可以建议「补入自选/提醒买入」，但不虚构推荐）
2. 回测口径是 source of truth：regime=Weak 的 A 股不开新仓；HK 按 HK 线规则
3. 区分「回测说该做」与「paper 实况」，漂移（对账缺/多票）要在行动里体现
4. 输出纯 markdown，不要解释生成过程`;

async function collectPlan(bundle: ResolvedModelBundle, context: string): Promise<string> {
  const result = await streamText({
    model: bundle.model,
    temperature: 0.2,
    messages: [
      { role: 'system', content: SYSTEM_PROMPT },
      { role: 'user', content: context },
    ],
    providerOptions:
      bundle.provider === 'google'
        ? { googleGenerativeAI: { thinkingConfig: { thinkingLevel: 'high' as const } } }
        : undefined,
  });
  const stripper = new ThinkingStreamStripper();
  let full = '';
  for await (const chunk of result.textStream) {
    const visible = stripper.push(chunk);
    if (visible) full += visible;
  }
  const rest = stripper.flush();
  if (rest) full += rest;
  console.error('[weekly-plan] finishReason=', result.finishReason, 'textLen=', full.length);
  return full.trim();
}

weeklyPlanRoutes.post('/', async (c) => {
  // Prefetch: weekly review markdown (week ending last Friday), live health,
  // recon, rolling OOS + long-window overview.
  const endDate = new Date();
  const backDays = (endDate.getDay() + 2) % 7;
  const friday = new Date(endDate);
  friday.setDate(friday.getDate() - backDays);
  const fridayStr = friday.toISOString().slice(0, 10);

  const review = await fetchJson<{ markdown?: string; week?: { start?: string; end?: string } }>(
    `/api/backtest/weekly-review?end=${fridayStr}`,
  );
  const health = await fetchJson<Record<string, unknown>>('/v1/agent/portfolio-health?markets=CN,HK');
  const recon = await fetchJson<{ items?: Array<Record<string, unknown>> }>(
    '/api/backtest/recon/latest?limit=2',
  );
  const overview = await fetchJson<{ rollingOos?: Record<string, unknown>; longWindowCN?: Record<string, unknown> }>(
    '/api/backtest/overview',
  );

  const context = [
    review?.markdown ? `# 本周决策质量报告（${review.week?.start ?? '?'} ~ ${review.week?.end ?? '?'}）\n${review.markdown}` : '（周报聚合失败）',
    health ? `# 实时持仓体检（CN+HK）\n${JSON.stringify(health, null, 1).slice(0, 6000)}` : '（体检获取失败）',
    recon?.items?.length
      ? `# 回测 vs Paper 对账\n${JSON.stringify(recon.items, null, 1)}`
      : '（对账数据为空）',
    overview?.rollingOos
      ? `# 滚动 OOS\n${JSON.stringify(overview.rollingOos, null, 1)}`
      : '（滚动 OOS 数据为空）',
    overview?.longWindowCN
      ? `# 长窗定案\n${JSON.stringify(overview.longWindowCN, null, 1)}`
      : '',
  ].join('\n\n');

  let primary: ResolvedModelBundle;
  try {
    primary = await getDecisionModelBundle();
  } catch (err) {
    const message = err instanceof Error ? err.message : 'Invalid AI configuration';
    return c.json({ error: message }, 500);
  }

  let text = '';
  let usedProvider = primary.provider;
  try {
    text = await collectPlan(primary, context);
  } catch (err) {
    console.error('weekly plan primary failed:', err);
  }
  if (!text.trim()) {
    try {
      const fb = await getResolvedModel();
      text = await collectPlan(fb, context);
      usedProvider = fb.provider;
    } catch (err) {
      console.error('weekly plan fallback failed:', err);
    }
  }
  if (!text.trim()) {
    return c.json(
      { ok: false, error: '模型暂不可用（primary 与 fallback 均无输出），请稍后重试。' },
      200,
    );
  }

  const plan = `# 下周行动计划（${fridayStr} 周报 · ${usedProvider}）\n\n${text}`;
  // Store for the frontend / history (best-effort).
  try {
    await fetch(`${DATA_SYNC_BASE_URL}/api/backtest/weekly-plan`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ markdown: plan }),
      signal: AbortSignal.timeout(10_000),
    });
  } catch (err) {
    console.warn('store weekly plan failed:', err);
  }
  return c.json({ ok: true, plan, provider: usedProvider });
});
