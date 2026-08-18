import { describe, expect, it, vi, beforeEach } from 'vitest';

const mocks = vi.hoisted(() => ({
  streamText: vi.fn(),
  getDecisionModelBundle: vi.fn(),
  getResolvedModel: vi.fn(),
  fetchJson: vi.fn(),
  storeFetch: vi.fn(),
}));

vi.mock('ai', () => ({ streamText: mocks.streamText }));
vi.mock('../model', () => ({
  getResolvedModel: mocks.getResolvedModel,
  getDecisionModelBundle: mocks.getDecisionModelBundle,
}));

import { weeklyPlanRoutes } from './weekly_plan.js';
import { Hono } from 'hono';

async function* fakeStream(chunks: string[]) {
  for (const c of chunks) yield c;
}

beforeEach(() => {
  vi.clearAllMocks();
  mocks.fetchJson.mockImplementation(async (path: string) => {
    if (String(path).includes('/weekly-review')) {
      return { markdown: '# 周报 markdown', week: { start: '2026-08-03', end: '2026-08-07' } };
    }
    if (String(path).includes('/portfolio-health')) return { regime: 'Weak' };
    if (String(path).includes('/recon/latest')) {
      return { items: [{ market: 'HK', missing: 19, expected: 19, actual: 0 }] };
    }
    if (String(path).includes('/overview')) return { rollingOos: { warning: true } };
    return null;
  });
  mocks.getDecisionModelBundle.mockResolvedValue({ provider: 'google', model: {} });
  mocks.streamText.mockResolvedValue({
    textStream: fakeStream(['# 下周行动计划\n\n### 买入\n- HK:02343 补入核查']),
    finishReason: 'stop',
  });
});

describe('weekly-plan', () => {
  it('generates a plan from prefetched context with provider prefix', async () => {
    const app = new Hono();
    app.route('/weekly-plan', weeklyPlanRoutes);
    const res = await app.request('/weekly-plan', { method: 'POST' });
    const body = (await res.json()) as { ok?: boolean; provider?: string; plan?: string };
    const endDate = new Date();
    const backDays = (endDate.getDay() + 2) % 7;
    const friday = new Date(endDate);
    friday.setDate(friday.getDate() - backDays);
    const fridayStr = friday.toISOString().slice(0, 10);
    expect(res.status).toBe(200);
    expect(body.ok).toBe(true);
    expect(body.provider).toBe('google');
    expect(body.plan).toContain(`# 下周行动计划（${fridayStr} 周报 · google）`);
    expect(mocks.streamText).toHaveBeenCalledTimes(1);
  });

  it('falls back to resolved model when primary stream is empty', async () => {
    mocks.streamText
      .mockResolvedValueOnce({ textStream: fakeStream([]), finishReason: 'error' })
      .mockResolvedValueOnce({
        textStream: fakeStream(['# 下周行动计划（fallback）']),
        finishReason: 'stop',
      });
    mocks.getResolvedModel.mockResolvedValue({ provider: 'openai', model: {} });
    const app = new Hono();
    app.route('/weekly-plan', weeklyPlanRoutes);
    const res = await app.request('/weekly-plan', { method: 'POST' });
    const body = (await res.json()) as { provider?: string; plan?: string };
    expect(body.provider).toBe('openai');
    expect(body.plan).toContain('fallback');
  });

  it('returns error when both providers fail', async () => {
    mocks.streamText.mockResolvedValue({ textStream: fakeStream([]), finishReason: 'error' });
    mocks.getResolvedModel.mockResolvedValue({ provider: 'openai', model: {} });
    const app = new Hono();
    app.route('/weekly-plan', weeklyPlanRoutes);
    const res = await app.request('/weekly-plan', { method: 'POST' });
    const body = (await res.json()) as { ok?: boolean; error?: string };
    expect(body.ok).toBe(false);
    expect(body.error).toContain('模型暂不可用');
  });
});
