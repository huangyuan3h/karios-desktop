import { describe, expect, it } from 'vitest';
import {
  ConfigProfileCreateSchema,
  ConfigProfileUpdateSchema,
  ConfigSetActiveSchema,
  ConfigTestSchema,
  TitleRequestSchema,
  BrokerExtractRequestSchema,
  StrategyDailyRequestSchema,
  StrategyCandidatesRowSchema,
  LeaderDailyRequestSchema,
  MainlineExplainRequestSchema,
  QuantRankExplainRequestSchema,
  NewsSummaryRequestSchema,
} from './schemas';

describe('ConfigProfileCreateSchema', () => {
  it('validates valid openai profile', () => {
    const result = ConfigProfileCreateSchema.safeParse({
      name: 'Test Profile',
      provider: 'openai',
      modelId: 'gpt-4',
      openai: { apiKey: 'test-key' },
    });
    expect(result.success).toBe(true);
  });

  it('validates valid google profile', () => {
    const result = ConfigProfileCreateSchema.safeParse({
      name: 'Test Profile',
      provider: 'google',
      modelId: 'gemini-pro',
      google: { apiKey: 'test-key' },
    });
    expect(result.success).toBe(true);
  });

  it('validates valid ollama profile', () => {
    const result = ConfigProfileCreateSchema.safeParse({
      name: 'Test Profile',
      provider: 'ollama',
      modelId: 'llama2',
      ollama: { baseUrl: 'http://localhost:11434' },
    });
    expect(result.success).toBe(true);
  });

  it('rejects invalid provider', () => {
    const result = ConfigProfileCreateSchema.safeParse({
      name: 'Test',
      provider: 'invalid',
      modelId: 'test',
    });
    expect(result.success).toBe(false);
  });

  it('rejects missing name', () => {
    const result = ConfigProfileCreateSchema.safeParse({
      provider: 'openai',
      modelId: 'gpt-4',
    });
    expect(result.success).toBe(false);
  });

  it('accepts setActive optionally', () => {
    const result = ConfigProfileCreateSchema.safeParse({
      name: 'Test',
      provider: 'openai',
      modelId: 'gpt-4',
      setActive: true,
    });
    expect(result.success).toBe(true);
  });
});

describe('ConfigProfileUpdateSchema', () => {
  it('accepts partial updates', () => {
    const result = ConfigProfileUpdateSchema.safeParse({ name: 'New Name' });
    expect(result.success).toBe(true);
  });

  it('accepts empty object', () => {
    const result = ConfigProfileUpdateSchema.safeParse({});
    expect(result.success).toBe(true);
  });
});

describe('ConfigSetActiveSchema', () => {
  it('validates valid profileId', () => {
    const result = ConfigSetActiveSchema.safeParse({ profileId: 'test-id' });
    expect(result.success).toBe(true);
  });

  it('rejects empty profileId', () => {
    const result = ConfigSetActiveSchema.safeParse({ profileId: '' });
    expect(result.success).toBe(false);
  });
});

describe('ConfigTestSchema', () => {
  it('validates with profileId', () => {
    const result = ConfigTestSchema.safeParse({ profileId: 'test-id' });
    expect(result.success).toBe(true);
  });

  it('validates without profileId', () => {
    const result = ConfigTestSchema.safeParse({});
    expect(result.success).toBe(true);
  });
});

describe('TitleRequestSchema', () => {
  it('validates valid request', () => {
    const result = TitleRequestSchema.safeParse({ text: 'Hello world' });
    expect(result.success).toBe(true);
  });

  it('rejects empty text', () => {
    const result = TitleRequestSchema.safeParse({ text: '' });
    expect(result.success).toBe(false);
  });

  it('rejects text over 8000 chars', () => {
    const result = TitleRequestSchema.safeParse({ text: 'a'.repeat(8001) });
    expect(result.success).toBe(false);
  });

  it('accepts optional systemPrompt', () => {
    const result = TitleRequestSchema.safeParse({
      text: 'Hello',
      systemPrompt: 'Be helpful',
    });
    expect(result.success).toBe(true);
  });
});

describe('BrokerExtractRequestSchema', () => {
  it('validates valid imageDataUrl', () => {
    const result = BrokerExtractRequestSchema.safeParse({
      imageDataUrl: 'data:image/png;base64,test',
    });
    expect(result.success).toBe(true);
  });

  it('rejects empty imageDataUrl', () => {
    const result = BrokerExtractRequestSchema.safeParse({ imageDataUrl: '' });
    expect(result.success).toBe(false);
  });
});

describe('StrategyDailyRequestSchema', () => {
  it('validates valid request', () => {
    const result = StrategyDailyRequestSchema.safeParse({
      date: '2024-01-01',
      accountId: 'account-1',
      context: { stocks: [] },
    });
    expect(result.success).toBe(true);
  });

  it('accepts optional fields', () => {
    const result = StrategyDailyRequestSchema.safeParse({
      date: '2024-01-01',
      accountId: 'account-1',
      accountTitle: 'My Account',
      accountPrompt: 'Focus on tech',
      context: {},
    });
    expect(result.success).toBe(true);
  });

  it('rejects missing date', () => {
    const result = StrategyDailyRequestSchema.safeParse({
      accountId: 'account-1',
      context: {},
    });
    expect(result.success).toBe(false);
  });
});

describe('StrategyCandidatesRowSchema', () => {
  it('validates valid candidate', () => {
    const result = StrategyCandidatesRowSchema.safeParse({
      symbol: 'AAPL',
      market: 'US',
      ticker: 'AAPL',
      name: 'Apple',
      score: 85,
      rank: 1,
      why: 'Strong fundamentals',
    });
    expect(result.success).toBe(true);
  });

  it('accepts optional scoreBreakdown', () => {
    const result = StrategyCandidatesRowSchema.safeParse({
      symbol: 'AAPL',
      market: 'US',
      ticker: 'AAPL',
      name: 'Apple',
      score: 85,
      rank: 1,
      why: 'Strong fundamentals',
      scoreBreakdown: { trend: 30, flow: 20, structure: 15, risk: 5 },
    });
    expect(result.success).toBe(true);
  });

  it('rejects score over 100', () => {
    const result = StrategyCandidatesRowSchema.safeParse({
      symbol: 'AAPL',
      market: 'US',
      ticker: 'AAPL',
      name: 'Apple',
      score: 150,
      rank: 1,
      why: 'Test',
    });
    expect(result.success).toBe(false);
  });

  it('rejects rank over 5', () => {
    const result = StrategyCandidatesRowSchema.safeParse({
      symbol: 'AAPL',
      market: 'US',
      ticker: 'AAPL',
      name: 'Apple',
      score: 85,
      rank: 6,
      why: 'Test',
    });
    expect(result.success).toBe(false);
  });
});

describe('LeaderDailyRequestSchema', () => {
  it('validates valid request', () => {
    const result = LeaderDailyRequestSchema.safeParse({
      date: '2024-01-01',
      context: { candidates: [] },
    });
    expect(result.success).toBe(true);
  });

  it('rejects missing date', () => {
    const result = LeaderDailyRequestSchema.safeParse({ context: {} });
    expect(result.success).toBe(false);
  });
});

describe('MainlineExplainRequestSchema', () => {
  it('validates valid request', () => {
    const result = MainlineExplainRequestSchema.safeParse({
      date: '2024-01-01',
      themes: [{ kind: 'industry', name: 'Tech', evidence: {} }],
    });
    expect(result.success).toBe(true);
  });

  it('rejects empty themes', () => {
    const result = MainlineExplainRequestSchema.safeParse({
      date: '2024-01-01',
      themes: [],
    });
    expect(result.success).toBe(false);
  });

  it('rejects themes over 20', () => {
    const themes = Array(21).fill({ kind: 'industry', name: 'Test', evidence: {} });
    const result = MainlineExplainRequestSchema.safeParse({
      date: '2024-01-01',
      themes,
    });
    expect(result.success).toBe(false);
  });

  it('rejects invalid kind', () => {
    const result = MainlineExplainRequestSchema.safeParse({
      date: '2024-01-01',
      themes: [{ kind: 'invalid', name: 'Test', evidence: {} }],
    });
    expect(result.success).toBe(false);
  });
});

describe('QuantRankExplainRequestSchema', () => {
  it('validates valid request', () => {
    const result = QuantRankExplainRequestSchema.safeParse({
      asOfTs: '2024-01-01T10:00:00',
      asOfDate: '2024-01-01',
      horizon: '2d',
      objective: 'profit_probability',
      candidates: [{ symbol: 'AAPL', ticker: 'AAPL', evidence: {} }],
    });
    expect(result.success).toBe(true);
  });

  it('rejects invalid horizon', () => {
    const result = QuantRankExplainRequestSchema.safeParse({
      asOfTs: '2024-01-01T10:00:00',
      asOfDate: '2024-01-01',
      horizon: '1d',
      objective: 'profit_probability',
      candidates: [{ symbol: 'AAPL', ticker: 'AAPL', evidence: {} }],
    });
    expect(result.success).toBe(false);
  });

  it('rejects invalid objective', () => {
    const result = QuantRankExplainRequestSchema.safeParse({
      asOfTs: '2024-01-01T10:00:00',
      asOfDate: '2024-01-01',
      horizon: '2d',
      objective: 'max_return',
      candidates: [{ symbol: 'AAPL', ticker: 'AAPL', evidence: {} }],
    });
    expect(result.success).toBe(false);
  });

  it('rejects candidates over 30', () => {
    const candidates = Array(31).fill({ symbol: 'AAPL', ticker: 'AAPL', evidence: {} });
    const result = QuantRankExplainRequestSchema.safeParse({
      asOfTs: '2024-01-01T10:00:00',
      asOfDate: '2024-01-01',
      horizon: '2d',
      objective: 'profit_probability',
      candidates,
    });
    expect(result.success).toBe(false);
  });
});

describe('NewsSummaryRequestSchema', () => {
  it('validates valid request', () => {
    const result = NewsSummaryRequestSchema.safeParse({
      items: [{ title: 'Market news' }],
    });
    expect(result.success).toBe(true);
  });

  it('accepts optional hours', () => {
    const result = NewsSummaryRequestSchema.safeParse({
      items: [{ title: 'Market news' }],
      hours: 48,
    });
    expect(result.success).toBe(true);
  });

  it('accepts optional sourceId and publishedAt', () => {
    const result = NewsSummaryRequestSchema.safeParse({
      items: [
        {
          title: 'Market news',
          sourceId: 'source-1',
          publishedAt: '2024-01-01T10:00:00',
        },
      ],
    });
    expect(result.success).toBe(true);
  });

  it('accepts empty items array', () => {
    const result = NewsSummaryRequestSchema.safeParse({ items: [] });
    expect(result.success).toBe(true);
  });
});
