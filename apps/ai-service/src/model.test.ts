import { describe, expect, it, beforeEach, afterEach } from 'vitest';
import {
  pickActiveProfile,
  modelFromProfile,
  getStrategyFallbackModelId,
  getDecisionModelBundle,
  DECISION_DEFAULT_MODEL_ID,
  rewriteDeveloperMessageRolesInJsonString,
  rewriteOpenAiCompatibleRequestBody,
} from './model.js';
import { AiProfileSchema, AiConfigStoreSchema } from './config.js';

describe('modelFromProfile', () => {
  const originalEnv = process.env;

  beforeEach(() => {
    process.env = { ...originalEnv };
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  it('google provider builds a model without mutating global env', () => {
    const profile = AiProfileSchema.parse({
      id: 'test',
      name: 'Test',
      provider: 'google',
      modelId: 'gemini-pro',
      google: { apiKey: 'test-google-key' },
    });
    process.env.GOOGLE_GENERATIVE_AI_API_KEY = 'stale-key';
    const bundle = modelFromProfile(profile);
    expect(bundle.provider).toBe('google');
    expect(bundle.modelId).toBe('gemini-pro');
    // No global env side effects — concurrent openai requests must not be disturbed.
    expect(process.env.GOOGLE_GENERATIVE_AI_API_KEY).toBe('stale-key');
  });

  it('ollama provider keeps its baseUrl', () => {
    const profile = AiProfileSchema.parse({
      id: 'test',
      name: 'Test',
      provider: 'ollama',
      modelId: 'llama2',
      ollama: { baseUrl: 'http://localhost:11434/v1' },
    });
    const bundle = modelFromProfile(profile);
    expect(bundle.provider).toBe('ollama');
    expect(bundle.looseStructuredOutputs).toBe(true);
  });
});

describe('pickActiveProfile', () => {
  it('returns null when activeProfileId is null', () => {
    const store = AiConfigStoreSchema.parse({
      version: 2,
      activeProfileId: null,
      profiles: [
        {
          id: 'p1',
          name: 'Profile 1',
          provider: 'openai',
          modelId: 'gpt-4',
          openai: { apiKey: 'key' },
        },
      ],
    });
    expect(pickActiveProfile(store)).toBeNull();
  });

  it('returns active profile when found', () => {
    const store = AiConfigStoreSchema.parse({
      version: 2,
      activeProfileId: 'p1',
      profiles: [
        {
          id: 'p1',
          name: 'Profile 1',
          provider: 'openai',
          modelId: 'gpt-4',
          openai: { apiKey: 'key' },
        },
        {
          id: 'p2',
          name: 'Profile 2',
          provider: 'google',
          modelId: 'gemini-pro',
          google: { apiKey: 'key' },
        },
      ],
    });
    const result = pickActiveProfile(store);
    expect(result?.id).toBe('p1');
    expect(result?.provider).toBe('openai');
  });

  it('returns null when active profile not found', () => {
    const store = AiConfigStoreSchema.parse({
      version: 2,
      activeProfileId: 'nonexistent',
      profiles: [
        {
          id: 'p1',
          name: 'Profile 1',
          provider: 'openai',
          modelId: 'gpt-4',
          openai: { apiKey: 'key' },
        },
      ],
    });
    expect(pickActiveProfile(store)).toBeNull();
  });
});

describe('modelFromProfile', () => {
  it('returns google model for google provider', () => {
    const profile = AiProfileSchema.parse({
      id: 'test',
      name: 'Test',
      provider: 'google',
      modelId: 'gemini-pro',
      google: { apiKey: 'test-key' },
    });
    const result = modelFromProfile(profile);
    expect(result.provider).toBe('google');
    expect(result.modelId).toBe('gemini-pro');
    expect(result.looseStructuredOutputs).toBe(false);
  });

  it('returns ollama model for ollama provider', () => {
    const profile = AiProfileSchema.parse({
      id: 'test',
      name: 'Test',
      provider: 'ollama',
      modelId: 'llama2',
      ollama: { baseUrl: 'http://localhost:11434/v1' },
    });
    const result = modelFromProfile(profile);
    expect(result.provider).toBe('ollama');
    expect(result.modelId).toBe('llama2');
    expect(result.looseStructuredOutputs).toBe(true);
  });

  it('returns openai model for openai provider', () => {
    const profile = AiProfileSchema.parse({
      id: 'test',
      name: 'Test',
      provider: 'openai',
      modelId: 'gpt-4',
      openai: { apiKey: 'test-key' },
    });
    const result = modelFromProfile(profile);
    expect(result.provider).toBe('openai');
    expect(result.modelId).toBe('gpt-4');
    expect(result.looseStructuredOutputs).toBe(false);
  });

  it('sets looseStructuredOutputs when openai profile has custom baseUrl', () => {
    const profile = AiProfileSchema.parse({
      id: 'test',
      name: 'Test',
      provider: 'openai',
      modelId: 'local-model',
      openai: { apiKey: 'x', baseUrl: 'http://127.0.0.1:1234/v1' },
    });
    expect(modelFromProfile(profile).looseStructuredOutputs).toBe(true);
  });

  it('uses default baseUrl for ollama when not provided', () => {
    const profile = AiProfileSchema.parse({
      id: 'test',
      name: 'Test',
      provider: 'ollama',
      modelId: 'llama2',
      ollama: { baseUrl: 'http://127.0.0.1:11434/v1' },
    });
    const result = modelFromProfile(profile);
    expect(result.provider).toBe('ollama');
    expect(result.looseStructuredOutputs).toBe(true);
  });
});

describe('rewriteOpenAiCompatibleRequestBody', () => {
  it('sets reasoning_split for MiniMax model ids', () => {
    const raw = JSON.stringify({
      model: 'MiniMax-M3',
      messages: [{ role: 'user', content: 'hi' }],
    });
    const out = JSON.parse(rewriteOpenAiCompatibleRequestBody(raw)) as {
      reasoning_split?: boolean;
    };
    expect(out.reasoning_split).toBe(true);
  });

  it('sets reasoning_split when baseURL is MiniMax', () => {
    const raw = JSON.stringify({
      model: 'custom-alias',
      messages: [{ role: 'user', content: 'hi' }],
    });
    const out = JSON.parse(
      rewriteOpenAiCompatibleRequestBody(raw, { baseURL: 'https://api.minimaxi.com/v1' }),
    ) as { reasoning_split?: boolean };
    expect(out.reasoning_split).toBe(true);
  });

  it('does not set reasoning_split for unrelated models', () => {
    const raw = JSON.stringify({
      model: 'gpt-4o',
      messages: [{ role: 'user', content: 'hi' }],
    });
    expect(rewriteOpenAiCompatibleRequestBody(raw)).toBe(raw);
  });
});

describe('rewriteDeveloperMessageRolesInJsonString', () => {
  it('rewrites developer role to system in messages array', () => {
    const raw = JSON.stringify({
      model: 'llama3',
      messages: [
        { role: 'developer', content: 'sys' },
        { role: 'user', content: 'hi' },
      ],
    });
    const out = JSON.parse(rewriteDeveloperMessageRolesInJsonString(raw)) as {
      messages: Array<{ role: string; content: string }>;
    };
    expect(out.messages[0]?.role).toBe('system');
    expect(out.messages[0]?.content).toBe('sys');
    expect(out.messages[1]?.role).toBe('user');
  });

  it('returns original string when no messages key', () => {
    const raw = JSON.stringify({ foo: 1 });
    expect(rewriteDeveloperMessageRolesInJsonString(raw)).toBe(raw);
  });

  it('returns original string on invalid JSON', () => {
    const raw = 'not-json';
    expect(rewriteDeveloperMessageRolesInJsonString(raw)).toBe(raw);
  });
});

describe('getStrategyFallbackModelId', () => {
  const originalEnv = process.env;

  beforeEach(() => {
    process.env = { ...originalEnv };
  });

  afterEach(() => {
    process.env = originalEnv;

  });

  it('returns null when env not set', () => {
    delete process.env.AI_STRATEGY_FALLBACK_MODEL;
    expect(getStrategyFallbackModelId()).toBeNull();
  });

  it('returns trimmed model id when set', () => {
    process.env.AI_STRATEGY_FALLBACK_MODEL = '  gpt-4-turbo  ';
    expect(getStrategyFallbackModelId()).toBe('gpt-4-turbo');
  });

  it('returns null for empty string', () => {
    process.env.AI_STRATEGY_FALLBACK_MODEL = '';
    expect(getStrategyFallbackModelId()).toBeNull();
  });
});

describe('getDecisionModelBundle', () => {
  const originalEnv = process.env;

  beforeEach(() => {
    process.env = { ...originalEnv };
  });

  afterEach(() => {
    process.env = originalEnv;

  });

  it('returns gemini model with explicit API key when GEMINI_API_KEY is set', async () => {
    process.env.GEMINI_API_KEY = 'decision-gemini-key';
    const bundle = await getDecisionModelBundle();
    expect(bundle.provider).toBe('google');
    expect(bundle.modelId).toBe(DECISION_DEFAULT_MODEL_ID);
    expect(bundle.looseStructuredOutputs).toBe(false);
    // Key is passed to the provider explicitly — no global env mutation, so
    // concurrent requests to other providers are never disturbed.
    expect(process.env.GOOGLE_GENERATIVE_AI_API_KEY).toBeUndefined();
    expect(process.env.GOOGLE_API_KEY).toBeUndefined();
  });

  it('respects AI_DECISION_MODEL override', async () => {
    process.env.GEMINI_API_KEY = 'decision-gemini-key';
    process.env.AI_DECISION_MODEL = 'gemini-2.5-flash';
    const bundle = await getDecisionModelBundle();
    expect(bundle.modelId).toBe('gemini-2.5-flash');
  });

  it('falls back to normal resolution when GEMINI_API_KEY is missing', async () => {
    delete process.env.GEMINI_API_KEY;
    delete process.env.OPENAI_BASE_URL;
    process.env.KARIOS_APP_DATA_DIR = '/tmp/karios-ai-test-nonexistent-dir';
    process.env.AI_PROVIDER = 'openai';
    process.env.AI_MODEL = 'gpt-test-model';
    const bundle = await getDecisionModelBundle();
    expect(bundle.provider).toBe('openai');
    expect(bundle.modelId).toBe('gpt-test-model');
    expect(bundle.looseStructuredOutputs).toBe(false);
  });
});
