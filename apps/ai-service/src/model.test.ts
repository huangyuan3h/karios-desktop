import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest';
import {
  applyProviderEnv,
  pickActiveProfile,
  modelFromProfile,
  getStrategyFallbackModelId,
  rewriteDeveloperMessageRolesInJsonString,
} from './model';
import { AiProfileSchema, AiConfigStoreSchema } from './config';

describe('applyProviderEnv', () => {
  const originalEnv = process.env;

  beforeEach(() => {
    process.env = { ...originalEnv };
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  it('sets Google API keys for google provider', () => {
    const profile = AiProfileSchema.parse({
      id: 'test',
      name: 'Test',
      provider: 'google',
      modelId: 'gemini-pro',
      google: { apiKey: 'test-google-key' },
    });
    applyProviderEnv(profile);
    expect(process.env.GOOGLE_GENERATIVE_AI_API_KEY).toBe('test-google-key');
    expect(process.env.GOOGLE_API_KEY).toBe('test-google-key');
  });

  it('deletes OPENAI_BASE_URL for google provider', () => {
    process.env.OPENAI_BASE_URL = 'https://test.openai.com';
    const profile = AiProfileSchema.parse({
      id: 'test',
      name: 'Test',
      provider: 'google',
      modelId: 'gemini-pro',
      google: { apiKey: 'test-key' },
    });
    applyProviderEnv(profile);
    expect(process.env.OPENAI_BASE_URL).toBeUndefined();
  });

  it('does nothing for openai provider', () => {
    const profile = AiProfileSchema.parse({
      id: 'test',
      name: 'Test',
      provider: 'openai',
      modelId: 'gpt-4',
      openai: { apiKey: 'test-key' },
    });
    applyProviderEnv(profile);
  });

  it('does nothing for ollama provider', () => {
    const profile = AiProfileSchema.parse({
      id: 'test',
      name: 'Test',
      provider: 'ollama',
      modelId: 'llama2',
      ollama: { baseUrl: 'http://localhost:11434/v1' },
    });
    applyProviderEnv(profile);
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
