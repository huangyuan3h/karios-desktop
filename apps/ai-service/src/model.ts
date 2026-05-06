import { generateText } from 'ai';
import { createOpenAI, openai } from '@ai-sdk/openai';
import { google } from '@ai-sdk/google';
import { z } from 'zod';

import { AiConfigStoreSchema, AiProfileSchema, loadConfigStore } from './config';
import { asTrimmedString } from './utils';

type AiModel = Parameters<typeof generateText>[0]['model'];

export type { AiModel };

/** Resolved model plus flags for OpenAI-compatible backends that are not api.openai.com. */
export type ResolvedModelBundle = {
  model: AiModel;
  provider: string;
  modelId: string;
  /**
   * When true, pass `providerOptions.openai.structuredOutputs: false` to `generateObject` so the
   * SDK uses `response_format: { type: "json_object" }` instead of `json_schema`. Ollama and many
   * local gateways return: "This response_format type is unavailable now" for json_schema.
   */
  looseStructuredOutputs: boolean;
};

/**
 * Extra options for `generateObject` on OpenAI-compatible servers that do not support json_schema.
 */
export function generateObjectCompatOptions(looseStructuredOutputs: boolean): {
  providerOptions: { openai: { structuredOutputs: false } };
} | Record<string, never> {
  if (!looseStructuredOutputs) return {};
  return { providerOptions: { openai: { structuredOutputs: false as const } } };
}

/**
 * Hint OpenAI-compatible APIs to return JSON for `generateText` fallbacks when `json_schema` is unavailable.
 */
export function generateTextJsonObjectModeOptions(looseStructuredOutputs: boolean): {
  providerOptions: { openai: { responseFormat: { type: 'json_object' } } };
} | Record<string, never> {
  if (!looseStructuredOutputs) return {};
  return {
    providerOptions: { openai: { responseFormat: { type: 'json_object' as const } } },
  };
}

/**
 * OpenAI-compatible servers (Ollama /v1, LM Studio, etc.) often reject the `developer`
 * role that @ai-sdk/openai emits for "reasoning-style" model IDs — which includes any
 * model id not matching gpt-3*, gpt-4*, chatgpt-4o, or gpt-5-chat (e.g. llama3, qwen).
 * Rewrite to `system` before the request leaves the process.
 */
export function rewriteDeveloperMessageRolesInJsonString(body: string): string {
  try {
    const parsed = JSON.parse(body) as Record<string, unknown>;
    const messages = parsed.messages;
    if (!Array.isArray(messages)) return body;
    let changed = false;
    const next = messages.map((m: unknown) => {
      if (
        m !== null &&
        typeof m === 'object' &&
        'role' in m &&
        (m as { role: string }).role === 'developer'
      ) {
        changed = true;
        return { ...(m as Record<string, unknown>), role: 'system' };
      }
      return m;
    });
    if (!changed) return body;
    return JSON.stringify({ ...parsed, messages: next });
  } catch {
    return body;
  }
}

function openAiCompatibleFetchRewriteDeveloper(innerFetch: typeof fetch = globalThis.fetch): typeof fetch {
  return async (input, init) => {
    if (!init?.body || typeof init.body !== 'string') {
      return innerFetch(input, init);
    }
    const body = rewriteDeveloperMessageRolesInJsonString(init.body);
    if (body === init.body) {
      return innerFetch(input, init);
    }
    return innerFetch(input, { ...init, body });
  };
}

export function applyProviderEnv(p: z.infer<typeof AiProfileSchema>): void {
  if (p.provider === 'google') {
    const key = p.google?.apiKey?.trim();
    if (key) {
      process.env.GOOGLE_GENERATIVE_AI_API_KEY = key;
      process.env.GOOGLE_API_KEY = key;
    }
    delete process.env.OPENAI_BASE_URL;
    return;
  }
}

export function pickActiveProfile(
  store: z.infer<typeof AiConfigStoreSchema>,
): z.infer<typeof AiProfileSchema> | null {
  const id = store.activeProfileId;
  if (!id) return null;
  return store.profiles.find((p) => p.id === id) ?? null;
}

export function modelFromProfile(p: z.infer<typeof AiProfileSchema>): ResolvedModelBundle {
  if (p.provider === 'google') {
    applyProviderEnv(p);
    return {
      model: google(p.modelId),
      provider: 'google',
      modelId: p.modelId,
      looseStructuredOutputs: false,
    };
  }

  if (p.provider === 'ollama') {
    const baseURL = p.ollama?.baseUrl?.trim() || 'http://127.0.0.1:11434/v1';
    const apiKey = p.ollama?.apiKey?.trim() || 'ollama';
    const ollamaClient = createOpenAI({
      apiKey,
      baseURL,
      fetch: openAiCompatibleFetchRewriteDeveloper(),
    });
    return {
      model: ollamaClient.chat(p.modelId),
      provider: 'ollama',
      modelId: p.modelId,
      looseStructuredOutputs: true,
    };
  }

  const apiKey = p.openai?.apiKey?.trim() || '';
  const baseURL = p.openai?.baseUrl?.trim() || undefined;
  const openaiClient =
    apiKey || baseURL
      ? createOpenAI({
          apiKey,
          baseURL,
          ...(baseURL ? { fetch: openAiCompatibleFetchRewriteDeveloper() } : {}),
        })
      : openai;
  return {
    model: openaiClient.chat(p.modelId),
    provider: 'openai',
    modelId: p.modelId,
    looseStructuredOutputs: Boolean(baseURL),
  };
}

export async function getResolvedModel(): Promise<ResolvedModelBundle> {
  const provider = asTrimmedString(process.env.AI_PROVIDER).toLowerCase() || 'openai';
  const envModelId = asTrimmedString(process.env.AI_MODEL);

  const store = await loadConfigStore();
  const active = store ? pickActiveProfile(store) : null;

  if (!active) {
    if (!envModelId) throw new Error('Missing AI_MODEL');
    if (provider === 'google') {
      return {
        model: google(envModelId),
        modelId: envModelId,
        provider,
        looseStructuredOutputs: false,
      };
    }
    const envOpenAiBase = asTrimmedString(process.env.OPENAI_BASE_URL);
    return {
      model: openai.chat(envModelId),
      modelId: envModelId,
      provider,
      looseStructuredOutputs: Boolean(envOpenAiBase),
    };
  }

  return modelFromProfile(active);
}

export function getStrategyFallbackModelId(): string | null {
  const id = (process.env.AI_STRATEGY_FALLBACK_MODEL ?? '').trim();
  return id || null;
}

export async function getStrategyPrimaryAndFallbackModels(): Promise<{
  model: AiModel;
  modelId: string;
  fallbackModel: AiModel | null;
  fallbackModelId: string | null;
  looseStructuredOutputs: boolean;
}> {
  const store = await loadConfigStore();
  const primary = await getResolvedModel();

  if (store && store.activeProfileId) {
    return {
      model: primary.model,
      modelId: primary.modelId,
      fallbackModel: null,
      fallbackModelId: null,
      looseStructuredOutputs: primary.looseStructuredOutputs,
    };
  }

  const fbId = getStrategyFallbackModelId();
  if (!fbId) {
    return {
      model: primary.model,
      modelId: primary.modelId,
      fallbackModel: null,
      fallbackModelId: null,
      looseStructuredOutputs: primary.looseStructuredOutputs,
    };
  }

  const provider = asTrimmedString(process.env.AI_PROVIDER).toLowerCase() || 'openai';
  const fb = provider === 'google' ? google(fbId) : openai.chat(fbId);
  return {
    model: primary.model,
    modelId: primary.modelId,
    fallbackModel: fb,
    fallbackModelId: fbId,
    looseStructuredOutputs: primary.looseStructuredOutputs,
  };
}
