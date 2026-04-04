import { generateText } from 'ai';
import { createOpenAI, openai } from '@ai-sdk/openai';
import { google } from '@ai-sdk/google';
import { z } from 'zod';

import { AiConfigStoreSchema, AiProfileSchema, loadConfigStore } from './config';
import { asTrimmedString } from './utils';

type AiModel = Parameters<typeof generateText>[0]['model'];

export type { AiModel };

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

export function modelFromProfile(p: z.infer<typeof AiProfileSchema>): {
  model: AiModel;
  provider: string;
  modelId: string;
} {
  if (p.provider === 'google') {
    applyProviderEnv(p);
    return { model: google(p.modelId), provider: 'google', modelId: p.modelId };
  }

  if (p.provider === 'ollama') {
    const baseURL = p.ollama?.baseUrl?.trim() || 'http://127.0.0.1:11434/v1';
    const apiKey = p.ollama?.apiKey?.trim() || 'ollama';
    const ollamaClient = createOpenAI({
      apiKey,
      baseURL,
    });
    return { model: ollamaClient.chat(p.modelId), provider: 'ollama', modelId: p.modelId };
  }

  const apiKey = p.openai?.apiKey?.trim() || '';
  const baseURL = p.openai?.baseUrl?.trim() || undefined;
  const openaiClient = apiKey || baseURL ? createOpenAI({ apiKey, baseURL }) : openai;
  return { model: openaiClient.chat(p.modelId), provider: 'openai', modelId: p.modelId };
}

export async function getResolvedModel(): Promise<{
  model: AiModel;
  modelId: string;
  provider: string;
}> {
  const provider = asTrimmedString(process.env.AI_PROVIDER).toLowerCase() || 'openai';
  const envModelId = asTrimmedString(process.env.AI_MODEL);

  const store = await loadConfigStore();
  const active = store ? pickActiveProfile(store) : null;

  if (!active) {
    if (!envModelId) throw new Error('Missing AI_MODEL');
    if (provider === 'google') return { model: google(envModelId), modelId: envModelId, provider };
    return { model: openai.chat(envModelId), modelId: envModelId, provider };
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
}> {
  const store = await loadConfigStore();
  const primary = await getResolvedModel();

  if (store && store.activeProfileId) {
    return {
      model: primary.model,
      modelId: primary.modelId,
      fallbackModel: null,
      fallbackModelId: null,
    };
  }

  const fbId = getStrategyFallbackModelId();
  if (!fbId) {
    return {
      model: primary.model,
      modelId: primary.modelId,
      fallbackModel: null,
      fallbackModelId: null,
    };
  }

  const provider = asTrimmedString(process.env.AI_PROVIDER).toLowerCase() || 'openai';
  const fb = provider === 'google' ? google(fbId) : openai.chat(fbId);
  return {
    model: primary.model,
    modelId: primary.modelId,
    fallbackModel: fb,
    fallbackModelId: fbId,
  };
}
