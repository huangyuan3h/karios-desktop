import { Hono } from 'hono';
import { generateText } from 'ai';
import { z } from 'zod';

import {
  AiProfileSchema,
  loadConfigStore,
  saveConfigStore,
  toPublicConfigFromEnv,
  toPublicProfile,
  newProfileId,
} from '../config';
import {
  ConfigProfileCreateSchema,
  ConfigProfileUpdateSchema,
  ConfigSetActiveSchema,
  ConfigTestSchema,
} from '../schemas';
import { normalizeOptionalString, asTrimmedString } from '../utils';
import { modelFromProfile, pickActiveProfile } from '../model';

export const configRoutes = new Hono();

configRoutes.get('/', async (c) => {
  const store = await loadConfigStore();
  const env = toPublicConfigFromEnv();
  if (!store) {
    return c.json({
      source: env.source,
      activeProfileId: null,
      profiles: [],
      env: { provider: env.provider, modelId: env.modelId, configured: env.configured },
    });
  }
  return c.json({
    source: 'file',
    activeProfileId: store.activeProfileId,
    profiles: store.profiles.map(toPublicProfile),
    env: { provider: env.provider, modelId: env.modelId, configured: env.configured },
  });
});

configRoutes.post('/profiles', async (c) => {
  const body = await c.req.json().catch(() => null);
  const parsed = ConfigProfileCreateSchema.safeParse(body);
  if (!parsed.success) {
    return c.json({ error: 'Invalid request body', issues: parsed.error.issues }, 400);
  }

  const req = parsed.data;
  const store = (await loadConfigStore()) ?? {
    version: 2 as const,
    activeProfileId: null,
    profiles: [],
  };

  const id = newProfileId();
  if (req.provider === 'openai' && !normalizeOptionalString(req.openai?.apiKey)) {
    return c.json({ error: 'Missing OpenAI API key' }, 400);
  }
  if (req.provider === 'google' && !normalizeOptionalString(req.google?.apiKey)) {
    return c.json({ error: 'Missing Google API key' }, 400);
  }
  const profile = AiProfileSchema.parse({
    id,
    name: req.name.trim(),
    provider: req.provider,
    modelId: req.modelId.trim(),
    openai:
      req.provider === 'openai'
        ? {
            apiKey: normalizeOptionalString(req.openai?.apiKey) ?? 'missing',
            baseUrl: normalizeOptionalString(req.openai?.baseUrl),
          }
        : undefined,
    google:
      req.provider === 'google'
        ? {
            apiKey: normalizeOptionalString(req.google?.apiKey) ?? 'missing',
          }
        : undefined,
    ollama:
      req.provider === 'ollama'
        ? {
            baseUrl: normalizeOptionalString(req.ollama?.baseUrl) ?? 'http://127.0.0.1:11434/v1',
            apiKey: normalizeOptionalString(req.ollama?.apiKey),
          }
        : undefined,
  });

  store.profiles.push(profile);
  if (req.setActive || !store.activeProfileId) {
    store.activeProfileId = id;
  }

  await saveConfigStore(store);
  const env = toPublicConfigFromEnv();
  return c.json({
    source: 'file',
    activeProfileId: store.activeProfileId,
    profiles: store.profiles.map(toPublicProfile),
    env: { provider: env.provider, modelId: env.modelId, configured: env.configured },
  });
});

configRoutes.put('/profiles/:id', async (c) => {
  const id = c.req.param('id');
  const body = await c.req.json().catch(() => null);
  const parsed = ConfigProfileUpdateSchema.safeParse(body);
  if (!parsed.success) {
    return c.json({ error: 'Invalid request body', issues: parsed.error.issues }, 400);
  }

  const store = await loadConfigStore();
  if (!store) return c.json({ error: 'No config store found' }, 404);

  const idx = store.profiles.findIndex((p) => p.id === id);
  if (idx < 0) return c.json({ error: 'Profile not found' }, 404);

  const prev = store.profiles[idx]!;
  const req = parsed.data;

  const merged = AiProfileSchema.parse({
    ...prev,
    name: req.name ? req.name.trim() : prev.name,
    provider: req.provider ?? prev.provider,
    modelId: req.modelId ? req.modelId.trim() : prev.modelId,
    openai:
      (req.provider ?? prev.provider) === 'openai'
        ? {
            apiKey: normalizeOptionalString(req.openai?.apiKey) ?? prev.openai?.apiKey ?? '',
            baseUrl:
              normalizeOptionalString(req.openai?.baseUrl) ?? prev.openai?.baseUrl ?? undefined,
          }
        : undefined,
    google:
      (req.provider ?? prev.provider) === 'google'
        ? {
            apiKey: normalizeOptionalString(req.google?.apiKey) ?? prev.google?.apiKey ?? '',
          }
        : undefined,
    ollama:
      (req.provider ?? prev.provider) === 'ollama'
        ? {
            baseUrl:
              normalizeOptionalString(req.ollama?.baseUrl) ??
              prev.ollama?.baseUrl ??
              'http://127.0.0.1:11434/v1',
            apiKey: normalizeOptionalString(req.ollama?.apiKey) ?? prev.ollama?.apiKey ?? undefined,
          }
        : undefined,
  });

  store.profiles[idx] = merged;
  if (req.setActive) store.activeProfileId = id;
  await saveConfigStore(store);
  const env = toPublicConfigFromEnv();
  return c.json({
    source: 'file',
    activeProfileId: store.activeProfileId,
    profiles: store.profiles.map(toPublicProfile),
    env: { provider: env.provider, modelId: env.modelId, configured: env.configured },
  });
});

configRoutes.delete('/profiles/:id', async (c) => {
  const id = c.req.param('id');
  const store = await loadConfigStore();
  if (!store) return c.json({ error: 'No config store found' }, 404);

  const before = store.profiles.length;
  store.profiles = store.profiles.filter((p) => p.id !== id);
  if (store.profiles.length === before) return c.json({ error: 'Profile not found' }, 404);

  if (store.activeProfileId === id) {
    store.activeProfileId = store.profiles[0]?.id ?? null;
  }
  await saveConfigStore(store);
  const env = toPublicConfigFromEnv();
  return c.json({
    source: 'file',
    activeProfileId: store.activeProfileId,
    profiles: store.profiles.map(toPublicProfile),
    env: { provider: env.provider, modelId: env.modelId, configured: env.configured },
  });
});

configRoutes.post('/active', async (c) => {
  const body = await c.req.json().catch(() => null);
  const parsed = ConfigSetActiveSchema.safeParse(body);
  if (!parsed.success) {
    return c.json({ error: 'Invalid request body', issues: parsed.error.issues }, 400);
  }
  const store = await loadConfigStore();
  if (!store) return c.json({ error: 'No config store found' }, 404);
  const exists = store.profiles.some((p) => p.id === parsed.data.profileId);
  if (!exists) return c.json({ error: 'Profile not found' }, 404);
  store.activeProfileId = parsed.data.profileId;
  await saveConfigStore(store);
  const env = toPublicConfigFromEnv();
  return c.json({
    source: 'file',
    activeProfileId: store.activeProfileId,
    profiles: store.profiles.map(toPublicProfile),
    env: { provider: env.provider, modelId: env.modelId, configured: env.configured },
  });
});

configRoutes.post('/test', async (c) => {
  const body = await c.req.json().catch(() => ({}));
  const parsed = ConfigTestSchema.safeParse(body);
  if (!parsed.success) {
    return c.json({ ok: false, error: 'Invalid request body', issues: parsed.error.issues }, 200);
  }

  const store = await loadConfigStore();
  const profile = store
    ? parsed.data.profileId
      ? (store.profiles.find((p) => p.id === parsed.data.profileId) ?? null)
      : pickActiveProfile(store)
    : null;

  if (!profile) {
    const env = toPublicConfigFromEnv();
    if (!env.configured) return c.json({ ok: false, error: 'AI model is not configured' }, 200);
    return c.json(
      { ok: true, note: 'Using env/default config', provider: env.provider, modelId: env.modelId },
      200,
    );
  }

  if (profile.provider === 'openai' && !profile.openai?.apiKey?.trim()) {
    return c.json({ ok: false, error: 'Missing OpenAI API key' }, 200);
  }
  if (profile.provider === 'google' && !profile.google?.apiKey?.trim()) {
    return c.json({ ok: false, error: 'Missing Google API key' }, 200);
  }
  if (profile.provider === 'ollama') {
    const baseUrl = profile.ollama?.baseUrl?.trim() || '';
    if (!baseUrl) return c.json({ ok: false, error: 'Missing Ollama baseUrl' }, 200);
  }

  try {
    const { model, modelId, provider } = modelFromProfile(profile);
    const { text } = await generateText({
      model,
      system: 'You are a connectivity test endpoint. Reply with a single word: OK.',
      prompt: 'OK',
      temperature: 0,
      maxOutputTokens: 8,
    });
    return c.json({ ok: true, provider, modelId, reply: text.trim().slice(0, 32) }, 200);
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return c.json({ ok: false, error: msg }, 200);
  }
});
