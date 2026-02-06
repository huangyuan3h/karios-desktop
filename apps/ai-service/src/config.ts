import { randomUUID } from 'node:crypto';
import { promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { z } from 'zod';

export const AiProfileSchema = z.object({
  id: z.string().min(1),
  name: z.string().min(1),
  provider: z.enum(['openai', 'google', 'ollama']),
  modelId: z.string().min(1),
  openai: z
    .object({
      apiKey: z.string().min(1),
      baseUrl: z.string().min(1).optional(),
    })
    .optional(),
  google: z
    .object({
      apiKey: z.string().min(1),
    })
    .optional(),
  ollama: z
    .object({
      baseUrl: z.string().min(1),
      apiKey: z.string().min(1).optional(),
    })
    .optional(),
});

export type AiProfile = z.infer<typeof AiProfileSchema>;

export const AiConfigStoreSchema = z.object({
  version: z.literal(2),
  activeProfileId: z.string().nullable(),
  profiles: z.array(AiProfileSchema),
});

export type AiConfigStore = z.infer<typeof AiConfigStoreSchema>;

// v1 legacy single-config format (for migration only)
export const AiRuntimeConfigV1Schema = z.object({
  provider: z.enum(['openai', 'google', 'ollama']),
  modelId: z.string().min(1),
  openai: z
    .object({
      apiKey: z.string().min(1),
      baseUrl: z.string().min(1).optional(),
    })
    .optional(),
  google: z
    .object({
      apiKey: z.string().min(1),
    })
    .optional(),
  ollama: z
    .object({
      baseUrl: z.string().min(1),
      apiKey: z.string().min(1).optional(),
    })
    .optional(),
});

export type AiRuntimeConfigV1 = z.infer<typeof AiRuntimeConfigV1Schema>;

export type AiProfilePublic = {
  id: string;
  name: string;
  provider: 'openai' | 'google' | 'ollama';
  modelId: string;
  openai?: { hasKey: boolean; keyLast4: string | null; baseUrl: string | null };
  google?: { hasKey: boolean; keyLast4: string | null };
  ollama?: { baseUrl: string | null; hasKey: boolean; keyLast4: string | null };
};

export type AiConfigPublic = {
  source: 'file' | 'env' | 'default';
  activeProfileId: string | null;
  profiles: AiProfilePublic[];
  env?: {
    provider: 'openai' | 'google' | 'ollama';
    modelId: string;
    configured: boolean;
  };
};

function keyLast4(key: string | undefined | null): string | null {
  const k = (key ?? '').trim();
  if (!k) return null;
  return k.length <= 4 ? k : k.slice(-4);
}

export function configPath(): string {
  const base =
    (process.env.KARIOS_APP_DATA_DIR ?? '').trim() || path.join(os.homedir(), '.karios');
  return path.join(base, 'ai-service.config.json');
}

async function ensureParentDir(p: string): Promise<void> {
  const dir = path.dirname(p);
  await fs.mkdir(dir, { recursive: true });
}

async function atomicWriteFile(target: string, content: string): Promise<void> {
  await ensureParentDir(target);
  const dir = path.dirname(target);
  const tmp = path.join(dir, `.tmp-${path.basename(target)}-${process.pid}-${Date.now()}`);
  await fs.writeFile(tmp, content, { encoding: 'utf8' });
  // Best-effort: restrict permissions (POSIX only).
  await fs.chmod(tmp, 0o600).catch(() => undefined);
  await fs.rename(tmp, target);
  await fs.chmod(target, 0o600).catch(() => undefined);
}

export function newProfileId(): string {
  return randomUUID();
}

export function migrateV1ToV2(v1: AiRuntimeConfigV1): AiConfigStore {
  const id = 'default';
  return {
    version: 2,
    activeProfileId: id,
    profiles: [
      {
        id,
        name: 'Default',
        provider: v1.provider,
        modelId: v1.modelId,
        openai: v1.openai,
        google: v1.google,
        ollama: v1.ollama,
      },
    ],
  };
}

export async function loadConfigStore(): Promise<AiConfigStore | null> {
  const p = configPath();
  try {
    const raw = await fs.readFile(p, 'utf8');
    const json = JSON.parse(raw) as unknown;
    const v2 = AiConfigStoreSchema.safeParse(json);
    if (v2.success) return v2.data;

    const v1 = AiRuntimeConfigV1Schema.safeParse(json);
    if (v1.success) {
      const migrated = migrateV1ToV2(v1.data);
      // Persist migration so we don't repeat it forever.
      await atomicWriteFile(p, JSON.stringify(migrated, null, 2));
      return migrated;
    }

    return null;
  } catch (e) {
    // Missing file is normal.
    const err = e as NodeJS.ErrnoException | null | undefined;
    if (err?.code === 'ENOENT') return null;
    return null;
  }
}

export async function saveConfigStore(next: AiConfigStore): Promise<void> {
  const p = configPath();
  await atomicWriteFile(p, JSON.stringify(next, null, 2));
}

export function toPublicProfile(p: AiProfile): AiProfilePublic {
  if (p.provider === 'openai') {
    return {
      id: p.id,
      name: p.name,
      provider: 'openai',
      modelId: p.modelId,
      openai: {
        hasKey: Boolean(p.openai?.apiKey?.trim()),
        keyLast4: keyLast4(p.openai?.apiKey),
        baseUrl: (p.openai?.baseUrl ?? '').trim() || null,
      },
    };
  }
  if (p.provider === 'google') {
    return {
      id: p.id,
      name: p.name,
      provider: 'google',
      modelId: p.modelId,
      google: {
        hasKey: Boolean(p.google?.apiKey?.trim()),
        keyLast4: keyLast4(p.google?.apiKey),
      },
    };
  }
  return {
    id: p.id,
    name: p.name,
    provider: 'ollama',
    modelId: p.modelId,
    ollama: {
      baseUrl: (p.ollama?.baseUrl ?? '').trim() || null,
      hasKey: Boolean(p.ollama?.apiKey?.trim()),
      keyLast4: keyLast4(p.ollama?.apiKey),
    },
  };
}

export function toPublicConfigFromEnv(): {
  provider: 'openai' | 'google' | 'ollama';
  modelId: string;
  configured: boolean;
  source: 'env' | 'default';
} {
  const provider = ((process.env.AI_PROVIDER ?? 'openai').trim().toLowerCase() ||
    'openai') as 'openai' | 'google' | 'ollama';
  const modelId = (process.env.AI_MODEL ?? '').trim();
  const pid = modelId || 'unknown';

  if (provider === 'google') {
    return {
      configured: Boolean(modelId),
      source: modelId ? 'env' : 'default',
      provider: 'google',
      modelId: pid,
    };
  }

  if (provider === 'ollama') {
    return {
      configured: Boolean(modelId),
      source: modelId ? 'env' : 'default',
      provider: 'ollama',
      modelId: pid,
    };
  }

  return {
    configured: Boolean(modelId),
    source: modelId ? 'env' : 'default',
    provider: 'openai',
    modelId: pid,
  };
}

