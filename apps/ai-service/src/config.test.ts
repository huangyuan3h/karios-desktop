import { promises as fs } from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { app } from './app';
import { configPath, loadConfigStore, saveConfigStore } from './config';

async function makeTempDir(): Promise<string> {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'karios-ai-service-'));
  return dir;
}

describe('ai-service runtime config', () => {
  let dir: string;

  beforeEach(async () => {
    dir = await makeTempDir();
    process.env.KARIOS_APP_DATA_DIR = dir;
  });

  afterEach(async () => {
    delete process.env.KARIOS_APP_DATA_DIR;
    await fs.rm(dir, { recursive: true, force: true }).catch(() => undefined);
  });

  it('loadRuntimeConfig returns null when missing', async () => {
    const cfg = await loadConfigStore();
    expect(cfg).toBeNull();
  });

  it('saveConfigStore round-trips and uses expected path', async () => {
    await saveConfigStore({
      version: 2,
      activeProfileId: 'p1',
      profiles: [
        {
          id: 'p1',
          name: 'Default',
          provider: 'openai',
          modelId: 'gpt-5.2',
          openai: { apiKey: 'sk-test-123456' },
        },
      ],
    });

    const p = configPath();
    expect(p).toBe(path.join(dir, 'ai-service.config.json'));

    const cfg = await loadConfigStore();
    expect(cfg?.version).toBe(2);
    expect(cfg?.activeProfileId).toBe('p1');
    expect(cfg?.profiles[0]?.provider).toBe('openai');
    expect(cfg?.profiles[0]?.modelId).toBe('gpt-5.2');
    expect(cfg?.profiles[0]?.openai?.apiKey).toBe('sk-test-123456');
  });

  it('migrates legacy v1 config file to v2 on load', async () => {
    const p = configPath();
    await fs.mkdir(path.dirname(p), { recursive: true });
    await fs.writeFile(
      p,
      JSON.stringify({
        provider: 'openai',
        modelId: 'gpt-5.2',
        openai: { apiKey: 'sk-test-aaaa1111' },
      }),
      'utf8',
    );

    const cfg = await loadConfigStore();
    expect(cfg?.version).toBe(2);
    expect(cfg?.activeProfileId).toBe('default');
    expect(cfg?.profiles[0]?.provider).toBe('openai');
    expect(cfg?.profiles[0]?.modelId).toBe('gpt-5.2');
    expect(cfg?.profiles[0]?.openai?.apiKey).toBe('sk-test-aaaa1111');
  });
});

describe('ai-service /config routes', () => {
  let dir: string;

  beforeEach(async () => {
    dir = await makeTempDir();
    process.env.KARIOS_APP_DATA_DIR = dir;
  });

  afterEach(async () => {
    delete process.env.KARIOS_APP_DATA_DIR;
    await fs.rm(dir, { recursive: true, force: true }).catch(() => undefined);
  });

  it('POST /config/profiles persists and GET /config returns masked key fields', async () => {
    const key = 'sk-test-abcdef1234';

    const create = await app.request('/config/profiles', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        name: 'OpenAI default',
        provider: 'openai',
        modelId: 'gpt-5.2',
        openai: { apiKey: key },
        setActive: true,
      }),
    });
    expect(create.status).toBe(200);
    const createJson = (await create.json()) as {
      activeProfileId: string | null;
      profiles: Array<{ id: string; provider: string; modelId: string; openai?: { hasKey: boolean; keyLast4: string | null } }>;
    };
    expect(typeof createJson.activeProfileId).toBe('string');
    expect(createJson.profiles.length).toBe(1);
    expect(createJson.profiles[0]?.provider).toBe('openai');
    expect(createJson.profiles[0]?.modelId).toBe('gpt-5.2');
    expect(createJson.profiles[0]?.openai?.hasKey).toBe(true);
    expect(createJson.profiles[0]?.openai?.keyLast4).toBe('1234');
    expect(JSON.stringify(createJson)).not.toContain(key);

    const get = await app.request('/config');
    expect(get.status).toBe(200);
    const getJson = (await get.json()) as {
      source: string;
      activeProfileId: string | null;
      profiles: Array<{ id: string; provider: string; modelId: string; openai?: { hasKey: boolean; keyLast4: string | null } }>;
    };
    expect(getJson.source).toBe('file');
    expect(getJson.activeProfileId).toBe(createJson.activeProfileId);
    expect(getJson.profiles[0]?.openai?.hasKey).toBe(true);
    expect(getJson.profiles[0]?.openai?.keyLast4).toBe('1234');
    expect(JSON.stringify(getJson)).not.toContain(key);
  });

  it('POST /config/profiles rejects invalid body', async () => {
    const resp = await app.request('/config/profiles', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ provider: 'openai' }),
    });
    expect(resp.status).toBe(400);
  });
});

