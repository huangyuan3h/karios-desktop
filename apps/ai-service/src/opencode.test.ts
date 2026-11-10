import { chmodSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import {
  buildOpenCodeArgs,
  OPENCODE_DEFAULT_MODEL,
  parseOpenCodeJsonl,
  runOpenCode,
} from './opencode.js';

const ENV_KEYS = [
  'NEWS_OPENCODE_BIN',
  'NEWS_OPENCODE_CWD',
  'NEWS_OPENCODE_PROXY',
  'NEWS_LLM_MODEL',
] as const;

let savedEnv: Record<string, string | undefined> = {};
let tmpDir: string | null = null;

beforeEach(() => {
  savedEnv = {};
  for (const key of ENV_KEYS) {
    savedEnv[key] = process.env[key];
    delete process.env[key];
  }
});

afterEach(() => {
  for (const key of ENV_KEYS) {
    const value = savedEnv[key];
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
  if (tmpDir) {
    rmSync(tmpDir, { recursive: true, force: true });
    tmpDir = null;
  }
});

function writeScript(body: string): string {
  tmpDir = mkdtempSync(path.join(os.tmpdir(), 'karios-opencode-test-'));
  const script = path.join(tmpDir, 'fake-opencode.sh');
  writeFileSync(script, `#!/bin/sh\n${body}\n`);
  chmodSync(script, 0o755);
  return script;
}

describe('parseOpenCodeJsonl', () => {
  it('collects text events and ignores reasoning, logs and malformed lines', () => {
    const stdout = [
      'log: server started',
      '{"type":"step_start","part":{"type":"step-start"}}',
      '{"type":"reasoning","part":{"text":"hidden"}}',
      '{"type":"text","part":{"text":"1. 财经摘要"}}',
      'not json',
      '{"type":"text","part":{"text":"\\n2. 第二条"}}',
    ].join('\n');
    expect(parseOpenCodeJsonl(stdout)).toBe('1. 财经摘要\n2. 第二条');
  });

  it('returns an empty string when there is no text event', () => {
    expect(parseOpenCodeJsonl('{"type":"step_start"}')).toBe('');
  });
});

describe('buildOpenCodeArgs', () => {
  it('runs standalone with the requested model and prompt', () => {
    const args = buildOpenCodeArgs('hello', OPENCODE_DEFAULT_MODEL);
    expect(args).toContain('run');
    expect(args).toContain('--standalone');
    expect(args).toContain('--format');
    expect(args).toContain('json');
    expect(args).toContain('--model');
    expect(args[args.indexOf('--model') + 1]).toBe(OPENCODE_DEFAULT_MODEL);
    expect(args[args.length - 1]).toBe('hello');
  });
});

describe('runOpenCode', () => {
  it('returns visible text from a successful run', async () => {
    process.env.NEWS_OPENCODE_PROXY = '';
    const bin = writeScript(`echo '{"type":"text","part":{"text":"OK"}}'`);
    await expect(
      runOpenCode({ prompt: 'hi', bin, timeoutMs: 5_000, cwd: tmpDir! }),
    ).resolves.toBe('OK');
  });

  it('rejects with the exit code and stderr tail on failure', async () => {
    process.env.NEWS_OPENCODE_PROXY = '';
    const bin = writeScript('echo "model exploded" >&2\nexit 3');
    await expect(
      runOpenCode({ prompt: 'hi', bin, timeoutMs: 5_000, cwd: tmpDir! }),
    ).rejects.toThrow(/exited with code 3.*model exploded/s);
  });

  it('rejects when the CLI prints no text', async () => {
    process.env.NEWS_OPENCODE_PROXY = '';
    const bin = writeScript(`echo '{"type":"step_start"}'`);
    await expect(
      runOpenCode({ prompt: 'hi', bin, timeoutMs: 5_000, cwd: tmpDir! }),
    ).rejects.toThrow(/returned no text/);
  });

  it('kills a hung child at the timeout boundary', async () => {
    process.env.NEWS_OPENCODE_PROXY = '';
    const bin = writeScript('exec sleep 30');
    const started = Date.now();
    await expect(
      runOpenCode({ prompt: 'hi', bin, timeoutMs: 150, cwd: tmpDir! }),
    ).rejects.toThrow(/timed out after 150ms/);
    expect(Date.now() - started).toBeLessThan(5_000);
  });

  it('rejects a missing binary instead of hanging', async () => {
    process.env.NEWS_OPENCODE_PROXY = '';
    await expect(
      runOpenCode({
        prompt: 'hi',
        bin: path.join(os.tmpdir(), 'karios-definitely-missing-opencode'),
        timeoutMs: 1_000,
        cwd: tmpDir ?? os.tmpdir(),
      }),
    ).rejects.toThrow(/Failed to start OpenCode/);
  });
});
