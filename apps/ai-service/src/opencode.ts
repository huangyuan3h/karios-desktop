import { spawn } from 'node:child_process';
import { mkdirSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';

/**
 * Bounded OpenCode CLI driver for news text generation.
 *
 * We deliberately spawn `opencode run --standalone` per request instead of
 * talking to the developer's long-running interactive/service servers
 * (`opencode serve` on :5551 and `opencode serve --service`). Those servers
 * belong to the user; a product request must not share, mutate, or restart
 * their sessions. `--standalone` starts a private, short-lived server and
 * exits when the run finishes.
 */
export const OPENCODE_DEFAULT_BIN = '/Users/huangyuan/.opencode/bin/opencode';
export const OPENCODE_DEFAULT_MODEL = 'opencode-go/deepseek-v4.1-flash';
export const OPENCODE_DEFAULT_PROXY = 'http://127.0.0.1:7897';
export const OPENCODE_DEFAULT_TIMEOUT_MS = 180_000;

const MAX_CAPTURE_CHARS = 1_000_000;
const KILL_GRACE_MS = 3_000;

type RunOpenCodeOptions = {
  prompt: string;
  model?: string;
  timeoutMs?: number;
  bin?: string;
  cwd?: string;
  /** Empty string disables proxy injection (useful for local-only tests). */
  proxy?: string;
};

function envTrim(name: string): string {
  return (process.env[name] ?? '').trim();
}

export function getOpenCodeBin(): string {
  return envTrim('NEWS_OPENCODE_BIN') || OPENCODE_DEFAULT_BIN;
}

export function getOpenCodeModel(): string {
  return envTrim('NEWS_LLM_MODEL') || OPENCODE_DEFAULT_MODEL;
}

export function getOpenCodeProxy(): string {
  const raw = process.env.NEWS_OPENCODE_PROXY;
  if (raw === undefined) return OPENCODE_DEFAULT_PROXY;
  return raw.trim();
}

export function getOpenCodeCwd(): string {
  return envTrim('NEWS_OPENCODE_CWD') || path.join(os.tmpdir(), 'karios-news-opencode');
}

/**
 * The JSON event stream printed by `opencode run --format json` is JSONL.
 * Visible assistant output arrives as `{"type":"text","part":{"text":...}}`
 * events; reasoning/tool events are ignored.
 */
export function parseOpenCodeJsonl(stdout: string): string {
  const parts: string[] = [];
  for (const rawLine of (stdout ?? '').split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line.startsWith('{')) continue;
    try {
      const event = JSON.parse(line) as {
        type?: string;
        part?: { type?: string; text?: unknown };
      };
      if (event?.type === 'text' && typeof event.part?.text === 'string') {
        parts.push(event.part.text);
      }
    } catch {
      // Non-JSON diagnostic lines are ignored; the CLI may print logs.
    }
  }
  return parts.join('');
}

export function buildOpenCodeArgs(prompt: string, model: string): string[] {
  return [
    'run',
    // Private server: never touch the user's long-running serve processes.
    '--standalone',
    '--format',
    'json',
    '--title',
    'karios-news',
    '--model',
    model,
    prompt,
  ];
}

function appendCapped(current: string, chunk: string): string {
  const next = current + chunk;
  return next.length > MAX_CAPTURE_CHARS ? next.slice(-MAX_CAPTURE_CHARS) : next;
}

/**
 * Run one bounded generation. Resolves with the concatenated visible text.
 * Rejects on spawn failure, non-zero exit, timeout, or empty output. The
 * child is always reaped; on timeout we send SIGTERM then SIGKILL.
 */
export function runOpenCode(options: RunOpenCodeOptions): Promise<string> {
  const prompt = (options.prompt ?? '').trim();
  if (!prompt) return Promise.reject(new Error('OpenCode prompt is empty'));

  const bin = options.bin ?? getOpenCodeBin();
  const model = options.model ?? getOpenCodeModel();
  const timeoutMs = options.timeoutMs ?? OPENCODE_DEFAULT_TIMEOUT_MS;
  const cwd = options.cwd ?? getOpenCodeCwd();
  const proxy = options.proxy ?? getOpenCodeProxy();

  try {
    mkdirSync(cwd, { recursive: true });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return Promise.reject(new Error(`Cannot prepare OpenCode working directory ${cwd}: ${message}`));
  }

  const env: NodeJS.ProcessEnv = { ...process.env };
  if (proxy) {
    env.HTTPS_PROXY = proxy;
    env.HTTP_PROXY = proxy;
    env.ALL_PROXY = proxy;
    env.https_proxy = proxy;
    env.http_proxy = proxy;
    env.all_proxy = proxy;
  }

  return new Promise<string>((resolve, reject) => {
    let settled = false;
    let timedOut = false;
    let stdout = '';
    let stderr = '';
    let killTimer: NodeJS.Timeout | undefined;

    const child = spawn(bin, buildOpenCodeArgs(prompt, model), {
      cwd,
      env,
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    const timeoutTimer = setTimeout(() => {
      timedOut = true;
      child.kill('SIGTERM');
      killTimer = setTimeout(() => {
        child.kill('SIGKILL');
      }, KILL_GRACE_MS);
      killTimer.unref?.();
    }, timeoutMs);
    timeoutTimer.unref?.();

    const finish = (err: Error | null, text?: string) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeoutTimer);
      clearTimeout(killTimer);
      if (err) reject(err);
      else resolve(text ?? '');
    };

    child.stdout?.on('data', (chunk: Buffer) => {
      stdout = appendCapped(stdout, chunk.toString('utf8'));
    });
    child.stderr?.on('data', (chunk: Buffer) => {
      stderr = appendCapped(stderr, chunk.toString('utf8'));
    });

    child.on('error', (err) => {
      finish(new Error(`Failed to start OpenCode (${bin}): ${err.message}`));
    });

    child.on('close', (code, signal) => {
      if (timedOut) {
        finish(new Error(`OpenCode timed out after ${timeoutMs}ms`));
        return;
      }
      if (code !== 0) {
        const detail = stderr.trim().slice(-500);
        finish(
          new Error(
            `OpenCode exited with code ${code ?? 'null'}${signal ? ` (${signal})` : ''}` +
              (detail ? `: ${detail}` : ''),
          ),
        );
        return;
      }
      const text = parseOpenCodeJsonl(stdout);
      if (!text.trim()) {
        const detail = stderr.trim().slice(-500);
        finish(
          new Error(`OpenCode returned no text${detail ? ` (stderr: ${detail})` : ''}`),
        );
        return;
      }
      finish(null, text);
    });
  });
}
