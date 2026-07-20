import { execSync } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const ENV_PATH = path.join(ROOT, '.env');
const SERVICE = 'rsshub';
const DEFAULT_BASE_URL = 'http://127.0.0.1:1200';
const STARTUP_TIMEOUT_MS = 90_000;

function sh(cmd, opts = {}) {
  return execSync(cmd, {
    cwd: ROOT,
    stdio: ['ignore', 'pipe', 'pipe'],
    encoding: 'utf8',
    ...opts,
  }).trim();
}

function loadRootEnv() {
  if (!existsSync(ENV_PATH)) return;
  const content = readFileSync(ENV_PATH, 'utf8');
  for (const rawLine of content.split('\n')) {
    const line = rawLine.trim();
    if (!line || line.startsWith('#')) continue;
    const idx = line.indexOf('=');
    if (idx <= 0) continue;
    const key = line.slice(0, idx).trim();
    let value = line.slice(idx + 1).trim();
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }
    if (process.env[key] === undefined) process.env[key] = value;
  }
}

function isTruthy(value, defaultValue = true) {
  const raw = (value ?? (defaultValue ? '1' : '0')).trim().toLowerCase();
  if (['0', 'false', 'no', 'off'].includes(raw)) return false;
  if (['1', 'true', 'yes', 'on'].includes(raw)) return true;
  return defaultValue;
}

function isCommandAvailable(name) {
  try {
    sh(`command -v ${name}`);
    return true;
  } catch {
    return false;
  }
}

function dockerComposeCmd() {
  if (isCommandAvailable('docker')) {
    try {
      sh('docker compose version');
      return 'docker compose';
    } catch {
      /* fall through */
    }
  }
  if (isCommandAvailable('docker-compose')) {
    return 'docker-compose';
  }
  return null;
}

function parseBaseUrl(raw) {
  try {
    return new URL(raw || DEFAULT_BASE_URL);
  } catch {
    return new URL(DEFAULT_BASE_URL);
  }
}

function httpProbe(url) {
  return new Promise((resolve) => {
    const req = http.get(url, { timeout: 3000 }, (res) => {
      res.resume();
      resolve(res.statusCode !== undefined && res.statusCode < 500);
    });
    req.on('timeout', () => {
      req.destroy();
      resolve(false);
    });
    req.on('error', () => resolve(false));
  });
}

async function waitForHealthy(baseUrl) {
  const deadline = Date.now() + STARTUP_TIMEOUT_MS;
  const probeUrl = new URL('/', baseUrl).toString();
  while (Date.now() < deadline) {
    if (await httpProbe(probeUrl)) return true;
    await new Promise((r) => setTimeout(r, 1500));
  }
  return false;
}

function containerRunning(composeCmd) {
  try {
    const out = sh(`${composeCmd} ps -q ${SERVICE}`);
    if (!out) return false;
    const status = sh(`docker inspect -f '{{.State.Running}}' ${out.split('\n')[0]}`);
    return status === 'true';
  } catch {
    return false;
  }
}

async function main() {
  loadRootEnv();

  if (!isTruthy(process.env.KARIOS_AUTO_START_RSSHUB, true)) {
    console.log('[ensure-rsshub] auto-start disabled (KARIOS_AUTO_START_RSSHUB=0).');
    return;
  }

  const baseUrl = process.env.ALPHA_RADAR_RSSHUB_BASE_URL || DEFAULT_BASE_URL;
  const parsed = parseBaseUrl(baseUrl);
  if (parsed.hostname !== '127.0.0.1' && parsed.hostname !== 'localhost') {
    console.log(`[ensure-rsshub] skip remote RSSHub base URL: ${baseUrl}`);
    return;
  }

  const composeCmd = dockerComposeCmd();
  if (!composeCmd) {
    console.warn('[ensure-rsshub] docker compose not found; skipping RSSHub auto-start.');
    return;
  }

  try {
    sh(`${composeCmd} ps`);
  } catch {
    console.warn('[ensure-rsshub] docker daemon unavailable; skipping RSSHub auto-start.');
    return;
  }

  const alreadyHealthy = await httpProbe(new URL('/', baseUrl).toString());
  if (alreadyHealthy && containerRunning(composeCmd)) {
    console.log(`[ensure-rsshub] RSSHub already running at ${baseUrl}`);
    return;
  }

  if (alreadyHealthy) {
    console.log(`[ensure-rsshub] RSSHub reachable at ${baseUrl} (non-compose instance).`);
    return;
  }

  console.log(`[ensure-rsshub] starting ${SERVICE} via ${composeCmd}...`);
  try {
    sh(`${composeCmd} up -d ${SERVICE}`);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.warn(`[ensure-rsshub] failed to start ${SERVICE}: ${msg}`);
    return;
  }

  const ok = await waitForHealthy(baseUrl);
  if (ok) {
    console.log(`[ensure-rsshub] RSSHub ready at ${baseUrl}`);
  } else {
    console.warn(
      `[ensure-rsshub] ${SERVICE} container started but ${baseUrl} not reachable yet. Check: ${composeCmd} logs ${SERVICE}`,
    );
  }
}

await main();
