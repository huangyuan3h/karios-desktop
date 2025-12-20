import { execSync } from 'node:child_process';

const ports = (process.env.KARIOS_PORTS ?? '3000,3001,4310,4320')
  .split(',')
  .map((p) => Number(p.trim()))
  .filter((n) => Number.isFinite(n) && n > 0);

function sh(cmd) {
  return execSync(cmd, { stdio: ['ignore', 'pipe', 'pipe'] }).toString().trim();
}

function isCommandAvailable(name) {
  try {
    sh(`command -v ${name}`);
    return true;
  } catch {
    return false;
  }
}

function listPidsForPort(port) {
  // Works on macOS/Linux with lsof.
  // -t: only pids, -iTCP:port: filter port, -sTCP:LISTEN: only listeners
  try {
    const out = sh(`lsof -nP -tiTCP:${port} -sTCP:LISTEN || true`);
    if (!out) return [];
    return Array.from(new Set(out.split('\n').map((x) => x.trim()).filter(Boolean)));
  } catch {
    return [];
  }
}

function killPids(pids) {
  if (pids.length === 0) return;
  // Try TERM first, then KILL.
  try {
    sh(`kill -TERM ${pids.join(' ')} || true`);
  } catch {}

  // Give the process a moment to exit.
  try {
    sh(`sleep 0.2 || true`);
  } catch {}

  try {
    sh(`kill -KILL ${pids.join(' ')} || true`);
  } catch {}
}

function main() {
  if (!isCommandAvailable('lsof')) {
    console.warn('[ensure-ports] lsof not found; skipping port checks.');
    return;
  }

  const killed = [];

  for (const port of ports) {
    const pids = listPidsForPort(port);
    if (pids.length === 0) continue;
    killPids(pids);
    killed.push({ port, pids });
  }

  if (killed.length > 0) {
    for (const item of killed) {
      console.log(`[ensure-ports] freed port ${item.port} (killed pids: ${item.pids.join(', ')})`);
    }
  } else {
    console.log('[ensure-ports] ports are free.');
  }
}

main();


