import { existsSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';

import { describe, expect, it } from 'vitest';

const REPO_ROOT = resolve(__dirname, '../../../..');
const DESKTOP_UI_DIR = resolve(__dirname, '../..');
const SRC_TAURI_DIR = resolve(DESKTOP_UI_DIR, 'src-tauri');
const SIDECARS_SCRIPT = resolve(REPO_ROOT, 'scripts/build-sidecars-macos.sh');

const ROOT_PACKAGE_JSON = resolve(REPO_ROOT, 'package.json');
const DESKTOP_UI_PACKAGE_JSON = resolve(DESKTOP_UI_DIR, 'package.json');

function readJson(path: string): Record<string, unknown> {
  return JSON.parse(readFileSync(path, 'utf-8')) as Record<string, unknown>;
}

describe('Tauri deprecation (OPT-060)', () => {
  it('root package.json has no Tauri-related scripts', () => {
    const pkg = readJson(ROOT_PACKAGE_JSON);
    const scripts = Object.keys((pkg.scripts ?? {}) as Record<string, string>);
    const tauriScripts = scripts.filter((s) => /tauri/i.test(s));
    expect(tauriScripts).toEqual([]);
  });

  it('root package.json does not depend on `concurrently` (only used by the removed `dev:tauri`)', () => {
    const pkg = readJson(ROOT_PACKAGE_JSON);
    const devDeps = Object.keys(
      (pkg.devDependencies ?? {}) as Record<string, string>,
    );
    expect(devDeps).not.toContain('concurrently');
  });

  it('desktop-ui package.json has no Tauri build/dev scripts', () => {
    const pkg = readJson(DESKTOP_UI_PACKAGE_JSON);
    const scripts = Object.keys((pkg.scripts ?? {}) as Record<string, string>);
    const tauriScripts = scripts.filter((s) => /tauri/i.test(s));
    expect(tauriScripts).toEqual([]);
  });

  it('desktop-ui package.json has no @tauri-apps/* dependencies', () => {
    const pkg = readJson(DESKTOP_UI_PACKAGE_JSON);
    const deps = Object.keys((pkg.dependencies ?? {}) as Record<string, string>);
    const devDeps = Object.keys(
      (pkg.devDependencies ?? {}) as Record<string, string>,
    );
    const tauriDeps = [...deps, ...devDeps].filter((d) =>
      d.startsWith('@tauri-apps/'),
    );
    expect(tauriDeps).toEqual([]);
  });

  it('preserves src-tauri/ build config (Cargo.toml, tauri.conf.json, sidecar binaries)', () => {
    expect(existsSync(resolve(SRC_TAURI_DIR, 'Cargo.toml'))).toBe(true);
    expect(existsSync(resolve(SRC_TAURI_DIR, 'Cargo.lock'))).toBe(true);
    expect(existsSync(resolve(SRC_TAURI_DIR, 'tauri.conf.json'))).toBe(true);
    expect(existsSync(resolve(SRC_TAURI_DIR, 'src/lib.rs'))).toBe(true);
    expect(existsSync(resolve(SRC_TAURI_DIR, 'src/backends.rs'))).toBe(true);
  });

  it('preserves scripts/build-sidecars-macos.sh (Tauri sidecar build entrypoint)', () => {
    expect(existsSync(SIDECARS_SCRIPT)).toBe(true);
  });
});