use std::path::PathBuf;
use std::fs::{create_dir_all, OpenOptions};
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use tauri::{AppHandle, Manager};

#[derive(Debug)]
struct BackendChild {
  name: &'static str,
  port: u16,
  child: Child,
}

#[derive(Default)]
pub struct BackendManager {
  children: Mutex<Vec<BackendChild>>,
}

fn is_port_open(port: u16) -> bool {
  std::net::TcpStream::connect_timeout(
    &std::net::SocketAddr::from(([127, 0, 0, 1], port)),
    Duration::from_millis(200),
  )
  .is_ok()
}

fn wait_port(port: u16, timeout: Duration) -> bool {
  let start = Instant::now();
  while start.elapsed() < timeout {
    if is_port_open(port) {
      return true;
    }
    std::thread::sleep(Duration::from_millis(120));
  }
  false
}

fn exe_suffix() -> &'static str {
  if cfg!(windows) { ".exe" } else { "" }
}

fn tauri_target_triple() -> Option<&'static str> {
  // Provided by tauri-build at compile time for bundle builds.
  option_env!("TAURI_ENV_TARGET_TRIPLE")
}

fn candidate_dirs(app: &AppHandle) -> Vec<PathBuf> {
  // Tauri bundles external binaries near the main executable on most platforms.
  // We also search resource_dir as a fallback to be more robust across bundlers.
  let mut out: Vec<PathBuf> = vec![];

  if let Ok(exe) = std::env::current_exe() {
    if let Some(dir) = exe.parent() {
      out.push(dir.to_path_buf());
    }
  }

  if let Ok(res) = app.path().resource_dir() {
    out.push(res);
  }

  // De-dup
  out.sort();
  out.dedup();
  out
}

fn find_external_bin(app: &AppHandle, base_name: &str) -> Option<PathBuf> {
  // Tauri bundles sidecars with a `-{target_triple}` suffix by default.
  // We search both `{name}-{target}` and `{name}` for dev / custom layouts.
  let suffix = exe_suffix();
  let mut candidates: Vec<String> = vec![];

  if let Some(triple) = tauri_target_triple() {
    candidates.push(format!("{base_name}-{triple}{suffix}"));
  }
  candidates.push(format!("{base_name}{suffix}"));

  for dir in candidate_dirs(app) {
    for file in &candidates {
      let p = dir.join(file);
      if p.exists() {
        return Some(p);
      }
    }
  }
  None
}

fn spawn_backend(
  app: &AppHandle,
  name: &'static str,
  port: u16,
  timeout: Duration,
  envs: &[(&str, String)],
) -> Result<Child, String> {
  let bin = find_external_bin(app, name).ok_or_else(|| {
    format!(
      "Sidecar binary not found: {} (searched in {:?})",
      name,
      candidate_dirs(app)
    )
  })?;

  let log_dir = app
    .path()
    .app_data_dir()
    .map(|p| p.join("logs"))
    .map_err(|e| format!("Failed to resolve app_data_dir for logs: {e}"))?;
  let _ = create_dir_all(&log_dir);

  let log_path = log_dir.join(format!("{name}.log"));
  let log_file = OpenOptions::new()
    .create(true)
    .append(true)
    .open(&log_path)
    .map_err(|e| format!("Failed to open log file {:?}: {e}", log_path))?;

  let mut cmd = Command::new(&bin);
  cmd.stdin(Stdio::null())
    .stdout(Stdio::from(
      log_file
        .try_clone()
        .map_err(|e| format!("Failed to clone log file handle {:?}: {e}", log_path))?,
    ))
    .stderr(Stdio::from(log_file));

  for (k, v) in envs {
    cmd.env(k, v);
  }

  cmd.env("KARIOS_SIDE_CAR", "1");

  // NOTE: We intentionally don't read child stdout/stderr in release to avoid blocking.
  // If you need troubleshooting, check the log file under app_data_dir/logs/.
  let child = cmd
    .spawn()
    .map_err(|e| format!("Failed to spawn {name} ({:?}): {e}", bin))?;

  if !wait_port(port, timeout) {
    return Err(format!(
      "{name} did not become ready on port {port} within timeout"
    ));
  }

  Ok(child)
}

static START_ONCE: OnceLock<()> = OnceLock::new();

impl BackendManager {
  /// Starts bundled backends (sidecars) in release builds.
  /// This is idempotent within a single app process.
  pub fn start_on_launch(&self, app: &AppHandle) {
    if START_ONCE.get().is_some() {
      return;
    }
    START_ONCE.set(()).ok();

    // Avoid spawning in dev; the repo uses `pnpm dev:tauri` to run backends separately.
    if cfg!(debug_assertions) {
      return;
    }

    // Start ai-service first (quant-service depends on it).
    let ai_port: u16 = 4310;
    let quant_port: u16 = 4320;

    let mut spawned: Vec<BackendChild> = vec![];

    // Provide a stable app-specific data directory so ai-service can persist runtime config
    // (e.g. model provider, model id, API keys) without relying on env vars.
    let app_data_dir = app
      .path()
      .app_data_dir()
      .ok()
      .and_then(|p| {
        let _ = std::fs::create_dir_all(&p);
        Some(p.to_string_lossy().to_string())
      })
      .unwrap_or_else(|| ".".to_string());

    let ai = spawn_backend(
      app,
      "karios-ai-service",
      ai_port,
      Duration::from_secs(10),
      &[
        ("PORT", ai_port.to_string()),
        ("NODE_ENV", "production".to_string()),
        ("KARIOS_APP_DATA_DIR", app_data_dir.clone()),
      ],
    );

    match ai {
      Ok(child) => {
        eprintln!("[karios] started sidecar: karios-ai-service on 127.0.0.1:{ai_port}");
        spawned.push(BackendChild {
          name: "karios-ai-service",
          port: ai_port,
          child,
        });
      }
      Err(err) => {
        eprintln!("[karios] failed to start ai-service sidecar: {err}");
        // If AI is unavailable, quant-service will still run but strategy features will fail.
      }
    }

    let quant_envs = [
      ("HOST", "127.0.0.1".to_string()),
      ("PORT", quant_port.to_string()),
      ("AI_SERVICE_BASE_URL", format!("http://127.0.0.1:{ai_port}")),
      ("PYTHONUNBUFFERED", "1".to_string()),
      (
        "DATABASE_PATH",
        app
          .path()
          .app_data_dir()
          .ok()
          .and_then(|p| {
            let _ = std::fs::create_dir_all(&p);
            Some(p.join("karios.sqlite3").to_string_lossy().to_string())
          })
          .unwrap_or_else(|| "karios.sqlite3".to_string()),
      ),
    ];

    let quant = spawn_backend(
      app,
      "karios-quant-service",
      quant_port,
      Duration::from_secs(25),
      &quant_envs,
    );
    match quant {
      Ok(child) => {
        eprintln!("[karios] started sidecar: karios-quant-service on 127.0.0.1:{quant_port}");
        spawned.push(BackendChild {
          name: "karios-quant-service",
          port: quant_port,
          child,
        });
      }
      Err(err) => {
        eprintln!("[karios] failed to start quant-service sidecar: {err}");
      }
    }

    *self.children.lock().expect("backend children lock poisoned") = spawned;
  }

  pub fn stop_all(&self) {
    let mut children = self.children.lock().expect("backend children lock poisoned");
    for c in children.iter_mut() {
      eprintln!("[karios] stopping sidecar: {} on 127.0.0.1:{}", c.name, c.port);
      // Best-effort: ignore failures
      let _ = c.child.kill();
    }
    children.clear();
  }
}


