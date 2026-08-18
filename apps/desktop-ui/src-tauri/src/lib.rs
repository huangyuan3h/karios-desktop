mod backends;

use tauri::Manager;

use backends::BackendManager;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
  let app = tauri::Builder::default()
    .manage(BackendManager::default())
    .setup(|app| {
      if cfg!(debug_assertions) {
        app.handle().plugin(
          tauri_plugin_log::Builder::default()
            .level(log::LevelFilter::Info)
            .build(),
        )?;
      }

      // In release builds, start bundled backend sidecars (ai-service + data-sync-service).
      // In dev, backends are started externally via `pnpm dev:tauri`.
      let mgr = app.state::<BackendManager>();
      mgr.start_on_launch(app.handle());

      Ok(())
    })
    .on_window_event(|window, event| {
      // Ensure sidecars are terminated when the main window is closed.
      if matches!(event, tauri::WindowEvent::CloseRequested { .. }) {
        let mgr = window.state::<BackendManager>();
        mgr.stop_all();
      }
    })
    .build(tauri::generate_context!());

  let app = match app {
    Ok(app) => app,
    Err(err) => {
      eprintln!("[karios] failed to build tauri app: {err}");
      std::process::exit(1);
    }
  };

  app.run(|app_handle, event| {
    // macOS Cmd+Q / app quit menu goes through RunEvent::ExitRequested,
    // which does not hit on_window_event — clean up sidecars there too
    // so we never leave orphaned backend processes holding the ports.
    if matches!(event, tauri::RunEvent::ExitRequested { .. }) {
      let mgr = app_handle.state::<BackendManager>();
      mgr.stop_all();
    }
  });
}
