use crate::store::{health, log, manifest, schema, state};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Json, Response};
use axum::{routing::get, Router};
use serde::Serialize;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;

#[derive(Clone)]
struct AppState {
    data_dir: Arc<PathBuf>,
}

pub async fn run(port: u16, data_dir: PathBuf) -> anyhow::Result<()> {
    let state = AppState {
        data_dir: Arc::new(data_dir),
    };

    let app = Router::new()
        .route("/", get(homepage))
        .route("/api/manifest", get(manifest_handler))
        .route("/api/state", get(state_handler))
        .route("/api/schema", get(schema_handler))
        .route("/api/logs", get(logs_handler))
        .route("/api/health", get(health_handler))
        .with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    println!("Serving situational awareness UI on http://{}", addr);

    let listener = TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn homepage(State(_): State<AppState>) -> impl IntoResponse {
    Html(home_page_html())
}

async fn manifest_handler(State(state): State<AppState>) -> Response {
    let data_dir = state.data_dir.clone();
    match manifest::write(&data_dir) {
        Ok(m) => Json(m).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("manifest: {}", e),
        )
            .into_response(),
    }
}

async fn state_handler(State(st): State<AppState>) -> Response {
    let data_dir = st.data_dir.clone();
    match state::write(&data_dir) {
        Ok(s) => Json(s).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("state: {}", e)).into_response(),
    }
}

async fn logs_handler(State(state): State<AppState>) -> Response {
    let entries = log::read(&state.data_dir, 20);
    Json(entries).into_response()
}

async fn health_handler(State(state): State<AppState>) -> Response {
    match health::report(&state.data_dir) {
        Ok(h) => Json(h).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, format!("health: {}", e)).into_response(),
    }
}

async fn schema_handler(State(_): State<AppState>) -> impl IntoResponse {
    let columns = schema::csv_columns();
    Json(SchemaResponse {
        version: schema::SCHEMA_VERSION,
        columns,
    })
}

#[derive(Serialize)]
struct SchemaResponse {
    version: &'static str,
    columns: Vec<schema::ColumnSpec>,
}

fn home_page_html() -> &'static str {
    r#"<!DOCTYPE html>
<html lang='en'>
<head>
  <meta charset='UTF-8' />
  <title>smoothscraper situational awareness</title>
  <meta name='viewport' content='width=device-width,initial-scale=1' />
  <style>
    body {
      margin: 0;
      background: radial-gradient(circle at top, #12162c, #0b0d18 60%);
      color: #e0e7ff;
      font-family: 'Space Grotesk', 'Inter', system-ui, sans-serif;
      min-height: 100vh;
    }
    .shell {
      padding: 24px;
      max-width: 1100px;
      margin: 0 auto;
    }
    header {
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      flex-wrap: wrap;
      gap: 12px;
    }
    h1 {
      margin: 0;
      font-size: 2.4rem;
      letter-spacing: -0.02em;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 16px;
      margin-top: 32px;
    }
    .card {
      background: rgba(21, 27, 45, 0.8);
      border: 1px solid rgba(255, 255, 255, 0.1);
      border-radius: 16px;
      padding: 18px;
      box-shadow: 0 20px 40px rgba(0, 0, 0, 0.4);
    }
    .refresh-btn {
      border: 1px solid rgba(236, 72, 153, 0.4);
      background: rgba(236, 72, 153, 0.15);
      border-radius: 999px;
      color: #f3c6e5;
      font-size: 0.8rem;
      padding: 6px 14px;
      cursor: pointer;
      transition: opacity 0.15s ease;
    }
    .refresh-btn:disabled {
      opacity: 0.4;
      cursor: default;
    }
    .log-entry {
      display: flex;
      flex-direction: column;
      font-size: 0.8rem;
      gap: 2px;
      padding: 6px 0;
      border-bottom: 1px solid rgba(255, 255, 255, 0.05);
    }
    .log-ts {
      font-size: 0.7rem;
      color: rgba(255, 255, 255, 0.5);
    }
    .log-msg {
      color: #cbd5ff;
    }
    .log-empty {
      font-size: 0.9rem;
      color: rgba(255, 255, 255, 0.6);
    }
    .mini-chip {
      padding: 4px 10px;
      border-radius: 999px;
      background: rgba(236, 72, 153, 0.25);
      color: #fce7f3;
      font-size: 0.8rem;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      font-weight: 600;
    }
    .footer {
      margin-top: 32px;
      font-size: 0.8rem;
      text-align: center;
      color: rgba(255, 255, 255, 0.6);
    }
  </style>
</head>
<body>
  <div class='shell'>
    <header>
      <div>
        <h1>smoothscraper situational awareness</h1>
        <p>Live manifest, state, schema, and log views for the running scraper.</p>
      </div>
      <div class='mini-chip'>Secure cascade harness</div>
    </header>

    <div class='grid'>
      <article class='card'>
        <h2>Manifest snapshot</h2>
        <p id='manifest-meta'>Loading manifest…</p>
        <div id='manifest-table'></div>
      </article>
      <article class='card'>
        <h2>State snapshot</h2>
        <p id='state-meta'>Loading state…</p>
        <div id='state-table'></div>
      </article>
      <article class='card'>
        <h2>Schema</h2>
        <p id='schema-meta'>Loading schema…</p>
        <div id='schema-table'></div>
      </article>
      <article class='card'>
        <h2>Health & logs</h2>
        <p id='health-meta'>Loading health…</p>
        <div id='health-table'></div>
        <p>
          <button id='refresh-btn' class='refresh-btn' type='button'>Refresh manifest/state</button>
        </p>
        <div id='log-table'></div>
      </article>
    </div>
    <div class='footer'>Updated every load or manual refresh. Log entries preserve cascade proof.</div>
  </div>
  <script>
    async function load() {
      const refreshBtn = document.getElementById('refresh-btn');
      if (refreshBtn) {
        refreshBtn.disabled = true;
        refreshBtn.textContent = 'Refreshing…';
      }
      try {
      const [manifest, state, schema, logs, health] = await Promise.all([
        fetch('/api/manifest').then((r) => r.json()),
        fetch('/api/state').then((r) => r.json()),
        fetch('/api/schema').then((r) => r.json()),
        fetch('/api/logs').then((r) => r.json()),
        fetch('/api/health').then((r) => r.json()),
      ]);

        document.getElementById('manifest-meta').textContent =
          `${manifest.file_count} files · schema ${manifest.schema_version}`;

        const manifestTable = document.getElementById('manifest-table');
        manifestTable.innerHTML = manifest.files
          .map(
            (file) => `
          <div style='margin-bottom:12px'>
            <strong>${file.file}</strong><br/>
            rows: ${file.rows} · ${new Date(file.first_ts * 1000).toISOString().slice(0, 10)} → ${new Date(file.last_ts * 1000).toISOString().slice(0, 10)}<br/>
            coverage: ${Object.entries(file.coverage)
              .map(([k, v]) => `${k} ${(v * 100).toFixed(1)}%`)
              .join(' · ')}
          </div>`
          )
          .join('');

        document.getElementById('state-meta').textContent =
          `${state.stream_count} streams · ${state.generated_at_utc}`;

        const stateTable = document.getElementById('state-table');
        stateTable.innerHTML = state.streams
          .map(
            (stream) => `
        <div style='margin-bottom:12px'>
          <strong>${stream.id}</strong><br/>
          file: ${stream.file} · rows ${stream.rows}<br/>
          last: ${stream.last_utc} · ${Object.entries(stream.coverage)
            .map(([k, v]) => `${k} ${(v * 100).toFixed(1)}%`)
            .join(' · ')}
        </div>`
          )
          .join('');

        document.getElementById('schema-meta').textContent =
          `Columns (${schema.columns.length}) version ${schema.version}`;
        const schemaTable = document.getElementById('schema-table');
        schemaTable.innerHTML = schema.columns
          .map(
            (col) => `
          <div style='margin-bottom:10px'>
            <strong>${col.name}</strong> <small>${col.dtype}</small><br/>
            ${col.semantic}<br/>
            source: ${col.source}
          </div>`
          )
          .join('');

        const logTable = document.getElementById('log-table');
        logTable.innerHTML = logs.length
          ? logs
              .map(
                (entry) => `
          <div class='log-entry'>
            <span class='log-ts'>${new Date(entry.timestamp).toLocaleString()}</span>
            <span class='log-msg'>${entry.message}</span>
          </div>`
              )
              .join('')
          : "<div class='log-empty'>No log entries yet.</div>";
        document.getElementById('health-meta').textContent =
          `Health snapshot: ${new Date(health.generated_at_utc).toLocaleString()}`;
        const healthTable = document.getElementById('health-table');
        const logNote = health.last_log
          ? `${new Date(health.last_log.timestamp).toLocaleString()} – ${health.last_log.message}`
          : 'No log traced yet';
        healthTable.innerHTML = `
          <div style='margin-bottom:8px'>Manifest rows: ${health.manifest_rows}</div>
          <div style='margin-bottom:8px'>Manifest files: ${health.manifest_files}</div>
          <div style='margin-bottom:8px'>Streams: ${health.state_streams}</div>
          <div style='margin-bottom:0'>Last logged cascade: ${logNote}</div>
        `;
      } catch (err) {
        console.error('UI load failed', err);
      } finally {
        if (refreshBtn) {
          refreshBtn.disabled = false;
          refreshBtn.textContent = 'Refresh manifest/state';
        }
      }
    }
    document.getElementById('refresh-btn').addEventListener('click', load);
    load().catch((err) => console.error('UI load failed', err));
  </script>
</body>
</html>"#
}
