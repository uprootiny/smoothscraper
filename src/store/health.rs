//! Health snapshot derived from manifest/state/log.

use super::{log, manifest, state};
use chrono::Utc;
use serde::Serialize;
use std::path::Path;

#[derive(Debug, Serialize)]
pub struct HealthReport {
    pub manifest_rows: usize,
    pub manifest_files: usize,
    pub state_streams: usize,
    pub generated_at_utc: String,
    pub last_log: Option<log::LogEntry>,
}

pub fn report(data_dir: &Path) -> anyhow::Result<HealthReport> {
    let manifest = manifest::write(data_dir)?;
    let state = state::write(data_dir)?;
    let last_log = log::last_entry(data_dir);
    let total_rows = manifest.files.iter().map(|f| f.rows).sum();
    Ok(HealthReport {
        manifest_rows: total_rows,
        manifest_files: manifest.file_count,
        state_streams: state.stream_count,
        generated_at_utc: Utc::now().to_rfc3339(),
        last_log,
    })
}
