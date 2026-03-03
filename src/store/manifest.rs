//! Dataset manifest writer for downstream systems.

use super::{csv, schema};
use chrono::Utc;
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::Path;

#[derive(Debug, Serialize)]
pub struct FileManifest {
    pub file: String,
    pub rows: usize,
    pub first_ts: u64,
    pub last_ts: u64,
    pub coverage: BTreeMap<String, f64>,
}

#[derive(Debug, Serialize)]
pub struct Manifest {
    pub generated_at_utc: String,
    pub schema_version: String,
    pub columns: Vec<schema::ColumnSpec>,
    pub file_count: usize,
    pub files: Vec<FileManifest>,
}

pub fn write(data_dir: &Path) -> anyhow::Result<Manifest> {
    let mut entries: Vec<_> = std::fs::read_dir(data_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map(|ext| ext == "csv")
                .unwrap_or(false)
        })
        .collect();
    entries.sort_by_key(|e| e.file_name());

    let mut files = Vec::new();
    for entry in entries {
        let path = entry.path();
        let rows = csv::read(&path);
        if rows.is_empty() {
            continue;
        }

        let total = rows.len() as f64;
        let first_ts = rows.first().map(|r| r.ts).unwrap_or(0);
        let last_ts = rows.last().map(|r| r.ts).unwrap_or(0);
        let mut coverage = BTreeMap::new();
        coverage.insert(
            "funding".to_string(),
            rows.iter().filter(|r| r.funding != 0.0).count() as f64 / total,
        );
        coverage.insert(
            "oi".to_string(),
            rows.iter().filter(|r| r.oi != 0.0).count() as f64 / total,
        );
        coverage.insert(
            "borrow".to_string(),
            rows.iter().filter(|r| r.borrow != 0.0).count() as f64 / total,
        );
        coverage.insert(
            "liq".to_string(),
            rows.iter().filter(|r| r.liq != 0.0).count() as f64 / total,
        );
        coverage.insert(
            "depeg".to_string(),
            rows.iter().filter(|r| r.depeg != 0.0).count() as f64 / total,
        );

        files.push(FileManifest {
            file: path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("?")
                .to_string(),
            rows: rows.len(),
            first_ts,
            last_ts,
            coverage,
        });
    }

    let manifest = Manifest {
        generated_at_utc: Utc::now().to_rfc3339(),
        schema_version: schema::SCHEMA_VERSION.to_string(),
        columns: schema::csv_columns(),
        file_count: files.len(),
        files,
    };
    let path = data_dir.join("manifest.json");
    std::fs::write(&path, serde_json::to_string_pretty(&manifest)?)?;
    Ok(manifest)
}
