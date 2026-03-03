//! Materialized scraper state snapshots for monitoring and downstream orchestration.

use super::csv;
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::Path;

#[derive(Debug, Serialize)]
pub struct StreamState {
    pub id: String,
    pub target: String,
    pub symbol: String,
    pub interval: String,
    pub file: String,
    pub rows: usize,
    pub last_ts: u64,
    pub last_utc: String,
    pub coverage: BTreeMap<String, f64>,
}

#[derive(Debug, Serialize)]
pub struct StateSnapshot {
    pub generated_at_utc: String,
    pub stream_count: usize,
    pub streams: Vec<StreamState>,
}

pub fn write(data_dir: &Path) -> anyhow::Result<StateSnapshot> {
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

    let mut streams = Vec::new();
    for entry in entries {
        let path = entry.path();
        let file = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("?")
            .to_string();
        let rows = csv::read(&path);
        if rows.is_empty() {
            continue;
        }

        let total = rows.len() as f64;
        let last_ts = rows.last().map(|r| r.ts).unwrap_or(0);
        let last_utc = DateTime::from_timestamp(last_ts as i64, 0)
            .map(|d| d.with_timezone(&Utc).to_rfc3339())
            .unwrap_or_else(|| "?".to_string());

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

        let (target, symbol, interval) = parse_stream_key(&file);
        let id = format!("{}:{}:{}", target, symbol, interval);

        streams.push(StreamState {
            id,
            target,
            symbol,
            interval,
            file,
            rows: rows.len(),
            last_ts,
            last_utc,
            coverage,
        });
    }

    let snapshot = StateSnapshot {
        generated_at_utc: Utc::now().to_rfc3339(),
        stream_count: streams.len(),
        streams,
    };

    let path = data_dir.join("state.json");
    std::fs::write(&path, serde_json::to_string_pretty(&snapshot)?)?;
    Ok(snapshot)
}

fn parse_stream_key(file: &str) -> (String, String, String) {
    let stem = file.strip_suffix(".csv").unwrap_or(file);

    if let Some((target, rest)) = stem.split_once("__") {
        if let Some((symbol, interval)) = split_symbol_interval(rest) {
            return (
                target.to_string(),
                symbol.to_uppercase(),
                interval.to_string(),
            );
        }
    }

    if let Some((symbol, interval)) = split_symbol_interval(stem) {
        return (
            "binance".to_string(),
            symbol.to_uppercase(),
            interval.to_string(),
        );
    }

    (
        "unknown".to_string(),
        stem.to_uppercase(),
        "unknown".to_string(),
    )
}

fn split_symbol_interval(stem: &str) -> Option<(&str, &str)> {
    stem.rsplit_once('_')
}
