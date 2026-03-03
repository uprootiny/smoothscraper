//! Event log writer/reader used by the UI harness.

use chrono::Utc;
use serde::Serialize;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

#[derive(Debug, Serialize, Clone)]
pub struct LogEntry {
    pub timestamp: String,
    pub message: String,
}

const LOG_FILE: &str = "scraper.log";

pub fn append(data_dir: &Path, message: &str) -> anyhow::Result<()> {
    let path = data_dir.join(LOG_FILE);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new().create(true).append(true).open(&path)?;

    writeln!(file, "{}\t{}", Utc::now().to_rfc3339(), message)?;
    Ok(())
}

pub fn read(data_dir: &Path, limit: usize) -> Vec<LogEntry> {
    let path = data_dir.join(LOG_FILE);
    let content = match fs::read_to_string(&path) {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };

    let mut entries: Vec<_> = content
        .lines()
        .rev()
        .filter_map(|line| {
            let mut parts = line.splitn(2, '\t');
            let ts = parts.next()?;
            let msg = parts.next().unwrap_or("").to_string();
            Some(LogEntry {
                timestamp: ts.to_string(),
                message: msg,
            })
        })
        .take(limit)
        .collect();
    entries.reverse();
    entries
}

pub fn last_entry(data_dir: &Path) -> Option<LogEntry> {
    let mut entries = read(data_dir, 1);
    entries.pop()
}
