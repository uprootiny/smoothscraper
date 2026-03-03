//! CSV persistence — read, merge, write with backup.
//!
//! Safety invariants:
//! - Never overwrites with empty data
//! - Creates .bak before overwrite
//! - Integrity check: monotonic timestamps, gap detection

use super::{Row, HEADER};
use anyhow::{anyhow, Result};
use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;

/// Read existing rows from CSV, skipping header.
pub fn read(path: &Path) -> Vec<Row> {
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };
    content
        .lines()
        .filter(|l| !l.starts_with("ts") && !l.starts_with('#') && !l.is_empty())
        .filter_map(Row::parse)
        .collect()
}

/// Merge existing + new, dedup by timestamp (new wins), sort, write.
/// Creates .bak backup before overwriting.
pub fn merge_and_write(existing: &[Row], new_rows: &[Row], path: &Path) -> Result<usize> {
    let mut by_ts: BTreeMap<u64, Row> = BTreeMap::new();
    for r in existing {
        by_ts.entry(r.ts).or_insert_with(|| r.clone());
    }
    for r in new_rows {
        by_ts.insert(r.ts, r.clone()); // new overwrites
    }

    if by_ts.is_empty() && path.exists() {
        return Err(anyhow!(
            "refusing to overwrite {} with empty data",
            path.display()
        ));
    }

    // Backup
    if path.exists() {
        let bak = path.with_extension("csv.bak");
        let _ = std::fs::copy(path, &bak);
    }

    // Ensure parent dir
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let mut f = std::fs::File::create(path)?;
    writeln!(f, "{}", HEADER)?;
    for (_, row) in &by_ts {
        writeln!(f, "{}", row.to_csv())?;
    }

    Ok(by_ts.len())
}

/// Check monotonic timestamps and report gaps.
pub fn integrity_check(path: &Path, step_secs: u64) -> IntegrityReport {
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(_) => return IntegrityReport::default(),
    };

    let mut prev_ts: Option<u64> = None;
    let mut gaps = 0;
    let mut rows = 0;
    let mut monotonic = true;

    for line in content.lines() {
        if line.starts_with("ts") || line.starts_with('#') || line.is_empty() {
            continue;
        }
        if let Some(ts) = line
            .split(',')
            .next()
            .and_then(|s| s.trim().parse::<u64>().ok())
        {
            rows += 1;
            if let Some(prev) = prev_ts {
                if ts <= prev {
                    monotonic = false;
                }
                if ts - prev > step_secs * 2 {
                    gaps += 1;
                }
            }
            prev_ts = Some(ts);
        }
    }

    IntegrityReport {
        rows,
        gaps,
        monotonic,
    }
}

#[derive(Debug, Default)]
pub struct IntegrityReport {
    pub rows: usize,
    pub gaps: usize,
    pub monotonic: bool,
}

impl std::fmt::Display for IntegrityReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.monotonic {
            write!(f, "ERROR: non-monotonic timestamps")?;
        } else if self.gaps > 0 {
            write!(f, "{} rows, {} gap(s)", self.rows, self.gaps)?;
        } else {
            write!(f, "{} rows, continuous", self.rows)?;
        }
        Ok(())
    }
}
