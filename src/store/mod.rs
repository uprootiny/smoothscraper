//! CSV store — incremental, append-only, dedup-by-timestamp.
//!
//! The Row type matches arbitragefx's 11-column CSV format.
//! merge_and_write is total: it either succeeds with a consistent file
//! or returns Err without modifying the original.

pub mod csv;
pub mod health;
pub mod log;
pub mod manifest;
pub mod schema;
pub mod state;

use std::collections::BTreeMap;

pub const HEADER: &str = "ts,open,high,low,close,volume,funding,borrow,liq,depeg,oi";

#[derive(Debug, Clone)]
pub struct Row {
    pub ts: u64,
    pub o: f64,
    pub h: f64,
    pub l: f64,
    pub c: f64,
    pub v: f64,
    pub funding: f64,
    pub borrow: f64,
    pub liq: f64,
    pub depeg: f64,
    pub oi: f64,
}

impl Row {
    pub fn to_csv(&self) -> String {
        format!(
            "{},{},{},{},{},{},{},{},{},{},{}",
            self.ts,
            self.o,
            self.h,
            self.l,
            self.c,
            self.v,
            self.funding,
            self.borrow,
            self.liq,
            self.depeg,
            self.oi
        )
    }

    pub fn parse(line: &str) -> Option<Self> {
        let p: Vec<&str> = line.split(',').collect();
        if p.len() < 6 {
            return None;
        }
        Some(Row {
            ts: p[0].trim().parse().ok()?,
            o: p[1].trim().parse().ok()?,
            h: p[2].trim().parse().ok()?,
            l: p[3].trim().parse().ok()?,
            c: p[4].trim().parse().ok()?,
            v: p[5].trim().parse().ok()?,
            funding: p.get(6).and_then(|s| s.trim().parse().ok()).unwrap_or(0.0),
            borrow: p.get(7).and_then(|s| s.trim().parse().ok()).unwrap_or(0.0),
            liq: p.get(8).and_then(|s| s.trim().parse().ok()).unwrap_or(0.0),
            depeg: p.get(9).and_then(|s| s.trim().parse().ok()).unwrap_or(0.0),
            oi: p.get(10).and_then(|s| s.trim().parse().ok()).unwrap_or(0.0),
        })
    }
}

/// Find nearest prior value in a sorted map.
pub fn nearest_prior(map: &BTreeMap<u64, f64>, ts: u64) -> f64 {
    map.range(..=ts).next_back().map(|(_, v)| *v).unwrap_or(0.0)
}

#[derive(Debug, Default)]
pub struct EnrichCounts {
    pub funding: usize,
    pub oi: usize,
    pub borrow: usize,
    pub liq: usize,
    pub depeg: usize,
}

/// Enrich rows with aux data.
pub fn enrich(
    rows: &mut [Row],
    funding: &BTreeMap<u64, f64>,
    oi: &BTreeMap<u64, f64>,
    taker: &BTreeMap<u64, f64>,
    long_short: &BTreeMap<u64, f64>,
    premium: &BTreeMap<u64, f64>,
) -> EnrichCounts {
    let mut counts = EnrichCounts::default();
    for row in rows.iter_mut() {
        if row.funding == 0.0 {
            let val = nearest_prior(funding, row.ts);
            if val != 0.0 {
                row.funding = val;
                counts.funding += 1;
            }
        }
        if row.oi == 0.0 {
            let val = nearest_prior(oi, row.ts);
            if val != 0.0 {
                row.oi = val;
                counts.oi += 1;
            }
        }
        if row.borrow == 0.0 {
            let val = nearest_prior(taker, row.ts);
            if val != 0.0 {
                row.borrow = val;
                counts.borrow += 1;
            }
        }
        if row.liq == 0.0 {
            let val = nearest_prior(long_short, row.ts);
            if val != 0.0 {
                row.liq = val;
                counts.liq += 1;
            }
        }
        if row.depeg == 0.0 {
            let val = nearest_prior(premium, row.ts);
            if val != 0.0 {
                row.depeg = val;
                counts.depeg += 1;
            }
        }
    }
    counts
}
