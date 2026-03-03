//! OHLCV kline fetcher — paginated, bounded, gentle.
//!
//! Fetches from Binance spot `/api/v3/klines`.
//! Each page: up to 1000 candles. Rate-limited at 200ms between pages.
//! Safety: max 50 pages per invocation, cursor-stuck detection.

use super::retry::{retry, RetryConfig};
use crate::store::Row;
use anyhow::{anyhow, Result};
use reqwest::Client;

const MAX_PAGES: usize = 50;
const BASE_PAGE_DELAY_MS: u64 = 200;

pub async fn fetch(
    client: &Client,
    cfg: &RetryConfig,
    spot_base: &str,
    symbol: &str,
    interval: &str,
    start_after_ts: Option<u64>,
    max_candles: usize,
    step_secs: u64,
) -> Result<Vec<Row>> {
    let mut rows = Vec::new();
    let mut start_ms = start_after_ts.map(|ts| (ts + step_secs) * 1000);

    for page in 0..MAX_PAGES {
        if rows.len() >= max_candles {
            break;
        }

        let batch = (max_candles - rows.len()).min(1000);
        let mut url = format!(
            "{spot_base}/api/v3/klines?symbol={}&interval={}&limit={}",
            symbol, interval, batch
        );
        if let Some(ms) = start_ms {
            url.push_str(&format!("&startTime={}", ms));
        }

        let url_c = url.clone();
        let cl = client.clone();
        let data: Vec<Vec<serde_json::Value>> = retry(cfg, "klines", || {
            let u = url_c.clone();
            let c = cl.clone();
            async move {
                let resp = c.get(&u).send().await?;
                let status = resp.status();
                if !status.is_success() {
                    let body = resp.text().await.unwrap_or_default();
                    return Err(anyhow!("klines HTTP {}: {}", status, body));
                }
                Ok(resp.json().await?)
            }
        })
        .await?;

        if data.is_empty() {
            break;
        }

        let batch_count = data.len();
        for k in &data {
            if k.len() < 6 {
                continue;
            }
            let ts = k[0].as_u64().unwrap_or(0) / 1000;
            if ts == 0 {
                continue;
            }
            rows.push(Row {
                ts,
                o: parse_str_f64(&k[1]),
                h: parse_str_f64(&k[2]),
                l: parse_str_f64(&k[3]),
                c: parse_str_f64(&k[4]),
                v: parse_str_f64(&k[5]),
                funding: 0.0,
                borrow: 0.0,
                liq: 0.0,
                depeg: 0.0,
                oi: 0.0,
            });
        }

        if batch_count < batch {
            break;
        }

        // Cursor advance with stuck detection
        if let Some(last) = rows.last() {
            let new_start = (last.ts + step_secs) * 1000;
            if Some(new_start) <= start_ms {
                eprintln!("[warn] klines cursor stuck at page {}, breaking", page);
                break;
            }
            start_ms = Some(new_start);
        }

        if page > 0 {
            eprint!("  klines page {}: {} total\r", page + 1, rows.len());
        }
        super::retry::page_delay(cfg, BASE_PAGE_DELAY_MS).await;
    }

    Ok(rows)
}

fn parse_str_f64(v: &serde_json::Value) -> f64 {
    v.as_str().unwrap_or("0").parse().unwrap_or(0.0)
}
