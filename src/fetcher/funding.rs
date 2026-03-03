//! Historical funding rate fetcher — paginated, bounded.
//!
//! Fetches from Binance Futures `/fapi/v1/fundingRate`.
//! Funding rates are emitted every 8h. Pagination needed for >1000 entries (~333 days).
//! Safety: max 20 pages, cursor-stuck detection, 400 → empty (not retried).

use super::retry::{retry, RetryConfig};
use anyhow::anyhow;
use reqwest::Client;
use std::collections::BTreeMap;

const MAX_PAGES: usize = 20;
const BASE_PAGE_DELAY_MS: u64 = 200;

/// Returns BTreeMap<ts_seconds, funding_rate>.
pub async fn fetch(
    client: &Client,
    cfg: &RetryConfig,
    futures_base: &str,
    symbol: &str,
    start_ts: u64,
    end_ts: u64,
) -> BTreeMap<u64, f64> {
    let mut map = BTreeMap::new();
    let mut cursor_ms = start_ts * 1000;
    let end_ms = end_ts * 1000;

    for page in 0..MAX_PAGES {
        if cursor_ms >= end_ms {
            break;
        }

        let url = format!(
            "{futures_base}/fapi/v1/fundingRate?symbol={}&startTime={}&endTime={}&limit=1000",
            symbol, cursor_ms, end_ms
        );
        let url_c = url.clone();
        let cl = client.clone();

        let data: Vec<serde_json::Value> = match retry(cfg, "funding", || {
            let u = url_c.clone();
            let c = cl.clone();
            async move {
                let resp = c.get(&u).send().await?;
                let status = resp.status();
                if !status.is_success() {
                    let body = resp.text().await.unwrap_or_default();
                    return Err(anyhow!("funding HTTP {}: {}", status, body));
                }
                Ok(resp.json().await?)
            }
        })
        .await
        {
            Ok(d) => d,
            Err(e) => {
                eprintln!("[warn] funding fetch failed: {}", e);
                break;
            }
        };

        if data.is_empty() {
            break;
        }

        let batch_len = data.len();
        for entry in &data {
            if let (Some(ts_ms), Some(rate_str)) = (
                entry.get("fundingTime").and_then(|v| v.as_u64()),
                entry.get("fundingRate").and_then(|v| v.as_str()),
            ) {
                if let Ok(rate) = rate_str.parse::<f64>() {
                    map.insert(ts_ms / 1000, rate);
                }
            }
        }

        // Advance cursor with stuck detection
        if let Some(last) = data.last() {
            let new_cursor = last
                .get("fundingTime")
                .and_then(|v| v.as_u64())
                .unwrap_or(end_ms)
                + 1;
            if new_cursor <= cursor_ms {
                eprintln!("[warn] funding cursor stuck at page {}", page);
                break;
            }
            cursor_ms = new_cursor;
        } else {
            break;
        }

        // Under-full page means we've exhausted the range
        if batch_len < 1000 {
            break;
        }

        super::retry::page_delay(cfg, BASE_PAGE_DELAY_MS).await;
    }

    eprintln!("  funding: {} rates for {}", map.len(), symbol);
    map
}
