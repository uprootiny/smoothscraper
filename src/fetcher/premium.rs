//! Historical premium index fetcher.
//!
//! Fetches from Binance Futures `/fapi/v1/premiumIndexKlines`.
//! We use the close value as a depeg/basis-style signal.

use super::retry::{retry, RetryConfig};
use anyhow::anyhow;
use reqwest::Client;
use std::collections::BTreeMap;

const MAX_PAGES: usize = 20;
const BASE_PAGE_DELAY_MS: u64 = 200;

/// Returns BTreeMap<ts_seconds, premium_close>.
pub async fn fetch(
    client: &Client,
    cfg: &RetryConfig,
    futures_base: &str,
    symbol: &str,
    period: &str,
    start_ts: u64,
    end_ts: u64,
) -> BTreeMap<u64, f64> {
    let mut map = BTreeMap::new();
    let mut cursor_ms = start_ts * 1000;
    let end_ms = end_ts * 1000;

    let period_str = match period {
        "1m" | "3m" | "5m" | "15m" | "30m" | "1h" | "2h" | "4h" | "6h" | "8h" | "12h" | "1d"
        | "3d" | "1w" | "1M" => period,
        _ => "1h",
    };

    for page in 0..MAX_PAGES {
        if cursor_ms >= end_ms {
            break;
        }

        let url = format!(
            "{futures_base}/fapi/v1/premiumIndexKlines?symbol={}&interval={}&startTime={}&endTime={}&limit=1000",
            symbol, period_str, cursor_ms, end_ms
        );

        let url_c = url.clone();
        let cl = client.clone();
        let data: Vec<Vec<serde_json::Value>> = match retry(cfg, "premium", || {
            let u = url_c.clone();
            let c = cl.clone();
            async move {
                let resp = c.get(&u).send().await?;
                let status = resp.status();
                if !status.is_success() {
                    let body = resp.text().await.unwrap_or_default();
                    return Err(anyhow!("premium HTTP {}: {}", status, body));
                }
                Ok(resp.json().await?)
            }
        })
        .await
        {
            Ok(d) => d,
            Err(e) => {
                eprintln!("[warn] premium fetch failed: {}", e);
                break;
            }
        };

        if data.is_empty() {
            break;
        }

        let batch_len = data.len();
        for k in &data {
            if k.len() < 5 {
                continue;
            }
            let ts = k[0].as_u64().unwrap_or(0) / 1000;
            let close = k[4]
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            if ts > 0 {
                map.insert(ts, close);
            }
        }

        let new_cursor = data
            .last()
            .and_then(|k| k.get(0))
            .and_then(|v| v.as_u64())
            .unwrap_or(end_ms)
            + 1;

        if new_cursor <= cursor_ms {
            eprintln!("[warn] premium cursor stuck at page {}", page);
            break;
        }
        cursor_ms = new_cursor;

        if batch_len < 1000 {
            break;
        }

        super::retry::page_delay(cfg, BASE_PAGE_DELAY_MS).await;
    }

    eprintln!("  premium: {} entries for {}", map.len(), symbol);
    map
}
