//! Historical taker buy/sell ratio fetcher.
//!
//! Fetches from Binance Futures `/futures/data/takerlongshortRatio`.
//! We use buySellRatio as a borrow/flow pressure proxy.

use super::retry::{retry, RetryConfig};
use anyhow::anyhow;
use reqwest::Client;
use std::collections::BTreeMap;

const MAX_PAGES: usize = 20;
const BASE_PAGE_DELAY_MS: u64 = 200;

/// Returns BTreeMap<ts_seconds, buy_sell_ratio>.
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
        "5m" | "15m" | "30m" | "1h" | "2h" | "4h" | "6h" | "12h" | "1d" => period,
        _ => "1h",
    };

    for page in 0..MAX_PAGES {
        if cursor_ms >= end_ms {
            break;
        }

        let url = format!(
            "{futures_base}/futures/data/takerlongshortRatio?symbol={}&period={}&startTime={}&endTime={}&limit=500",
            symbol, period_str, cursor_ms, end_ms
        );

        let url_c = url.clone();
        let cl = client.clone();
        let data: Vec<serde_json::Value> = match retry(cfg, "taker", || {
            let u = url_c.clone();
            let c = cl.clone();
            async move {
                let resp = c.get(&u).send().await?;
                let status = resp.status();
                if !status.is_success() {
                    let body = resp.text().await.unwrap_or_default();
                    return Err(anyhow!("taker HTTP {}: {}", status, body));
                }
                Ok(resp.json().await?)
            }
        })
        .await
        {
            Ok(d) => d,
            Err(e) => {
                let msg = format!("{}", e);
                if msg.contains("400") {
                    eprintln!("  taker: lookback exceeded for {} (expected)", symbol);
                } else {
                    eprintln!("[warn] taker fetch failed: {}", e);
                }
                break;
            }
        };

        if data.is_empty() {
            break;
        }

        let batch_len = data.len();
        for entry in &data {
            if let (Some(ts_ms), Some(ratio_str)) = (
                entry.get("timestamp").and_then(|v| v.as_u64()),
                entry.get("buySellRatio").and_then(|v| v.as_str()),
            ) {
                if let Ok(ratio) = ratio_str.parse::<f64>() {
                    map.insert(ts_ms / 1000, ratio);
                }
            }
        }

        let new_cursor = data
            .last()
            .and_then(|e| e.get("timestamp"))
            .and_then(|v| v.as_u64())
            .unwrap_or(end_ms)
            + 1;

        if new_cursor <= cursor_ms {
            eprintln!("[warn] taker cursor stuck at page {}", page);
            break;
        }
        cursor_ms = new_cursor;

        if batch_len < 500 {
            break;
        }

        super::retry::page_delay(cfg, BASE_PAGE_DELAY_MS).await;
    }

    eprintln!("  taker: {} entries for {}", map.len(), symbol);
    map
}
