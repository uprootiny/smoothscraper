//! Historical open interest fetcher — single page, lookback-clamped.
//!
//! Fetches from Binance Futures `/futures/data/openInterestHist`.
//! This endpoint has a ~30-day lookback limit; older startTimes get 400.
//! We clamp the start time and return what's available.

use super::retry::{retry, RetryConfig};
use anyhow::anyhow;
use reqwest::Client;
use std::collections::BTreeMap;

const MAX_LOOKBACK_SECS: u64 = 29 * 24 * 3600;

/// Returns BTreeMap<ts_seconds, open_interest>.
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

    // Clamp to 29-day lookback
    let clamped_start = if end_ts > MAX_LOOKBACK_SECS {
        start_ts.max(end_ts - MAX_LOOKBACK_SECS)
    } else {
        start_ts
    };

    let period_str = match period {
        "5m" | "15m" | "1h" | "4h" | "1d" => period,
        _ => "1h",
    };

    let url = format!(
        "{futures_base}/futures/data/openInterestHist?symbol={}&period={}&startTime={}&endTime={}&limit=500",
        symbol, period_str, clamped_start * 1000, end_ts * 1000
    );
    let url_c = url.clone();
    let cl = client.clone();

    let data: Vec<serde_json::Value> = match retry(cfg, "oi", || {
        let u = url_c.clone();
        let c = cl.clone();
        async move {
            let resp = c.get(&u).send().await?;
            let status = resp.status();
            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                return Err(anyhow!("OI HTTP {}: {}", status, body));
            }
            Ok(resp.json().await?)
        }
    })
    .await
    {
        Ok(d) => d,
        Err(e) => {
            // 400 errors are expected for old data — not a failure
            let msg = format!("{}", e);
            if msg.contains("400") {
                eprintln!("  oi: lookback exceeded for {} (expected)", symbol);
            } else {
                eprintln!("[warn] OI fetch failed: {}", e);
            }
            return map;
        }
    };

    for entry in &data {
        if let (Some(ts_ms), Some(oi_str)) = (
            entry.get("timestamp").and_then(|v| v.as_u64()),
            entry.get("sumOpenInterest").and_then(|v| v.as_str()),
        ) {
            if let Ok(oi) = oi_str.parse::<f64>() {
                map.insert(ts_ms / 1000, oi);
            }
        }
    }

    eprintln!("  oi: {} entries for {}", map.len(), symbol);
    map
}
