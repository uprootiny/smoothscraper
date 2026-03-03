//! TOML-based configuration. Declarative, data-driven.
//!
//! The config defines *what* to scrape, not *how*. The how is fixed:
//! exponential backoff, gentle rate limits, incremental append.

use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
pub struct Config {
    /// Output directory for CSV files
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,

    /// Scrape loop interval in seconds (0 = one-shot, no daemon)
    #[serde(default)]
    pub loop_interval_secs: u64,

    /// Max candles to fetch per symbol-interval pair per cycle
    #[serde(default = "default_max_candles")]
    pub max_candles: usize,

    /// HTTP port for health endpoint (0 = disabled)
    #[serde(default)]
    pub health_port: u16,

    /// Pairs to scrape
    pub pairs: Vec<PairConfig>,
}

#[derive(Debug, Deserialize)]
pub struct PairConfig {
    #[serde(default = "default_target")]
    pub target: String,
    pub symbol: String,
    pub intervals: Vec<String>,
    /// Fetch funding rates
    #[serde(default = "default_true")]
    pub funding: bool,
    /// Fetch open interest
    #[serde(default = "default_true")]
    pub oi: bool,
    /// Fetch premium index
    #[serde(default = "default_true")]
    pub premium: bool,
    /// Fetch global long/short ratio
    #[serde(default = "default_true")]
    pub long_short: bool,
    /// Fetch taker buy/sell ratio
    #[serde(default = "default_true")]
    pub taker: bool,
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("data")
}

fn default_max_candles() -> usize {
    2000
}

fn default_true() -> bool {
    true
}

fn default_target() -> String {
    "binance".into()
}

impl Config {
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&content)?)
    }

    /// Generate a default config for common use
    pub fn default_config() -> Self {
        Config {
            data_dir: default_data_dir(),
            loop_interval_secs: 0,
            max_candles: 2000,
            health_port: 0,
            pairs: vec![
                PairConfig {
                    target: "binance".into(),
                    symbol: "BTCUSDT".into(),
                    intervals: vec!["1h".into(), "4h".into(), "15m".into()],
                    funding: true,
                    oi: true,
                    premium: true,
                    long_short: true,
                    taker: true,
                },
                PairConfig {
                    target: "binance".into(),
                    symbol: "ETHUSDT".into(),
                    intervals: vec!["1h".into(), "4h".into()],
                    funding: true,
                    oi: true,
                    premium: true,
                    long_short: true,
                    taker: true,
                },
                PairConfig {
                    target: "binance".into(),
                    symbol: "SOLUSDT".into(),
                    intervals: vec!["1h".into(), "4h".into()],
                    funding: true,
                    oi: true,
                    premium: true,
                    long_short: true,
                    taker: true,
                },
            ],
        }
    }
}

pub fn interval_secs(interval: &str) -> u64 {
    match interval {
        "1m" => 60,
        "3m" => 180,
        "5m" => 300,
        "15m" => 900,
        "30m" => 1800,
        "1h" => 3600,
        "2h" => 7200,
        "4h" => 14400,
        "6h" => 21600,
        "8h" => 28800,
        "12h" => 43200,
        "1d" => 86400,
        "1w" => 604800,
        _ => 3600,
    }
}
