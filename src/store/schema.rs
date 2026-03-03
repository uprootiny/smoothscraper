//! Generic schema descriptors for downstream consumers.

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct ColumnSpec {
    pub name: &'static str,
    pub dtype: &'static str,
    pub semantic: &'static str,
    pub source: &'static str,
}

pub const SCHEMA_VERSION: &str = "1.0.0";

pub fn csv_columns() -> Vec<ColumnSpec> {
    vec![
        ColumnSpec {
            name: "ts",
            dtype: "u64",
            semantic: "candle_open_time_utc_seconds",
            source: "derived",
        },
        ColumnSpec {
            name: "open",
            dtype: "f64",
            semantic: "spot_open",
            source: "klines",
        },
        ColumnSpec {
            name: "high",
            dtype: "f64",
            semantic: "spot_high",
            source: "klines",
        },
        ColumnSpec {
            name: "low",
            dtype: "f64",
            semantic: "spot_low",
            source: "klines",
        },
        ColumnSpec {
            name: "close",
            dtype: "f64",
            semantic: "spot_close",
            source: "klines",
        },
        ColumnSpec {
            name: "volume",
            dtype: "f64",
            semantic: "spot_volume",
            source: "klines",
        },
        ColumnSpec {
            name: "funding",
            dtype: "f64",
            semantic: "funding_rate",
            source: "fundingRate",
        },
        ColumnSpec {
            name: "borrow",
            dtype: "f64",
            semantic: "taker_buy_sell_ratio",
            source: "takerlongshortRatio.buySellRatio",
        },
        ColumnSpec {
            name: "liq",
            dtype: "f64",
            semantic: "global_long_short_account_ratio",
            source: "globalLongShortAccountRatio.longShortRatio",
        },
        ColumnSpec {
            name: "depeg",
            dtype: "f64",
            semantic: "premium_index_close",
            source: "premiumIndexKlines.close",
        },
        ColumnSpec {
            name: "oi",
            dtype: "f64",
            semantic: "sum_open_interest",
            source: "openInterestHist.sumOpenInterest",
        },
    ]
}
