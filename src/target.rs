//! Target registry for multi-target scraping.

#[derive(Debug, Clone)]
pub struct Target {
    pub id: &'static str,
    pub spot_base: &'static str,
    pub futures_base: &'static str,
    pub supports_futures_aux: bool,
}

pub fn resolve(id: &str) -> Option<Target> {
    match id {
        "binance" => Some(Target {
            id: "binance",
            spot_base: "https://api.binance.com",
            futures_base: "https://fapi.binance.com",
            supports_futures_aux: true,
        }),
        "binance_testnet" => Some(Target {
            id: "binance_testnet",
            spot_base: "https://testnet.binance.vision",
            futures_base: "https://testnet.binancefuture.com",
            supports_futures_aux: true,
        }),
        "binance_spot_only" => Some(Target {
            id: "binance_spot_only",
            spot_base: "https://api.binance.com",
            futures_base: "",
            supports_futures_aux: false,
        }),
        _ => None,
    }
}
