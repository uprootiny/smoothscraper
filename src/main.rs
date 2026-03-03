//! smoothscraper — elastic, gentle, relentless market data scraper.
//!
//! Usage:
//!   smoothscraper                          # one-shot, default config (BTC/ETH/SOL × 1h/4h)
//!   smoothscraper config.toml              # one-shot from config file
//!   smoothscraper --daemon config.toml     # continuous loop
//!   smoothscraper --enrich                 # backfill aux columns in existing CSVs
//!   smoothscraper --status                 # show data inventory

mod config;
mod fetcher;
mod store;
mod target;
mod ui;

use config::{interval_secs, Config};
use fetcher::retry::{RateBudget, RetryConfig};
use reqwest::Client;
use std::path::PathBuf;
use tokio::task::JoinHandle;

fn data_path_for(
    data_dir: &PathBuf,
    target: &target::Target,
    symbol: &str,
    interval: &str,
) -> PathBuf {
    if target.id == "binance" {
        data_dir.join(format!("{}_{}.csv", symbol.to_lowercase(), interval))
    } else {
        data_dir.join(format!(
            "{}__{}_{}.csv",
            target.id,
            symbol.to_lowercase(),
            interval
        ))
    }
}

async fn scrape_pair(
    client: &Client,
    retry_cfg: &RetryConfig,
    target: &target::Target,
    symbol: &str,
    interval: &str,
    max_candles: usize,
    data_dir: &PathBuf,
    enrich_only: bool,
    fetch_funding: bool,
    fetch_oi: bool,
    fetch_premium: bool,
    fetch_long_short: bool,
    fetch_taker: bool,
) -> anyhow::Result<()> {
    let step = interval_secs(interval);
    let path = data_path_for(data_dir, target, symbol, interval);
    let id = format!("{}:{}:{}", target.id, symbol, interval);

    let wants_futures =
        fetch_funding || fetch_oi || fetch_premium || fetch_long_short || fetch_taker;
    if wants_futures && !target.supports_futures_aux {
        eprintln!(
            "[{}] futures aux requested but target does not support futures endpoints; continuing with spot-only",
            id
        );
    }

    let fetch_funding = fetch_funding && target.supports_futures_aux;
    let fetch_oi = fetch_oi && target.supports_futures_aux;
    let fetch_premium = fetch_premium && target.supports_futures_aux;
    let fetch_long_short = fetch_long_short && target.supports_futures_aux;
    let fetch_taker = fetch_taker && target.supports_futures_aux;

    let mut existing = store::csv::read(&path);
    let existing_count = existing.len();
    eprintln!("[{}] existing: {} candles", id, existing_count);

    if !enrich_only {
        let last_ts = existing.last().map(|r| r.ts);

        let mut new_rows = fetcher::ohlcv::fetch(
            client,
            retry_cfg,
            target.spot_base,
            symbol,
            interval,
            last_ts,
            max_candles,
            step,
        )
        .await?;

        let new_count = new_rows.len();
        if new_count == 0 {
            eprintln!("[{}] up to date", id);
        } else {
            eprintln!("[{}] fetched {} new candles", id, new_count);

            let start_ts = new_rows.first().map(|r| r.ts).unwrap_or(0);
            let end_ts = new_rows.last().map(|r| r.ts).unwrap_or(0) + step;

            let (funding_map, oi_map, premium_map, long_short_map, taker_map) = tokio::join!(
                async {
                    if fetch_funding {
                        fetcher::funding::fetch(
                            client,
                            retry_cfg,
                            target.futures_base,
                            symbol,
                            start_ts,
                            end_ts,
                        )
                        .await
                    } else {
                        std::collections::BTreeMap::new()
                    }
                },
                async {
                    if fetch_oi {
                        fetcher::oi::fetch(
                            client,
                            retry_cfg,
                            target.futures_base,
                            symbol,
                            interval,
                            start_ts,
                            end_ts,
                        )
                        .await
                    } else {
                        std::collections::BTreeMap::new()
                    }
                },
                async {
                    if fetch_premium {
                        fetcher::premium::fetch(
                            client,
                            retry_cfg,
                            target.futures_base,
                            symbol,
                            interval,
                            start_ts,
                            end_ts,
                        )
                        .await
                    } else {
                        std::collections::BTreeMap::new()
                    }
                },
                async {
                    if fetch_long_short {
                        fetcher::long_short::fetch(
                            client,
                            retry_cfg,
                            target.futures_base,
                            symbol,
                            interval,
                            start_ts,
                            end_ts,
                        )
                        .await
                    } else {
                        std::collections::BTreeMap::new()
                    }
                },
                async {
                    if fetch_taker {
                        fetcher::taker::fetch(
                            client,
                            retry_cfg,
                            target.futures_base,
                            symbol,
                            interval,
                            start_ts,
                            end_ts,
                        )
                        .await
                    } else {
                        std::collections::BTreeMap::new()
                    }
                },
            );

            let counts = store::enrich(
                &mut new_rows,
                &funding_map,
                &oi_map,
                &taker_map,
                &long_short_map,
                &premium_map,
            );
            eprintln!(
                "[{}] enriched: funding:{} oi:{} taker:{} long_short:{} premium:{}",
                id, counts.funding, counts.oi, counts.borrow, counts.liq, counts.depeg
            );

            let total = store::csv::merge_and_write(&existing, &new_rows, &path)?;
            eprintln!(
                "[{}] total: {} (was {}, +{})",
                id, total, existing_count, new_count
            );
        }
    } else {
        if existing.is_empty() {
            eprintln!("[{}] no data to enrich", id);
            return Ok(());
        }

        let start_ts = existing.first().map(|r| r.ts).unwrap_or(0);
        let end_ts = existing.last().map(|r| r.ts).unwrap_or(0) + step;

        let (funding_map, oi_map, premium_map, long_short_map, taker_map) = tokio::join!(
            async {
                if fetch_funding {
                    fetcher::funding::fetch(
                        client,
                        retry_cfg,
                        target.futures_base,
                        symbol,
                        start_ts,
                        end_ts,
                    )
                    .await
                } else {
                    std::collections::BTreeMap::new()
                }
            },
            async {
                if fetch_oi {
                    fetcher::oi::fetch(
                        client,
                        retry_cfg,
                        target.futures_base,
                        symbol,
                        interval,
                        start_ts,
                        end_ts,
                    )
                    .await
                } else {
                    std::collections::BTreeMap::new()
                }
            },
            async {
                if fetch_premium {
                    fetcher::premium::fetch(
                        client,
                        retry_cfg,
                        target.futures_base,
                        symbol,
                        interval,
                        start_ts,
                        end_ts,
                    )
                    .await
                } else {
                    std::collections::BTreeMap::new()
                }
            },
            async {
                if fetch_long_short {
                    fetcher::long_short::fetch(
                        client,
                        retry_cfg,
                        target.futures_base,
                        symbol,
                        interval,
                        start_ts,
                        end_ts,
                    )
                    .await
                } else {
                    std::collections::BTreeMap::new()
                }
            },
            async {
                if fetch_taker {
                    fetcher::taker::fetch(
                        client,
                        retry_cfg,
                        target.futures_base,
                        symbol,
                        interval,
                        start_ts,
                        end_ts,
                    )
                    .await
                } else {
                    std::collections::BTreeMap::new()
                }
            },
        );

        let counts = store::enrich(
            &mut existing,
            &funding_map,
            &oi_map,
            &taker_map,
            &long_short_map,
            &premium_map,
        );
        eprintln!(
            "[{}] enriched: funding:{} oi:{} taker:{} long_short:{} premium:{}",
            id, counts.funding, counts.oi, counts.borrow, counts.liq, counts.depeg
        );

        let total = store::csv::merge_and_write(&[], &existing, &path)?;
        eprintln!("[{}] wrote {} enriched rows", id, total);
    }

    let report = store::csv::integrity_check(&path, step);
    eprintln!("[{}] integrity: {}", id, report);
    Ok(())
}

fn show_status(data_dir: &PathBuf) {
    eprintln!("Data inventory in {}:", data_dir.display());
    let mut entries: Vec<_> = std::fs::read_dir(data_dir)
        .into_iter()
        .flatten()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map(|ext| ext == "csv")
                .unwrap_or(false)
        })
        .collect();
    entries.sort_by_key(|e| e.file_name());

    for entry in entries {
        let path = entry.path();
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("?");
        let rows = store::csv::read(&path);
        let total = rows.len();
        if total == 0 {
            eprintln!("  {} — empty", name);
            continue;
        }
        let first_ts = rows.first().map(|r| r.ts).unwrap_or(0);
        let last_ts = rows.last().map(|r| r.ts).unwrap_or(0);
        let f_nz = rows.iter().filter(|r| r.funding != 0.0).count();
        let oi_nz = rows.iter().filter(|r| r.oi != 0.0).count();
        let borrow_nz = rows.iter().filter(|r| r.borrow != 0.0).count();
        let liq_nz = rows.iter().filter(|r| r.liq != 0.0).count();
        let depeg_nz = rows.iter().filter(|r| r.depeg != 0.0).count();

        let first = chrono::DateTime::from_timestamp(first_ts as i64, 0)
            .map(|d| d.format("%Y-%m-%d").to_string())
            .unwrap_or_else(|| "?".into());
        let last = chrono::DateTime::from_timestamp(last_ts as i64, 0)
            .map(|d| d.format("%Y-%m-%d").to_string())
            .unwrap_or_else(|| "?".into());

        eprintln!(
            "  {} — {} rows [{} → {}] funding:{:.0}% oi:{:.0}% taker:{:.0}% long_short:{:.0}% premium:{:.0}%",
            name,
            total,
            first,
            last,
            if total > 0 { 100.0 * f_nz as f64 / total as f64 } else { 0.0 },
            if total > 0 { 100.0 * oi_nz as f64 / total as f64 } else { 0.0 },
            if total > 0 { 100.0 * borrow_nz as f64 / total as f64 } else { 0.0 },
            if total > 0 { 100.0 * liq_nz as f64 / total as f64 } else { 0.0 },
            if total > 0 { 100.0 * depeg_nz as f64 / total as f64 } else { 0.0 },
        );
    }
}

fn write_views(data_dir: &PathBuf) {
    match store::manifest::write(data_dir) {
        Ok(m) => {
            eprintln!(
                "manifest: {} files -> {}/manifest.json",
                m.file_count,
                data_dir.display()
            );
            let _ = store::log::append(
                data_dir,
                &format!("manifest refreshed: {} files", m.file_count),
            );
        }
        Err(e) => {
            eprintln!("[warn] manifest write failed: {}", e);
            let _ = store::log::append(data_dir, &format!("manifest refresh failed: {}", e));
        }
    }
    match store::state::write(data_dir) {
        Ok(s) => {
            eprintln!(
                "state: {} streams -> {}/state.json",
                s.stream_count,
                data_dir.display()
            );
            let _ = store::log::append(
                data_dir,
                &format!("state refreshed: {} streams", s.stream_count),
            );
        }
        Err(e) => {
            eprintln!("[warn] state write failed: {}", e);
            let _ = store::log::append(data_dir, &format!("state refresh failed: {}", e));
        }
    }
}

fn show_view(data_dir: &PathBuf, name: &str) -> anyhow::Result<()> {
    let path = match name {
        "manifest" => data_dir.join("manifest.json"),
        "state" => data_dir.join("state.json"),
        _ => anyhow::bail!("unknown view '{}' (supported: manifest, state)", name),
    };
    let content = std::fs::read_to_string(&path)?;
    println!("{}", content);
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let daemon = args.iter().any(|a| a == "--daemon");
    let enrich = args.iter().any(|a| a == "--enrich");
    let status = args.iter().any(|a| a == "--status");
    let view_manifest = args.iter().any(|a| a == "--view-manifest");
    let view_state = args.iter().any(|a| a == "--view-state");

    let config_path = args
        .iter()
        .filter(|a| !a.starts_with('-') && *a != &args[0])
        .find(|a| a.ends_with(".toml"));

    let config = match config_path {
        Some(path) => Config::from_file(path)?,
        None => Config::default_config(),
    };

    let _ui_handle: Option<JoinHandle<()>> = if config.health_port > 0 {
        let data_dir = config.data_dir.clone();
        let port = config.health_port;
        Some(tokio::spawn(async move {
            if let Err(e) = ui::run(port, data_dir).await {
                eprintln!("[ui] server failed: {}", e);
            }
        }))
    } else {
        None
    };

    if view_manifest {
        write_views(&config.data_dir);
        show_view(&config.data_dir, "manifest")?;
        return Ok(());
    }
    if view_state {
        write_views(&config.data_dir);
        show_view(&config.data_dir, "state")?;
        return Ok(());
    }

    if status {
        show_status(&config.data_dir);
        write_views(&config.data_dir);
        return Ok(());
    }

    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .user_agent("smoothscraper/0.1")
        .build()?;

    let budget = RateBudget::binance();
    let retry_cfg = RetryConfig {
        rate_budget: Some(budget.clone()),
        ..Default::default()
    };

    std::fs::create_dir_all(&config.data_dir)?;

    eprintln!(
        "smoothscraper v0.1 — {} pairs, {} max candles",
        config.pairs.len(),
        config.max_candles
    );
    if daemon {
        eprintln!("Daemon mode: loop every {}s", config.loop_interval_secs);
    }

    loop {
        let cycle_start = std::time::Instant::now();

        for pair in &config.pairs {
            let target = match target::resolve(&pair.target) {
                Some(t) => t,
                None => {
                    eprintln!(
                        "[ERROR] unknown target '{}' for symbol {} (supported: binance, binance_testnet, binance_spot_only)",
                        pair.target, pair.symbol
                    );
                    continue;
                }
            };

            for interval in &pair.intervals {
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

                if let Err(e) = scrape_pair(
                    &client,
                    &retry_cfg,
                    &target,
                    &pair.symbol,
                    interval,
                    config.max_candles,
                    &config.data_dir,
                    enrich,
                    pair.funding,
                    pair.oi,
                    pair.premium,
                    pair.long_short,
                    pair.taker,
                )
                .await
                {
                    eprintln!(
                        "[ERROR] {}:{}:{} — {}",
                        pair.target, pair.symbol, interval, e
                    );
                }
            }
        }

        write_views(&config.data_dir);

        let elapsed = cycle_start.elapsed();
        eprintln!("\nCycle complete in {:.1}s", elapsed.as_secs_f64());

        if !daemon || config.loop_interval_secs == 0 {
            break;
        }

        let wait = config.loop_interval_secs.saturating_sub(elapsed.as_secs());
        if wait > 0 {
            eprintln!("Next cycle in {}s...\n", wait);
            tokio::time::sleep(tokio::time::Duration::from_secs(wait)).await;
        }
    }

    Ok(())
}
