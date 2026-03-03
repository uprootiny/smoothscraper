# smoothscraper — AGENTS.md

## What This Is

Elastic, gentle, relentless market data scraper. Single Rust binary. Feeds `../arbitragefx/data/` with incremental, append-only CSV files enriched with funding rates and open interest.

## Invariants — Do Not Break These

1. **CSV schema is a shared contract.** 11 columns, exact order: `ts,open,high,low,close,volume,funding,borrow,liq,depeg,oi`. arbitragefx parses this. Changing column order, names, or count breaks the downstream consumer.

2. **Append-only, dedup-by-timestamp.** Never delete rows from CSVs. New data merges in; duplicate timestamps take the newer value. `.bak` backup before every overwrite.

3. **Never overwrite with empty data.** If a fetch returns 0 rows, refuse to write. Log it. This prevents a transient API failure from wiping months of data.

4. **400 errors are never retried.** A 400 is a client bug (bad params, expired lookback window). Retrying wastes time and risks rate limiting. Return empty, log, move on.

5. **`data_dir` in config.toml points to arbitragefx/data.** This is the coupling point. Changing it changes what arbitragefx sees.

## Architecture

```
config.toml
    │
    ▼
main.rs ──► for each pair × interval:
    │           scrape_pair()
    │               ├── ohlcv::fetch()      ← paginated, 50-page max
    │               ├── funding::fetch()     ← paginated, 20-page max
    │               ├── oi::fetch()          ← single request, 29-day lookback
    │               ├── store::enrich()      ← nearest-prior interpolation
    │               └── csv::merge_and_write() + integrity_check()
    │
    ▼
    data_dir/*.csv  (consumed by arbitragefx)
```

### Module Responsibilities

| Module | Does | Does Not |
|--------|------|----------|
| `fetcher/ohlcv.rs` | Paginated Binance Spot klines, cursor-stuck detection | Transform or interpret candle data |
| `fetcher/funding.rs` | Paginated Binance Futures funding rates | Interpolate — that's `store::enrich` |
| `fetcher/oi.rs` | Single-request OI history, 29-day clamp | Paginate (OI endpoint is bounded) |
| `fetcher/retry.rs` | Exponential backoff + jitter, 400-rejection | Decide what to retry — callers classify errors |
| `store/mod.rs` | Row type, CSV header, `nearest_prior()`, `enrich()` | File I/O — that's `store/csv.rs` |
| `store/csv.rs` | Read, merge, write, backup, integrity check | Network calls |
| `config.rs` | TOML parsing, interval→seconds mapping | Validation beyond type checking |
| `main.rs` | CLI dispatch, daemon loop, pair iteration | Business logic beyond orchestration |

## Modes

```
smoothscraper config.toml              # one-shot: fetch all pairs, exit
smoothscraper config.toml --daemon     # loop every loop_interval_secs
smoothscraper config.toml --enrich     # backfill aux columns on existing CSVs
smoothscraper config.toml --status     # show inventory: rows, date ranges, coverage %
```

## Binance API Constraints

- **Spot klines** (`/api/v3/klines`): 1000 candles/request, no auth needed
- **Funding rates** (`/fapi/v1/fundingRate`): 1000 entries/request, every 8h
- **OI history** (`/futures/data/openInterestHist`): 500 entries/request, **30-day lookback limit** (hard — returns 400 for older `startTime`)
- **Rate limits**: 1200 weight/minute. Each klines call costs ~5 weight. 200ms pause between pages is sufficient.

## What's Missing (Known Gaps)

- **Borrow rates**: No historical Binance API. Column stays 0.0.
- **Liquidations**: No historical API. Column stays 0.0.
- **Depeg**: No historical API. Column stays 0.0.
- **OI older than 30 days**: Binance doesn't serve it. Coverage tops out at ~17% for longer datasets.
- **Health endpoint**: `health_port` config exists but not implemented.
- **Tests**: Only `retry.rs` has tests. `ohlcv`, `funding`, `oi`, `csv` need unit tests with mock HTTP responses.

## When Modifying This Project

- Run `cargo build --release` — it must compile clean
- Run `cargo test` — retry tests must pass
- After changes, run `smoothscraper config.toml --status` to verify CSV integrity
- If changing Row fields or CSV format: **update arbitragefx simultaneously** — the schema is shared
- Keep fetcher modules pure: each takes a `reqwest::Client` + config, returns data. No file I/O in fetchers.
