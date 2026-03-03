# smoothscraper

`smoothscraper` builds incremental, multi-target CSV datasets for `arbitragefx`, then materializes generic schema/state views for downstream systems.

## Data contract

Primary row format (stable):

`ts,open,high,low,close,volume,funding,borrow,liq,depeg,oi`

Signal grounding:

- `open/high/low/close/volume`: spot klines (`/api/v3/klines`)
- `funding`: futures funding rate (`/fapi/v1/fundingRate`)
- `borrow`: taker buy/sell ratio (`/futures/data/takerlongshortRatio.buySellRatio`)
- `liq`: global long/short account ratio (`/futures/data/globalLongShortAccountRatio.longShortRatio`)
- `depeg`: premium index close (`/fapi/v1/premiumIndexKlines`)
- `oi`: open interest (`/futures/data/openInterestHist.sumOpenInterest`)

Enrichment uses nearest-prior value (`<= ts`) and never overwrites non-zero existing values.

## Targets

Per pair, set `target`:

- `binance` (default)
- `binance_testnet`
- `binance_spot_only`

For spot-only targets, futures auxiliary signals are skipped gracefully.

## Views

`smoothscraper` writes machine-readable views into `data_dir`:

- `manifest.json`: schema version, column metadata, per-file ranges and coverage
- `state.json`: stream-level operational snapshot (`target:symbol:interval`, last timestamp, coverage)

CLI access:

- `smoothscraper --view-manifest config.toml`
- `smoothscraper --view-state config.toml`

## Core commands

- `smoothscraper`
- `smoothscraper config.toml`
- `smoothscraper --daemon config.toml`
- `smoothscraper --enrich config.toml`
- `smoothscraper --status config.toml`
- `smoothscraper --view-manifest config.toml`
- `smoothscraper --view-state config.toml`

## Situational awareness UI

- set `health_port = <port>` in `config.toml` to run the Axum-based UI server.
- the root UI is served at `http://localhost:<port>/` and renders manifest/state/schema status cards.
- JSON endpoints (`/api/manifest`, `/api/state`, `/api/schema`) mirror the files written to `data_dir`.
- new endpoint `/api/health` summarizes manifest rows, streams, and the latest log entry so you can trace each cascade; `/api/logs` tails the last 20 entries.
- the UI now offers a “Refresh manifest/state” button that re-triggers the cascade, rewrites `manifest.json`/`state.json`, appends log entries, and updates the health card.

## CI & deployment

- GitHub Actions workflow `.github/workflows/ci.yml` runs `cargo fmt --check`, `cargo check`, and `cargo test` on pushes/PRs.
- A `release-package` job builds `cargo build --release` and uploads `target/release/smoothscraper` as `smoothscraper-release`.
- Hook this repo to GitHub, push, and rely on the workflow for continuous validation and deployable artifacts.
