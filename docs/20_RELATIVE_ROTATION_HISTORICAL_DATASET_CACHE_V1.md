# Relative Rotation Historical Dataset Cache v1

Date: 2026-09-28  
Workflow mode: PATCH_FIX  
Risk class: L2  
Capability ID: `RELATIVE_ROTATION_HISTORICAL_DATASET_CACHE_V1`

## User result

Build one immutable, reusable market-data bundle for Relative Rotation research so repeated experiments do not need to re-download the same Binance history.

## Canonical source of truth

Raw market source remains Binance Spot public klines.

The GitHub Release is a frozen reproducibility cache, not a new authoritative market-data provider.

## Scope

Included:
- all assets currently monitored by Relative Rotation;
- `1H` and `1D` closed candles;
- default history start `2023-05-05T00:00:00Z`;
- one fixed `as_of` timestamp per build;
- existing Binance downloader and candle validator;
- one per-dataset manifest;
- one bundle-level `catalog.json`;
- SHA-256 for every CSV and for the final archive;
- GitHub Actions artifact as short-term backup;
- GitHub prerelease asset as persistent GitHub-hosted cache.

Current monitor universe is imported from the strategy code instead of duplicated manually.

## Out of scope

- trading-rule changes;
- paper-live scheduling changes;
- automatic order execution;
- automatic replacement of live Binance ingestion;
- Google Sheets candle storage;
- committing large raw OHLCV files into Git history;
- incremental refresh/append logic;
- strategy optimization.

## Storage boundary

Repository Git history stores:
- code;
- tests;
- workflow;
- documentation.

GitHub Release assets store:
- frozen historical data archive;
- bundle catalog;
- archive checksum.

This preserves the repository rule that GitHub code history should not become the long-term raw-market-data store while still keeping the reproducibility bundle on GitHub.

## Dataset plan

For each asset in the current Relative Rotation monitor universe:

- `1H`
- `1D`

At the current 13-asset universe this produces 26 datasets.

Each CSV is accompanied by the existing canonical per-dataset evidence manifest.

The bundle catalog records:
- dataset ID;
- symbol;
- timeframe;
- requested and actual range;
- row count;
- listing truncation;
- data-quality counts;
- CSV SHA-256;
- relative paths.

## Quality gate

Publication fails closed if any dataset reports critical candle-quality issues:
- missing candles;
- duplicates;
- off-grid timestamps;
- invalid OHLC;
- null cells;
- negative volume.

Listing truncation is recorded and is not by itself a critical issue.

## GitHub workflow

Workflow:

`.github/workflows/historical-dataset-cache-v1.yml`

The workflow:
1. runs targeted unit/data-contract tests;
2. downloads all planned datasets;
3. validates data quality;
4. builds the catalog;
5. packages the full bundle;
6. writes archive SHA-256;
7. uploads a 90-day Actions artifact;
8. publishes the same files as an immutable GitHub prerelease.

The release tag includes the GitHub run ID so reruns do not overwrite prior frozen datasets.

## Research usage

Future backtests should prefer a pinned cache release/dataset ID when reproducing an experiment.

A later cache-consumer patch may add:

`CACHE FIRST -> DOWNLOAD ONLY IF MISSING`

That consumer behavior is intentionally not part of this bounded patch.

## Migration

MIGRATION_REQUIRED: NO

No schema, stored strategy state, IDs or production relations change.

## Acceptance

Required evidence:
- new unit tests pass;
- existing Binance historical/data CLI tests pass;
- live GitHub Actions build completes;
- 26 datasets are present;
- bundle catalog reports zero critical datasets;
- GitHub Release contains archive + checksum + catalog;
- no secret is introduced.

## Rollback

Code rollback:
- revert the workflow, builder, tests and this document.

Data rollback:
- do not use the generated release in research;
- frozen release may remain as historical evidence or be manually deleted if explicitly desired.

No production trading state is affected.
