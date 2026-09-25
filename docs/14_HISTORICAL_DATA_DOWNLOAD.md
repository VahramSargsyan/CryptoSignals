# Historical Market Data Download v1

Status: IMPLEMENTATION CANDIDATE  
Workflow mode: PATCH_FIX  
Risk class: L2  
Runtime target: research / test-data preparation only

## Capability

`HISTORICAL_DATA_DOWNLOAD_V1`

User result:

> From GitHub Actions or the command line, download Binance Spot OHLCV history, keep only fully closed candles, validate the dataset, and produce a canonical CSV plus a reproducibility manifest.

## Source of truth

Market source:

- Binance public Spot kline endpoint through the existing repository adapter.

Repository contracts:

- `integrations/binance/rest_client.py`
- `integrations/binance/historical.py`
- `core/data/candles.py`

No API key is required.

## Scope

Included:

- symbols such as `LINKUSDT`;
- `1D`, `4H`, and `1H` because those intervals already exist in the shared candle contract;
- explicit start/end or a default N-year lookback;
- exclusion of the currently open candle;
- listing-aware history;
- data-quality checks;
- canonical CSV export;
- JSON evidence manifest;
- manually triggered GitHub Action;
- artifact upload for review before any dataset is committed.

Out of scope:

- automatic commit of downloaded datasets to `main`;
- live trading;
- exchange credentials;
- strategy execution;
- automatic range/level generation;
- Google Sheets storage.

## Command-line example

For LINK daily candles using the latest fully closed candle and a 3-year lookback:

```bash
python scripts/download_historical_data.py \
  --symbol LINKUSDT \
  --timeframe 1D \
  --years 3
```

Generated files:

```text
data/historical/LINKUSDT/1d/LINKUSDT_1d_<actual-start>_<actual-end>.csv
evidence/datasets/LINKUSDT_1d_<actual-start>_<actual-end>.json
```

The JSON manifest records:

- dataset ID;
- requested and actual range;
- listing truncation;
- candle counts;
- missing candles;
- duplicates;
- off-grid timestamps;
- invalid OHLC rows;
- null cells;
- negative volume rows;
- retrieval timestamp;
- path to the generated CSV.

## GitHub Action

Open:

```text
Actions
-> Download Historical Market Data
-> Run workflow
```

Default inputs are suitable for the first LINK experiment:

```text
symbol    = LINKUSDT
timeframe = 1D
years     = 3
```

The workflow uploads the generated bundle as a GitHub Actions artifact. It deliberately does **not** commit market data automatically.

That separation keeps downloaded data reviewable before it becomes a frozen backtest fixture.

## Quality behavior

The downloader fails closed when the shared candle validator reports critical issues.

The generated bundle is still written so the failure can be inspected.

`--allow-quality-issues` exists only for diagnostics and should not be used when freezing an acceptance dataset without a documented reason.

## Acceptance

Required before merge:

1. existing Binance adapter tests remain green;
2. new CLI/export tests pass;
3. all repository unit/regression tests pass;
4. one manual GitHub Action run for `LINKUSDT / 1D / 3 years`;
5. generated manifest reports the exact data-quality result;
6. no secret or credential is introduced.

## Rollback

Documentation/script/workflow-only change.

Rollback is deletion/revert of:

- `scripts/download_historical_data.py`;
- `.github/workflows/download-historical-data.yml`;
- `tests/test_download_historical_data_cli.py`;
- this document.

No schema or migration is required.
