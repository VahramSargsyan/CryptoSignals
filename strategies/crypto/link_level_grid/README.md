# VAHRAM_LINK_LEVEL_GRID_V1

Status: **IMPLEMENTED FOR RESEARCH / NOT YET CANONICAL**  
Timeframe: **1D only**  
Initial symbol: **LINKUSDT**

The strategy now has executable code in:

- `strategy.py` — grid geometry, capital reservation, Micro/Mid execution and portfolio-aware backtest;
- `scripts/run_link_level_grid_backtest.py` — end-to-end Binance download + backtest runner;
- `.github/workflows/link-level-grid-backtest.yml` — GitHub Actions runner.

## Confirmed owner rules implemented

- 16 main levels;
- 4 sublevels per main level;
- 64 total sublevels;
- dynamic broad H/L range from up to roughly 3 years of daily candles;
- Micro and Mid are independent capital pools;
- deeper Mid entries can exit at the first of:
  - the current percentage target;
  - a 10-sublevel recovery;
- example `64 -> 54`;
- lower-level capital is protected through reserved per-slot budgets.

## Research assumptions that are NOT owner-frozen yet

To make the strategy executable without pretending unresolved details are known, the code labels these assumptions explicitly:

1. trailing H/L range refresh every **30 daily candles** by default;
2. first usable range after **90 historical daily candles**;
3. default clean capital curve is `linear_depth_reserved`:
   weights 1..16, normalized to 100%, so deeper levels receive progressively more reserved capital;
4. Mid entries occur at each main level's lower **A** boundary;
5. Micro exit is one sublevel above entry, matching historical workbook behavior;
6. open lots retain the target calculated from the grid that existed when they were opened;
7. same-candle buy and sell of the same slot is blocked.

These are research parameters, not hidden claims about the final strategy.

## Capital presets

### `linear_depth_reserved` — default research baseline

Each layer owns its own 100% capital pool.

Main-level weights:

```text
1, 2, 3, ..., 16
```

normalized to 100%.

Micro divides each main-level reserve equally among its four sublevels.

### `equal_reserved`

Every main level receives the same capital share.

Useful as a neutral comparison.

### `historical_observed`

Reproduces the historical workbook allocation shape as closely as the current evidence allows.

This preset is retained for comparison only because the owner explicitly identified the original capital distribution as a mistake.

## Look-ahead protection

The rolling range on candle T is built only from candles before T.

The current candle's high/low cannot change the range used to trade that same candle.

This is essential for a meaningful historical test.

## Run locally

```bash
python -m scripts.run_link_level_grid_backtest \
  --symbol LINKUSDT \
  --years 3 \
  --allocation-preset linear_depth_reserved
```

Outputs:

```text
research_artifacts/link_level_grid/<RUN_ID>/
  canonical_candles.csv
  trades.csv
  equity_curve.csv
  range_history.csv
  allocation_table.csv
  summary.json
  download_metadata.json
```

## Important interpretation boundary

A successful run proves that the strategy mechanics can be simulated deterministically.

It does **not** prove the strategy is profitable, accepted, or that the remaining assumptions match the owner's final intended rules.

Those assumptions should be frozen after reviewing the first LINK evidence.
