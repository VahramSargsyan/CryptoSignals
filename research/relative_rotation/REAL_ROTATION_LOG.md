# 8-Token Relative Rotation — Real Manual Rotation Log

Status: ACTIVE_MANUAL_EXECUTION_LOG  
Strategy: RELATIVE_ROTATION_GRAPH_8_V1  
Execution authority: VAHRAM_MANUAL_ONLY

## Current configured held asset

`ATOM`

Source of initial state:

- real TWT -> ATOM rotation on 2026-09-26;
- detailed quantities and screenshot-derived evidence remain in `research/atom_twt_rotation/REAL_ROTATION_LOG.md`;
- this log does not duplicate those sensitive execution details.

## Recording rule

For every future real rotation record:

- rotation ID;
- signal closed-candle date;
- signal state (`CONFIRMED` required for canonical strategy evidence);
- from asset;
- to asset;
- historical-model pair and dislocation;
- manual execution timestamp;
- quantity sent;
- quantity received;
- fee and slippage if known;
- order/trade ID if available;
- configured `held_asset` after execution;
- notes about any divergence from the paper-live signal.

## Entries

No new 8-token graph rotation has been logged after the 2026-09-26 TWT -> ATOM starting-state transfer yet.
