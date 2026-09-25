# Strategy Lab Warm-up / Evaluation Boundary

WORKFLOW_MODE: **BUILD_NEW_APP**  
RISK_CLASS: **L3 — shared research semantics**  
MIGRATION_REQUIRED: **NO**

## Problem

A strategy may need more historical candles to calculate indicators than should be included in the measured test period.

Example:

- evaluate 2026 YTD;
- SMA200 requires history before 2026-01-01.

If the runner downloads only from the evaluation start, early SMA200 context is unavailable.

If the runner simply downloads older candles and backtests the full dataset, the warm-up period contaminates:

- trades;
- benchmark return;
- total return;
- drawdown;
- exposure;
- event-study observations.

Both behaviors are wrong for a formal warm-up-aware research run.

## Contract

Research requests may now specify:

```json
{
  "data_start": "2025-01-01T00:00:00Z",
  "start": "2026-01-01T00:00:00Z",
  "end": "2026-09-25T00:00:00Z"
}
```

Semantics:

- `data_start`: first candle requested for indicator warm-up;
- `start`: evaluation boundary; equivalent to `evaluation_start`;
- `end`: exclusive data/evaluation end boundary.

Invariant:

`data_start <= start < end`

If `data_start` is omitted, it defaults to `start`, preserving legacy request behavior.

## Computation rules

1. Download canonical candles from `data_start`.
2. Validate the complete downloaded dataset.
3. Calculate strategy indicators and outputs using the complete dataset.
4. Discard strategy outputs whose signal timestamp is before `start`.
5. Slice candles to `timestamp >= start` before:
   - event study;
   - trading backtest;
   - equity curve;
   - benchmark calculation;
   - drawdown;
   - exposure.
6. Manifest `period_start` records the first actual evaluation candle, not the warm-up candle.
7. Dataset ID continues to identify the complete canonical dataset including warm-up history.

## No-lookahead

Warm-up history is strictly before the evaluation period. Future candles are never used to calculate an earlier signal.

This feature changes data availability before the test boundary, not the direction of information flow.

## Listing-aware behavior

A late-listed token may not have the requested amount of prehistory.

The downloader retains its existing listing-aware behavior. No synthetic candles are created.

The actual canonical dataset start remains recorded in download metadata and dataset ID.

## Backward compatibility

Existing requests without `data_start` are unchanged because:

`data_start = start`

No strategy formula, V1/V2 default comparison, live workflow, schema, or existing evidence artifact is rewritten.

## Verification

Required regression checks:

- evaluation outputs never precede `start`;
- equity curve never includes warm-up candles;
- benchmark starts from the evaluation slice;
- manifest period starts at the evaluation slice;
- evaluation start beyond the available dataset fails closed;
- all legacy tests remain green.
