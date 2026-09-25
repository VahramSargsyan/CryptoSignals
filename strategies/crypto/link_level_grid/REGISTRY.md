# VAHRAM_LINK_LEVEL_GRID_V1 — registry record

strategy_id: VAHRAM_LINK_LEVEL_GRID_V1  
strategy_name: VAHRAM_LINK_LEVEL_GRID  
strategy_version: 1.0.0-experimental  
status: BACKTESTED  
created_at: 2026-09-25  
source_commit_sha: f33dde450eb33edb9ebc1b56b76445fa7245c020  
supersedes_strategy_id: none

## Decision

The executable research implementation has completed a real historical LINKUSDT backtest and is therefore recorded as **BACKTESTED**.

It is **not** recorded as **ACCEPTED**, **PAPER_LIVE**, or **LIVE_SIGNALS**.

Reason: the repository acceptance standard requires predefined acceptance gates and stronger validation before a strategy can be promoted to ACCEPTED. Several product rules are also still explicitly non-canonical.

## Strategy definition

- market: Binance Spot
- initial symbol: LINKUSDT
- timeframe: 1D
- direction: long-only accumulation / staged exit
- grid: 16 main levels x 4 sublevels = 64 sublevels
- capital architecture:
  - independent Micro pool
  - independent Mid pool
  - reserved per-slot budgets
- Micro research behavior:
  - entries on sublevels
  - exit one sublevel above entry
- Mid research behavior:
  - entry at main-level A boundary
  - upper levels exit at current percentage target
  - deeper levels exit at first of percentage target or 10-sublevel recovery
- range:
  - trailing H/L built causally from prior daily candles only
  - up to approximately 3 years of history

## Current non-canonical research assumptions

- range refresh cadence: 30 daily candles
- minimum history before first range: 90 candles
- default capital allocation: linear_depth_reserved
- Mid entry location: A boundary
- open lots retain entry-time targets after range refresh

These assumptions are configurable/research-only and must not be confused with owner-frozen rules.

## Backtest evidence

backtest_run_id: RUN-ec8cfd26fda605c20008  
dataset_id: BINANCE:LINKUSDT:1D:07c433b5760a267a  
period: 2023-09-25 through 2026-09-24  
candles: 1096  
timeframe: 1D  
fees: 10 bps  
slippage: 5 bps  
allocation_preset: linear_depth_reserved  
closed_trade_count: 1495  
total_return: +140.4020%  
micro_total_return: +140.4247%  
mid_total_return: +140.3793%  
benchmark_buy_hold_return: +89.6299%  
max_drawdown: 41.1522%  
open_micro_lots_end: 47  
open_mid_lots_end: 13  
test_level: LIVE DATA BACKTEST + FULL REPOSITORY REGRESSION SUITE  
tests: 60 passed

GitHub Actions run: 36132903127  
Artifact ID: 10862951975  
Evidence document: docs/evidence/2026-09-25_LINK_LEVEL_GRID_FIRST_RUN.md

## Promotion requirements

Before status can move from BACKTESTED to ACCEPTED:

- freeze the exact H/L refresh rule;
- freeze canonical Micro and Mid allocation curves;
- freeze canonical Mid entry placement;
- confirm percentage-target derivation;
- run sensitivity tests around unresolved assumptions;
- perform held-out/out-of-sample validation;
- apply the repository acceptance gates defined before reviewing that validation.

## Notes

The first historical run is evidence that the implementation works and can be tested reproducibly. It is not evidence that the current research assumptions are the final intended strategy or that future returns are expected.
