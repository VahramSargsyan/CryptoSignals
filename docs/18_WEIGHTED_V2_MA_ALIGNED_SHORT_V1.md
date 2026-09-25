# WEIGHTED_V2_MA_ALIGNED_SHORT_V1 — Frozen Research Profile

WORKFLOW_MODE: **BUILD_NEW_APP**  
RISK_CLASS: **L2 — research execution profile**  
MIGRATION_REQUIRED: **NO**  
LIVE/PAPER-LIVE IMPACT: **NONE**  
STATUS: **FROZEN BEFORE PROFILE RUNTIME**

## Identity

PROFILE_ID: `WEIGHTED_V2_MA_ALIGNED_SHORT_V1`

Signal source:

- strategy: `WEIGHTED_MULTI_SIGNAL_V2/2.0.0`
- execution engine: `SHORT_ONLY_SIGNAL_FLIP_V1`

No strategy formula is changed.

## Hypothesis origin

Short R1 diagnostics showed a repeated descriptive separation:

Development 2021–2025:

- entry strength 80 -> PF 0.865
- entry strength 100 -> PF 1.351

Nonblind corrected 2026 YTD:

- entry strength 80 -> PF 0.846
- entry strength 100 -> PF 1.728

This is post-hoc evidence. Therefore the profile below is a new frozen hypothesis, not a correction to prior evidence.

## Frozen execution rule

Short entry:

- signal must be SELL;
- `entry_strength_min = 100`.

For V2, SELL strength 100 means:

1. bearish core confirmation is present:
   - MACD < signal;
   - StochRSI K < D;
   - volume > volume MA20;
   - candle body strength >= 0.50;
   - close < open;
2. MA context is aligned:
   - SMA50 < SMA100 < SMA200;
   - close < SMA200.

Short exit:

- any V2 BUY signal;
- `exit_strength_min = 0`.

All other short-engine rules remain unchanged:

- 1x notional;
- next-open execution;
- 10 bps fee;
- 5 bps slippage;
- intraday-high zero-equity liquidation;
- bankruptcy stops later trades for that symbol/run.

## Why exit remains unfiltered

The hypothesis is specifically about **entry quality under bearish MA alignment**.

Changing both entry and exit simultaneously would make it impossible to identify which change caused the result.

## Research boundaries

Development descriptive rerun:

- warm-up: 2020-01-01;
- evaluation: 2021-01-01 through 2025-12-31.

Nonblind temporal descriptive rerun:

- warm-up: 2025-01-01;
- evaluation: 2026-01-01 through 2026-09-24.

Neither run can validate the profile because the hypothesis was created after inspecting both periods.

## Future unseen boundary

`2026-09-26T00:00:00Z` onward is reserved as future unseen evidence for this profile.

Do not change:

- strategy formula;
- entry threshold 100;
- exit threshold 0;
- engine economics;
- MA definition;

after inspecting future data without creating a new profile/version.

## Promotion rule

Development/nonblind runs may only establish implementation sanity and descriptive behavior.

Acceptance requires future unseen evidence after the freeze boundary.
