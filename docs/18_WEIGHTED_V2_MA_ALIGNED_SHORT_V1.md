# WEIGHTED_V2_MA_ALIGNED_SHORT_V1 — Frozen Research Profile

WORKFLOW_MODE: **BUILD_NEW_APP**  
RISK_CLASS: **L2 — research execution profile**  
MIGRATION_REQUIRED: **NO**  
LIVE/PAPER-LIVE IMPACT: **NONE**  
STATUS: **DESCRIPTIVE_RERUN_COMPLETED / FROZEN FOR FUTURE UNSEEN**

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


## Descriptive rerun results

These reruns execute the exact frozen profile above. They are **not validation** because the profile was designed after inspecting both source periods.

### Development 2021–2025

Run: `WEIGHTED_V2_MA_ALIGNED_SHORT_2021_2025_R1`

- 108/108 tests PASS
- 13/13 symbols PASS
- 80 closed trades
- win rate: 47.50%
- average net trade: +1.90%
- median net trade: -2.03%
- pooled PF: 1.285
- positive total return: 7/13 symbols
- median symbol max drawdown: 30.96%
- median symbol total return: +17.36%
- zero-equity liquidations: 1, OSMOUSDT

Year PF:

- 2021: 0.379
- 2022: 3.608
- 2023: 0.341
- 2024: 0.608
- 2025: 1.932

Relative to the unfiltered short R1, the frozen MA-aligned entry profile improves PF from 0.989 to 1.285 and reduces median drawdown from 68.35% to 30.96%. It still fails materially in 2023 and 2024 and still contains a bankruptcy event.

### Nonblind 2026 YTD

Run: `WEIGHTED_V2_MA_ALIGNED_SHORT_2026_YTD_R1`

- 108/108 tests PASS
- 13/13 symbols PASS
- 32 closed trades
- win rate: 46.88%
- average net trade: +2.58%
- median net trade: -1.46%
- pooled PF: 1.436
- positive total return: 8/13 symbols
- median symbol max drawdown: 18.26%
- median symbol total return: +7.29%
- zero-equity liquidations: 0

Relative to the unfiltered 2026 short R1:

- trades: 48 -> 32
- PF: 1.280 -> 1.436
- median symbol max drawdown: 28.04% -> 18.26%
- median symbol total return: +4.83% -> +7.29%
- positive-symbol breadth remains 8/13.

This confirms that the explicitly executed threshold-100 profile behaves better than the unfiltered short baseline on the already-known 2026 sample. It does **not** turn that sample into validation.

## Freeze after descriptive reruns

The profile remains frozen exactly as specified above.

No changes are authorized to:

- V2 signal formula;
- bearish MA definition;
- entry strength threshold 100;
- exit strength threshold 0;
- fee/slippage assumptions;
- 1x short notional;
- liquidation semantics.

The next meaningful test is future unseen evidence beginning at:

`2026-09-26T00:00:00Z`

Any tuning before or after reading that future evidence requires a new profile/version and a new unseen boundary.
