# Recovery Architecture Long-History Validation V1 — Result

Date: 2026-09-27
Workflow mode: DIAGNOSTIC_ONLY
Status: RETROSPECTIVE_TRANSFER_WALK_FORWARD / HORIZON_SPLIT_NOT_PORTABLE / NOT_TRADING_READY

## Objective

Test whether the recovery architecture discovered on the current 8-asset universe transfers to an older large-cap crypto universe without retuning.

Frozen architecture under test:

- stress entry: third consecutive close with breadth <= 3;
- stress recovery: third consecutive close with breadth >= 5;
- SMA200 breadth across 8 assets;
- Breadth + Median SMA200 Gap logistic recovery evidence;
- Breadth+Gap assigned to 7d / 14d horizons;
- V2 survival assigned to 30d / 45d / 60d horizons;
- OUT_OF_DURATION_SUPPORT remains an uncertainty flag;
- no threshold optimization;
- no feature changes;
- no trading mapping.

This is a retrospective transfer walk-forward test on a different historical universe.
It is NOT blind current-universe OOS evidence.

## Transfer universe

Frozen large-cap universe:

- BTC
- ETH
- BNB
- XRP
- ADA
- LTC
- LINK
- TRX

Source:

Binance Spot D1 canonical candles through the existing Strategy Lab pipeline.

GitHub Actions:

- data run: `36306970593`
- request commit: `25cd03a4b877871ef97900e6ce71f19c035007fe`
- artifact: `10927536952`

Requested download start:

`2018-01-01`

Common 8-asset overlap:

`2019-01-16 -> 2026-09-26`

Common rows:

`2811`

Usable after SMA200 warm-up:

`2019-08-03 -> 2026-09-26`

Usable rows:

`2612`

No candle-field NA values or duplicate timestamps were observed in the canonical source files.

## Stress episodes

Twelve completed stress episodes were reconstructed:

| Episode | Start | End | Duration |
|---|---|---|---:|
| 001 | 2019-08-16 | 2020-02-01 | 169d |
| 002 | 2020-03-10 | 2020-05-01 | 52d |
| 003 | 2021-06-23 | 2021-08-11 | 49d |
| 004 | 2021-09-26 | 2021-10-03 | 7d |
| 005 | 2021-12-18 | 2023-01-16 | 394d |
| 006 | 2023-03-11 | 2023-03-14 | 3d |
| 007 | 2023-08-19 | 2023-10-26 | 68d |
| 008 | 2024-06-18 | 2024-07-18 | 30d |
| 009 | 2024-08-05 | 2024-11-09 | 96d |
| 010 | 2025-03-23 | 2025-05-11 | 49d |
| 011 | 2025-05-30 | 2025-07-11 | 42d |
| 012 | 2025-11-04 | 2026-08-21 | 290d |

The first three completed episodes were retained as historical initialization.

Whole-episode expanding walk-forward scoring therefore covered:

`9 held-out completed episodes`

Scored pre-recovery daily states:

`979`

## Cross-implementation reproduction gate

Before accepting the transfer result, the same calculation engine was rerun on the original current-universe development sample.

It reproduced the previously recorded values exactly to displayed precision:

- Breadth+Gap overall episode-balanced Brier: `0.1844`
- KM overall: `0.2595`
- Weibull overall: `0.2827`
- HSEP_KM: `0.1690`
- HSEP_WEIBULL: `0.1771`

By horizon:

- 7d BG: 0.2104
- 14d BG: 0.2066
- 30d KM: 0.1246
- 45d KM: 0.1383
- 60d KM: 0.1649

Result:

`REPRODUCTION_GATE = PASS`

Therefore the transfer contradiction is not a calculation drift.

## Primary transfer result — episode-balanced

Lower is better.

### Overall 7/14/30/45/60

| Model | Episode-balanced Brier |
|---|---:|
| **Breadth+Gap all horizons** | **0.1790** |
| HSEP_WEIBULL | 0.2875 |
| HSEP_KM | 0.3064 |
| Weibull all horizons | 0.3315 |
| KM all horizons | 0.3525 |

The prior horizon-separated architecture does NOT transfer.

### Short horizons — 7d / 14d

| Model | Episode-balanced Brier |
|---|---:|
| **Breadth+Gap** | **0.1946** |
| Weibull | 0.3047 |
| KM | 0.3099 |

Per-episode wins:

- BG beats KM in 7 / 9 episodes;
- BG beats Weibull in 5 / 9 episodes.

The short-horizon state-evidence finding is directionally supported, especially versus KM, but not universally dominant episode-by-episode.

### Long horizons — 30d / 45d / 60d

| Model | Episode-balanced Brier |
|---|---:|
| **Breadth+Gap** | **0.1686** |
| Weibull | 0.3494 |
| KM | 0.3809 |

Per-episode wins:

- BG beats KM in **8 / 9** episodes;
- BG beats Weibull in **6 / 9** episodes.

This directly falsifies the portable hypothesis:

`30D+ -> SURVIVAL IS GENERALLY BETTER`

## By horizon

| Horizon | Breadth+Gap | KM | Weibull |
|---:|---:|---:|---:|
| 7d | **0.1894** | 0.2729 | 0.2815 |
| 14d | **0.1998** | 0.3468 | 0.3279 |
| 30d | **0.1836** | 0.4461 | 0.3795 |
| 45d | **0.1663** | 0.4181 | 0.3571 |
| 60d | **0.1560** | 0.2785 | 0.3115 |

The transfer universe does not reproduce the original short-vs-long horizon decomposition.

## Why episode balancing matters

State-weighted scoring gives a materially different picture because the 394d and 290d crises dominate the daily rows.

State-weighted overall:

| Model | Brier |
|---|---:|
| **Weibull** | **0.1693** |
| HSEP_WEIBULL | 0.1743 |
| Breadth+Gap | 0.2083 |
| HSEP_KM | 0.2229 |
| KM | 0.2283 |

If daily rows were treated as independent equal evidence, one could incorrectly conclude that Weibull is the universal winner.

Episode-balanced scoring shows that Weibull is highly specialized toward the longest crises and performs poorly on many shorter independent episodes.

Primary conclusions therefore use episode-balanced results.

## Survival is still useful in extreme long crises

The failure of the 30d+ horizon split does NOT mean survival is useless.

### 394-day STRESS_005

Overall Brier:

- Weibull: **0.1214**
- KM: 0.1924
- Breadth+Gap: 0.2572

Long-horizon Brier:

- Weibull: **0.1805**
- KM: 0.2769
- Breadth+Gap: 0.3859

### 290-day STRESS_012

Overall Brier:

- Weibull: **0.1248**
- Breadth+Gap: 0.1663
- KM: 0.1878

Long-horizon Brier:

- Weibull: **0.1812**
- Breadth+Gap: 0.2556
- KM: 0.2732

Therefore survival appears useful for very long stress-duration behavior.

The error was assigning it by forecast horizon rather than by uncertainty / duration context.

## Duration-support diagnostic

Only one transfer episode exceeded all previously completed duration support:

`STRESS_005: 394d vs prior max 169d`

It contributed 224 OUT_OF_DURATION_SUPPORT states.

State-weighted Brier across those OOS states:

- Weibull: **0.1290**
- KM: **0.1393**
- Breadth+Gap: 0.2343

Inside duration support:

- Weibull: **0.1812**
- Breadth+Gap: 0.2006
- KM: 0.2547

The OOS direction is consistent with the earlier current-universe diagnostic:

survival remains valuable as a conservative duration / uncertainty layer when the episode becomes historically extreme.

However, transfer OOS evidence comes from only ONE independent episode and must not be overgeneralized.

## False-early alert audit

The transfer test strongly confirms that good probability calibration does not make Breadth+Gap a trading trigger.

Diagnostic boundary only:

`P(recovery <= horizon) >= 0.50`

### 7-day signal

Across 9 held-out episodes:

- episodes with a signal: 8
- timely first signal: **2**
- false-early first signal: **6**
- worst false lead: **384 days too early**

The 394-day crisis produced its first 7d recovery alert with 391 days actually remaining.

### 14-day signal

Across 9 held-out episodes:

- episodes with a signal: 9
- timely first signal: **3**
- false-early first signal: **6**
- worst false lead: **377 days too early**

Therefore:

`BREADTH_GAP_PROBABILITY != EXECUTION_TRIGGER`

This failure mode is now reproduced on a materially larger and different historical universe.

## Formal conclusions

### Supported

`BREADTH_GAP_STATE_SIGNAL_TRANSFERS`

The simple Breadth + current Median SMA200 Gap model remains competitive and, episode-balanced, is the strongest tested probability model on the transfer universe.

### Not supported

`HORIZON_SEPARATED_SURVIVAL_RECOVERY_ARCHITECTURE_IS_PORTABLE`

The frozen rule:

- state evidence for 7/14d;
- survival for 30/45/60d

is falsified on the transfer universe.

### Still plausible but not independently proven

`SURVIVAL_AS_EXTREME_DURATION / OOS_UNCERTAINTY_GUARD`

Survival is especially strong in the 394d and 290d crises and during the single OOS episode.

This role is more consistent with the evidence than assigning survival based purely on forecast horizon.

### Reconfirmed

`RAW_RECOVERY_PROBABILITY_NOT_TRIGGER_READY`

Severe false-early alerts remain.

## Research implication

The architecture should be simplified again.

Do NOT promote the frozen HSEP rule.

The evidence now supports separating:

1. state-conditioned recovery probability;
2. duration-support / survival uncertainty context;

without hard-routing models by 7/14 versus 30/45/60 horizons.

A future combination rule would require a new preregistered diagnostic and must not be selected from this transfer result after the fact.

## Validation classification

`RETROSPECTIVE_LONG_HISTORY_TRANSFER_WALK_FORWARD`

This is stronger than repeatedly reusing the same four current-universe episodes because it adds nine held-out episodes from a different frozen universe.

It is still historical retrospective evidence and is NOT equivalent to future unseen validation.

## Runtime impact

Production code changed: NONE
Paper-live code changed: NONE
Strategy parameters changed: NONE
Schema changed: NONE
Migration required: NO

Only the research data-request JSON was added on a separate data-run branch.

## Test level

`TEST_LEVEL: CANONICAL_BINANCE_DATA_ACTION_RUN + CROSS_IMPLEMENTATION_REPRODUCTION_GATE + 9_EPISODE_EXPANDING_WALK_FORWARD_TRANSFER_TEST + EPISODE_BALANCED_HORIZON_COMPARISON + DURATION_SUPPORT_DIAGNOSTIC + FALSE_EARLY_ALERT_AUDIT`

## Residual risks

- transfer universe differs materially from the production/current universe;
- this is retrospective historical evidence, not future blind OOS;
- only nine scored independent episodes;
- only one transfer episode exceeds prior duration support;
- the 394d and 290d episodes dominate state-weighted metrics;
- recovery remains defined from breadth, creating structural relationship with the state model;
- multiple prior diagnostics increase research-selection risk;
- no probability model is approved as a capital-allocation trigger.
