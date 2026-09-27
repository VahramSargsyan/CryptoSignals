# Survival Guard + Breadth/Gap Contextual Switch V1 — Diagnostic Result

Date: 2026-09-27
Workflow mode: DIAGNOSTIC_ONLY
Status: RESEARCH_ONLY / SIMPLE_CONTEXTUAL_SWITCH_REJECTED / NO TRADING

## Question

Can the existing V2 duration-support guard and the development-supported `BREADTH + MEDIAN_SMA200_GAP` recovery evidence be combined by a simple contextual switch?

Frozen contextual rule, declared before calculation:

- if current stress age is `IN_DURATION_SUPPORT`, use Kaplan-Meier V2 recovery probability;
- if current stress age is `OUT_OF_DURATION_SUPPORT`, use Breadth+Gap recovery probability.

No probability weights, thresholds, or parameter tuning were introduced.

Acceptance required BOTH:

1. lower episode-balanced mean Brier than `KM_ALL`;
2. lower episode-balanced mean Brier than `BREADTH_PLUS_GAP_ALL`;

and the OOS states had to improve versus KM.

## Data

Canonical Binance D1 panel reused from the successful Strategy Lab data artifact:

- data run: `36299600335`
- artifact: `10924993695`
- universe: ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK
- last fully closed candle: 2026-09-26

Stress reconstruction reproduced the canonical V1/V2 episode dates exactly.

## Model reproduction

The local Breadth+Gap diagnostic reproduced the previous feature-ablation episode Brier values exactly to displayed precision:

- STRESS_004: 0.078898
- STRESS_005: 0.102420
- STRESS_006: 0.083378
- STRESS_007: 0.472810

This is a reproduction gate for the read-only comparator.

## Development results

Held-out episodes:

- STRESS_004 — 30d
- STRESS_005 — 11d
- STRESS_006 — 4d
- STRESS_007 — 141d

Confirmed recovery close was excluded.

### Per-episode mean Brier

| Episode | OOS states | Breadth+Gap | KM V2 | Contextual switch |
|---|---:|---:|---:|---:|
| STRESS_004 | 0 | 0.0789 | **0.0228** | **0.0228** |
| STRESS_005 | 0 | **0.1024** | 0.2830 | 0.2830 |
| STRESS_006 | 0 | **0.0834** | 0.3340 | 0.3340 |
| STRESS_007 | 107 | 0.4728 | **0.3981** | **0.4920** |

### Episode-balanced mean Brier

| Model | Episode-balanced Brier |
|---|---:|
| Breadth+Gap all states | **0.1844** |
| KM V2 all states | 0.2595 |
| Contextual KM→BG switch | **0.2829** |

The contextual switch is worse than BOTH components.

Acceptance gate:

`FAIL`

## Support-stratified result

### IN_DURATION_SUPPORT

79 scored states:

- Breadth+Gap mean Brier: **0.3298**
- KM mean Brier: 0.3806

Breadth+Gap is better in aggregate inside historical duration support.

### OUT_OF_DURATION_SUPPORT

107 scored states:

- KM mean Brier: **0.2916**
- Breadth+Gap mean Brier: 0.4153

KM is materially better once the long 141-day episode exceeds prior completed duration support.

This directly falsifies the proposed switching assumption.

## Why the switch fails

The assumption was:

> once historical duration support is exhausted, use state evidence instead of KM.

Historical evidence says the opposite is often safer.

During a very long stress episode, Breadth and SMA-gap can temporarily improve without a true near-term recovery.

The nonparametric survival model remains conservative because no prior completed event supports recovery at those ages.

Therefore:

`OUT_OF_SUPPORT != STATE_MODEL_SHOULD_TAKE_OVER`

The OOS guard is an uncertainty warning, not a routing switch between models.

## Post-hoc 2026 replay

Label:

`POST_HOC_2026_DIAGNOSTIC_NOT_VALIDATION`

Replay period:

`2026-03-29 -> 2026-08-21`

Every replay state is OUT_OF_DURATION_SUPPORT.

Therefore the contextual switch collapses to Breadth+Gap for the entire replay.

Mean Brier:

- KM V2: **0.2137**
- Breadth+Gap: 0.2457
- Contextual switch: 0.2457

First Breadth+Gap probability >= 0.50:

- recovery <=7d: 2026-08-19, actual remaining 3d
- recovery <=14d: 2026-08-13, actual remaining 9d
- recovery <=30d: **2026-03-31, actual remaining 144d**

The 30-day false-early signal is particularly important.

It shows that Breadth+Gap is useful as short-horizon recovery evidence but should not replace the survival guard for broader duration inference.

## Main conclusion

`SIMPLE_CONTEXTUAL_SWITCH = REJECTED`

Do not implement:

`IF OUT_OF_SUPPORT THEN TRUST BREADTH+GAP`

Supported architecture is narrower:

1. V2 survival/OOS remains the duration and uncertainty layer.
2. Breadth+Gap remains a short-horizon recovery-evidence layer.
3. The two channels should coexist rather than one replacing the other.
4. Any future combination should preserve the OOS warning even when Breadth+Gap becomes optimistic.

The prior feature ablation already showed that the incremental value of SMA gap is concentrated mainly at 7d and 14d horizons.

The current replay reinforces that result:

- 7d/14d evidence became useful close to recovery;
- the 30d signal became optimistic far too early.

## Next justified diagnostic

A separate preregistered horizon-separated architecture:

`SURVIVAL_DURATION_CONTEXT + SHORT_HORIZON_RECOVERY_EVIDENCE`

Concept:

- V2/KM/Weibull remains responsible for duration uncertainty and 30d+ context;
- Breadth+Gap is evaluated only as 7d/14d recovery evidence;
- OOS status remains visible and can never be overridden by the state model;
- no trading threshold or capital allocation is selected.

No such model is implemented in this result.

## Runtime impact

Production code changed: NONE
Paper-live code changed: NONE
Strategy parameters changed: NONE
Schema changed: NONE
Migration required: NO

## Test level

`TEST_LEVEL: LOCAL_READ_ONLY_DIAGNOSTIC + CANONICAL_BINANCE_DATA_REUSE + STRESS_SEMANTIC_REPRODUCTION_GATE + BREADTH_GAP_REPRODUCTION_GATE + EPISODE_WALK_FORWARD_CONTEXTUAL_COMPARATOR + POST_HOC_2026_REPLAY`

## Residual risks

- only four independently held-out development crises;
- 107 OOS development states all come from one long episode;
- daily states are correlated within episodes;
- KM conservatism can miss an actual recovery while out of support;
- Breadth+Gap target is structurally related to the breadth-based recovery definition;
- 2026 replay is post-hoc;
- no trading mapping has been tested.
