# RR DYNAMIC / REGIME-CONDITIONED RISK V1 — EVIDENCE

Date: 2026-10-01
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Runtime identity

- Research branch: `research-run/dynamic-regime-risk-v1`
- GitHub Actions run: `36820703622`
- Source commit: `fda7d3321badeeaf66554568a833f0c52d21264b`
- Artifact ID: `11143022507`
- Artifact SHA256: `a78ab43099a5fe09d86ee8268e9af11a823f649bb4544d6f6728802524d1cacb`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Research question

Does explicit market-regime conditioning improve the large-panel probabilistic RR risk model, especially the previously observed relationship between recent losing-route streaks and next-route loss probability?

The tested dynamic models were fixed before results.

## Dataset

Same causal current-U10 route-opportunity panel as BigData V1:

- 2527 executable RR+DDG route episodes
- 899 unique signal dates
- 973 any-loss episodes
- 443 material losses <= -10%

The panel contains counterfactual executable source/date routes, not 2527 independent portfolio trades.

## Pre-registered models

### STATIC_STATE — control

- warning-domain count
- prior negative exit-batch streak
- recent five-route loss rate

### REGIME_COMPACT — primary dynamic hypothesis

STATIC_STATE plus:
- adverse market count from BTC_BEAR, BROAD_BEAR, TOTAL_NOT_BULL
- streak x adverse-count interaction
- recent-loss-rate x adverse-count interaction

### REGIME_DETAIL — diagnostic

Separate BTC/BREADTH/TOTAL regime flags plus separate streak interactions.

### DECAY_365 / DECAY_180 — diagnostic recency models

REGIME_COMPACT structure with exponential training weights:
- 365-day half-life
- 180-day half-life

## Pre-registered robustness gate

A dynamic model had to:

1. beat the constant baseline on full monthly causal walk-forward Brier;
2. beat STATIC_STATE on full monthly causal walk-forward Brier;
3. beat baseline Brier in at least 2 of 3 calendar years;
4. achieve AUC > 0.5 in at least 2 of 3 calendar years.

No model passed.

## Final 1Y holdout

### Any-loss target

| Model | Brier | Baseline | AUC |
|---|---:|---:|---:|
| **STATIC_STATE** | **0.209** | 0.229 | **0.649** |
| REGIME_COMPACT | 0.255 | 0.229 | 0.373 |
| REGIME_DETAIL | 0.216 | 0.229 | 0.532 |
| DECAY_365 | 0.249 | 0.229 | 0.394 |
| DECAY_180 | 0.250 | 0.229 | 0.390 |

The simple state control is clearly better than the explicit dynamic models on the final-year any-loss holdout.

### Material-loss target <= -10%

| Model | Brier | Baseline | AUC |
|---|---:|---:|---:|
| STATIC_STATE | 0.116 | 0.111 | 0.471 |
| **REGIME_COMPACT** | **0.104** | 0.111 | 0.544 |
| REGIME_DETAIL | 0.106 | 0.111 | **0.549** |
| DECAY_365 | 0.105 | 0.111 | 0.504 |
| DECAY_180 | 0.105 | 0.111 | 0.484 |

The final year alone gives a superficially interesting material-loss result for regime models. It does not survive the full walk-forward robustness test.

## Full monthly causal walk-forward

### Any-loss

| Model | Brier | Baseline | AUC |
|---|---:|---:|---:|
| **STATIC_STATE** | **0.270** | 0.279 | 0.448 |
| REGIME_COMPACT | 0.290 | 0.279 | 0.439 |
| REGIME_DETAIL | 0.297 | 0.279 | 0.406 |
| DECAY_365 | 0.295 | 0.279 | 0.441 |
| DECAY_180 | 0.301 | 0.279 | 0.448 |

Every explicit regime/recency model is worse than STATIC_STATE and worse than the constant baseline on Brier.

### Material loss <= -10%

| Model | Brier | Baseline | AUC |
|---|---:|---:|---:|
| **STATIC_STATE** | **0.15942** | 0.15975 | 0.424 |
| REGIME_COMPACT | 0.15945 | 0.15975 | 0.413 |
| REGIME_DETAIL | 0.16311 | 0.15975 | 0.400 |
| DECAY_365 | 0.16297 | 0.15975 | 0.411 |
| DECAY_180 | 0.16685 | 0.15975 | 0.413 |

REGIME_COMPACT is essentially tied with STATIC_STATE on Brier but does not improve ranking and does not satisfy the robustness gate.

## Year-by-year instability

### Any-loss Brier vs baseline

REGIME_COMPACT:
- 2024: 0.336 vs 0.339 — small improvement
- 2025: 0.235 vs 0.243 — improvement
- 2026: **0.305 vs 0.237 — large failure**

AUC:
- 2024: 0.498
- 2025: 0.509
- 2026: **0.296**

STATIC_STATE:
- 2024 Brier: 0.322 vs 0.339
- 2025: 0.254 vs 0.243
- 2026: **0.206 vs 0.237**
- 2026 AUC: **0.680**

The explicit regime interactions that were intended to improve 2026 actually destroy much of the useful 2026 state signal.

## Regime x streak descriptive matrix

The relationship is real but highly non-simple.

Any-loss rates:

| Adverse regime count | streak 0 | streak 1 | streak 2+ |
|---:|---:|---:|---:|
| 0 | 30.0% | 66.3% | 78.0% |
| 1 | 33.2% | 58.9% | **88.1%** |
| 2 | 33.2% | 33.7% | 38.2% |
| 3 | 19.6% | **11.9%** | 55.6% |

Therefore the intuitive rule:

`more adverse macro regime + longer loss streak = monotonically higher risk`

is false.

The state/regime surface contains strong interactions and reversals.

This is exactly why hand-crafted regime multipliers are unsafe.

## Monthly cluster bootstrap

Paired bootstrap by month confirms the lack of robust improvement.

### Any-loss: probability dynamic model has lower Brier than STATIC_STATE

- REGIME_COMPACT: **14.6%**
- REGIME_DETAIL: **5.2%**
- DECAY_365: **9.9%**
- DECAY_180: **7.2%**

Thus the evidence favors STATIC_STATE over all tested dynamic variants.

### Material loss

REGIME_COMPACT vs STATIC_STATE:
- probability of lower Brier: ~52.4%
- bootstrap 95% interval for delta includes zero broadly

This is effectively a coin flip, not evidence of improvement.

## Interpretation

The BigData V1 observation that recent negative-route state contains information remains valid.

What fails is the next intuitive step:

> explicitly condition that state on coarse BTC / breadth / TOTAL regimes or rapidly down-weight older history.

The added regime structure does not generalize.

Frozen interpretation:

`LOSS_STATE_HAS_INFORMATION`

but

`HANDCRAFTED_REGIME_CONDITIONING_DOES_NOT_ADD_ROBUST_PREDICTIVE_VALUE`

and

`RECENCY_DECAY_180_365_NOT_SUPPORTED`.

The useful state signal may already encode regime information indirectly, or the true interaction may be more complex/nonstationary than the tested coarse regime definitions.

## Decision

No dynamic model is promoted.

STATIC_STATE remains the least-bad research control for any-loss probability, but it is itself not production-approved.

No probability output should control:
- entry veto;
- route selection;
- position sizing;
- universe composition.

## No-repeat rule

Do not repeat the same coarse regime-interaction or 180/365-day recency-decay experiment on the same history.

A new run requires:
- genuinely new forward route outcomes;
- a materially different point-in-time feature family;
- a preregistered nonparametric/state-space hypothesis;
- or a new fundamental layer.

## Production boundary

No production/live/paper/Telegram/exchange/universe/sizing behavior changed.

## Residual risks

- route episodes share dates/assets/regimes and are correlated;
- regime definitions are coarse;
- state variables and macro features may be redundant;
- the useful 2026 state effect may be transient;
- full walk-forward AUC remains weak even for STATIC_STATE.
