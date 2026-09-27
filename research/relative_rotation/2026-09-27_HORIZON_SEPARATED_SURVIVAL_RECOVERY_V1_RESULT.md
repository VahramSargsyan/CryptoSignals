# Horizon-Separated Survival + Recovery Evidence V1 — Diagnostic Result

Date: 2026-09-27
Workflow mode: DIAGNOSTIC_ONLY
Status: RESEARCH_ONLY / PROMISING_PROBABILITY_ARCHITECTURE / NOT_TRIGGER_READY

## Objective

Test the horizon-separated architecture proposed after the rejected contextual switch:

- Breadth + Median SMA200 Gap is responsible only for SHORT-HORIZON recovery evidence:
  - 7 days
  - 14 days
- V2 survival is responsible for DURATION context:
  - 30 days
  - 45 days
  - 60 days
- OUT_OF_DURATION_SUPPORT remains visible at all times.
- No model replaces the other.
- No trading threshold, capital weight, or exit rule is selected.

No strategy code was changed.

## Data and semantic reproduction

Canonical Binance D1 data reused from:

- Strategy Lab data run: `36299600335`
- artifact: `10924993695`
- universe: ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK

The stress episode reconstruction and Breadth+Gap probability reproduction gates both matched the prior diagnostics.

Confirmed recovery closes were excluded from scoring.

Scored development states:

`186`

Held-out episodes:

- STRESS_004 — 30d
- STRESS_005 — 11d
- STRESS_006 — 4d
- STRESS_007 — 141d

## Frozen horizon-separated formulations

### HSEP_KM

- 7d / 14d: Breadth+Gap
- 30d / 45d / 60d: Kaplan-Meier V2

### HSEP_WEIBULL

- 7d / 14d: Breadth+Gap
- 30d / 45d / 60d: Weibull V2

No horizon reassignment was made after seeing results.

## Development — episode-balanced Brier

Lower is better.

| Model | Episode-balanced mean Brier |
|---|---:|
| **HSEP_KM** | **0.1690** |
| HSEP_WEIBULL | **0.1771** |
| Breadth+Gap all horizons | 0.1844 |
| KM all horizons | 0.2595 |
| Weibull all horizons | 0.2827 |

Relative improvements:

- HSEP_KM vs KM_ALL: ~34.9%
- HSEP_KM vs Breadth+Gap_ALL: ~8.4%
- HSEP_WEIBULL vs Weibull_ALL: ~37.4%
- HSEP_WEIBULL vs Breadth+Gap_ALL: ~4.0%

This is the first combined architecture in this branch of research that improves the episode-balanced probability score versus both of its component families.

## Development — state-weighted Brier

| Model | State-weighted mean Brier |
|---|---:|
| **HSEP_KM** | **0.3244** |
| KM all horizons | 0.3294 |
| HSEP_WEIBULL | 0.3464 |
| Weibull all horizons | 0.3650 |
| Breadth+Gap all horizons | 0.3790 |

The improvement is much smaller under state weighting, but remains directionally consistent.

## Episode-balanced score by horizon

| Horizon | Breadth+Gap | KM | Weibull | HSEP_KM | HSEP_WEIBULL |
|---:|---:|---:|---:|---:|---:|
| 7d | **0.2104** | 0.4505 | 0.4414 | **0.2104** | **0.2104** |
| 14d | **0.2066** | 0.4191 | 0.5036 | **0.2066** | **0.2066** |
| 30d | 0.1910 | **0.1246** | 0.1644 | **0.1246** | 0.1644 |
| 45d | 0.1702 | **0.1383** | 0.1588 | **0.1383** | 0.1588 |
| 60d | **0.1436** | 0.1649 | 0.1452 | 0.1649 | 0.1452 |

The predeclared 30d+ survival split is not optimal at every individual horizon:

- Breadth+Gap happened to beat KM at 60d.

No horizon was reassigned because doing so after viewing this table would be optimization on the development evidence.

## Short-vs-long information split

Episode-balanced:

### Short horizons: 7d / 14d

- Breadth+Gap: **0.2085**
- KM: 0.4348
- Weibull: 0.4725

### Long horizons: 30d / 45d / 60d

- KM: **0.1426**
- Weibull: 0.1561
- Breadth+Gap: 0.1683

This supports the qualitative separation:

`STATE EVIDENCE -> SHORT-TERM RECOVERY`

`SURVIVAL -> LONGER DURATION CONTEXT`

## Important support-conditioned caveat

Inside historical duration support:

- HSEP_KM mean Brier: **0.3242**
- Breadth+Gap all-horizon mean Brier: 0.3298
- KM all-horizon mean Brier: 0.3806

Outside historical duration support:

- KM all-horizon mean Brier: **0.2916**
- HSEP_KM: 0.3246
- Breadth+Gap all-horizon mean Brier: 0.4153

Therefore the HSEP architecture must NOT be interpreted as:

> short-horizon state evidence becomes more trustworthy merely because the episode is OOS.

The duration-support warning remains independent.

## >=50% short-horizon signal audit

This audit is diagnostic only.
No 50% trading threshold is proposed.

### Breadth+Gap 7d

Across four held-out development episodes:

- produced a >=50% signal in 2/4 episodes;
- 1 was appropriately inside the 7-day horizon;
- 1 was severely false-early:
  - STRESS_007 signaled with **137 days remaining**.

### Breadth+Gap 14d

Across four held-out episodes:

- produced a >=50% signal in 4/4 episodes;
- 2 were inside the true 14-day horizon;
- 2 were false-early:
  - STRESS_004 signaled with 30 days remaining;
  - STRESS_007 signaled with **141 days remaining**.

This is a critical constraint.

A good Brier score does NOT mean the raw probability is ready to become an execution threshold.

## Post-hoc 2026 replay

Label:

`POST_HOC_2026_DIAGNOSTIC_NOT_VALIDATION`

Period:

`2026-03-29 -> 2026-08-21`

All 146 replay states were OUT_OF_DURATION_SUPPORT.

### Mean Brier

| Model | 2026 mean Brier |
|---|---:|
| Weibull all horizons | **0.1756** |
| HSEP_WEIBULL | 0.1767 |
| KM all horizons | 0.2137 |
| HSEP_KM | 0.2143 |
| Breadth+Gap all horizons | 0.2457 |

The horizon-separated architecture does NOT improve the already-opened 2026 episode versus its survival parent.

This is not a selection failure because 2026 was excluded from model selection.

It is a warning that the development gain is not yet independently validated.

### Timing behavior

HSEP_KM / HSEP_WEIBULL inherit the Breadth+Gap short-horizon timing:

- P(recovery <=7d) >= 50%: 2026-08-19, actual remaining 3d
- P(recovery <=14d) >= 50%: 2026-08-13, actual remaining 9d

The long-horizon probabilities remain survival-based, preventing the Breadth+Gap false-early 30d signal observed in the prior diagnostic.

## Main conclusion

The horizon-separated architecture is supported as a PROBABILITY-DECOMPOSITION hypothesis:

`SHORT RECOVERY EVIDENCE != LONG DURATION CONTEXT`

Specifically:

- Breadth+Gap is materially better for 7d / 14d probability scoring in development;
- KM/Weibull are materially better for 30d+ duration context in development;
- combining them by horizon improves episode-balanced development scoring.

However:

`HORIZON_SEPARATED_PROBABILITY != TRADING TRIGGER`

The raw short-horizon probabilities still create severe false-early alerts in long stress episodes.

Therefore:

`PROMISING_PROBABILITY_ARCHITECTURE / NOT_TRIGGER_READY`

## What this changes conceptually

The next research problem is no longer:

> Which model should replace the other?

It is:

> What independent confirmation is required before short-horizon recovery evidence is allowed to influence actual capital?

Any such confirmation must be preregistered separately.

Candidate classes include, but are not tested here:

- persistence / multi-close confirmation;
- monotonic probability improvement;
- survival-aware uncertainty veto;
- partial rather than full capital exposure.

No confirmation mechanism or threshold is selected in this result.

## Runtime impact

Production code changed: NONE
Paper-live code changed: NONE
Strategy parameters changed: NONE
Schema changed: NONE
Migration required: NO

## Test level

`TEST_LEVEL: LOCAL_READ_ONLY_DIAGNOSTIC + CANONICAL_BINANCE_DATA_REUSE + STRESS_SEMANTIC_REPRODUCTION_GATE + BREADTH_GAP_REPRODUCTION_GATE + EPISODE_BALANCED_HORIZON_DECOMPOSITION + SHORT_SIGNAL_FALSE_POSITIVE_AUDIT + POST_HOC_2026_REPLAY`

## Residual risks

- only four independently held-out development crises;
- daily states remain correlated within episodes;
- the architecture is selected from already-researched model families;
- 2026 is post-hoc and does not validate the development improvement;
- raw 7d/14d probabilities have severe false-early cases;
- the recovery label remains breadth-derived;
- no future clean stress episode has validated the horizon split.
