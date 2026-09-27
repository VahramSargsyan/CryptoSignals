# U8 Universe Selection — First Structural Findings

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Status: HYPOTHESIS_GENERATION / NOT_PRODUCTION_APPROVED

## Context

The exhaustive U8 audit evaluated all 1,716 eight-token sets containing ATOM and TWT from the documented 15-token pool.

The historical U8 is:
ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK.

Historical reconstruction indicates that U8 originated from an ATOM/TWT seed plus a PEPE-centered pair screen. The exact old PEPE shortlist was not perfectly reproduced (4/5 overlap), so the original selection implementation/data slice was not fully preserved.

The old 2025-03-29 -> 2026-03-28 graph 'OOS' result was out-of-sample for routing, but not fully out-of-sample for universe selection because the PEPE shortlist was screened using history through 2026-03-28.

## Exhaustive causal audit recap

### Selection cutoff 2024-10-31; future 2024-11-01 -> 2026-09-26

Historical U8 future median: +319.20%.
Future rank: 515 / 1,716.
Train-vs-future rank correlation: +0.3165.

### Selection cutoff 2025-03-28; future 2025-03-29 -> 2026-09-26

Historical U8 future median: +153.38%.
Future rank: 337 / 1,716.
Train-vs-future rank correlation: -0.0257.

Conclusion: selecting the U8 with the highest trailing graph return is not a reliable universe-selection rule.

## Simple pair-quality topology diagnostics

For every U8, training-only pair-screen statistics were calculated across its 28 internal pairs, then compared with future graph return. These are diagnostic correlations on already-opened history, not prospective validation.

### Primary case — 2024-10-31 selection

Spearman correlation with future U8 median return:

- mean historical pair quality: -0.491
- median historical pair quality: -0.249
- worst pair quality: -0.504
- pair-quality dispersion (standard deviation): +0.535
- dispersion of node-average pair quality: +0.521
- trailing graph return: +0.316

### Secondary case — 2025-03-28 selection

Spearman correlation with future U8 median return:

- mean historical pair quality: -0.665
- median historical pair quality: -0.445
- worst pair quality: -0.689
- pair-quality dispersion: +0.710
- dispersion of node-average pair quality: +0.655
- trailing graph return: -0.026

This rejects the naive hypothesis that a strong future rotation universe should simply contain many individually strong historical pairs.

A more plausible hypothesis is regime complementarity: a useful rotation graph may need heterogeneous assets whose relative relationships create large, alternating dislocations.

## PEPE confound check

PEPE was extremely overrepresented in the strongest future U8 sets. Restricting the analysis only to the 792 sets that already contain PEPE materially weakens the structural correlations:

Primary / secondary correlations within PEPE sets:
- trailing graph return: +0.069 / -0.002
- pair-quality dispersion: +0.219 / +0.190
- node-quality dispersion: +0.179 / +0.083

Therefore the initial full-sample relationship is partly driven by PEPE's special role and must not be generalized yet.

## Hindsight-only node marginal diagnostics

Difference in average future median return between U8 sets containing a token and sets not containing it showed the following tokens positive in both opened future periods:

- PEPE
- HBAR
- AAVE
- FIL
- ALGO

This is HINDSIGHT ONLY. These token names must not be hard-coded into a production selector from this evidence.

The important observation is that several of these future-useful nodes did not look strong by average historical pair quality. This supports the bridge/complementarity hypothesis already observed in the U20 FIL/UNI/NEAR ablations.

## Current working hypotheses

H1 — REGIME_COMPLEMENTARITY:
Good U8 sets contain assets with sufficiently different relative regimes rather than uniformly strong pair histories.

H2 — SIGNAL_DIVERSITY:
Useful graphs distribute confirmed transitions across multiple edges and time periods instead of relying on one persistent pair/hub.

H3 — LOW_STALE_HOLD:
Good graphs avoid training structures where one node captures capital for long periods after a strong relative signal.

H4 — ROUTE_REDUNDANCY:
Good graphs preserve several viable routes between regimes and do not collapse when one non-seed node is removed.

H5 — HUB_PLUS_BRIDGES:
A hub such as PEPE may be useful, but the five neighbors should be selected for complementary network roles, not just top standalone pair score.

## Next fixed test

Build training-only features for every U8:
- daily-return correlation / diversification;
- cross-sectional return dispersion;
- relative-ratio volatility dispersion;
- confirmed-event count and entropy across edges;
- route occupancy concentration;
- longest stale hold;
- transition-edge diversity;
- conflict dependence;
- leave-one-node-out training robustness.

Compare these features against future outcomes on the two already-opened long-horizon cases.

Any feature discovered there remains hypothesis-generation only.

A later selection rule must be frozen before a separate forward/holdout evaluation.

Live U8 remains unchanged.

TEST_LEVEL: HISTORICAL_EXHAUSTIVE_DIAGNOSTIC + GITHUB_ACTIONS_EVIDENCE
