# U8 Diversity Selection Engine v1 — preregistration

Date: 2026-09-27
Mode: STRESS_TEST_ONLY

## Goal

Test whether the historical U8's strength can be explained and reproduced by a structural rule:

1. maximize economic-niche diversity;
2. among equally diverse sets, prefer training-only behavioral/topological complementarity;
3. do not use trailing strategy return in the selector.

Live U8 remains unchanged.

## Candidate pool

Fixed 15-token pool from the original U8 discovery research:

ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, AVAX, FIL, ETH, ALGO, ADA, XRP, HBAR.

ATOM and TWT remain mandatory seed assets for this v1 audit.
All 1,716 U8 sets containing ATOM and TWT are evaluated.

## Frozen primary-niche taxonomy

- ATOM: INTEROPERABILITY
- TWT: WALLET
- PEPE: MEME
- BNB: EXCHANGE_PLATFORM
- SOL: SMART_CONTRACT_L1
- TRX: PAYMENTS
- AAVE: LENDING
- LINK: ORACLE
- AVAX: SMART_CONTRACT_L1
- FIL: DECENTRALIZED_STORAGE
- ETH: SMART_CONTRACT_L1
- ALGO: SMART_CONTRACT_L1
- ADA: SMART_CONTRACT_L1
- XRP: PAYMENTS
- HBAR: ENTERPRISE_DLT

The historical U8 has 8 unique primary niches.

## Frozen strategy engine

- Binance Spot 1D closed candles
- rolling median 180d
- ARM 15%
- reversal confirmation 3%
- strongest confirmed max-dislocation router
- next-open execution
- transition cost 0.1%

No signal parameter tuning.

## Causal cases

### PRIMARY
Selection cutoff: 2024-10-31
Training: 2023-10-31 -> 2024-10-31
Future evaluation: 2024-11-01 -> 2026-09-26

### SECONDARY
Selection cutoff: 2025-03-28
Training: 2024-03-29 -> 2025-03-28
Future evaluation: 2025-03-29 -> 2026-09-26

## Structural selector

Stage 1:
Keep only candidate U8 sets with the maximum number of unique niches available. In this pool that target is expected to be 8.

Stage 2:
Rank only those max-niche sets using training-only structural features. Equal-weight percentile score; no future data and no trailing graph return.

Features, fixed before outcome review:

1. LOW_MEAN_ABS_CORR
   Lower mean absolute correlation of daily token returns is better.

2. LOW_PCA1_SHARE
   Lower first-principal-component share of return covariance is better.

3. HIGH_RELATIVE_VOL
   Higher mean standard deviation of pairwise daily log-ratio changes is better.

4. HIGH_SIGNAL_EDGE_ENTROPY
   More evenly distributed confirmed relative-rotation signals across directed edges is better.

5. HIGH_OCCUPANCY_ENTROPY
   More evenly distributed route occupancy across nodes in training is better.

6. HIGH_ROUTE_EDGE_ENTROPY
   More distributed realized training transitions across directed edges is better.

7. LOW_CONFLICT_DEPENDENCE
   Lower fraction of realized training transitions selected from multi-candidate conflicts is better.

Each feature is converted to a percentile rank among max-niche candidates. The final STRUCTURAL_SCORE is the unweighted mean of the seven percentiles.

Tie-break:
1. higher niche count;
2. higher structural score;
3. lexicographically sorted set key.

## Comparators

For each causal case report:
- structurally selected U8;
- historical U8;
- training-return-selected U8;
- median of all max-niche sets;
- best future U8, explicitly HINDSIGHT ONLY.

Also report the future rank of the structural selection among all 1,716 U8s and among max-niche U8s.

## Interpretation

This is hypothesis-generation on already-opened historical periods. A good result does not authorize a live universe change.

If structural selection beats the historical U8 in both long future cases without using training return, it becomes a candidate rule for a later frozen holdout/forward test.

If it fails, do not tune weights on these same outcomes.

## Deferred to v2

Historical market-cap diversity and external fundamental/team proxies require separate point-in-time data and are not mixed into v1.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
