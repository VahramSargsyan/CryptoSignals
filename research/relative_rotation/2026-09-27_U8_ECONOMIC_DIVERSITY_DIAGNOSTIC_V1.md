# U8 Economic Diversity Diagnostic v1

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Status: HYPOTHESIS_GENERATION / NOT_PRODUCTION_APPROVED

## User hypothesis

A strong relative-rotation universe may need diversity not only in price behavior, but also in:
- market capitalization regimes;
- economic niches / use cases;
- ecosystems;
- project/fundamental quality.

The first diagnostic tests niche diversity only. Market-cap and fundamental/team proxies are deferred to separate causal-data work.

## Fixed descriptive primary-niche taxonomy used for this diagnostic

- ATOM -> interoperability
- TWT -> wallet
- PEPE -> meme
- BNB -> exchange/platform
- SOL -> smart-contract L1
- TRX -> payments network
- AAVE -> lending
- LINK -> oracle
- AVAX -> smart-contract L1
- FIL -> decentralized storage
- ETH -> smart-contract L1
- ALGO -> smart-contract L1
- ADA -> smart-contract L1
- XRP -> payments network
- HBAR -> enterprise DLT

This taxonomy is a post-hoc diagnostic classification created after outcomes were opened. It must not be treated as validated selection logic.

The historical U8 contains eight distinct primary niches under this taxonomy:
ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK -> 8/8 unique niches.

## Exhaustive result across all 1,716 U8 sets

### Future period 2024-11-01 -> 2026-09-26

Median future graph return by number of distinct niches:

| Unique niches | Sets | Median future return |
|---:|---:|---:|
| 4 | 13 | +98.92% |
| 5 | 195 | +107.58% |
| 6 | 650 | +181.16% |
| 7 | 665 | +255.40% |
| 8 | 193 | +420.06% |

Spearman correlation:
- unique niche count vs future return: +0.315
- niche entropy vs future return: +0.295
- duplicate-niche count vs future return: -0.315

Top-50 future sets:
- 31/50 had 8 unique niches
- 18/50 had 7 unique niches
- 1/50 had 6 unique niches

Only 193/1,716 = 11.25% of the full universe had 8 unique niches, yet they made up 62% of the top-50: approximately 5.5x enrichment.

### Future period 2025-03-29 -> 2026-09-26

| Unique niches | Sets | Median future return |
|---:|---:|---:|
| 4 | 13 | +32.37% |
| 5 | 195 | +27.02% |
| 6 | 650 | +34.38% |
| 7 | 665 | +75.19% |
| 8 | 193 | +137.17% |

Spearman correlation:
- unique niche count vs future return: +0.301
- niche entropy vs future return: +0.282
- duplicate-niche count vs future return: -0.301

Top-50 future sets:
- 27/50 had 8 unique niches
- 20/50 had 7 unique niches
- 3/50 had 6 unique niches

The 8-niche sets were 54% of the top-50 while only 11.25% of all sets: approximately 4.8x enrichment.

## PEPE-only confound check

Restricting to the 792 U8 sets that already contain PEPE:

Primary period median future return:
- 5 niches: +240.61%
- 6 niches: +289.07%
- 7 niches: +384.31%
- 8 niches: +592.42%

Secondary period:
- 5 niches: +121.17%
- 6 niches: +121.98%
- 7 niches: +145.37%
- 8 niches: +224.02%

Spearman unique-niche correlation within PEPE sets:
- primary: +0.324
- secondary: +0.276

Therefore niche diversity is not explained only by the PEPE effect.

## Interpretation

This is the strongest current structural clue for why the historical U8 may have worked well.

The result supports a more precise hypothesis:

GOOD_ROTATION_UNIVERSE != maximum number of assets.

A good rotation universe may instead require:
- high economic-role diversity;
- low redundant exposure to the same niche/regime;
- enough behavioral divergence to create relative dislocations;
- while preserving route liquidity and avoiding stale-hold nodes.

The result also helps explain why naive U20 expansion can degrade performance: adding many tokens may increase nominal diversity while simultaneously adding redundant smart-contract L1/payment/broad-alt-beta exposures that compete for the same routing events.

## Important limitations

1. The niche taxonomy was defined after outcomes were already visible.
2. Several assets are multi-role; a single primary label is an approximation.
3. Correlation is diagnostic, not causal proof.
4. The next rule must use an externally anchored/frozen taxonomy and be selected before a separate holdout/forward evaluation.
5. Market-cap diversity, ecosystem diversity and behavioral correlation must be tested independently rather than folded into one hindsight score.

## Next test

Combine training-only:
- niche entropy;
- historical market-cap bucket entropy;
- ecosystem diversity;
- daily-return correlation/PCA concentration;
- relative-ratio volatility;
- signal/edge entropy;
- occupancy concentration / stale hold;
- leave-one-node-out route robustness.

Do not optimize weights on the full opened sample.

Live U8 remains unchanged.

TEST_LEVEL: HISTORICAL_EXHAUSTIVE_DIAGNOSTIC
