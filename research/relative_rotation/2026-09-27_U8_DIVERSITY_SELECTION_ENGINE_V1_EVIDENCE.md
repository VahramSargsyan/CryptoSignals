# U8 Diversity Selection Engine v1 — Evidence

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Branch: research/u8-diversity-selection-engine-v1
GitHub Actions run: 36323878047
Result: PASS
TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST

## Scope

Candidate pool: the documented original 15-token discovery pool.

Mandatory seeds:
- ATOM
- TWT

All candidate U8s in this v1 must have the maximum available primary-niche diversity: 8 unique niches.

There are 193 such max-niche U8 sets.

The selector did NOT use trailing strategy return. It used seven training-only structural features with equal percentile weights:

1. low mean absolute daily-return correlation;
2. low PCA1 covariance share;
3. high pairwise relative-return volatility;
4. high confirmed-signal edge entropy;
5. high route occupancy entropy;
6. high realized route-edge entropy;
7. low conflict dependence.

Frozen relative-rotation engine:
- Binance Spot 1D closed candles
- 180d rolling median
- 15% ARM
- 3% reversal confirmation
- next-open execution
- 0.1% transition cost
- strongest confirmed max-dislocation router

Live U8 was not changed.

## Result 1 — selection cutoff 2024-10-31

Training:
2023-10-31 -> 2024-10-31

Future:
2024-11-01 -> 2026-09-26

### Structural selection

ATOM, TWT, PEPE, BNB, TRX, AAVE, AVAX, HBAR

Eight unique niches:
- interoperability
- wallet
- meme
- exchange/platform
- payments
- lending
- smart-contract L1
- enterprise DLT

Future performance:
- median across eight starts: +479.34%
- ATOM-start: +486.61%
- median max drawdown: -61.93%
- future rank among 193 max-niche U8s: 82 / 193

Historical U8:
ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK

Future:
- median: +319.20%
- ATOM-start: +333.58%
- median max drawdown: -71.23%
- rank among max-niche sets: 107 / 193

Structural selector improvement versus historical U8:
- median return: +160.14 percentage points
- ATOM-start: +153.03 percentage points
- drawdown improved by about 9.30 percentage points

Median future return of all 193 max-niche sets:
+420.06%

All 193 max-niche sets were positive in this future period.

Structural-score Spearman correlation with future return:
+0.282

## Result 2 — selection cutoff 2025-03-28

Training:
2024-03-29 -> 2025-03-28

Future:
2025-03-29 -> 2026-09-26

### Structural selection

ATOM, TWT, PEPE, BNB, SOL, TRX, FIL, HBAR

Eight unique niches:
- interoperability
- wallet
- meme
- exchange/platform
- smart-contract L1
- payments
- decentralized storage
- enterprise DLT

Future performance:
- median across eight starts: +348.20%
- ATOM-start: +355.37%
- median max drawdown: -53.29%
- future rank among 193 max-niche U8s: 42 / 193

Historical U8 future:
- median: +153.38%
- ATOM-start: +152.76%
- median max drawdown: -71.23%
- rank among max-niche sets: 89 / 193

Structural selector improvement versus historical U8:
- median return: +194.83 percentage points
- ATOM-start: +202.61 percentage points
- drawdown improved by about 17.94 percentage points

Median future return of all 193 max-niche sets:
+137.17%

Positive max-niche sets:
89.12%

Structural-score Spearman correlation with future return:
+0.407

## Feature attribution

Spearman correlation of each TRAINING-ONLY feature with future return among max-niche sets:

| Feature | Primary | Secondary | Direction consistency |
|---|---:|---:|---|
| mean relative volatility | +0.774 | +0.590 | strong positive / consistent |
| PCA1 share | -0.354 | -0.338 | lower is better / consistent |
| mean absolute correlation | -0.112 | -0.139 | lower is better / consistent but weak |
| route-edge entropy | +0.419 | +0.085 | positive but unstable strength |
| occupancy entropy | +0.049 | -0.076 | no stable signal |
| signal-edge entropy | -0.403 | +0.120 | unstable / sign flip |
| conflict dependence | +0.197 | -0.204 | unstable / sign flip |

## Strongest structural clue

The clearest repeated relationship is:

1. first require high economic-niche diversity;
2. then prefer assets with high RELATIVE movement versus each other;
3. avoid a universe dominated by one common market factor.

Mean pairwise relative volatility was by far the strongest repeated feature:
- +0.774 primary
- +0.590 secondary

Lower PCA1 concentration also repeated:
- -0.354
- -0.338

Lower mean absolute return correlation repeated in the expected direction:
- -0.112
- -0.139

This supports the working mechanism:

A relative-rotation graph needs assets that do different economic jobs AND actually move differently enough to create repeatable relative dislocations.

## Important negative finding

Not all intuitively attractive topology features generalized:
- signal entropy flipped sign;
- conflict dependence flipped sign;
- occupancy entropy was essentially noise.

Therefore the seven-feature equal-weight composite should NOT be tuned on these same opened periods.

A cleaner next-generation selector should treat niche diversity + relative-volatility diversity + low common-factor concentration as the structural core, but that reduced rule requires a new preregistered evaluation.

## Hindsight ceiling — not a selector

Best future max-niche U8 in primary period:
ATOM, TWT, PEPE, BNB, AAVE, FIL, ALGO, HBAR
Future median: +1921.91%

Best future max-niche U8 in secondary period:
ATOM, TWT, PEPE, BNB, AAVE, LINK, FIL, HBAR
Future median: +551.62%

These are hindsight-only and are not candidates for live promotion from this evidence.

## Interpretation

The historical U8 was not uniquely optimal.

However, the user's hypothesis that diversity should be economic and behavioral is supported by this diagnostic:

- 8 unique niches strongly enriched future winners in the prior exhaustive audit;
- a training-only structural selector improved on the historical U8 in both long future periods;
- relative-return dispersion and low common-factor concentration generalized across both cases.

This suggests that the important design variable may be UNIVERSE ARCHITECTURE rather than universe size.

## Next gate

Do not change live U8.

Next research:
1. build a simplified preregistered CORE selector using:
   - maximum primary-niche diversity;
   - high relative-return volatility;
   - low PCA1 share;
   - low mean absolute correlation;
2. add historical point-in-time market-cap buckets as an independent feature, not today's market cap;
3. expand the candidate pool beyond the original 15 using a frozen taxonomy;
4. use rolling/walk-forward selection dates;
5. reserve any genuinely future data as the final validation gate.

No production/live files changed.
