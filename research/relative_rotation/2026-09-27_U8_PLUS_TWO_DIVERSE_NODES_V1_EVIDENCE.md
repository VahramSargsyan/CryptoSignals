# U8 + FIL + HBAR v1 — Evidence

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Branch: research/u8-plus-two-diverse-nodes-v1
GitHub Actions run: 36325624396
Result: PASS
TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST

## Question

Test whether adding exactly two economically non-redundant nodes to the canonical U8 improves the relative-rotation graph without the topology pollution seen in broad U20 expansion.

The added nodes were fixed before this run:
- FIL = decentralized storage
- HBAR = enterprise DLT

They are the only two assets in the original documented 15-token pool that add primary niches not already represented by canonical U8.

Live U8 was not changed.

## Universes

U8:
ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK

U9_FIL:
U8 + FIL

U9_HBAR:
U8 + HBAR

U10_FIL_HBAR:
U8 + FIL + HBAR

Frozen engine:
- Binance Spot 1D closed candles
- 180d rolling median
- 15% ARM
- 3% reversal confirmation
- next-open execution
- 0.1% transition cost
- strongest confirmed max-dislocation router

Common history:
2023-05-05 -> 2026-09-26

Mature window after 180 rows:
2023-10-31 -> 2026-09-26
1062 days (~34.86 months)

Cross-universe comparison uses the same original eight starting assets.

## Full mature result

| Universe | Median return | ATOM-start | Median max DD | Median transitions |
|---|---:|---:|---:|---:|
| U8 | +922.31% | +875.75% | -71.23% | 21.0 |
| U9_FIL | +622.63% | +589.72% | -71.04% | 19.5 |
| U9_HBAR | +1357.01% | +1199.05% | -71.04% | 22.5 |
| U10_FIL_HBAR | +2249.75% | +2142.74% | -71.04% | 23.5 |

All eight original starting assets were positive in all four universes.

U10 versus U8:
- median return improvement: +1327.44 percentage points
- ATOM-start improvement: +1266.99 percentage points
- drawdown essentially unchanged (+0.19 percentage points)
- only ~2.5 additional median transitions over the mature span

Worst starting-asset return in U10:
+1821.89%

Best starting-asset return in U10:
+2744.14%

This is a large topology result, not a small starting-asset artifact.

## Rolling 12-month windows

23 monthly-start windows.

| Universe | Median of window medians | Worst window median | Positive windows | Worst ATOM |
|---|---:|---:|---:|---:|
| U8 | +52.82% | -41.90% | 69.57% | -56.60% |
| U9_FIL | +29.44% | -37.69% | 73.91% | -45.17% |
| U9_HBAR | +55.41% | -32.77% | 78.26% | -42.56% |
| U10_FIL_HBAR | +131.71% | -37.69% | 91.30% | -45.17% |

Recent rolling-year contrast is particularly notable:

For 2025-09-01 -> 2026-08-31:
- U8 median: -41.90%
- U10 median: +45.16%
- U8 ATOM: -37.09%
- U10 ATOM: +45.77%

This recent-window result is opened-history diagnostic evidence, not untouched validation.

## Rolling 24-month windows

11 monthly-start windows.

| Universe | Median of window medians | Worst window median | Positive windows | Worst ATOM |
|---|---:|---:|---:|---:|
| U8 | +108.99% | -16.92% | 90.91% | -26.89% |
| U9_FIL | +47.72% | -41.28% | 63.64% | -48.32% |
| U9_HBAR | +189.35% | +7.39% | 100.00% | -5.50% |
| U10_FIL_HBAR | +399.55% | +85.40% | 100.00% | +63.14% |

U10 had:
- 11/11 positive 24-month median windows
- 11/11 positive ATOM-start 24-month windows
- worst 24-month median: +85.40%
- worst 24-month ATOM-start: +63.14%

This improves materially on U8's worst 24-month median (-16.92%) and worst ATOM result (-26.89%).

## Node interaction / synergy

FIL alone is harmful in this sample.

U9_FIL:
- median return falls from +922.31% to +622.63%
- FIL occupancy across the eight-start aggregate: 18.50%
- FIL entries: 17
- FIL exits: 17

The route spends too much time in FIL, consistent with a stale-hold / route-capture problem.

HBAR alone is useful:

U9_HBAR:
- median +1357.01%
- HBAR occupancy: 9.40%
- HBAR entries/exits: 25 / 25

With HBAR present alongside FIL, FIL's role changes sharply:

U10:
- FIL occupancy falls to 4.00%
- FIL entries/exits remain 17 / 17
- HBAR occupancy: 9.13%
- HBAR entries/exits: 24 / 24

Thus FIL is no longer a long-duration holding node. It behaves more like a short bridge.

Observed U10 ATOM-start route:

ATOM -> BNB -> TRX -> TWT -> PEPE -> ATOM -> TWT -> ATOM -> BNB -> TWT -> FIL -> PEPE -> HBAR -> TWT -> ATOM -> FIL -> PEPE -> ATOM -> HBAR -> TWT -> AAVE -> HBAR -> PEPE -> ATOM

New repeated corridors include:
- TWT -> FIL -> PEPE
- ATOM -> FIL -> PEPE
- PEPE -> HBAR -> TWT
- AAVE -> HBAR -> PEPE
- ATOM -> HBAR -> TWT

Interpretation:
HBAR appears to supply exits/routes that prevent FIL from becoming a stale destination. The combined graph is therefore much stronger than the additive expectation from the two U9 tests.

This is evidence for genuine network interaction / complementarity.

## Router sensitivity — U10

| Router | Mature median | ATOM-start | Median DD |
|---|---:|---:|---:|
| strongest (canonical) | +2249.75% | +2142.74% | -71.04% |
| weakest | +2930.65% | +2567.76% | -71.04% |
| skip conflicts | +1665.01% | +1665.89% | -72.90% |
| deterministic first | +1653.83% | +1545.15% | -59.67% |

Every tested router remains strongly positive.

Do not change the live router to the visible best row. The useful conclusion is that the U10 result survives large router perturbations better than the earlier broad U20 topology.

## Cost sensitivity — U10

| Transition cost | Mature median | ATOM-start |
|---|---:|---:|
| 0.10% | +2249.75% | +2142.74% |
| 0.50% | +2042.62% | +1945.05% |
| 1.00% | +1808.20% | +1721.30% |

The result remains large even at an intentionally harsh 1% modeled cost per transition.

## Main interpretation

1. Adding nodes indiscriminately is not supported.
2. FIL alone degrades U8.
3. HBAR alone improves U8.
4. FIL + HBAR together produce a much stronger graph than either single-node result suggests.
5. The mechanism is consistent with topology complementarity: HBAR changes FIL from a stale holding destination into a bridge.
6. U10 also improves long-horizon robustness: all 11 available 24-month windows are positive, with a +85.40% worst median result.
7. Full-span max drawdown remains severe (~-71%), so the enormous terminal return does not imply a low-risk strategy.
8. Router and fee perturbations do not destroy the U10 result.

## Critical limitation

FIL and HBAR were selected after earlier opened-history diagnostics identified them as interesting structural nodes. Therefore this is HYPOTHESIS_GENERATION, not independent validation.

The result is too large to ignore but not clean enough to promote U10 to live.

## Next research gate

Keep production/paper-live U8 unchanged.

Preregister a causal walk-forward U10 admission rule based only on:
- new niche contribution;
- relative-volatility complementarity;
- low common-factor concentration;
- stale-hold prevention / route-exit availability.

Then test whether FIL/HBAR would have been admitted before the strong future intervals without using future returns.

No production/live files changed.
