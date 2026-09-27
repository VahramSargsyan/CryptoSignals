# U10 Last-Year vs Mandatory ATOM/TWT/PEPE U8 Distribution v1 — Evidence

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Branch: research/u10-last-year-vs-u8-universes-v1
Corrected GitHub Actions run: 36327999980
Result: PASS
TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST

## Method correction

An earlier run compared the U10 median over ten possible starting assets against U8 medians over eight starting assets. That was not apples-to-apples.

The corrected run uses the same original eight starts for U8 and U10:
ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK.

ATOM-start results were unaffected by this correction.

## Fixed one-year window

2025-09-27 -> 2026-09-26

Frozen strategy:
- Binance Spot 1D closed candles
- rolling median 180d
- ARM 15%
- reversal confirmation 3%
- strongest confirmed max-dislocation router
- next-open execution
- 0.1% transition cost

## Exhaustive comparator universe

Mandatory in every U8:
- ATOM
- TWT
- PEPE

Five more assets are chosen from:
BNB, SOL, TRX, AAVE, LINK, AVAX, FIL, ETH, ALGO, ADA, XRP, HBAR.

Total U8 combinations:
C(12,5) = 792.

Conclusions use the exhaustive distribution. A deterministic 12-set random sample is shown only for readability.

## One-year result

| Metric | Canonical U8 | U10 = U8 + FIL + HBAR |
|---|---:|---:|
| Median return across original 8 starts | -24.10% | +51.44% |
| ATOM-start return | -14.58% | +116.90% |
| Median max drawdown | -71.50% | -56.79% |
| Worst start return | -33.56% | -7.28% |
| Best start return | -11.41% | +124.95% |
| Positive original starts | 0/8 | 5/8 |
| Median transitions | 6 | 9 |
| Median conflict count | 1.5 | 2 |

Canonical U8:
- median rank among 792: 706 / 792
- median percentile: 10.98%
- ATOM-start rank: 551 / 792
- ATOM percentile: 30.56%

U10 compared with the 792 U8 distribution:
- median-return percentile-equivalent: 89.02%
- approximately 87 of 792 U8s are at or above that region; U10 is roughly top-11% equivalent by median return
- ATOM-start percentile-equivalent: 100%
- no mandatory-ATOM/TWT/PEPE U8 produced an ATOM-start return above U10's +116.90%; the best U8 tied it

Distribution of 792 U8 median returns:
- median: +4.52%
- 25th percentile: -19.60%
- 75th percentile: +30.25%
- 90th percentile: +54.57%
- 95th percentile: +80.91%
- positive-median U8s: 55.05%

The canonical U8 was unusually weak in this particular last-year regime.

## Best U8 in hindsight — not a selector

ATOM, TWT, PEPE, AAVE, AVAX, FIL, ALGO, HBAR

One-year:
- median +101.72%
- ATOM +116.90%
- median max DD -53.73%
- six of eight starts positive

This is hindsight-only and is not a live candidate from this test.

Notably, its ATOM route is compatible with the same FIL/HBAR bridge structure that appears in U10.

## Canonical U8 ATOM route — one year

ATOM -> PEPE -> ATOM -> TWT -> AAVE -> TWT -> ATOM

ATOM-start:
- return -14.58%
- max DD -65.42%
- 6 transitions

## U10 ATOM route — one year

ATOM -> FIL -> PEPE -> ATOM -> HBAR -> TWT -> AAVE -> HBAR -> PEPE -> ATOM

ATOM-start:
- return +116.90%
- max DD -53.73%
- 9 transitions

ATOM-route occupancy:
- ATOM: 86 days
- TWT: 95
- PEPE: 90
- AAVE: 41
- FIL: 20
- HBAR: 33

FIL occupancy share: 5.48%
HBAR occupancy share: 9.04%

This again supports FIL as a short bridge rather than a stale holding when HBAR is present.

## Two-year context

Window:
2024-09-27 -> 2026-09-26

| Metric | U8 | U10 |
|---|---:|---:|
| Median return | +256.13% | +718.55% |
| ATOM-start | +252.17% | +709.46% |
| Median max DD | -71.23% | -53.73% |
| Worst original-start return | +250.61% | +705.86% |
| Positive original starts | 8/8 | 8/8 |
| Median transitions | 15 | 17 |

U10 therefore outperformed U8 not only on the difficult last year but also over the exact latest two-year horizon.

## Deterministic random examples

Fixed presentation seed: 20260927.

| Random U8 | Median 1Y | ATOM 1Y | Median DD |
|---|---:|---:|---:|
| ATOM TWT PEPE BNB TRX FIL ADA XRP | +29.28% | +46.07% | -58.94% |
| ATOM TWT PEPE SOL AVAX ETH XRP HBAR | -11.15% | -9.71% | -53.41% |
| ATOM TWT PEPE BNB LINK AVAX FIL XRP | +11.61% | +27.09% | -63.49% |
| ATOM TWT PEPE BNB TRX AAVE FIL ETH | +43.82% | +62.43% | -65.04% |
| ATOM TWT PEPE AAVE LINK FIL ALGO XRP | +54.66% | +57.17% | -65.04% |
| ATOM TWT PEPE LINK FIL ALGO XRP HBAR | +54.57% | +57.08% | -53.29% |
| ATOM TWT PEPE BNB AAVE LINK AVAX ALGO | -12.77% | -11.35% | -65.72% |
| ATOM TWT PEPE TRX AAVE AVAX ETH HBAR | +2.75% | +15.40% | -59.86% |
| ATOM TWT PEPE BNB LINK FIL ETH XRP | +6.36% | +14.36% | -64.70% |
| ATOM TWT PEPE BNB SOL LINK ADA XRP | -23.93% | -21.59% | -62.08% |
| ATOM TWT PEPE BNB AVAX FIL XRP HBAR | +46.29% | +68.20% | -51.68% |
| ATOM TWT PEPE BNB SOL AAVE LINK FIL | +12.16% | +59.12% | -67.35% |

The sample illustrates why arbitrary U8 selection is unreliable: results range from materially negative to strongly positive despite all sets containing ATOM/TWT/PEPE.

## Baseline decision

Evidence now supports promoting U10 to NEW_RESEARCH_BASELINE while retaining canonical U8 as the production/paper-live baseline until an explicit live-change decision.

Reasons:
1. U10 beat U8 over the ~34.9-month mature span in the prior preregistered U8+FIL+HBAR test.
2. U10 beat U8 over the exact latest two years.
3. U10 turned the exact latest year from a negative U8 regime into a strongly positive result.
4. U10's latest-year median result is equivalent to the top ~11% of all 792 mandatory-ATOM/TWT/PEPE U8 universes.
5. U10's ATOM-start result matches the maximum observed among all 792 U8s.
6. U10 had materially smaller drawdown in the exact latest one- and two-year windows.
7. FIL/HBAR have an interpretable bridge/topology role rather than merely increasing node count.

## Remaining limitations

- FIL/HBAR were discovered after earlier opened-history diagnostics; selection leakage remains.
- U10 still has severe historical drawdowns.
- The latest-year distribution is a single market regime.
- U10 is not independently future-validated after its discovery.
- No live automatic execution is authorized.

## Operational status

RESEARCH_BASELINE: U10 candidate supported
LIVE/PAPER-LIVE BASELINE: U8 unchanged
