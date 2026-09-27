# U10 Last-Year vs U8 Universe Distribution v1 — Evidence

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Branch: research/u10-last-year-vs-u8-universes-v1
GitHub Actions run: 36327701598
Source commit: 87b07d494969baba8020dbb4c8fde5f5d9b7e9c9
Result: PASS
TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST

## Research question

Should the research U10 = canonical U8 + FIL + HBAR be investigated as a stronger relative-rotation universe than canonical U8?

No live promotion was performed.

## Universes

Canonical U8:

ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK

Research U10:

ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

## Frozen mechanics

- Binance Spot 1D closed candles
- rolling median 180 days
- ARM 15%
- reversal confirmation 3%
- strongest confirmed max-dislocation router
- next-open execution
- 0.1% modeled transition cost

## Latest fully closed one-year window

2025-09-27 -> 2026-09-26

| Metric | Canonical U8 | Research U10 |
|---|---:|---:|
| Median return across start assets | -24.10% | +90.21% |
| ATOM-start return | -14.58% | +116.90% |
| Median max drawdown | -71.50% | -53.73% |
| Median transitions | 6.0 | 9.0 |
| Positive start-assets | 0/8 | 7/10 |

U10 ATOM route:

ATOM -> FIL -> PEPE -> ATOM -> HBAR -> TWT -> AAVE -> HBAR -> PEPE -> ATOM

ATOM-start transitions: 9

ATOM-start max drawdown: -53.73%

FIL occupancy on U10 ATOM route: 5.48% of daily holdings.
HBAR occupancy on U10 ATOM route: 9.04% of daily holdings.

## Exhaustive U8 comparison

All 792 possible U8 universes containing mandatory ATOM, TWT and PEPE were tested.

- canonical U8 median-return rank: 706 / 792
- canonical U8 ATOM-start rank: 551 / 792
- median of the 792 U8 median returns: +4.52%
- positive U8-universe rate: 55.05%
- U10 median-return percentile-equivalent versus the 792 U8s: 97.98%
- U10 ATOM-start percentile-equivalent versus the 792 U8s: 100.00%

This does not prove U10 is universally superior. FIL and HBAR were specifically proposed additions and therefore residual selection leakage remains a research concern.

## Two-year context

Window:

2024-09-27 -> 2026-09-26

| Metric | Canonical U8 | Research U10 |
|---|---:|---:|
| Median return | +256.13% | +718.68% |
| ATOM-start return | +252.17% | +709.46% |
| Median max drawdown | -71.23% | -53.73% |
| Median transitions | 15.0 | 17.0 |

U10 ATOM route over two years:

ATOM -> BNB -> TWT -> FIL -> PEPE -> HBAR -> TWT -> ATOM -> FIL -> PEPE -> ATOM -> HBAR -> TWT -> AAVE -> HBAR -> PEPE -> ATOM

## Interpretation

On the latest one-year and two-year windows, research U10 materially outperformed canonical U8 under the same frozen mechanics and showed a smaller median drawdown.

This is strong research evidence, but it is not an automatic live-promotion decision because:
- FIL/HBAR selection may contain historical selection leakage;
- longer-path principal-risk behavior still needs separate accounting;
- live forward observation is not yet equivalent to historical backtest evidence.

## Residual risks

- historical performance does not establish future performance;
- U10 was not selected blindly from an untouched universe;
- 0.1% modeled transition cost does not separately model additional spread/slippage;
- no automatic live promotion is authorized by this result.
