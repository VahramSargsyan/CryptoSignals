# Pre-OOS H8 Market Regime Results — 2026-09-25 R1

Status: BACKTEST_EXECUTED_PRE_OOS_REGIME_RESEARCH  
Acceptance decision: NOT ALLOWED YET  
Protected blind period: 2026 (not opened)  
Accepted source commit: `61de61d4e34a997dd15e31ab8d80b851038f3e3b`  
GitHub Actions run: `36115936390`  
Artifact ID: `10854967356`  
Artifact digest: `sha256:5e23e48dfd7a07349fbf625eb63a1e2be476452a52129d585315693975e4c497`

## Verification

- 51 / 51 unit and regression tests passed.
- 13 / 13 tracked symbols downloaded successfully.
- 13 / 13 strategy comparisons completed.
- 1,826 daily regime rows were produced.
- 9,804 event-study rows were regime-tagged.
- 295 closed trades were regime-tagged across V1 and V2.
- Real Binance Spot public data pipeline completed successfully.
- 2026 data remained outside the run.

## Regime definitions were frozen before runtime inspection

H8 used:

- BTC trend: 50D / 200D trailing SMA relationship;
- broad trend: tracked-universe breadth above trailing 50D SMA;
- volatility: BTC 20D realized volatility vs trailing 252-observation median;
- volume participation: fraction of eligible symbols above their own trailing 20D volume mean;
- late-listed assets become eligible only after enough actual history exists.

No future candle is used in the regime at candle T.

## Regime coverage

Across 1,826 daily rows:

| Dimension | Main observed states |
|---|---|
| BTC trend | BULL 851; BEAR 543; TRANSITION 233; UNKNOWN 199 |
| Broad crypto | BEAR 951; BULL 663; SIDEWAYS 163; UNKNOWN 49 |
| Volatility | NORMAL 750; LOW 477; HIGH 328; UNKNOWN 271 |
| Volume participation | LOW 1081; HIGH 498; NORMAL 228; UNKNOWN 19 |

UNKNOWN is retained explicitly instead of silently backfilling insufficient-history periods.

## Strongest signal-level hypothesis: BUY + high volatility

For BUY signals under `VOL_HIGH`:

| Strategy | Horizon | n | Win rate | Avg directional return |
|---|---:|---:|---:|---:|
| VAHRAM_ORIGINAL_V1 | +1 | 70 | 64.29% | +2.43% |
| VAHRAM_ORIGINAL_V1 | +3 | 70 | 67.14% | +3.24% |
| VAHRAM_ORIGINAL_V1 | +7 | 70 | 62.86% | +1.87% |
| VAHRAM_ORIGINAL_V1 | +14 | 69 | 57.97% | +3.12% |
| VAHRAM_TRUE_STOCHRSI_V2 | +1 | 59 | 62.71% | +3.39% |
| VAHRAM_TRUE_STOCHRSI_V2 | +3 | 59 | 77.97% | +5.38% |
| VAHRAM_TRUE_STOCHRSI_V2 | +7 | 59 | 74.58% | +6.97% |
| VAHRAM_TRUE_STOCHRSI_V2 | +14 | 58 | 67.24% | +7.36% |

This is materially stronger than the undifferentiated all-signal averages from H7, especially for V2.

It is still pre-OOS evidence. The bucket is large enough to take seriously, but not to declare the strategy accepted.

## BUY also behaves like a mean-reversion signal in broad weakness

Under `BROAD_BEAR`, BUY average directional return remained positive at all four horizons for both strategies.

V1:

- +1: +0.90%, n=498;
- +3: +1.51%;
- +7: +2.47%;
- +14: +2.76%, n=486.

V2:

- +1: +1.11%, n=436;
- +3: +1.83%;
- +7: +3.03%;
- +14: +3.34%, n=424.

The yearly breakdown is not uniformly positive, however. In particular, some 2022/2023 short-horizon averages were weak or negative. Therefore `BROAD_BEAR` alone is not enough evidence for a regime-specific acceptance rule.

## SELL remains the main warning

No non-UNKNOWN regime bucket with at least 30 observations had positive average SELL directional return at all +1 / +3 / +7 / +14 horizons for either V1 or V2.

A large example is `BROAD_BULL`.

V1 SELL:

- +1: -0.49%, n=600;
- +3: -2.45%;
- +7: -4.23%;
- +14: -6.61%.

V2 SELL:

- +1: -0.40%, n=527;
- +3: -2.24%;
- +7: -3.57%;
- +14: -6.19%.

This strengthens the hypothesis that the historical SELL condition may be a poor directional short/exit signal under the current interpretation.

It does **not** authorize changing `VAHRAM_ORIGINAL_V1` or silently deleting SELL from V2.

## Trading-engine regime observations are still thin

Some `LONG_ONLY_SIGNAL_FLIP_V1` regime buckets look positive, but sample sizes are much smaller than the event study.

Examples:

- V2 + `VOL_HIGH`: 10 trades, 60% win rate, +6.56% average return, profit factor 1.61;
- V2 + `BROAD_BULL`: 18 trades, 66.67% win rate, +3.67% average return, profit factor 1.49;
- V1 + `BTC_TRANSITION`: 22 trades, 63.64% win rate, +7.24% average return, profit factor 1.80.

These are hypothesis-generating samples. They are too small to promote a strategy on their own.

## What H8 changes in our understanding

H7 answered:

> Overall, are the current strategies convincing?

The answer was no: global long-only trading evidence was weak.

H8 adds a more useful answer:

> The BUY side contains a measurable mean-reversion effect that is concentrated in some regimes, while SELL remains structurally suspicious under the tested directional interpretation.

That means the project should not throw away the original strategy. The evidence is pointing toward a regime/direction-specific research branch rather than a global yes/no verdict.

## Next gate

Before any 2026 candle is opened for strategy evaluation:

1. freeze numeric acceptance gates;
2. define minimum sample requirements;
3. define what qualifies as global ACCEPTED vs REGIME_SPECIFIC vs NEEDS_MORE_DATA vs REJECTED;
4. freeze exactly which candidate identities may be evaluated;
5. only then run the held-out 2026 period.

No acceptance status is assigned in this document.
