# Relative Rotation Universe Scale Stress Test v1 — Evidence

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Branch: research/relative-rotation-universe-scale-v1
GitHub Actions run: 36315870382
Result: PASS

## Scope

Canonical live graph remains unchanged at 8 assets / 28 pairs. This experiment used the current live-style conflict router: strongest confirmed max-dislocation, with the frozen 180d / 15% ARM / 3% reversal / next-open / 0.1% transition-cost rules.

Fixed universe sizes:

- U8: 8 assets / 28 pairs
- U20: 20 assets / 190 pairs
- U30: 30 assets / 435 pairs
- U40: 40 assets / 780 pairs

EOSUSDT was found stale after 2025-05-26 before any U40 result existed. The preregistered U40 run was therefore skipped. EOS was replaced by LDO solely for current-data availability and the U40 result was then generated.

## Key results

| Window | U8 | U20 | U30 | U40 |
|---|---:|---:|---:|---:|
| 2023-10-31 -> 2024-04-27 | +374.23% | +50.47% | +32.42% | +32.57% |
| 2024-04-28 -> 2024-10-24 | -34.46% | -34.63% | +5.48% | -27.81% |
| 2024-10-25 -> 2025-04-22 | +93.67% | +65.00% | -8.03% | -3.23% |
| 2025-04-23 -> 2025-10-19 | +20.08% | +102.83% | +20.55% | +39.28% |
| 2025-10-20 -> 2026-03-28 | -47.05% | -31.84% | -20.96% | -20.89% |
| 2026-03-29 -> 2026-09-26 (opened diagnostic) | +103.36% | +45.27% | +46.84% | +15.52% |
| 2025-03-29 -> 2026-03-28 highlighted 1Y | +43.82% | +205.86% | +22.71% | +31.53% |

Highlighted one-year median max drawdown:

- U8: -61.57%
- U20: -47.70%
- U30: -63.58%
- U40: -63.58%

Highlighted one-year ATOM-start return:

- U8: +43.47%
- U20: +137.74%
- U30: +27.28%
- U40: +35.84%

## ATOM highlighted routes

- U8: ATOM -> PEPE -> TRX -> LINK -> TWT -> ATOM -> PEPE -> ATOM -> TWT
- U20: ATOM -> UNI -> LTC -> NEAR -> TWT -> ATOM -> FIL -> PEPE -> ATOM -> ADA -> TWT
- U30: ATOM -> RUNE -> OP -> TWT -> ATOM -> FIL -> PEPE -> APE
- U40: ATOM -> RUNE -> THETA -> TWT -> ATOM -> FIL -> PEPE -> APE

## Strong chain-effect example

In 2025-04-23 -> 2025-10-19, ATOM-start performance changed from -26.79% in U8 to +101.26% in U20. The U20 route was ATOM -> NEAR -> UNI -> LTC -> NEAR -> TWT -> ATOM. This directly supports the hypothesis that nodes that are not necessarily strong as isolated neighbors can become valuable bridge nodes inside a routing chain.

## Counterexample

In the already-opened 2026-03-29 -> 2026-09-26 period, U8 median return was +103.36%, while U20/U30/U40 were +45.27%/+46.84%/+15.52%. More nodes therefore do not monotonically improve the graph. New nodes can also steal routing priority from a better established path.

## Reproduction note

The U8 highlighted 1Y control reproduced at +43.82% versus the earlier approximately +41.6% reference, within the historical graph-intelligence tolerance of +/-5 percentage points. The opened 2026 control reproduced exactly at +103.36%. Some older sequential tables used a causal pair-specific percentile conflict router, whereas this test intentionally uses the current live-style strongest max-dislocation router, so not every old sequential figure is expected to match.

## Decision

- Do not expand the live 8-token monitor yet.
- U20 is a high-priority research candidate, not a production winner.
- U30 and U40 demonstrate topology pollution risk.
- Next research step: attribution/ablation of the 12 U20 additions plus NEW_NODE_PROBATION, without tuning the 180/15/3 signal.

TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST