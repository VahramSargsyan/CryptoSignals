# ATOM OUT Expanded Replacement Stress v2 — Preregistration

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Branch: research/atom-out-replacement-v1
Production changes: NONE

## Why v2 exists

The preregistered v1 screen tested AVAX, ETH, ALGO, ADA and XRP against the ATOM-free U9.

V1 produced AVAX as the formal 5/8 leader, but its temporal robustness was weak:
- rolling 12M improvement: 1/23 windows;
- rolling 24M improvement: 1/11 windows;
- shifted 1Y endpoint improvement: 1/6 endpoints.

Therefore v2 expands the candidate pool instead of promoting AVAX.

## Frozen base

U9 / NOTHING:

TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

## Expanded candidates

DOGE, LTC, BCH, DOT, NEAR, UNI, ETC, XLM, SHIB, OP, ARB, SUI

These candidates are preregistered as a broad Binance-available cohort before the v2 run.

## Frozen mechanics and tests

Identical to ATOM OUT Replacement Stress v1:
- Binance Spot 1D closed candles;
- common research start target 2023-05-05;
- 180d rolling median;
- ARM 15%;
- reversal 3%;
- strongest confirmed max-dislocation router;
- next-open execution;
- 0.1% modeled transition cost;
- same nine common start assets;
- 1Y / 2Y / long mature;
- rolling 12M / 24M;
- max DD;
- occupancy and entries/exits;
- shifted endpoint sensitivity;
- broad-bull neutralization;
- leave-one-neighbor-out dependency.

## Stronger promotion gate

V1 showed that a simple 5-of-8 score can admit a temporally fragile candidate.

For v2 a candidate is considered a robust replacement only if ALL of these temporal conditions are met:
1. latest 1Y median return > U9;
2. latest 2Y median return > U9;
3. rolling 12M improvement rate > 50%;
4. rolling 24M improvement rate > 50%;
5. shifted-endpoint improvement rate >= 50%;

and these safety checks also hold:
6. latest 1Y median max DD is not >3 percentage points worse than U9;
7. bull-neutralized long median return > U9;
8. leave-one-neighbor-out return improvement is positive in >=50% of removals for both 1Y and 2Y.

If no expanded candidate passes all eight, the v2 result is NOTHING / U9.

AVAX remains the v1 challenger but is not reclassified by this post-v1 stronger gate.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
