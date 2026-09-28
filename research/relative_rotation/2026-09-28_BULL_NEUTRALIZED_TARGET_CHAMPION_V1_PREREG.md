# Bull-Neutralized Target Champion v1 — preregistration

Date: 2026-09-28
Mode: STRESS_TEST_ONLY

## Why this second gate exists

The exhaustive raw-profit search found:
RAW CHAMPION = TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, HBAR
with about +6921% mature median.

But simultaneous neutralization of all preregistered PRIMARY explosive-member strongest 90d intervals reduced it to about +464%.

The user has explicitly chosen the conservative interpretation: historical explosive rallies should not be allowed to define the target strategy.

Therefore no migration target is accepted from RAW return alone.

## Search space

Same fixed 15-token pool.
Same U6..U15 exhaustive space.
27,824 universes.
No mandatory assets.

## Frozen PRIMARY explosive rule

Within strategy-common history, token is PRIMARY explosive if:
- strongest 90d low-to-high drawup >= +200%, OR
- strongest 180d low-to-high drawup >= +400%.

The threshold is unchanged from prior audits.

For every PRIMARY token, freeze its single strongest 90d interval before universe ranking.

## Conservative P&L metric

Routes/signals are unchanged.

During a PRIMARY token's strongest frozen 90d interval:
- if the strategy is holding that token and the daily strategy-equity ratio is positive (>1),
- clip that day's ratio to 1.0.

Apply simultaneously to every PRIMARY member of each universe.

BULL_NEUTRALIZED_MATURE_MEDIAN:
median terminal return across every member starting asset, mature 2023-10-31 -> 2026-09-26.

## Selection

Exhaustively rank all 27,824 universes by BULL_NEUTRALIZED_MATURE_MEDIAN.

Freeze shortlist:
- top 100 by neutralized mature median;
- best 5 per size U6..U15;
- current U10;
- raw champion from prior run;
- robust target from prior run;
deduplicate.

For shortlist evaluate raw:
- latest 1Y median
- latest 2Y median
- rolling 12m positive rate and worst return
- rolling 24m positive rate and worst return
- mature max drawdown

Eligibility:
- latest 1Y > 0
- latest 2Y > 0
- rolling 12m positive rate >= 60%
- rolling 24m positive rate >= 80%

CONSERVATIVE_TARGET_CHAMPION:
eligible universe with highest BULL_NEUTRALIZED_MATURE_MEDIAN.

Tie-break:
1. higher latest 2Y
2. higher latest 1Y
3. smaller size
4. lexicographic key

## Endpoint gate

Compare CONSERVATIVE_TARGET_CHAMPION and current U10 on trailing 365d windows ending:
2026-05-31, 2026-06-30, 2026-07-31, 2026-08-31, 2026-09-26.

Report; do not tune selection from these endpoint results.

## Migration map

Compare current U10:
ATOM,TWT,PEPE,BNB,SOL,TRX,AAVE,LINK,FIL,HBAR

against CONSERVATIVE_TARGET_CHAMPION:
- KEEP
- EXIT
- ADD

This map, not universe size, defines the gradual transition.

No live membership change in this research run.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
