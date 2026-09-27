# U9 Meme-Node Substitution v1 — preregistration

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Status: HYPOTHESIS_GENERATION / NO LIVE CHANGE

## Question

Is PEPE structurally special in the cleaner rotation graph, or can the MEME node be replaced by a more mature meme asset without losing the graph effect?

## Fixed non-meme core

ATOM, TWT, BNB, TRX, AAVE, LINK, FIL, HBAR

## Fixed universes

- U8_NO_MEME = core only
- U9_PEPE = core + PEPE
- U9_DOGE = core + DOGE
- U9_SHIB = core + SHIB
- U9_BONK = core + BONK

No other membership changes are allowed in this test.

## Frozen signal/router

- Binance Spot 1D fully closed candles
- rolling median 180d
- ARM 15%
- reversal confirmation 3%
- strongest confirmed max-dislocation router
- next-open execution
- transition cost 0.1%

## Fair comparison

Primary comparison window:
- download all candidate assets through 2026-09-26;
- find the latest first available daily candle across all assets including BONK;
- require 180 common daily rows of warm-up;
- primary common mature start is the 180th common row;
- evaluate all five universes from that exact start through 2026-09-26.

This avoids giving PEPE/DOGE/SHIB extra history that BONK cannot have.

Secondary fixed windows:
- latest 1Y: 2025-09-27 -> 2026-09-26
- latest 2Y: 2024-09-27 -> 2026-09-26, only if all assets have adequate data
- rolling 12m and 24m windows from common mature start where available.

Cross-universe median uses the exact same eight non-meme starters:
ATOM, TWT, BNB, TRX, AAVE, LINK, FIL, HBAR.

ATOM-start is reported separately.
Meme-start is reported separately but is not used in the fair cross-universe median.

## Bull-run confound control

For each meme candidate separately:
1. measure its strongest low-to-high 90d drawup over its available Binance history;
2. flag PRIMARY explosive if 90d drawup >= +200% or 180d drawup >= +400%;
3. keep the signal/router path unchanged;
4. neutralize positive daily equity gains only while the strategy is actually holding that meme token during its single strongest 90d drawup interval;
5. report raw and own-bull-neutralized performance.

This prevents DOGE/SHIB/BONK from winning merely because one of them also had an exceptional historical price explosion.

## Diagnostics

For each universe report:
- common-mature median return across 8 common starters;
- ATOM-start;
- max drawdown;
- latest 1Y and 2Y;
- rolling 12m and 24m worst windows;
- transitions/conflicts;
- meme occupancy;
- meme entries/exits;
- own-bull-neutralized result;
- route from ATOM.

## Interpretation guardrail

PEPE remains mandatory in the user's real portfolio regardless of this research result. This experiment tests the mechanism, not an instruction to sell PEPE.

No live/paper-live universe change is authorized.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
