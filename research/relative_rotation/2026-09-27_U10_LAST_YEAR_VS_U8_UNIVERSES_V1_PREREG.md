# U10 Last-Year vs U8 Universe Distribution v1 — preregistration

Date: 2026-09-27
Mode: STRESS_TEST_ONLY

## Question

Should U10 = canonical U8 + FIL + HBAR become the new research baseline rather than canonical U8?

Before any live change, compare U10 and canonical U8 on the latest fully closed one-year period and against the full distribution of all U8 universes that must contain ATOM, TWT and PEPE.

Live monitor remains canonical U8 during this test.

## Fixed assets and pool

Mandatory in every random/exhaustive U8:
- ATOM
- TWT
- PEPE

Candidate pool:
ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, AVAX, FIL, ETH, ALGO, ADA, XRP, HBAR

With 3 mandatory assets and 5 chosen from the remaining 12, this creates exactly 792 possible U8 universes.

Canonical U8:
ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK

U10:
ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

## Evaluation window

Latest fully closed one-year period:
2025-09-27 -> 2026-09-26

No outcome from this window was used to select the random/exhaustive U8s.

## Frozen strategy

- Binance Spot 1D closed candles
- 180d rolling median
- ARM 15%
- reversal confirmation 3%
- strongest confirmed max-dislocation router
- next-open execution
- transition cost 0.1%

## Comparisons

1. Canonical U8 performance over the one-year period.
2. U10 performance over the same period.
3. All 792 mandatory-ATOM/TWT/PEPE U8 universes over the same period.
4. Rank/percentile of canonical U8 among the 792.
5. For U10, compare its result with the distribution of the 792 U8s:
   - percentile-equivalent by median return;
   - percentile-equivalent by ATOM-start return;
   - drawdown comparison.
6. Show deterministic random examples for readability:
   - 12 U8 universes sampled with fixed seed 20260927.
   - These examples are presentation only; conclusions use the exhaustive 792 distribution.
7. Report transitions/conflicts and ATOM route for U8 and U10.
8. Report FIL/HBAR occupancy in U10.
9. Also report two-year window 2024-09-27 -> 2026-09-26 for U8 and U10 as secondary context, because user's decision horizon is 1–3 years.

## Decision guardrail

No automatic live promotion.

A switch from U8 to U10 requires:
- last-year evidence;
- long-cycle evidence;
- topology robustness;
- explicit user decision after reviewing residual selection leakage.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
