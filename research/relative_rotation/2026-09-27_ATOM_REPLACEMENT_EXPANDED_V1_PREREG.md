# ATOM Replacement Expanded v1 — preregistration

Date: 2026-09-27
Mode: STRESS_TEST_ONLY

## Goal

Search beyond the original 15-token research pool for a tenth node that can replace ATOM without degrading the current U9_NO_ATOM graph.

No live/paper-live change is authorized.

## Fixed base

U9_NO_ATOM:

TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

## Fixed expanded candidate list

The candidate list is frozen from the previously documented U20/U30/U40 scale universes, excluding:
- ATOM
- assets already present in U9_NO_ATOM

Candidates:

BTC, ETH, XRP, DOGE, ADA, AVAX, DOT, LTC, BCH, NEAR, UNI,
ICP, XLM, ETC, RUNE, CRV, SAND, MANA, OP, ARB, APE,
GALA, AXS, THETA, VET, ALGO, XTZ, CHZ, ENJ, COMP, LDO

No new candidate may be added after outcomes are viewed in this v1.

## Availability gate

A candidate is eligible only if Binance Spot daily data:
- is available through 2026-09-26;
- provides sufficient common daily history for the frozen 180-row warm-up before the mature evaluation;
- has no critical quality issue reported by the repository data loader.

Unavailable or insufficient candidates are reported and not silently replaced.

## Frozen strategy

- Binance Spot 1D fully closed candles
- rolling median 180d
- ARM 15%
- reversal confirmation 3%
- strongest confirmed max-dislocation router
- next-open execution
- transition cost 0.1%

No strategy tuning.

## Fair starters

Every candidate variant is evaluated from the exact same nine base starters:

TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

The candidate itself is not added as a primary starter.

## Fixed windows

- MATURE: 2023-10-31 -> 2026-09-26
- LATEST_2Y: 2024-09-27 -> 2026-09-26
- LATEST_1Y: 2025-09-27 -> 2026-09-26

Also:
- rolling 12m windows
- rolling 24m windows
- five shifted trailing-365d endpoints ending:
  - 2026-05-31
  - 2026-06-30
  - 2026-07-31
  - 2026-08-31
  - 2026-09-26

## Candidate diagnostics

For each eligible candidate report:

- mature / 2Y / 1Y return
- delta versus U9_NO_ATOM
- worst rolling 12m / 24m
- endpoint incremental return versus U9
- occupancy
- entries / exits
- ending-route rate
- strongest 90d / 180d drawup
- PRIMARY explosive flag under the frozen rule
- own-bull-neutralized mature return

## Promotion evidence

A candidate is interesting only if it demonstrates broad incremental value, not one lucky endpoint.

Flag a candidate as BROAD_POSITIVE only if:
- mature delta > 0
- latest-2Y delta >= 0
- latest-1Y delta >= 0
- at least 4 of 5 endpoint increments are >= 0

Flag as STRICT_POSITIVE if:
- all three fixed-window deltas > 0
- all five endpoint increments > 0

These flags are descriptive and do not automatically authorize production promotion.

If no candidate passes BROAD_POSITIVE, U9_NO_ATOM remains the research baseline.

## Guardrails

- This is one-at-a-time marginal testing only.
- No pair/combo search among new candidates in v1.
- No future return claim.
- No live production changes.
- PR #54 remains unmerged while replacement research is unresolved.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
