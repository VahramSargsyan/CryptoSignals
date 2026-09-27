# Defensive Probation Memory Router V4 — Preregistered Research Plan

Date: 2026-09-27
Workflow mode: STRESS_TEST_ONLY
Status: RESEARCH_ONLY / POST-OOS REDESIGN ATTEMPT 3 / NOT PRODUCTION APPROVED

## User hypothesis

During broad crypto stress, actual capital may move into the low-vol defensive token, but the relative router must keep its own logical state and continue following the 8-node / 28-pair network.

When the shadow router later produces a new confirmed transition, actual capital is allowed to leave defense and follow the router for a fixed 14-day probation period.

If broad recovery confirms during probation, remain with the router.
If broad recovery does not confirm within 14 days, actual capital returns to the defensive token while the shadow router retains its latest logical target.

## Candidate

Reference label:

`DEFENSIVE_LOW_VOL_CRYPTO_PROBATION14_MEMORY_ROUTER_V4`

### Base router

Unchanged:

- assets: ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK;
- 180d rolling median;
- 15% ARM;
- post-ARM extreme tracking;
- 3% reversal confirmation;
- next-open execution;
- 0.1% cost per actual transition.

### Defensive entry

Unchanged from the frozen low-vol candidate:

1. Breadth = count of 8 assets above own causal SMA200.
2. After 3 consecutive closes with breadth <= 3, enter defensive mode at the next daily open.
3. Defensive token = lowest trailing 30d realized-volatility token at the entry signal.
4. Defensive token stays fixed for that defensive episode.
5. Relative router continues in shadow at all times.

### Defensive state

Keep two distinct states:

- `shadow_target` = where the normal relative router logically resides;
- `actual_holding` = where capital is physically held.

Example:

`shadow_target = ATOM`
`actual_holding = TRX`

A defensive move does NOT rewrite the shadow router to TRX.

### Probe / probation entry

While actual capital is defensive:

1. If broad recovery already confirms with breadth >= 5 for 3 consecutive closes, use the original exit: at next open move to current shadow target and return to NORMAL.
2. Otherwise, wait for the first NEW confirmed outbound transition generated from the current shadow target after defensive entry / after the latest failed probation.
3. Execute that shadow transition at the next open.
4. At the same open, actual capital leaves the defensive token and enters the new shadow target.
5. State becomes `PROBATION`.
6. Fixed probation length = **14 daily candles**.
7. This 14-day value is user-proposed before the test. No neighboring values are tested in V4.

### During probation

1. The relative router controls actual capital normally.
2. Any further confirmed shadow transitions are executed at next open.
3. Such transitions do NOT reset the 14-day probation clock.
4. Continue monitoring broad recovery.
5. Broad recovery = breadth >= 5 for 3 consecutive daily closes.
6. If broad recovery confirms before probation expires:
   - state becomes NORMAL;
   - remain in the current shadow/actual router position;
   - no forced trade is required.
7. If 14 probation daily closes complete without broad recovery:
   - at the next daily open move actual capital back to the defensive token;
   - state returns to DEFENSIVE;
   - preserve the current shadow target exactly where the router logically ended;
   - future NEW shadow transitions may trigger another 14-day probation.

No early probation abort is allowed in V4.
No new momentum, volatility, SMA, stop-loss, or graph-ranking filter is added.

## Comparison

Development history, data only through 2026-03-28:

- BASE router;
- ORIGINAL low-vol breadth 3/5 defense;
- V4 probation14 memory router.

Required development views:

- 2025-03-29 -> 2026-03-28 historical year;
- five existing non-overlapping 180d windows;
- seven existing non-overlapping 120d windows.

## Predeclared development gates

Reuse the same gates used for V2/V3.

1. Protection retention:
   in each known weak 180d window, V4 median max drawdown may be no more than 10 percentage points worse than ORIGINAL defense.

2. Opportunity-cost improvement:
   V4 median return must exceed ORIGINAL defense in at least 2 of 3 non-weak 180d windows.

3. Defensive-occupancy improvement:
   V4 time physically held in the defensive token must be lower than ORIGINAL defense in at least 2 of 3 non-weak 180d windows.

4. Churn control:
   V4 median defensive transitions must remain <= 6 per 180d window.

5. No catastrophic regression:
   no ORIGINAL-defense positive 180d window may become a V4 return below -10%.

Passing all gates means only:

`DEVELOPMENT_PASS_FORWARD_CANDIDATE`

It does NOT mean OOS validation.

## Opened-2026 diagnostic replay

After the development result is computed, run the exact same already-frozen V4 rule on:

`2026-03-29 -> 2026-09-26`

This is explicitly:

`POST_HOC_DIAGNOSTIC_REPLAY`

because the V4 idea was designed after seeing the original defense fail on this period.

The replay is used only to answer:

- would the proposed mechanism have reduced the 144-day TRX opportunity cost?
- how many 14-day probes occurred?
- how many probes ended in broad recovery vs fallback?
- return vs BASE and ORIGINAL defense;
- max drawdown vs BASE and ORIGINAL defense;
- time in defensive token;
- time in probation.

The 2026 replay MUST NOT be called OOS, blind, untouched, validation, or promotion evidence.

## Multiple-hypothesis record

Post-untouched redesign attempts:

1. V2 shadow-target SMA200 confirm3 — FAILED.
2. V3 router reactivation — FAILED.
3. V4 probation14 memory router — this test.

No V5 is authorized by this plan.
