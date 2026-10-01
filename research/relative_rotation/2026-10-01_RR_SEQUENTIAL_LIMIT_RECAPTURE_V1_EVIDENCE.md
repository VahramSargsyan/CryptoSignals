# RR SEQUENTIAL LIMIT RECAPTURE V1 — EVIDENCE

Date: 2026-10-01
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Research question

Can the 3% reversal used to confirm a Relative Rotation signal be mechanically recaptured through sequential spot limit orders without changing the RR route?

Proposed sequence:

1. RR confirms at the daily close.
2. At signal availability (next 00:00 UTC / 04:00 Yerevan), place source-asset limit SELL.
3. The destination BUY order may be placed only after the source SELL has actually filled.
4. Use hourly high/low wicks to test whether each target price existed.
5. Same-hour sell+buy is not credited because 1H candles cannot prove intrahour ordering.
6. Acceptance hypothesis: ideally 100% source-sell then destination-buy execution.

## Runtime identity

### Primary feasibility run

- Branch: `research-run/sequential-limit-recapture-v1`
- GitHub Actions run: `36847550260`
- Source commit: `0c034fdada7e58a28e3991d2dd0ac2a4b6b9ae07`
- Artifact ID: `11154830238`
- Artifact SHA256: `86840dd62578e60c66a8a766b908440caf016bdf4b85dd26d8bc020204e9f991`

### Long-horizon / path-integrity run

- GitHub Actions run: `36848310267`
- Source commit: `3e257d9705a6d166569b16fe65fa4d3a348e304e`
- Artifact ID: `11154741909`
- Artifact SHA256: `d7c9da10769ad679a1daef3a392764eae6c3b72732573cfedca92a143528a4a9`

TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Current universe

Current production/research U10:

`TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR`

LINK is not currently in U10. LINK->PEPE was the user’s conceptual example only.

## Target constructions

### EXTREME_REPLAY

Use the source and destination daily closing prices from the exact pair-ratio extreme preceding confirmation.

This replays the full observed reversal back to the extreme.

Because confirmation can overshoot the 3% threshold, this construction often attempts to recover more than 3%.

### EXACT_3PCT_LOG — primary interpretation

For a direct confirmed route, construct source SELL and destination BUY targets on the log-price line between:

- confirmation prices;
- prior extreme prices;

such that their ratio restores exactly the 3% confirmation threshold geometry.

For HIGH reversal:

`target exchange-rate factor = 1 / (1 - 0.03)`

For LOW reversal:

`target exchange-rate factor = 1 + 0.03`

This preserves the pair geometry instead of naively subtracting 3 percentage points from dislocation.

## Ordering / fill assumptions

- SELL target fills if hourly HIGH >= sell limit.
- BUY target fills if hourly LOW <= buy limit.
- BUY cannot be credited in the same 1H candle as SELL.
- BUY eligibility starts from the next hourly candle.
- Limit fill is credited at the target price, not with favorable price improvement.
- This does not model queue priority, partial fills, spread, or exchange outages.

Therefore results are still optimistic regarding guaranteed full execution, despite conservative order sequencing.

## Primary 7-day feasibility

### All executable current-U10 source/date routes

| Variant | N | 24h complete | 48h | 72h | 5d | 7d | Sell filled 7d |
|---|---:|---:|---:|---:|---:|---:|---:|
| EXTREME_REPLAY | 2550 | 22.4% | 33.2% | 39.8% | 49.1% | 54.9% | 79.8% |
| EXACT_3PCT_LOG | 2081 | 31.0% | 44.4% | 52.2% | 61.3% | **67.0%** | 85.2% |

For direct confirmed routes, EXACT_3PCT_LOG applies to 2081 routes.

### Path-visited direct confirmed routes

28 unique direct route episodes were visited by the historical current-U10 path family.

| Horizon | Complete EXACT_3PCT |
|---:|---:|
| 24h | 46.4% |
| 48h | 53.6% |
| 72h | 67.9% |
| 5d | 71.4% |
| 7d | **71.4%** |

Source SELL target filled within 7 days in only **82.1%** of these cases.

Thus same-hour minute-level resolution cannot rescue the 100% hypothesis: some SELL prices were never revisited during the week.

## Economic target if filled

For path-visited direct routes:

- EXACT_3PCT_LOG median gross exchange-rate improvement vs ordinary next-open execution: **+3.09%**
- median improvement vs the current research engine’s one-cost transition convention after charging two sequential 0.1% spot fees: **+2.99%**

So the economic arithmetic works when both orders fill.

The failure is execution reliability, not the target-return calculation.

EXTREME_REPLAY attempts a larger median improvement:
- +6.05% gross for path-visited direct routes

but completes within 7 days in only:
- **50.0%**

This demonstrates the expected reward/fill-probability tradeoff.

## Extended waiting horizons

A common cohort of 25 historical direct path routes has a fully observable 90-day future.

On that same cohort:

| Horizon | SELL filled | Complete SELL->BUY |
|---:|---:|---:|
| 7d | 80% | 72% |
| 14d | 84% | 76% |
| 30d | 84% | 76% |
| 60d | 84% | 80% |
| 90d | **84%** | **80%** |

Waiting longer does not converge toward 100%.

At 90 days:
- 4/25 source SELL targets were never reached;
- 1 additional case sold the source but never reached the destination BUY target;
- only 20/25 completed.

Therefore:

`EXACT_3PCT_SEQUENTIAL_LIMIT_100_PERCENT_FILL = REJECTED`

under the tested historical U10 path.

## Path integrity versus next original RR signal

For 28 unique direct route / next-signal cases:

- median time until next original RR signal: **528h = 22.0 days**
- 25th percentile: **306h = 12.75 days**
- source SELL filled before next signal: **85.7%**
- full SELL->BUY recapture completed before next original signal: **75.0%**
- source sold but destination still not bought before next RR signal: **3 cases**

Therefore 7/28 historical cases would fail to complete the recapture before the next original strategy decision.

Once that happens, the proposal is no longer a pure execution-price optimization. It changes the actual RR path / state.

Example:

- HBAR -> PEPE signal available 2023-11-26
- next original RR signal arrived after 264h (~11 days)
- exact-3% recapture did eventually complete after 925h (~38.5 days)

Waiting for the target would therefore skip or alter the original next RR decision.

## Notable failed long-horizon cases

Among the 25 routes observable for 90 days, five did not complete:

- FIL -> HBAR, 2024-11-06: SELL filled quickly; BUY target did not complete.
- TWT -> BNB, 2024-12-08: source SELL target not reached.
- FIL -> PEPE, 2025-11-09: source SELL target not reached.
- PEPE -> AVAX, 2026-01-06: source SELL target not reached.
- TWT -> AAVE, 2026-05-19: source SELL target not reached.

These are not same-hour ordering ambiguities.

## Known XRP fork example

For the harmful historical TRX -> XRP signal dated 2024-01-14:

EXACT_3PCT targets were approximately:

- TRX SELL: **0.114229 USDT**
- XRP BUY: **0.574706 USDT**

Historical 1H feasibility:

- source sell target reached after ~13h;
- destination buy target reached in correct order after ~15h total;
- gross improvement vs next-open exchange ratio: about **+2.99%**.

Thus this execution method would have improved the XRP entry price, but it would not have prevented the harmful XRP route itself.

This distinction matters: execution improvement and route selection are separate problems.

## DDG limitation

Current path had 31 unique visited route episodes, of which:
- 28 were direct confirmed routes suitable for EXACT_3PCT_LOG;
- 3 involved DDG override behavior.

Across all executable routes, EXACT_3PCT was applicable to ~81.6% because DDG can redirect to an ARMED relation rather than the pair whose 3% reversal produced the confirmation.

Therefore a universal 3%-recapture rule does not map cleanly onto all DDG transitions.

## Decision

The literal proposal is economically attractive when filled, but fails the user’s required execution condition.

Rejected as a universal pure execution optimization:

`WAIT_FOR_EXACT_3PCT_WITH_SELL_FIRST_THEN_BUY = NOT_ACCEPTED`

Reasons:

1. only 71.4% of historical direct path routes complete within 7 days;
2. only 80% of the fully observable common cohort complete even within 90 days;
3. 16% of that 90-day cohort never reaches the source sell target;
4. 25% do not complete before the next original RR signal;
5. DDG overrides do not always have a directly corresponding confirmed 3% effective route;
6. real exchange fill certainty would be lower than wick-touch certainty because order-book queue / partial-fill risk is not modeled.

## Next admissible hypothesis

A materially different execution architecture could still be tested:

`LIMIT_ATTEMPT + TIMEOUT + MARKET_FALLBACK`

Example research family:
- attempt favorable limit for fixed 6h / 12h / 24h;
- if source SELL does not fill, execute source at market;
- after source sale, attempt destination BUY limit for a fixed causal window;
- if destination BUY does not fill, buy at market;
- require route completion before any later RR signal;
- compare actual full path return, drawdown, time in USDT, and execution improvement.

This would guarantee route completion through fallback but would no longer guarantee recapturing the full 3% on every rotation.

It requires a separate preregistered stress test.

## Production boundary

No production/live/paper/Telegram/exchange/order behavior changed.

## No-repeat

Do not rerun the same exact sequential exact-3% target on the same history.

A repeat requires:
- a different execution policy such as timeout/fallback;
- minute-level validation only for identified same-hour ambiguous cases;
- new forward signals;
- changed exchange fees/order mechanics.
