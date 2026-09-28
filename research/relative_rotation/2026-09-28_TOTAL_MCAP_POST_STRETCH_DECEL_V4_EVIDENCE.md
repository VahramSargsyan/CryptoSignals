# TOTAL MARKET CAP POST-STRETCH DECELERATION V4 — EVIDENCE

Date: 2026-09-28  
Workflow mode: STRESS_TEST_ONLY  
Production/live changes: NONE

## Runtime identity

- Source: `data/market/cryptocap_total_d1.csv`
- Symbol: `CRYPTOCAP:TOTAL`
- Provider: TradingView
- Range: 2022-11-29 through 2026-09-26
- Rows: 1,398
- GitHub Actions run: `36471645952`
- Source commit: `5e56df6f246de5f03ed9e60fc3b77e3a911a7aee`
- Artifact ID: `10991716670`
- Artifact SHA256: `c0ff7fa889c6d08b9b88bc1f8fef3a8c73c8ba852455ba72dcab2a310d5f5de7`
- TEST_LEVEL: `GITHUB_ACTIONS_REPOSITORY_CACHE_STRESS_TEST`

No external market data was used.

## Question

After TOTAL is already stretched above SMA100, does the first subsequent day when BOTH:
- TOTAL 5d momentum decelerates, and
- SMA25 5d speed decelerates

identify a useful turning-point zone?

Anchor stretches were preregistered at +15% and +20% above SMA100.

## +15% anchor

Episodes: **8**

Original anchor outcome:
- RETURN_FIRST: **0.0%**
- BREAKOUT_FIRST: **100.0%**

First post-stretch DUAL_DECELERATION:
- found in **8/8** episodes
- median delay from trigger: **12 days**
- RETURN_FIRST after deceleration: **37.5%**
- BREAKOUT_FIRST after deceleration: **62.5%**
- median gap at deceleration: **+16.90%**
- median further extension after deceleration: **+7.57 pp**
- median time to half-gap after deceleration: **25 days**
- median time to SMA100 touch: **64 days**
- a new running gap high still occurred before RETURN50 in **62.5%** of cases

Interpretation:
The first joint deceleration improved the return-first share relative to the raw +15% trigger, but continuation still occurred first in most cases. It is not a reliable turning-point marker.

## +20% anchor

Episodes: **8**

Original anchor outcome:
- RETURN_FIRST: **50.0%**
- BREAKOUT_FIRST: **50.0%**

First post-stretch DUAL_DECELERATION:
- found in **7/8** episodes
- median delay from trigger: **8 days**
- RETURN_FIRST after deceleration: **42.9%**
- BREAKOUT_FIRST after deceleration: **57.1%**
- median gap at deceleration: **+17.57%**
- median further extension after deceleration: **+6.22 pp**
- median time to half-gap after deceleration: **24.5 days**
- median time to SMA100 touch: **66 days**
- a new running gap high still occurred before RETURN50 in **57.1%** of deceleration cases

Interpretation:
At the +20% anchor, joint deceleration did not improve the return/continuation split. It slightly leaned continuation-first.

## Important negative result

`DUAL_DECELERATION != turning point`

Historically, the first joint slowdown often occurred while the broader trend still had enough persistence to make another extension.

This directly rejects the simple version of the user's hypothesis:
- price momentum slows
- SMA25 speed slows
- therefore the market is now returning

The data do not support that rule.

## Small exploratory clue — NOT validated

Among the V4 deceleration events, only two unique historical deceleration dates had BOTH:
- TOTAL 5d momentum still > 0
- SMA25 5d slope still > 0
while both accelerations had turned negative.

Those dates were:
- 2024-11-27
- 2025-07-23

Both were RETURN_FIRST under V4 semantics.

The same two dates appear under both +15% and +20% anchors, so they must not be counted as four independent observations.

This is only a **2-event clue**, not evidence for a rule.

A future independent test could distinguish:
- soft deceleration: momentum levels remain positive while acceleration turns negative
- hard deceleration: TOTAL 5d momentum is already <= 0

No such production rule is authorized here.

## Current 2026 stretch

From the verified episode output:

### +15% anchor
- trigger: **2026-08-21**
- first post-stretch DUAL_DECELERATION: **2026-08-30**
- V4 outcome from that deceleration: **BREAKOUT_FIRST**

### +20% anchor
- trigger: **2026-09-03**
- first post-stretch DUAL_DECELERATION: **2026-09-11**
- V4 outcome from that deceleration: **BREAKOUT_FIRST**

Thus the current cached cycle itself is an example where the first joint deceleration did **not** mark the final return point.

Latest cached 2026-09-26 values from V3:
- TOTAL vs SMA100: **+21.90%**
- TOTAL 5d momentum: **-1.52%**
- previous 5d TOTAL momentum: **+12.95%**
- price momentum change: **-14.47 pp**
- SMA25 5d slope: **+1.98%**
- previous SMA25 5d slope: **+0.87%**
- SMA25 speed change: **+1.10 pp**

Latest state is therefore MIXED:
- TOTAL itself has slowed sharply;
- SMA25 is still accelerating.

## Known research-script issue

The V4 workflow's `current.csv` block was empty because its reporting filter compares stored percentage anchors (15/20) with decimal anchor constants (0.15/0.20).

This does **not** affect:
- episode detection,
- V4 summary,
- per-episode results,
- the conclusions above.

Current-episode facts above were taken from the verified `episodes.csv` / workflow log rows.

Because workflow mode is STRESS_TEST_ONLY, this reporting bug is documented but not patched in this pass.

## Conclusion

V4 does not provide a sufficiently useful standalone return/breakout discriminator.

What remains plausible but unvalidated is a narrower concept:

`positive trend + decelerating acceleration`

rather than generic joint deceleration.

The present sample for that narrower condition is only two unique events, so further tuning on the same 2022-2026 cache would carry a high overfitting risk.

## Boundary

No paper/live, Telegram, allocation, execution, or universe behavior changed.

TEST_LEVEL: GITHUB_ACTIONS_REPOSITORY_CACHE_STRESS_TEST
