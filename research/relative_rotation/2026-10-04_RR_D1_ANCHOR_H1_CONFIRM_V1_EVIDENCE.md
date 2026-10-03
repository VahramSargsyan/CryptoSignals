# RR D1-ANCHOR + H1 CONFIRMATION V1 — EVIDENCE

Date: 2026-10-04
Workflow mode: STRESS_TEST_ONLY
Production/live/Telegram/exchange changes: NONE

## Runtime identity

- Branch: `research/rr-d1-anchor-h1-confirm-v1`
- Draft PR: #121
- Successful research run: `37156373766`
- Job: `111300470068`
- Artifact: `rr-d1-anchor-h1-confirm-v1-12`
- Artifact ID: `11285827493`
- Artifact SHA256: `2a29914fbf29a551125b3169d1cb81a5ca3450341a5ae3a6e96e1a073ec06150`
- Fixed as-of: `2026-10-03T20:30:00Z`
- Latest common D1 candle timestamp: `2026-10-02T00:00:00Z`
- Latest common H1 candle timestamp: `2026-10-03T19:00:00Z`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_H1_PATH_DEPENDENT_STRESS_TEST`

## Data validation

D1/H1 daily-close consistency:
- 13 / 13 assets checked;
- 1247 matched D1 closes per asset;
- 0 missing matched closes;
- max relative close error = 0 for every asset.

A raw TWT H1 gap existed at:
- `2023-03-24T13:00:00Z`

It was before the common multi-asset H1 panel start:
- common H1 start = `2023-05-05T18:00:00Z`

Final evaluated common H1 panel:
- 29,930 rows;
- common_gap_count = 0.

No interpolation or synthetic candle was used.

## Engines

### D1_CORE
Canonical:
- D1 MEDIAN180;
- D1 ARM;
- D1 extreme/reversal;
- 3% confirmation;
- DDG 1.5x;
- next D1 open.

### D1_ARM_H1_CONFIRM — PRIMARY
- MEDIAN180 remains D1;
- ARM only on closed D1;
- after ARM, extreme + 3% reversal tracked on closed H1;
- DDG evaluated at H1 confirmation;
- execution next H1 open.

### D1_ANCHOR_FULL_H1 — SECONDARY
- D1 MEDIAN180 anchor;
- ARM/extreme/reversal all allowed on H1;
- DDG + execution on H1.

## Frozen classification

`H1_CONFIRM_REJECTED`

## Full-path result at 0.10% modeled transition cost

### MATURE

D1_CORE:
- median return: **+3518.65%**
- normalized capital: **3618.65**
- median max DD: -64.24%
- median transitions: 23

D1_ARM_H1_CONFIRM:
- median return: **+515.10%**
- normalized capital: **615.10**
- median max DD: -65.78%
- median transitions: 22

D1_ANCHOR_FULL_H1:
- median return: **+95.38%**
- normalized capital: **195.38**
- median max DD: -80.49%
- median transitions: 21

PRIMARY versus D1_CORE:
- improved starts: **0 / 13**
- median final-capital ratio: **0.1684x**
- median return delta: **-3029.30 percentage points**
- median DD delta: -1.54pp

Aggressive full-H1 versus D1_CORE:
- improved starts: **0 / 13**
- median final-capital ratio: **0.0522x**
- median return delta: **-3443.17pp**
- median DD delta: -16.25pp

## LAST_2Y

D1_CORE:
- +2341.87%

D1_ARM_H1_CONFIRM:
- +412.87%

D1_ANCHOR_FULL_H1:
- +80.15%

PRIMARY improved starts:
- 0 / 13.

## LAST_1Y

D1_CORE:
- +165.41%

D1_ARM_H1_CONFIRM:
- +8.88%

D1_ANCHOR_FULL_H1:
- -25.34%

PRIMARY improved starts:
- 0 / 13.

## Timing result

The H1 engines really are earlier.

PRIMARY D1_ARM_H1_CONFIRM:

Pair confirmations:
- D1 pair confirmations: 8872
- matched H1 confirmations: 8163
- match rate: 92.0%
- median lead: **15h**
- p25/p75 lead: 9h / 19h

Effective routes:
- D1 routes: 3429
- same-route H1 matches: 2898
- match rate: 84.5%
- median lead: **15h**
- median execution unit improvement in matched same-route cases: **+1.50%**

Thus:
`H1_IS_EARLIER = TRUE`
and
`MATCHED_SAME_ROUTE_EXECUTION_PRICE_IMPROVES = TRUE`.

But this timing advantage does not survive full path-dependence.

## Intraday confirmation instability

PRIMARY H1 confirmations:
- count: 25,214
- preserved on next D1 close: 31.9%
- preserved within 3 D1 closes: 63.2%
- H1-only within 3 D1: **36.8%**

So more than one-third of H1 confirmations do not become the same canonical D1
confirmation within three daily closes.

## Why full path fails

The primary problem is not transaction count.

PRIMARY actually has slightly FEWER transitions than D1:
- 22 vs 23 median MATURE.

The problem is **route topology changes**.

Example, starting MATURE from LINK:

D1_CORE begins:
- 2023-11-02 LINK -> AVAX
- 2023-11-14 AVAX -> BNB
- 2023-12-30 BNB -> TRX
- ...

PRIMARY H1 begins:
- 2023-11-01 02:00 LINK -> PEPE
- 2023-12-05 03:00 PEPE -> BNB
- 2023-12-28 06:00 BNB -> XRP
- ...

The H1 confirmation does not merely execute the same D1 decision 15 hours
earlier. At many points it confirms a different pair first, resets pair state,
and changes the held asset from which every later RR decision is made.

Therefore:
`EARLIER_CONFIRMATION != SAME_ROUTE_EARLIER_EXECUTION`.

That distinction explains why local execution improvement can coexist with a
much worse full strategy path.

## AAVE / TRX current episode

H1 did NOT discover a RR rotation from TRX into AAVE.

The observed pair confirmations remain in the canonical mean-reversion
direction:
- **AAVE -> TRX**

Examples:

PRIMARY:
- 2026-09-28 02:00 UTC: CONFIRMED AAVE -> TRX
  - max dislocation +67.19%
  - reversal +3.38%
- 2026-09-29 17:00 UTC: CONFIRMED AAVE -> TRX
  - max dislocation +84.54%
  - reversal +3.55%
- 2026-09-30 03:00 UTC: CONFIRMED AAVE -> TRX
  - max dislocation +75.24%
  - reversal +3.26%
- 2026-10-02 19:00 UTC: CONFIRMED AAVE -> TRX
  - max dislocation +97.10%
  - reversal +3.19%

Thus:
`H1_RR_TRX_TO_AAVE = NOT_DETECTED`.

This confirms that AAVE's recent strength is trend-continuation information, not
a faster instance of the RR mean-reversion signal.

## LINK / TRX current timing

Canonical D1 route:
- 2026-09-29 00:00 UTC action time:
  - LINK -> TRX
  - baseline LINK -> ALGO
  - DDG override to TRX
  - strength ratio ~2.078x

The hybrid family emitted LINK -> TRX routes intraday earlier, but also emitted
multiple competing LINK routes on nearby H1 closes.

This illustrates the core problem: H1 timing is useful, but allowing H1 to
choose/reset route state creates route churn/topology divergence.

## Cost robustness — MATURE

At 0.50%:
- D1_CORE +3199.68%
- PRIMARY +463.63%
- FULL_H1 +79.59%

At 1.00%:
- D1_CORE +2838.67%
- PRIMARY +405.42%
- FULL_H1 +61.56%

At 3.00%:
- D1_CORE +1734.14%
- PRIMARY +224.57%
- FULL_H1 +5.25%

The rejection is not a 0.10%-cost artifact.

## Frozen decision-gate results

- mature_median_capital_improves: FAIL
- mature_improved_share_ge_50pct: FAIL
- median_dd_delta_ge_minus_10pp: PASS
- transition_ratio_le_1p5x: PASS
- matched_route_median_lead_gt_0h: PASS
- h1_only_within_3d_lt_50pct: PASS

Final:
`H1_CONFIRM_REJECTED`.

## Research implication

Do not replace canonical D1 confirmation with the tested H1 state machine.

The useful residual signal is narrower:

> when H1 and D1 ultimately choose the SAME route, H1 leads by about 15h and
> the median execution-unit improvement is about +1.50%.

A materially new future test may therefore investigate a
`D1_ROUTE_LOCKED_H1_EXECUTION` mechanism:
- D1 remains authoritative for route identity/state;
- H1 is not allowed to choose an alternative destination or reset unrelated
  pair states;
- H1 may only improve execution timing for a route whose destination is already
  locked by a causal D1-defined rule.

That is NOT tested or approved in V1.

## Production decision

No production change.
No Telegram change.
No exchange execution change.

Files to install/copy:
- none.

Residual risks:
- H1 route-lock concept remains untested;
- H1 execution can still false-confirm intraday;
- real effective execution costs may exceed modeled costs.
