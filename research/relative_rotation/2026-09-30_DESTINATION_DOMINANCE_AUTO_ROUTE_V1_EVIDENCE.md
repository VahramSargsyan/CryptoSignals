# Destination Dominance Immediate Stronger V1 — Evidence and Production Watch

Date: 2026-09-30  
Workflow history: STRESS_TEST_ONLY -> user-approved production promotion  
Production rule: DESTINATION_DOMINANCE_IMMEDIATE_STRONGER_V1

## Rule

Given a currently held source asset `S`:

1. baseline router has `S -> A = CONFIRMED`;
2. another destination `B` is currently `ARMED` or `CONFIRMED` from the same source;
3. `S -> B` has larger `max_dislocation` than `S -> A`;
4. the actual direct destination pair is oriented `A -> B` as `ARMED` or `CONFIRMED`.

Then the effective actionable route becomes immediately:

`S -> B`

instead of:

`S -> A`.

The strongest qualifying `B` by `max_dislocation` is selected.

This is not mathematical transitivity. The actual `A/B` Relative Rotation state machine must point from A toward B.

## Historical stress-test result

Frozen historical dataset:
- Binance Spot D1;
- common panel beginning 2023-05-05;
- lookback 180;
- ARM 15%;
- reversal 3%;
- modeled transition cost 0.1%;
- signal close T -> next-day open;
- frozen U10: TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR.

### Aggregate results

| Window | Baseline median | DD auto-route median | Delta |
|---|---:|---:|---:|
| 1Y | +155.75% | +170.78% | +15.03 pp |
| 2Y | +1850.89% | +1945.56% | +94.67 pp |
| Mature / approx 3Y | +3167.27% | +3359.32% | +192.05 pp |

Worst-start results also improved in the primary aggregate windows:
- 1Y: +21.70% -> +28.85%;
- 2Y: +1627.59% -> +1729.14%;
- Mature: +2462.14% -> +2612.75%.

Median transition counts declined:
- 1Y: 11.5 -> 8.5;
- 2Y: 20 -> 17;
- Mature: 26 -> 22.5.

Approximate realized override counts in the historical paths:
- 1Y: 32;
- 2Y: 33;
- Mature: 31.

## Critical instability that must NOT be forgotten

The aggregate improvement was not uniform across rolling windows.

Rolling 12-month windows:
- better: 9;
- equal: 5;
- worse: 9;
- median delta: 0 pp;
- worst observed delta: about -30.48 pp;
- best observed delta: about +12.25 pp.

Rolling 24-month windows:
- better: 3;
- equal: 4;
- worse: 4;
- median delta: 0 pp;
- worst observed delta: about -81.53 pp;
- best observed delta: about +107.57 pp.

Legacy LINK-start diagnostic:
- 1Y baseline: +229.58%;
- 1Y DD auto-route: +175.27% — materially worse;
- 2Y baseline: +1681.70%;
- 2Y DD auto-route: +1786.43%;
- Mature baseline: +3141.70%;
- Mature DD auto-route: +3332.25%.

This demonstrates that a shorter graph path can skip a beneficial intermediate asset. Relative Rotation signals are not static shortest-path costs.

## Production decision

Vahram explicitly chose on 2026-09-30 to promote this rule from warning-only behavior to the active routing rule.

Reasoning:
- aggregate historical results improved;
- Mature / approx 3Y median improved from +3167.27% to +3359.32%;
- real execution exposed that unnecessary intermediate swaps can also impose material execution costs;
- the rule can therefore potentially reduce both route inefficiency and extra conversion cost.

This is a deliberate production decision with known uncertainty, not a claim that the rule is universally superior.

## Mandatory forward watch

The following must be tracked separately for every automatic override:
- source asset;
- baseline confirmed destination A;
- selected override destination B;
- S->A max dislocation;
- S->B max dislocation;
- S->B own state at override time: ARMED or CONFIRMED;
- A->B state and max dislocation;
- whether the skipped A would later have produced a better realized path;
- number of conversions avoided;
- estimated execution cost avoided or added;
- realized result versus the baseline counterfactual when measurable.

Attention flag:

`FORWARD_WATCH_REQUIRED = TRUE`

Review trigger:
- repeated negative override outcomes;
- rolling-forward evidence materially worse than baseline;
- evidence that the rule systematically skips profitable intermediate assets;
- execution-cost savings fail to compensate for route-selection losses.

## Evidence boundary

The rule was selected after observing historical data and is therefore post-selected.

`HISTORICAL_IMPROVEMENT != GUARANTEED_FORWARD_IMPROVEMENT`

The historical transition cost assumption of 0.1% is also known to be optimistic relative to some observed real conversion quotes, so the exact real-net benefit remains unresolved.

TEST_LEVEL of this document:
`HISTORICAL_STRESS_RESULT_PRESERVATION + PRODUCTION_DECISION_RECORD`
