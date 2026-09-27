# ONE TRX DEFENSE PER CRISIS V4 — Evidence

Date: 2026-09-27  
Branch: `research/one-trx-defense-per-crisis-v4`  
Mode: STRESS_TEST_ONLY + ECOSYSTEM_PLANNING  
Status: EXECUTED / RESEARCH_ONLY / NO PRODUCTION CHANGE

## Run identity

- GitHub Actions run: `36302318501`
- Source commit: `a3445d70d981d5d7694e5bcc8e5b1bb326788c02`
- Artifact: `one-trx-defense-per-crisis-v4`
- Artifact ID: `10925409207`
- Workflow conclusion: SUCCESS
- reproduction gate: PASS

TEST_LEVEL: GITHUB_ACTIONS_EXECUTED + UNIT_TESTS + FROZEN_REPRODUCTION_GATE + REAL_BINANCE_1D + FULL_HISTORY + NON_OVERLAPPING_180D_120D_ROBUSTNESS

## State machine tested

1. ARMED:
   - crisis entry remains breadth<=3 for 3 closes;
   - next-open actual capital moves to fixed TRX.

2. TRX_DEFENSE:
   - shadow router continues;
   - breadth does not exit defense;
   - first NEW confirmed shadow-router transition after entry is treated as router reactivation.

3. Direct execution:
   - if shadow was ATOM and new signal is ATOM -> BNB while actual capital is in TRX,
     actual execution is directly TRX -> BNB;
   - no TRX -> ATOM -> BNB intermediate trade.

4. POST_TRX_DISARMED:
   - ordinary relative rotations continue;
   - another TRX defense is forbidden;
   - the old crisis must first recover with breadth>=5 for 3 closes.

5. REARM:
   - after recovery confirmation, detector becomes ARMED;
   - next crisis requires a fresh breadth<=3 x3 sequence.

No numeric parameter was added.

## Old V3 reference

Old router-reactivation V3:
- period 2025-03-29 -> 2026-03-28;
- median return: -2.78%;
- median max DD: -56.57%;
- median defensive transitions: 11.5;
- verdict: DEVELOPMENT_FAIL_DO_NOT_PROMOTE.

Failure mode:
- router signal exited TRX while breadth remained deeply stressed;
- three low closes then immediately re-entered TRX;
- repeated TRX -> risk asset -> TRX churn.

V4 removes this same-crisis re-entry behavior.

## Reproduction period 2025-03-29 -> 2026-03-28

Frozen comparators:
- BASELINE: +43.82% / -61.57%;
- LOW_VOL: +49.05% / -47.13%.

V4:
- median return: **+5.08%**
- worst start: **-5.56%**
- positive starts: **4/8**
- median max DD: **-57.05%**
- median actual transitions: **8**
- median TRX entries: **2**
- median router-reactivation exits: **2**
- median TRX defensive exposure: **15.21%**
- median POST_TRX_DISARMED exposure: **54.66%**
- median rearm count: **1**

Interpretation:
- V4 materially reduces the old V3 re-entry churn;
- but it still destroys most of the LOW_VOL return and protection in the historical-year reproduction period;
- the architecture is not rescued merely by forbidding repeated TRX entry inside the same crisis.

## Already-open 2026 period 2026-03-29 -> 2026-09-26

Prior references:
- BASELINE: +103.36% / -36.68%;
- frozen LOW_VOL: +32.92% / -18.34%.

V4:
- median return: **+105.02%**
- worst start: **+53.40%**
- positive starts: **8/8**
- median max DD: **-36.68%**
- median actual transitions: **5**
- median TRX entries: **1**
- median TRX defensive exposure: **0.55%**
- median POST_TRX_DISARMED exposure: **78.57%**

Interpretation:
In 2026 the first router-reactivation signal arrived almost immediately after TRX entry. V4 therefore behaved nearly like the ordinary router for the remainder of the same crisis. It recovered upside, but essentially gave back the LOW_VOL drawdown protection.

## Full eligible history 2023-11-20 -> 2026-09-26

Frozen comparators:
- BASELINE: +646.96% / -71.23%;
- LOW_VOL: +887.02% / -47.13%;
- frozen-timing CASH: +258.06% / -67.56%.

V4:
- median return: **+1,209.45%**
- worst start: **+1,179.25%**
- positive starts: **8/8**
- median max DD: **-67.85%**
- median actual transitions: **25**
- median TRX entries: **4**
- median router-reactivation exits: **4**
- median TRX defensive exposure: **18.52%**
- median POST_TRX_DISARMED exposure: **42.23%**
- median rearm count: **4**

The full-history return is higher than frozen LOW_VOL, but the max drawdown is much worse and close to the unprotected baseline.

## Why the full-history return is misleading

For an ATOM start, V4 episode chronology includes:

- 2024-04-20 ENTER TRX
- 2024-04-21 EXIT on router signal
- 2024-05-19 REARM

Then:

- 2024-06-14 ENTER TRX
- **2024-11-26 EXIT on router signal**
- 2024-11-28 REARM

This second TRX block lasted roughly 5.5 months.
Several breadth-defined stress/recovery episodes were effectively merged into one router-reactivation episode because V4 does not leave TRX until a new router transition appears.

Later:
- 2025-02-27 ENTER TRX
- 2025-03-03 EXIT to PEPE
- 2025-07-17 REARM

And:
- 2025-11-02 ENTER TRX
- 2025-11-25 EXIT to PEPE
- 2026-08-22 REARM

Therefore the +1,209% full-history figure is heavily path-dependent and cannot be treated as robust evidence.

## Robustness

### 180-day windows — 5 total

V4 beats LOW_VOL:
- return: **2/5**
- drawdown: **1/5**
- both return and drawdown: **1/5**

Median V4 across 180d windows:
- return: +33.16%
- max DD: -41.16%
- actual transitions: 4

Notable dispersion:
- one window 2024-05-18 -> 2024-11-13:
  - LOW_VOL return -20.69%, DD -33.03%
  - V4 return +33.16%, DD -17.84%
- late weak window 2025-11-09 -> 2026-05-07:
  - LOW_VOL return +14.87%, DD -17.44%
  - V4 return -47.65%, DD -61.58%

### 120-day windows — 8 total

V4 beats LOW_VOL:
- return: **2/8**
- drawdown: **0/8**
- both: **0/8**

Median V4 across 120d windows:
- return: +8.82%
- max DD: -34.25%
- actual transitions: 4

The shorter-window evidence does not support a stable advantage.

## Verdict

`ONE_TRX_DEFENSE_PER_CRISIS_V4_VERDICT = CHURN_FIXED / FULL_HISTORY_RETURN_HIGH / ROBUSTNESS_FAIL / LOW_VOL_STILL_BETTER_FOR_RISK_CONTROL / DO_NOT_PROMOTE`

Supported:
- the user's direct execution semantics are correct and work:
  TRX -> new router target directly;
- one TRX defense per crisis removes the old V3 same-crisis ping-pong;
- the design can recover much more upside than frozen LOW_VOL in some regimes.

Not supported:
- using the first new router transition alone as a robust reason to abandon TRX;
- replacing frozen LOW_VOL with V4;
- treating the strong full-history return as reliable evidence;
- production promotion.

Core finding:
`router activity != market safety`.

The no-repeat rule fixes churn, but it does not solve the underlying problem that the first router transition can occur while broad market stress remains severe.

## Next research implication

Do not tune another nearby router-transition threshold.

The remaining useful research direction is a hybrid re-entry condition:
- keep one defensive reaction per crisis;
- preserve direct TRX -> target execution;
- require additional independent recovery evidence before allowing the router signal to end defense, OR use a graduated/partial re-entry architecture.

Macro / official liquidity recovery research can be used only in a separately preregistered experiment.

## Runtime impact

Production changed: NONE  
Paper-live changed: NONE  
Migration required: NO  
Automatic execution authorized: NO  
Manual confirmation remains required.
