# RR EXTREME CONTINUATION CLASSIFIER V2 — EVIDENCE

Date: 2026-10-03
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Runtime identity

- Research branch: `research/rr-extreme-continuation-classifier-v2`
- Draft PR: #115
- GitHub Actions run: `37141135849`
- Job: `111255636746`
- Artifact ID: `11280746213`
- Artifact: `rr-extreme-continuation-classifier-v2-2`
- Artifact SHA256: `c346e35c32b99c9daa35adafaa3c40e47e264d8d3f550da472da776d437c0fc9`
- Fixed as-of: `2026-10-03T16:30:00Z`
- Latest common closed D1: `2026-10-02T00:00:00Z`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_STRESS_TEST`

## Preregistered primary classifier

Base event:
- corrected RR+DDG route entered at next D1 open;
- strongest alternative TARGET reaches >= +10% relative impulse within first 1-3 closed bars.

Primary V2 does **not** switch immediately.

`PERSIST2_REEXPAND` requires:
1. the same candidate remains #1 on the next closed D1 bar;
2. remains #1 again on the following closed D1 bar;
3. its relative impulse on that second confirmation close is strictly greater
   than its original trigger impulse;
4. hypothetical switch occurs next open.

Primary outcome horizon:
- 14 bars after switch.

## Base sample

Base +10% acceleration triggers:
- **1502**

## Fixed variant comparison

### PERSIST1

Passes:
- 1097 / 1502 = 73.0%

14d:
- N = 1061
- candidate beats corrected held = **42.9%**
- median relative excess = **-3.47%**
- mean = +1.17%
- >=20% continuation = 11.6%

Result:
`PERSIST1 = REJECTED_AS_QUALITY_FILTER`

One extra #1 close alone makes the typical result worse.

### PERSIST2

Passes:
- 859 / 1502 = 57.2%

14d:
- N = 824
- candidate beats corrected held = **47.7%**
- median relative excess = **-2.11%**
- mean = +2.31%
- >=20% continuation = 12.6%

Result:
`PERSIST2 = INSUFFICIENT`

Two #1 closes improve on PERSIST1 but still do not produce positive typical edge.

### PERSIST2_REEXPAND — PRIMARY

Passes:
- **500 / 1502 = 33.3%**

14d:
- N = **478**
- candidate beats corrected held = **53.6%**
- median relative excess = **+1.14%**
- mean relative excess = **+6.13%**
- >=20% continuation = **13.4%**
- false-positive/non-outperformance rate = 46.4%

V1 naive +10% reference at 14d:
- win rate = 46.5%
- median excess = -1.66%
- mean excess = +4.37%
- >=20% continuation = 15.0%

Changes versus naive V1:
- win rate: **+7.1 percentage points**
- median: **+2.80 percentage points**
- mean: **+1.76 percentage points**
- strong >=20% continuation share: **-1.6 percentage points**
- event count materially reduced.

Frozen classification:

`CLASSIFIER_IMPROVES_PRECISION_BUT_NOT_PRODUCTION_READY`

The primary pattern successfully changes the typical outcome from negative to
positive, but it does not increase the share of extreme >=20% continuations.

## Key interpretation

The useful information is not simple persistence.

The sequence:

`TRIGGER -> REMAIN #1 -> REMAIN #1 -> RE-EXPAND ABOVE ORIGINAL TRIGGER`

is materially different from:

`TRIGGER -> REMAIN #1`

or:

`TRIGGER -> REMAIN #1 TWICE`.

This isolates the first tested structural confirmation pattern that:
- has >50% 14d win rate;
- has positive 14d median relative excess;
- still retains hundreds of historical cases.

This is a meaningful research improvement, not yet an executable strategy rule.

## AAVE / corrected TRX case

Corrected route:
`LINK -> TRX`

Base trigger:
- Sep-29 close
- AAVE vs TRX: **+10.93%**

First confirmation close:
- Sep-30
- AAVE remains #1
- AAVE impulse: **+6.10%**

Second confirmation close:
- Oct-01
- AAVE remains #1
- AAVE impulse: **+14.93%**

Therefore:
- PERSIST1 = PASS
- PERSIST2 = PASS
- REEXPAND = PASS
- PRIMARY = **PASS**

Primary hypothetical switch:
- **Oct-02 open**
- `TRX -> AAVE`

Through the latest closed candle available in V2:
- AAVE relative excess vs TRX after the hypothetical switch = **+4.79%**

This is still a very short forward observation and must not be interpreted as a
completed 14d validation.

## Structural diagnostics

Among PRIMARY passes with complete 14d outcomes:

### NON_STRONG (< +20% relative continuation)
- N = 414
- median 14d excess = -2.22%
- trigger impulse = +13.55%
- confirm2 impulse = +20.57%
- trigger margin vs #2 = +7.39%
- confirm2 margin vs #2 = +9.17%
- trigger RR topology net OUT-IN = +6
- confirm2 RR topology net OUT-IN = +6

### STRONG_GE20
- N = 64
- median 14d excess = +38.80%
- trigger impulse = +12.86%
- confirm2 impulse = +28.66%
- trigger margin vs #2 = +7.60%
- confirm2 margin vs #2 = +10.98%
- trigger RR topology net OUT-IN = +6
- confirm2 RR topology net OUT-IN = +8

Important observations:
- initial trigger size itself does **not** distinguish the strong tail;
- strong continuations show materially larger re-expansion by confirm2;
- strong continuations also show a somewhat larger cross-sectional lead by
  confirm2;
- RR topology is more extremely "out of candidate" at confirm2 in strong cases,
  which is counterintuitive under pure mean-reversion semantics and may be a
  signature of trend persistence.

These are descriptive V2 diagnostics only. No threshold is authorized from them.

## Recent month

Recent-month base triggers:
- 55

PRIMARY passes:
- 22
- pass rate = 40.0%

Complete 14d outcomes:
- 0

Therefore no recent-month 14d performance claim is allowed yet.

## Decision

No production change.

Frozen labels:

- `PERSIST1 = REJECTED_AS_QUALITY_FILTER`
- `PERSIST2 = INSUFFICIENT`
- `PERSIST2_REEXPAND = PRECISION_IMPROVEMENT`
- `AAVE_2026_10_01 = PRIMARY_CLASSIFIER_PASS`
- `PRODUCTION_PROMOTION = NOT_AUTHORIZED`

## Next admissible step

The next meaningful test is a full path-dependent portfolio simulation of
`PERSIST2_REEXPAND` on top of corrected RR+DDG, including:

- actual route replacement timing;
- transition costs and extra swaps;
- effect on later RR state/path;
- total return;
- max drawdown;
- transition count;
- results by rolling windows / regimes.

This should use the already frozen V2 classifier unchanged.

Separately, the V2 diagnostics suggest a future V3 research question around the
**magnitude of re-expansion and confirm2 RR topology**, but those features must
not be threshold-tuned before the frozen V2 full-path test.

## Artifact files

- `base_trigger_classifier_events.csv`
- `variant_results.csv`
- `variant_summary.csv`
- `primary_outcome_diagnostics.csv`
- `recent_month_classifier_events.csv`
- `recent_month_variant_summary.csv`
- `aave_case.json`
- `summary.json`
- `report.md`
