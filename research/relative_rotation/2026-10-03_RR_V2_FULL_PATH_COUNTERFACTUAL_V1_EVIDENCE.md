# RR V2 FULL-PATH COUNTERFACTUAL V1 — EVIDENCE

Date: 2026-10-03
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Runtime identity

- Research branch: `research/rr-v2-full-path-counterfactual-v1`
- Draft PR: #116
- GitHub Actions run: `37142303774`
- Job: `111259095083`
- Artifact ID: `11281075887`
- Artifact: `rr-v2-full-path-counterfactual-v1-2`
- Artifact SHA256: `a71e48cf843a7ff790015a185b583fb192807e347549290b09440470efe44b94`
- Fixed as-of: `2026-10-03T16:30:00Z`
- Latest common closed D1: `2026-10-02T00:00:00Z`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_PATH_DEPENDENT_STRESS_TEST`

## Frozen experiment

Compared:

### CORE
Current:
`RR + Destination Dominance 1.5x`

### CORE_PLUS_V2
Same CORE plus the frozen:
`PERSIST2_REEXPAND`

The overlay actually changes the held asset and all later RR decisions continue
from that new actual asset.

Primary transition cost:
- 0.10%

Robustness:
- 0.50%
- 1.00%
- 3.00%

Primary same-close rule:
- V2 priority.

Robustness:
- CORE priority.

## Final classification

`FULL_PATH_REJECTED`

Frozen gate failures:
- MATURE median return delta > 0: FAIL
- >=50% MATURE start states improve: FAIL
- median DD degradation no worse than 10pp: PASS
- >=40% rolling windows improve: FAIL
- 0.50% cost MATURE median delta > 0: FAIL

## MATURE path result — primary 0.10% cost

Across all 13 supported starting assets:

### CORE
- median return: **+3506.80%**
- normalized capital: 100 -> **3606.80**
- worst start return: +2772.95%
- median max DD: -62.43%
- median transitions: 23

### CORE_PLUS_V2
- median return: **+2405.42%**
- normalized capital: 100 -> **2505.42**
- worst start return: +1895.66%
- median max DD: -62.43%
- median transitions: 34
- median extra transitions: +11

Difference:
- improved starts: **0 / 13**
- median final-capital ratio: **0.6946x**
- median return delta: **-1101.38 percentage points**
- median max-DD delta: 0.00pp

Every MATURE start state finished with less capital under the V2 overlay.

## MATURE start-state detail

At 0.10% cost, CORE_PLUS_V2 / CORE final-capital ratios:

- TWT: 0.5915x
- TRX: 0.5915x
- LINK: 0.5915x
- SOL: 0.6424x
- PEPE: 0.6946x
- XRP: 0.6946x
- BNB: 0.6946x
- HBAR: 0.6946x
- ATOM: 0.6946x
- ALGO: 0.8121x
- AAVE: 0.8121x
- FIL: 0.8121x
- AVAX: 0.8121x

All paths still ended in TRX, but V2 took lower-value detours before converging.

## LAST_2Y

At 0.10% cost:

CORE:
- median return: +2296.64%

CORE_PLUS_V2:
- median return: +1872.14%

Comparison:
- only **1 / 13** starts improved;
- median final-capital ratio: 0.8229x;
- median return delta: -424.51pp;
- median extra transitions: +5.

## LAST_1Y

At 0.10% cost:

CORE:
- median return: +162.93%

CORE_PLUS_V2:
- median return: +162.93%

The overlay executed no V2 moves in these exact start-state paths.

This is important:
the recent isolated AAVE setup exists, but the LAST_1Y generic start-state
portfolio paths did not encounter a completed V2 overlay opportunity that
survived the path interaction / cancellation rules.

## Cost robustness

MATURE V2 improved starts:
- 0.10%: 0 / 13
- 0.50%: 0 / 13
- 1.00%: 0 / 13
- 3.00%: 0 / 13

MATURE median final-capital ratio:
- 0.10%: 0.6946x
- 0.50%: 0.6781x
- 1.00%: 0.6579x
- 3.00%: 0.5821x

The rejection is therefore not an artifact of the primary cost assumption.

## Same-close priority robustness

`V2_PRIORITY` and `CORE_PRIORITY` produced the same primary-window aggregate
results in this run.

Therefore same-close conflict ordering is not driving the rejection.

## Rolling 365-day evidence

Rolling windows:
- **24**

Windows where V2 had higher median return:
- **4 / 24 = 16.7%**

Median rolling return delta:
- **-26.29 percentage points**

Worst rolling return delta:
- **-139.21 percentage points**

Median rolling max-DD delta:
- approximately 0.00pp

The four positive rolling windows were:

- 2024-12-01 -> 2025-11-30: +4.04pp median return delta
- 2025-01-01 -> 2025-12-31: +3.42pp
- 2025-02-01 -> 2026-01-31: +3.77pp
- 2025-03-01 -> 2026-02-28: +2.50pp

The positive windows are small improvements compared with several much larger
negative windows.

## Why isolated-trade V2 looked better but full path failed

The isolated V2 classifier test measured:

`candidate C vs staying in current B for a fixed horizon`.

That question is useful but incomplete.

The full-path test measures:

`switch B -> C, then continue all later RR+DDG decisions from actual C`.

The overlay can therefore:
- win the immediate local comparison;
- alter the next source asset;
- miss or delay a stronger core route;
- add transitions;
- converge later to the same eventual asset with less accumulated capital.

This path-dependence is the material failure mode revealed by V1 full-path
testing.

## Current LINK -> TRX -> AAVE case

Dedicated recent trace starting LINK on Sep-28:

Sep-28 close:
- CORE signal: LINK -> TRX

Sep-29 open:
- execute LINK -> TRX
- modeled fee: 0.11018 normalized units

Sep-29 close:
- AAVE base trigger vs TRX: +10.93%

Sep-30 close:
- AAVE remains #1
- impulse: +6.10%

Oct-01 close:
- AAVE remains #1
- re-expands to +14.93%
- V2 PRIMARY PASS

Oct-02 open:
- execute TRX -> AAVE
- modeled fee: 0.10984 normalized units

At latest closed D1:
- final asset: AAVE
- normalized capital: **114.942**
- return from Sep-28 open: **+14.94%**

Thus:
`AAVE_CURRENT_CASE = VALID_V2_LOCAL_SIGNAL`

but:
`V2_GENERIC_FULL_PATH_OVERLAY = REJECTED`

Both facts are simultaneously true.

## Decision

No production change.

Frozen labels:

- `PERSIST2_REEXPAND_ISOLATED_PRECISION = IMPROVED`
- `PERSIST2_REEXPAND_FULL_PATH = REJECTED`
- `AAVE_2026_10_01 = VALID_LOCAL_EXCEPTION_SO_FAR`
- `AUTOMATIC_GENERIC_TRX_TO_ACCELERATOR_SWITCH = NOT_AUTHORIZED`

## Research implication

The next useful question is no longer whether the acceleration candidate
outperforms the current holding for 14 days.

The important question is:

> Does switching to the acceleration candidate beat the **next several CORE
> path decisions**, including the opportunity cost of leaving the RR route?

Any future continuation overlay must be evaluated against that path opportunity
cost from the beginning.

A materially new hypothesis could study only the rare cases where:
- the acceleration candidate passes V2;
- AND no high-quality imminent CORE route is being sacrificed;
- or the acceleration candidate itself preserves / improves the next RR path.

That would require a new preregistered mechanism. Do not retune
PERSIST2_REEXPAND on the same history.

## Artifact files

- `primary_path_results.csv`
- `primary_summary.csv`
- `core_vs_v2_comparison.csv`
- `rolling_365_detail.csv`
- `rolling_365_summary.csv`
- `link_aave_case_trace.csv`
- `effective_route_ledger.csv`
- `summary.json`
- `report.md`
