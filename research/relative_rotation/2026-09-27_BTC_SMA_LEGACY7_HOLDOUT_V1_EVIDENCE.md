# BTC SMA LEGACY-7 HOLDOUT V1 — Evidence

Date: 2026-09-27
Branch: `research/btc-sma-legacy7-holdout-v1`
Mode: STRESS_TEST_ONLY + ECOSYSTEM_PLANNING
Status: EXECUTED / REVERSE-TIME HOLDOUT / NO PRODUCTION CHANGE

## Run identity

- GitHub Actions run: `36305802639`
- Source commit: `d07324b57d2dbf65fe94e701a81b439f21bcef93`
- Artifact: `btc-sma-legacy7-holdout-v1`
- Artifact ID: `10927261202`
- Workflow conclusion: SUCCESS

TEST_LEVEL: GITHUB_ACTIONS_EXECUTED + UNIT_TESTS + REAL_BINANCE_1D + REVERSE_TIME_LEGACY7_HOLDOUT + 180D_120D_ROBUSTNESS

## Why this is LEGACY-7, not exact 8-asset validation

The exact frozen 8-asset universe cannot be evaluated on an older unseen interval because PEPE is historically unavailable early enough to support the same universe plus causal SMA200 warmup.

Therefore the holdout universe was frozen before download as:

- ATOM
- TWT
- BNB
- SOL
- TRX
- AAVE
- LINK

PEPE excluded.

No substitute asset was inserted.

The same numeric breadth thresholds 3/5 were intentionally preserved without retuning.

This is a portability stress test, not exact validation of the current 8-asset system.

## Data actually obtained

Requested:
- 2020-01-01 -> 2023-05-04

Common LEGACY-7 panel:
- start: 2021-01-27
- end: 2023-05-04
- rows: 828
- no critical data-quality issues

Latest listing in the seven:
- TWT begins 2021-01-27

First fully eligible date after causal SMA200/VOL30 warmup:
- **2021-08-14**

Primary holdout:
- **2021-08-14 -> 2023-05-04**

BTC:
- Binance BTCUSDT 1D
- older history was not used in development and was opened only for this holdout.

## Frozen BTC candidates

Original development order was preserved:

1. BTC SMA25/100
2. BTC SMA30/100
3. BTC SMA12/100

No new SMA pair was tested.
No holdout ranking was used to replace the development ordering.

## Full holdout result

### Comparators

BASELINE:
- median return: **-70.96%**
- median max DD: **-92.94%**
- positive starts: 0/7

LEGACY-7 LOW_VOL:
- median return: **-40.08%**
- median max DD: **-76.24%**
- positive starts: 0/7
- defensive exposure: ~69.95%

FROZEN CASH:
- median return: **-36.13%**
- median max DD: **-69.77%**
- positive starts: 0/7
- defensive exposure: ~69.95%

### Frozen BTC candidates

BTC 25/100:
- median return: **-70.74%**
- median max DD: **-93.76%**
- worst return: -81.10%
- positive starts: 0/7
- cash exposure: ~38.63%
- median cash wait: **183d**
- unresolved cash starts at holdout end: **7/7**

BTC 30/100:
- median return: **-72.88%**
- median max DD: **-94.21%**
- worst return: -82.48%
- positive starts: 0/7
- cash exposure: ~38.95%
- median cash wait: **185d**
- unresolved cash starts: **7/7**

BTC 12/100:
- median return: **-64.69%**
- median max DD: **-92.46%**
- worst return: -77.19%
- positive starts: 0/7
- cash exposure: ~37.84%
- median cash wait: **178d**
- unresolved cash starts: **7/7**

## Primary holdout verdict

All three frozen BTC candidates failed the full-cycle older holdout versus LOW_VOL and frozen CASH.

Best BTC candidate on the old full holdout was 12/100:
- BTC 12/100: -64.69% / -92.46%
- LOW_VOL: -40.08% / -76.24%
- frozen CASH: -36.13% / -69.77%

The development winner 25/100 did not generalize:
- development period looked promising;
- older holdout produced essentially baseline-like loss and even slightly worse max DD than baseline.

This rejects any claim that the development shortlist is already a robust standalone cash re-entry solution.

## Robustness windows

Available complete windows:
- 3 x 180d
- 5 x 120d
- total: 8

For each frozen BTC candidate:
- return wins vs LOW_VOL: 6/8
- DD wins: 6/8
- simultaneous return+DD wins: 6/8

At first glance this appears strong, but the raw windows show an important truncation effect.

### Unresolved-cash truncation

For BTC 25/100, four of the six simultaneous wins ended while all seven starting portfolios were still in cash:
- 180d window 1
- 120d window 1
- 120d window 3
- 120d window 4

These windows show that cash avoided downside through the window endpoint.
They do NOT demonstrate that the BTC crossover successfully timed re-entry.

The same pattern affects 30/100 and 12/100.

Resolved simultaneous wins are much fewer:
- one 180d recovery window
- one 120d recovery window

Therefore raw 6/8 window wins materially overstate re-entry evidence.

## Critical failure window

180d:
- 2022-02-10 -> 2022-08-08

LOW_VOL:
- median return: **+2.34%**
- DD: **-36.79%**

BTC 25/100:
- return: **-58.08%**
- DD: **-79.09%**
- median cash wait: 47d

BTC 30/100:
- return: **-60.87%**
- DD: **-79.86%**
- wait: 49d

BTC 12/100:
- return: **-49.40%**
- DD: **-79.09%**
- wait: 42d

This is the key holdout failure.

A bullish BTC SMA crossover occurred, the strategy returned to risk, but the broader 2022 bear market was not actually safe.
The BTC trend crossover was therefore insufficient as a standalone systemic-recovery condition.

## What generalized

The BTC signal does have useful behavior:

- it can keep capital in cash through significant portions of stress;
- in several shorter windows it materially beats LOW_VOL while still in cash;
- in some resolved windows it successfully captures recovery.

So the BTC recovery concept is not random.

## What did not generalize

The standalone crossover does not reliably distinguish:
- durable market recovery
from
- a temporary BTC trend recovery inside a larger bear regime.

This is exactly the failure mode the older holdout was intended to reveal.

## Verdict

`BTC_SMA_LEGACY7_HOLDOUT_V1 = PARTIAL_SIGNAL_VALUE / PRIMARY_HOLDOUT_FAIL / STANDALONE_REENTRY_REJECTED`

Development shortlist:
- 25/100
- 30/100
- 12/100

remains historically documented but is NOT promoted.

Do not:
- retune these values on the old holdout;
- search new BTC SMA lengths using this holdout and call them validation;
- promote BTC crossover alone to production.

## Research implication

The old holdout strengthens the case that re-entry likely needs more than a BTC trend crossover alone.

A future separately preregistered design may test BTC recovery together with an independent systemic condition, for example:
- broad crypto breadth improvement;
- cross-asset market recovery;
- or another causal recovery indicator.

The old holdout must remain closed to further parameter tuning if it is to retain any diagnostic value.

## Runtime impact

Production changed: NONE
Paper-live changed: NONE
Migration required: NO
Automatic execution authorized: NO
Manual confirmation remains required.
