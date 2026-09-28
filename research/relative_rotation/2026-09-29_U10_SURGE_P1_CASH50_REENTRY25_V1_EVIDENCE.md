# U10 SURGE P1 CASH50 REENTRY25 V1 — EVIDENCE

Date: 2026-09-29
Mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Frozen rule

Control:
- single-month selected-extreme surge >= +100%
- arm and track post-surge running peak
- after >=5% pullback, move 30% of invested sleeve to USDT next open
- 0.1% cash-out cost
- lock the running peak
- re-enter full parked cash when frozen-U10 reference close is >=25% below locked peak
- next-open re-entry
- 0.1% re-entry cost

Candidate:
- identical mechanics
- only cash fraction changes: 30% -> 50%

## Runtime identity

Primary topology stress:
- run: 36476916640
- source SHA: ae55a3107b65768ae9027d0fffa4a77242f9a9c6
- artifact: 10993483988
- artifact SHA256: 50e8545f2f7d754a6a8e08d0f0ab66f246557e5c545975fd71ae823c25031c11

Independent canonical verification:
- run: 36477154823
- result: PASS

TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST

## Canonical U10

| Metric | Baseline | Cash 30% | Cash 50% |
|---|---:|---:|---:|
| Final equity | 219,485.20 | 270,653.52 | 308,829.05 |
| Max DD | -71.04% | -66.36% | -62.74% |
| Cash-outs | - | 3 | 3 |
| Re-entries | - | 3 | 3 |
| Days in cash | - | 66 | 66 |

Cash50 vs Cash30:
- +38,175.53 USDT
- +14.10%

Cash50 vs ordinary baseline:
- +89,343.86 USDT
- +40.71%

Max-DD improvement Cash50 vs Cash30:
- +3.62 percentage points

## 791 alternative U10s

- Cash50 > ordinary baseline: 96.21%
- Cash50 > Cash30: 96.21%
- Cash50 = Cash30: 0.00%
- Cash50 < Cash30: 3.79%
- Cash50 DD better than Cash30: 73.32%
- both final equity and DD better than Cash30: 69.53%
- median Cash50 delta vs Cash30: +14.18%
- q25/q75 delta vs Cash30: +11.39% / +15.44%
- unfinished candidate cash-cycle rate: 3.79%

## Interpretation

Historical evidence strongly favors the 50% protected sleeve over 30% under
this exact surge/pullback/re-entry rule.

The improvement is not only terminal equity:
- canonical terminal capital rises materially;
- canonical max drawdown also improves;
- the advantage versus Cash30 appears in 96.21% of alternative U10 topologies.

However, this is not enough to declare 50% optimal:
- only 30% and 50% are being compared in this preregistered test;
- no 40/60/70% sweep was run;
- all topology variants share the same historical dates;
- 3.79% of alternative topologies finished with an unfinished candidate cash cycle.

Frozen classification:

`CASH50_DOMINATES_CASH30_ON_TESTED_HISTORY_BUT_IS_NOT_PROVEN_OPTIMAL`

## Production boundary

No live/paper, Telegram, exchange, allocation, universe or execution behavior changed.
