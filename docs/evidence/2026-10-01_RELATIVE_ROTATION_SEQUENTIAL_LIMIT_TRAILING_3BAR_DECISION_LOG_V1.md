# RR Sequential Limit Trailing 3-Bar — Decision Log V1

Date: 2026-10-01
Status: ACTIVE RESEARCH MEMORY
Workflow mode: STRESS_TEST_ONLY
Production change: NONE

Canonical evidence:
`research/relative_rotation/2026-10-01_RR_SEQUENTIAL_LIMIT_TRAILING_3BAR_V1_EVIDENCE.md`

Runtime:
- run `36857241441`
- commit `7102a0de615a5073a9500ac60c383ddfe98070ce`
- artifact `11159690410`
- SHA256 `b66f766dc89d1d57e6c178ce5dcdca01b9234c574d10fc3dd4f3c0e214e8b2d3`

## Frozen result

3-day monotonic daily trailing improves execution fill-rate but does not restore strategy performance.

### MATURE

- canonical: +3588.4%, DD -62.4%
- two-fee next-open: +3502.7%, DD -62.5%
- fixed exact3 7d: +1538.5%, DD -71.6%
- trailing 3-day 7d: **+1577.5%, DD -71.6%**

### Validation 1Y

- fixed 7d: +42.2%
- trailing: **+40.8%**

### Last 2Y

- fixed 7d: +1024.5%
- trailing: **+957.6%**

## Execution

- direct attempts: 29
- fixed full-limit completions: 21
- trailing full-limit completions: **24**
- only 9/29 direct attempts actually tightened at least one side
- median SELL tightenings: 0
- median BUY tightenings: 0
- MATURE median pending days: 58
- MATURE median RR opportunities while pending: 6

## Classification

`3DAY_EXTREME_TRAILING_V1 = REJECTED`

`PROGRESSIVE_EXECUTION_GENERAL_FAMILY = STILL_UNRESOLVED`

The tested rule is too slow/inactive.

## Metric correction

Previous `skipped_signal_days` label in fixed-7d V1 should be interpreted as `pending days`, not literal skipped signal count.

Returns and DD are unaffected.

## No-repeat

Do not repeat this exact 3-day daily-extrema trailing rule on the same history.

## Production boundary

No live/paper/Telegram/exchange/order changes authorized.
