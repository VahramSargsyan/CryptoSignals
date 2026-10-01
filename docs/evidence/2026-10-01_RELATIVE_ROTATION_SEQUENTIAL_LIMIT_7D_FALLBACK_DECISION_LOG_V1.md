# RR Sequential Limit 7D Forced Fallback — Decision Log V1

Date: 2026-10-01
Status: ACTIVE RESEARCH MEMORY
Workflow mode: STRESS_TEST_ONLY
Production change: NONE

Canonical evidence:
`research/relative_rotation/2026-10-01_RR_SEQUENTIAL_LIMIT_7D_FORCED_FALLBACK_V1_EVIDENCE.md`

Runtime:
- run: `36853453789`
- commit: `d42dc1aec1342fa3fa37fbbc37190f614cd8cbaf`
- artifact: `11156592789`
- SHA256: `7cf5a68aa6dd2049260e616048763b1cefc285fbe14d0a8361962f24cd690292`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Frozen result

Fixed exact-3% sequential limits with a 7-day market fallback are rejected.

### MATURE

- canonical one-fee: +3588.4%, DD -62.4%
- next-open two-fee control: +3502.7%, DD -62.5%
- 7d limit/fallback: **+1538.5%, DD -71.6%**

Capital factor:
- vs same two-fee next-open: **0.448x**
- vs canonical: **0.437x**

### Validation 1Y

- canonical: +188.7%
- two-fee next-open: +185.8%
- 7d limit/fallback: **+42.2%**

Worst start:
- **-16.4%**

### Execution

MATURE unique direct attempts:
- 29
- full limit recapture: 21 = 72.4%
- sell timeout: 5
- buy timeout after successful sell: 3

Median skipped signal-days:
- MATURE: 61

## Key lesson

`FAVORABLE_EXECUTION_PRICE != BETTER STRATEGY IF IT DELAYS THE RR PATH`

The fixed one-week wait captures execution alpha but destroys more path alpha than it gains.

## Classification

`FIXED_7D_EXACT3_WAIT_THEN_MARKET = REJECTED`

`PROGRESSIVELY_TIGHTENING_LIMIT_EXECUTION = UNTESTED_NEXT_HYPOTHESIS`

## Production boundary

No live/paper/Telegram/exchange/order changes authorized.
