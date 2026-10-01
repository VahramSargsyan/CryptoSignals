# RR Sequential Limit Recapture — Decision Log V1

Date: 2026-10-01
Status: ACTIVE RESEARCH MEMORY
Workflow mode: STRESS_TEST_ONLY
Production change: NONE

Canonical evidence:

`research/relative_rotation/2026-10-01_RR_SEQUENTIAL_LIMIT_RECAPTURE_V1_EVIDENCE.md`

## Frozen result

The proposed strict execution policy:

1. wait for favorable source SELL limit;
2. only after SELL fill, place favorable destination BUY limit;
3. recover the 3% RR reversal threshold;

does **not** satisfy the required 100% historical execution condition.

### Path-visited direct confirmed routes

EXACT_3PCT_LOG:

- 24h complete: 46.4%
- 48h: 53.6%
- 72h: 67.9%
- 7d: 71.4%
- source SELL filled within 7d: 82.1%

Common 25-route cohort observable for 90 days:

- 7d: 72%
- 14d: 76%
- 30d: 76%
- 60d: 80%
- 90d: 80%

Waiting longer does not approach 100%.

### Path integrity

Before the next original RR signal:

- complete: 75%
- source SELL filled: 85.7%
- sold but not rebought before next signal: 3 cases

Therefore the strict waiting policy changes the strategy path in roughly one quarter of tested route/next-signal cases.

## Economic side

When EXACT_3PCT completes:

- median gross improvement vs next-open is about +3.09%;
- after comparing two sequential 0.1% fees against the current one-fee research convention, median advantage remains about +2.99%.

So the economic target is real, but fill reliability fails.

## XRP example

TRX -> XRP 2024-01-14 would have completed the exact-3% sequential limit pair in ~15h and improved execution by ~3%.

It would not have removed the XRP route.

## Classification

`STRICT_3PCT_SEQUENTIAL_LIMIT_RECAPTURE = REJECTED_AS_UNIVERSAL_EXECUTION_POLICY`

`LIMIT_TIMEOUT_MARKET_FALLBACK = UNTESTED_NEXT_HYPOTHESIS`

## Production boundary

No order logic, live/paper strategy, Telegram, universe, or RR routing changes authorized.
