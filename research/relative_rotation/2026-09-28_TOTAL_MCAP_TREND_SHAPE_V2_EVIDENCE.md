# TOTAL MARKET CAP TREND SHAPE V2 — EVIDENCE

Date: 2026-09-28
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Runtime identity

- Source: `data/market/cryptocap_total_d1.csv`
- Symbol: `CRYPTOCAP:TOTAL`
- Provider: TradingView
- Range: 2022-11-29 through 2026-09-26
- Rows: 1,398
- GitHub Actions run: `36462662965`
- Source commit: `dc3130518eb9247034fd78e79be848a168c63a1a`
- Artifact ID: `10988402560`
- Artifact SHA256: `3ba0746aa11ff56d7498af40b8d4a15b92a5305253f6d50128375da0ecb644b9`
- TEST_LEVEL: `GITHUB_ACTIONS_REPOSITORY_CACHE_STRESS_TEST`

No external market data was used.

## Frozen regimes

POWERED_TREND:
- SMA25 > SMA50 > SMA100
- all three 5-day slopes positive
- 25-to-100 SMA fan still expanding over the prior 5 days

CATCHUP_PHASE:
- same bullish stack
- all three slopes positive
- fan no longer expanding

TRANSITION_MIXED:
- everything else

V1 RETURN50 / BREAKOUT25 event semantics were reused unchanged.

## Aggregate result, +10% through +30% SMA100 observations

| Regime | N | Return first | Breakout first | Unresolved | Median extra extension | Median days to half-gap | Median days to SMA100 touch |
|---|---:|---:|---:|---:|---:|---:|---:|
| POWERED_TREND | 12 | 33.3% | 66.7% | 0.0% | +11.40 pp | 46 | 70 |
| TRANSITION_MIXED | 22 | 31.8% | 68.2% | 0.0% | +8.73 pp | 38 | 59 |
| CATCHUP_PHASE | 2 | 0.0% | 50.0% | 50.0% | +2.17 pp | 9 | 46 |

The simple 3-state shape classifier did **not** create a clean aggregate separation between POWERED_TREND and TRANSITION_MIXED:
- continuation-first rates were 66.7% vs 68.2%;
- POWERED_TREND did show larger median further extension (+11.40 pp vs +8.73 pp), but this is not enough to call the classifier discriminative.

CATCHUP_PHASE has only two observations and cannot support a conclusion.

## Threshold detail

| SMA100 trigger | Regime | N | Return first | Breakout first | Unresolved | Median extra extension |
|---:|---|---:|---:|---:|---:|---:|
| +10% | POWERED_TREND | 1 | 100.0% | 0.0% | 0.0% | +1.21 pp |
| +10% | TRANSITION_MIXED | 12 | 41.7% | 58.3% | 0.0% | +9.73 pp |
| +15% | CATCHUP_PHASE | 1 | 0.0% | 100.0% | 0.0% | +4.34 pp |
| +15% | TRANSITION_MIXED | 7 | 0.0% | 100.0% | 0.0% | +8.30 pp |
| +20% | POWERED_TREND | 5 | 40.0% | 60.0% | 0.0% | +5.16 pp |
| +20% | TRANSITION_MIXED | 3 | 66.7% | 33.3% | 0.0% | +0.43 pp |
| +25% | POWERED_TREND | 3 | 0.0% | 100.0% | 0.0% | +20.94 pp |
| +25% | CATCHUP_PHASE | 1 | 0.0% | 0.0% | 100.0% | 0.00 pp |
| +30% | POWERED_TREND | 3 | 33.3% | 66.7% | 0.0% | +12.21 pp |
| +40% | POWERED_TREND | 2 | 100.0% | 0.0% | 0.0% | +4.64 pp |

High-threshold samples are small and threshold rows inside one broad move are not fully independent.

## Current cached classification — 2026-09-26

- TOTAL distance above SMA100: **+21.90%**
- SMA25 5d slope: **+1.98%**
- SMA50 5d slope: **+2.79%**
- SMA100 5d slope: **+1.42%**
- fan spread: **14.92%**
- fan change over 5d: **+0.63 pp**
- regime: **POWERED_TREND**

The closest preregistered historical bucket is the +20% / POWERED_TREND group:
- N=5
- RETURN_FIRST: 40%
- BREAKOUT_FIRST: 60%
- median additional extension: +5.16 pp
- median days to half-gap: 38
- median days to SMA100 touch: 78.5

This is descriptive historical context, not a forecast or trading instruction.

## Interpretation

The V2 result rejects the simplistic idea that "opening fan" alone gives a clean continuation filter.

What survives:
1. distance from SMA100 alone is insufficient;
2. a strong rising stack can stay stretched for weeks;
3. at +20% with POWERED_TREND the historical sample still leaned continuation-first, but only 5 observations exist;
4. +25% POWERED_TREND showed 3/3 continuation-first in this cache, but this is too small for a production threshold;
5. +40% showed 2/2 return-first, also far too small.

A more useful next research direction would need either:
- more historical TOTAL data across older cycles; or
- a different causal feature such as change in TOTAL momentum / slope acceleration rather than fan shape alone.

No production promotion is justified from V2.

## Boundary

Do not change paper/live logic, Telegram, allocations or execution based on this evidence.

TEST_LEVEL: GITHUB_ACTIONS_REPOSITORY_CACHE_STRESS_TEST
