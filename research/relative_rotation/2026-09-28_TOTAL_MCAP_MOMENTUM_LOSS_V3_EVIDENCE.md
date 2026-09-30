# TOTAL MARKET CAP MOMENTUM-LOSS V3 — EVIDENCE

Date: 2026-09-28  
Workflow mode: STRESS_TEST_ONLY  
Production/live changes: NONE

## Runtime identity

- Source: `data/market/cryptocap_total_d1.csv`
- Symbol: `CRYPTOCAP:TOTAL`
- Provider: TradingView
- Range: 2022-11-29 through 2026-09-26
- Rows: 1,398
- GitHub Actions run: `36471341859`
- Source commit: `4b2bbfbce3ec7f92d939ce1e2c7de37e2d406758`
- Artifact ID: `10991269506`
- Artifact SHA256: `558a238d74e87d400c832767e5d114d80dc956496e6e8e0368d99d7d55997124`
- TEST_LEVEL: `GITHUB_ACTIONS_REPOSITORY_CACHE_STRESS_TEST`

No external market data was used.

## Frozen feature

At each upside SMA100 threshold trigger:

- TOTAL 5d momentum is compared with the previous 5d momentum;
- SMA25 5d speed is compared with the previous 5d speed.

Labels:
- DUAL_ACCELERATION: both changes positive
- DUAL_DECELERATION: both changes negative
- MIXED_MOMENTUM: otherwise

V1 RETURN50 / BREAKOUT25 semantics are reused unchanged.

## Main diagnostic result

Across all 38 upside SMA100 threshold observations from +10% through +40%:

**DUAL_DECELERATION occurred zero times at the threshold trigger.**

This means a deceleration-at-trigger classifier is structurally poorly matched to the question. A new stretch threshold is usually crossed while the move is still accelerating or while one component is accelerating and the other is slowing.

Therefore V3 does not provide the intended "turning point" signal at trigger time.

## Aggregate +10% through +30%

| Momentum label | N | Return first | Breakout first | Unresolved | Median extra extension | Median days to half-gap | Median days to SMA100 touch |
|---|---:|---:|---:|---:|---:|---:|---:|
| DUAL_ACCELERATION | 20 | 35.0% | 65.0% | 0.0% | +9.45 pp | 40 | 70 |
| MIXED_MOMENTUM | 16 | 25.0% | 68.8% | 6.3% | +8.17 pp | 41.5 | 64 |
| DUAL_DECELERATION | 0 | - | - | - | - | - | - |

The two observed trigger-state groups are very similar in continuation rate.

Therefore:
- acceleration at the initial threshold does not provide a clean discriminator;
- the intended "loss of speed" must be searched **after** the market is already stretched, not at the first threshold crossing.

## Threshold detail

- +10%: acceleration N=7, return-first 57.1%; mixed N=6, return-first 33.3%
- +15%: acceleration N=4, breakout-first 100%; mixed N=4, breakout-first 100%
- +20%: acceleration N=4 and mixed N=4; both 50/50 return vs breakout
- +25%: acceleration N=2, breakout-first 100%; mixed N=2, one breakout and one censored
- +30%: acceleration N=3, breakout-first 66.7%
- +40%: one acceleration and one mixed; both return-first

Samples are too small and inconsistent to justify a production interpretation.

## Current cached point — 2026-09-26

- distance above SMA100: +21.90%
- TOTAL 5d momentum: **-1.52%**
- prior 5d TOTAL momentum: **+12.95%**
- TOTAL momentum change: **-14.47 pp**
- SMA25 current 5d slope: **+1.98%**
- prior SMA25 5d slope: **+0.87%**
- SMA25 speed change: **+1.10 pp**
- label: **MIXED_MOMENTUM**

So the latest cached state already shows an important nuance:
- TOTAL price momentum slowed sharply / turned negative over 5 days;
- SMA25 itself is still accelerating.

This is exactly why a single "angle" or one acceleration metric is insufficient.

## Interpretation

V3 is useful mainly as a **negative result**:

`threshold crossing + acceleration label`

does not identify the return point.

The concept should be moved one step later in time:

`market already stretched -> first subsequent joint deceleration -> what happens next?`

That is a separate research question and must be preregistered separately.

## Boundary

No paper/live, Telegram, allocation, execution, or universe logic changed.

TEST_LEVEL: GITHUB_ACTIONS_REPOSITORY_CACHE_STRESS_TEST
