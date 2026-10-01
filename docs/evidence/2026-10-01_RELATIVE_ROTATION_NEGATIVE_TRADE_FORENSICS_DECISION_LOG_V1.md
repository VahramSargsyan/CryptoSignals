# Relative Rotation Negative Trade Forensics — Decision Log V1

Date: 2026-10-01  
Status: ACTIVE RESEARCH MEMORY  
Workflow mode: STRESS_TEST_ONLY  
Production change: NONE

Canonical evidence:

`research/relative_rotation/2026-10-01_NEGATIVE_TRADE_FORENSICS_V1_EVIDENCE.md`

## Frozen findings

Current U10+DDG MATURE:
- +3588.4% median baseline;
- 30 unique completed trade episodes;
- 7 losing episodes;
- unique-trade loss rate 23.3%.

### Key clustering result

All 7 historical losing episodes had **3 or 4 independent warning domains active**.

No losing episode had only 0-2 warning domains.

Warning domains:
1. RR confirmation weakness;
2. network/destination weakness;
3. BTC/breadth/defensive market weakness;
4. TOTAL/BTC-ETH/ETH-BTC risk state.

This is a useful risk-attention signal, but not a deterministic veto:
- 19 trades had 3-4 warnings;
- 12 of those were profitable.

### Hard-veto result

`SCORE_GE3_HOLD` is rejected as an automatic rule:
- MATURE +264.6% vs +3588.4% baseline;
- DD -78.6% vs -62.4%.

Path changes caused by vetoing trades destroy the apparent static benefit.

### Less aggressive composite gates

Some gates reduce DD while retaining substantial return, especially:
- RR + NETWORK + MARKET:
  - MATURE +2795.0%
  - DD -52.6%
  - 2Y +2369.0%
- signal <20% + BROAD_BEAR:
  - MATURE +3091.5%
  - DD -61.2%
  - validation 1Y +198.0%

These are **post-selection exploratory findings**, not production candidates.

## Decision classification

`MULTI_INDICATOR_WARNING_SCORE = RESEARCH-USEFUL / AUTO-VETO-NOT-APPROVED`

Preferred use:
- forward warning logging;
- manual review;
- possible separately preregistered position-sizing research.

## Delayed confirmation

Historical local check:
- reversal 4% changes 0/7 losses;
- reversal 5% changes 2/7;
- two-close persistence changes 0/7;
- three-close persistence changes 1/7 while changing 11/23 winners.

No delayed-confirmation rule is approved from this forensic pass.

## Fundamental analysis

Not evaluated because point-in-time historical fundamental data is not currently frozen.

The seven losing trades should be retained as a benchmark when a causal fundamental layer becomes available.

## No-repeat

Do not repeat unchanged on the same history.

## Production boundary

No live/paper/Telegram/exchange/universe/sizing changes authorized.
