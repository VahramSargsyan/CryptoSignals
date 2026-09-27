# CASH DEFENSE DESTINATION ABLATION V1 — Research Hypothesis

Date: 2026-09-27  
Branch: `research/global-macro-risk-regime-v1`  
Mode: ECOSYSTEM_PLANNING + STRESS_TEST_ONLY  
Status: PREREGISTERED / FROZEN_BEFORE_EXECUTION / NO PRODUCTION CHANGE

## Research question

When the already-frozen crypto breadth state machine enters defensive mode, should actual capital:
1. remain inside crypto in the frozen lowest-VOL30 token; or
2. leave the crypto asset class and hold a cash/stable-value proxy?

This document changes no trading rule. It defines the next clean comparison.

## Why this is now justified

### 1. Independent cross-asset stress is concentrated around crypto entries

Full-history OSS market-regime replication identified eight closed crypto-stress episodes.

Nearest preceding BROAD_RISK_OFF:
- same day: 4/8;
- within 7d: 4/8;
- within 14d: 5/8;
- within 30d: 8/8;
- median lead when found: 6.5 days.

The strongest descriptive concentration is same-day BROAD_RISK_OFF:
- event rate: 50.0%;
- generic crypto NORMAL-day baseline: 12.29%;
- descriptive lift: ~4.07x.

This supports studying a true crypto-vs-cash defensive layer. It does not prove a cash trading rule.

### 2. Frozen low-vol crypto defense is real risk control, but not full asset-class defense

Untouched 2026 validation:
- base rotation: +103.36% median return / -36.68% median max DD;
- frozen low-vol defense: +32.92% / -18.34%;
- defensive occupancy: 144/182 days (~79.1%).

Verdict:
`REAL_RISK_CONTROL_BUT_TOO_MUCH_UPSIDE_SACRIFICE`.

### 3. Earlier USDT smoke testing showed large drawdown compression under a different trigger

Same 2025-03-29 -> 2026-03-28 OOS year:
- no risk gate: +41.6% median / ~-62.2% max DD;
- SMA100 -> USDT: +63.5% / ~-27.0%;
- SMA200 -> USDT: +37.4% / ~-15.2%;
- SMA300 -> USDT: +42.7% / ~-15.2%.

This is supportive evidence for the destination hypothesis only.
It is NOT evidence that the new macro comparator should itself trigger USDT/cash, because that smoke test used a different absolute-SMA gate.

## Clean next experiment

### Frozen timing

Do not change:
- universe: ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK;
- breadth = count above own causal SMA200;
- defense entry after 3 consecutive closes breadth <=3;
- defense exit after 3 consecutive closes breadth >=5;
- next-open execution;
- relative router continues in shadow;
- 0.1% actual transition cost.

### Only variable under test

Variant A — `LOW_VOL_CRYPTO`
- hold the frozen lowest trailing 30d realized-volatility crypto token.

Variant B — `CASH_PROXY`
- hold a stable-value proxy;
- modeled yield = 0%;
- no internal crypto price exposure while defensive.

The research model deliberately separates destination from implementation.
USDT, USDC, fiat cash, money-market exposure, custody venue and counterparty/depeg risk are implementation questions for a later decision.

## Robustness-window contract

Before execution, the robustness windows are frozen as follows:
- build the same feature panel through 2026-09-26;
- find the first date on which every asset has a valid own SMA200 and the frozen LOW_VOL selector is valid;
- that first fully eligible date is the robustness anchor;
- generate consecutive, non-overlapping complete 180-calendar-day windows from that anchor;
- separately generate consecutive, non-overlapping complete 120-calendar-day windows from the same anchor;
- discard only the terminal incomplete remainder;
- reset portfolio/defensive state at each robustness-window start;
- do not shift the anchor or choose a subset after seeing results.

The full-history comparison is also reported once, with one state reset at the same first fully eligible date.

## Required metrics

For both variants:
- median return;
- median/max drawdown;
- worst starting asset;
- positive starts;
- defensive occupancy;
- actual transition count;
- recovery opportunity cost;
- sequential 180-day windows;
- 120-day robustness windows.

Also report:
- return difference CASH minus LOW_VOL by episode;
- drawdown difference;
- whether cash is better specifically during broad crypto selloffs;
- whether low-vol crypto is better during false/short defensive episodes.

## Interpretation gate

Possible outcomes:

### CASH_DOMINANT
Cash reduces drawdown materially and does not create unacceptable additional opportunity cost.

### LOW_VOL_DOMINANT
Low-vol crypto retains enough upside that full cash exit is not justified.

### REGIME_DEPENDENT
Cash is better in deep systemic stress, low-vol crypto is better in mild/short stress.

### MIXED
No stable advantage across windows.

Do not optimize a hybrid after seeing this comparison.
Any hybrid requires a new preregistration.

## Macro confirmation is a separate experiment

Only after the destination ablation should a separate test ask:

`Does BROAD_RISK_OFF improve the timing of CASH entry?`

Do not combine destination change and timing change in the same first experiment.

The broad 30-day 8/8 observation is descriptive context, not an automatic lookback rule.

## Exit problem

This is the main unresolved risk.

Cash has near-zero modeled return while defensive.
Therefore a slow exit can sacrifice even more upside than low-vol crypto.

The external market-regime full-history evidence suggests BROAD_RISK_ON is enriched roughly 1–2 weeks before crypto breadth recovery, but raw daily BROAD_RISK_ON is not a validated exit rule.

Official Fed net-liquidity research is being studied independently as possible recovery context. It must remain separate until evidence exists.

## Runtime impact

Production behavior changed: NONE  
Paper-live behavior changed: NONE  
Real execution authorized: NO  
Migration required: NO  
Manual confirmation remains required.

TEST_LEVEL: PREREGISTRATION_ONLY / BASED_ON_EXISTING_EXECUTED_EVIDENCE
