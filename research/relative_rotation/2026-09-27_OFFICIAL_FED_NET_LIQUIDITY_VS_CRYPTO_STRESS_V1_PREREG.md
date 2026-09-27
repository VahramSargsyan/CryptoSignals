# OFFICIAL FED NET LIQUIDITY VS CRYPTO STRESS V1 — Preregistration

Date: 2026-09-27
Branch: `research/global-macro-risk-regime-v1`
Mode: STRESS_TEST_ONLY + ECOSYSTEM_PLANNING
Status: RESEARCH_ONLY / NO PRODUCTION CHANGE

## Question

Do causal changes in official U.S. Fed net liquidity show a repeatable relationship with the starts and ends of the already-frozen 8-asset crypto stress episodes?

This pass is descriptive. It does not create a trading rule.

## Frozen crypto side

No change to:
- ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK
- own SMA200
- entry after 3 consecutive closes breadth <=3
- recovery after 3 consecutive closes breadth >=5
- next-open semantics
- VOL30 / frozen defensive asset
- relative router or costs

Use the same full-history eligible crypto breadth period already established in the successful OSS market-regime replication:
- first fully SMA200-ready date: 2023-11-20
- one explicit state reset at that boundary
- eight closed crypto-stress episodes expected

## Official liquidity identity

Weekly same-date composite:

`NET_LIQUIDITY = RESPPA_N.WW - RESPPLLDT_N.WW - ON_RRP`

Units: USD millions.

Inputs:
- `RESPPA_N.WW`: Fed total assets, H.4.1
- `RESPPLLDT_N.WW`: U.S. Treasury General Account, H.4.1
- ON RRP: NY Fed `totalAmtAccepted` on the same Wednesday, converted from USD to USD millions

No FRED endpoint is used.

## Causality

For each Wednesday H.4.1 observation:
- observation date = Wednesday
- conservative availability date = Friday (observation date +2 calendar days)
- the same-date Wednesday ON RRP value is held until the H.4.1 components are available
- daily crypto dates receive only the latest liquidity observation whose availability date is <= the crypto date

No backward fill before first availability.

## Preregistered transformations

No threshold optimization.

For each weekly net-liquidity observation compute absolute change over:
- 4 weeks
- 13 weeks
- 26 weeks

These three horizons are reported together. No best horizon will be selected post hoc and called a validated rule.

## Entry hypothesis

At crypto stress-entry signal dates, negative net-liquidity change may be enriched versus generic crypto NORMAL days.

Report, for each 4/13/26-week horizon:
- count and fraction of the 8 entry signals with negative change
- state-conditioned baseline fraction of NORMAL crypto days with negative change
- descriptive lift = event fraction / baseline fraction

## Exit hypothesis

At crypto recovery/exit signal dates, positive net-liquidity change may be enriched versus generic crypto DEFENSIVE days.

Report, for each 4/13/26-week horizon:
- count and fraction of the 8 exit signals with positive change
- state-conditioned baseline fraction of DEFENSIVE crypto days with positive change
- descriptive lift

## Required outputs

- official weekly component table
- causal weekly net-liquidity table with availability dates
- daily crypto + as-of liquidity alignment
- the eight crypto stress episodes
- per-episode entry/exit liquidity snapshots
- aggregate entry/exit sign statistics
- state-conditioned baselines
- latest fixed-end liquidity state through 2026-09-26

## Interpretation discipline

- No new threshold, score, or automatic mode switch.
- No retuning of 4/13/26 weeks on this sample.
- Do not treat descriptive lift as statistical significance.
- Weekly observations are serially correlated and the eight episodes are not IID.
- Preserve a negative or mixed result.
- Any possible trading use requires a new separately preregistered rule and validation boundary.

## Runtime impact

Production behavior changed: NONE
Paper-live behavior changed: NONE
Migration required: NO
