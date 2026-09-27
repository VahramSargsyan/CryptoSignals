# CASH SHADOW SMA25/50 REENTRY V1 — Evidence

Date: 2026-09-27
Branch: `research/cash-shadow-sma25-50-reentry-v1`
Mode: STRESS_TEST_ONLY + ECOSYSTEM_PLANNING
Status: EXECUTED / RESEARCH_ONLY / NO PRODUCTION CHANGE

## Run identity

- GitHub Actions run: `36303730983`
- Source commit: `1bd02b9a9ca637bad5bd8c869f045065daff42f2`
- Artifact: `cash-shadow-sma25-50-reentry-v1`
- Artifact ID: `10926721005`
- Workflow conclusion: SUCCESS
- reproduction gate: PASS

TEST_LEVEL: GITHUB_ACTIONS_EXECUTED + UNIT_TESTS + FROZEN_REPRODUCTION_GATE + REAL_BINANCE_1D + FULL_HISTORY + NON_OVERLAPPING_180D_120D_ROBUSTNESS

## State machine tested

Entry:
- frozen breadth<=3 for 3 closes;
- next-open actual capital moves to CASH_PROXY.

While in cash:
- shadow router continues;
- prospective next-open shadow target is determined causally;
- calculate own SMA25 and SMA50 for that target;
- only a fresh post-entry bullish crossover counts:
  SMA25(D) > SMA50(D) and SMA25(D-1) <= SMA50(D-1).

Exit:
- crossover is observed after close D;
- at D+1 open actual capital moves directly CASH_PROXY -> current shadow target.

After exit:
- ordinary router resumes;
- second cash entry inside the same crisis is forbidden;
- future crisis defense re-arms only after breadth>=5 for 3 closes.

No macro or Fed-liquidity signal was used.

## Re-entry timing

Full eligible history:
- median cash wait across starts: **47 days**
- maximum cash wait: **113 days**
- median cash exposure: **27.06%**
- median cash entries: 5
- median SMA re-entry exits: 5
- unresolved cash starts at full-history end: 0

Historical-year reproduction 2025-03-29 -> 2026-03-28:
- median cash wait: **44 days**
- maximum wait: **74 days**
- median cash exposure: **24.11%**

Opened 2026-03-29 -> 2026-09-26:
- cash entry count: 1
- wait until SMA re-entry: **39 days**
- cash exposure: **21.43%**

Example ATOM-start chronology:
- 2024-04-20 ENTER CASH -> 2024-05-30 SMA re-entry: 40d
- 2024-06-14 ENTER CASH -> 2024-10-05 SMA re-entry: 113d
- 2024-11-05 ENTER CASH -> 2024-11-13 SMA re-entry: 8d
- 2025-02-27 ENTER CASH -> 2025-04-15 SMA re-entry: 47d
- 2025-11-02 ENTER CASH -> 2026-01-15 SMA re-entry: 74d

## Reproduction period 2025-03-29 -> 2026-03-28

Comparators:
- BASELINE: +43.82% / -61.57%
- frozen LOW_VOL: +49.05% / -47.13%
- frozen-timing CASH: +5.93% / -43.20%

SMA25/50 re-entry:
- median return: **+5.41%**
- worst start: **-52.10%**
- positive starts: **7/8**
- median max DD: **-72.22%**
- worst max DD: **-72.22%**
- median actual transitions: 9
- median cash entries: 2
- median cash days: 88

Interpretation:
The SMA25/50 crossover did not solve the historical-year re-entry problem. It sacrificed most of the LOW_VOL return and produced a materially worse drawdown than both LOW_VOL and baseline.

## Opened 2026 period

2026-03-29 -> 2026-09-26:

Prior references:
- BASELINE: +103.36% / -36.68%
- frozen LOW_VOL: +32.92% / -18.34%

SMA25/50 re-entry:
- median return: **+69.15%**
- worst start: **+39.33%**
- positive starts: **8/8**
- median max DD: **-36.68%**
- worst max DD: **-40.80%**
- cash wait: 39d

Interpretation:
The signal recovered substantially more upside than frozen LOW_VOL, but it recovered essentially no drawdown protection versus baseline.

## Full eligible history 2023-11-20 -> 2026-09-26

Comparators:
- BASELINE: +646.96% / -71.23%
- frozen LOW_VOL: +887.02% / -47.13%
- frozen-timing CASH: +258.06% / -67.56%

SMA25/50 re-entry:
- median return: **+611.48%**
- worst start: **+595.07%**
- positive starts: 8/8
- median max DD: **-79.20%**
- worst max DD: **-79.20%**

The full-history SMA variant:
- underperformed BASELINE on return;
- underperformed frozen LOW_VOL on return;
- had worse max drawdown than BASELINE, LOW_VOL, and frozen CASH.

## Robustness

180d windows, 5 total:
- beats LOW_VOL return: **1/5**
- beats LOW_VOL drawdown: **2/5**
- beats LOW_VOL on both: **0/5**
- windows with unresolved cash at end: 2/5

120d windows, 8 total:
- beats LOW_VOL return: **3/8**
- beats LOW_VOL drawdown: **1/8**
- beats LOW_VOL on both: **1/8**
- windows with unresolved cash at end: 3/8

The result is not robust.

## Why the intuitive SMA rule failed

SMA25/SMA50 is a lagging, asset-specific trend confirmation.

Two failure modes appeared:

1. Too late:
   - some cash waits lasted 74-113 days;
   - much of the recovery move could occur before a fresh bullish crossover.

2. Still not market-wide safety:
   - a crossover on the current shadow target does not mean broad crypto stress is over;
   - after re-entry, the portfolio can still experience deep market drawdown.

Therefore the rule can be simultaneously late for upside and insufficient for systemic risk.

## Verdict

`CASH_SHADOW_SMA25_50_REENTRY_V1_VERDICT = FAIL / LAGGING_REENTRY / RISK_CONTROL_NOT_IMPROVED / DO_NOT_PROMOTE`

Supported:
- the hypothesis gives an objective, causal re-entry event;
- it produces reasonable finite waits rather than arbitrary fixed days;
- in opened 2026 it recovered materially more upside than LOW_VOL.

Not supported:
- SMA25/SMA50 on the shadow target as a standalone cash re-entry condition;
- production promotion;
- searching neighboring SMA lengths on the same history and calling the winner validated.

## Next research implication

Do not tune SMA20/40, SMA20/50, SMA30/50, etc. on this same history.

The unresolved problem remains a market-wide recovery signal, not merely a target-asset trend signal.

The already-independent macro / official liquidity recovery work remains the cleaner next family because it can test systemic recovery rather than one token's local trend crossover.

## Runtime impact

Production changed: NONE
Paper-live changed: NONE
Migration required: NO
Automatic execution authorized: NO
Manual confirmation remains required.
