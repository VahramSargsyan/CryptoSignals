# BTC SMA REENTRY DEVELOPMENT V1 — Evidence

Date: 2026-09-27
Branch: `research/btc-sma-reentry-development-v1`
Mode: STRESS_TEST_ONLY + ECOSYSTEM_PLANNING
Status: DEVELOPMENT_TUNING_EXECUTED / NOT_VALIDATED / NO PRODUCTION CHANGE

## Run identity

- GitHub Actions run: `36304444998`
- Source commit: `7122d2043c2fd504ff950757bfba3603ff20aa2e`
- Artifact: `btc-sma-reentry-development-v1`
- Artifact ID: `10926528362`
- Workflow conclusion: SUCCESS
- reproduction gate: PASS

TEST_LEVEL: GITHUB_ACTIONS_EXECUTED + UNIT_TESTS + REAL_BINANCE_BTC_1D + FROZEN_REPRODUCTION_GATE + DEVELOPMENT_GRID_SEARCH + 180D_120D_ROBUSTNESS

## Data boundary

BTC source:
- live Binance historical downloader
- BTCUSDT 1D
- requested and used only: 2023-05-05 -> 2026-09-26
- 1241 rows
- no missing candles
- no critical quality issues
- older BTC history requested: NO

Repository `data/BTCUSDT.csv` was NOT used because its content appears mislabeled/inconsistent with BTC pricing.

The future older holdout remains unopened.

## Development search

Frozen before execution:

Fast SMA:
- 5, 7, 10, 12, 15, 20, 25, 30

Slow SMA:
- 20, 25, 30, 40, 50, 60, 75, 100

Constraint:
- fast < slow

Pairs tested:
- 58

No EMA, slope, minimum wait, crossover confirmation, macro filter, or secondary threshold was added.

## Frozen development ranking

Lexicographic:
1. robustness windows beating frozen LOW_VOL on BOTH return and DD;
2. robustness DD wins;
3. robustness return wins;
4. reproduction DD;
5. reproduction return;
6. full-history return.

The visually highest full-history return was not used as the ranking rule.

## Development TOP 3 — frozen for future older-history test

### Rank 1 — BTC SMA25 / SMA100

Robustness:
- BOTH wins vs LOW_VOL: 2/13
- DD wins: 5/13
- return wins: 5/13

Reproduction 2025-03-29 -> 2026-03-28:
- median return: **+104.66%**
- median max DD: **-43.20%**
- median cash wait: **35d**

Frozen LOW_VOL comparator:
- +49.05% / -47.13%

Opened 2026:
- median return: **+77.96%**
- median max DD: **-36.68%**
- cash wait: **26d**

Frozen LOW_VOL:
- +32.92% / -18.34%

Full eligible history:
- median return: **+865.37%**
- median max DD: **-56.93%**
- worst start: **+843.11%**
- positive starts: 8/8
- cash exposure: ~48.37%
- median cash wait: **143d**
- max cash wait: **182d**

Frozen LOW_VOL:
- +887.02% / -47.13%

180d robustness:
- return wins: 1/5
- DD wins: 3/5
- BOTH wins: 0/5

120d robustness:
- return wins: 4/8
- DD wins: 2/8
- BOTH wins: 2/8

### Rank 2 — BTC SMA30 / SMA100

Robustness:
- BOTH wins: 2/13
- DD wins: 5/13
- return wins: 4/13

Reproduction:
- +46.13% / -43.20%
- cash wait: 38d

Opened 2026:
- +83.17% / -36.68%
- cash wait: 28d

Full history:
- +593.10% / -55.67%
- cash exposure: ~49.90%
- median cash wait: 146.5d
- max wait: 185d

### Rank 3 — BTC SMA12 / SMA100

Robustness:
- BOTH wins: 2/13
- DD wins: 4/13
- return wins: 6/13

Reproduction:
- +83.20% / -43.20%
- cash wait: 29d

Opened 2026:
- +91.34% / -36.68%
- cash wait: 21d

Full history:
- +846.30% / -53.69%
- cash exposure: ~50.29%
- median cash wait: 133.5d
- max wait: 209d

## Main development observation

All top-8 ranked candidates use slow SMA100.

This is search-boundary saturation:
the current development data prefer the slowest allowed recovery trend filter.

This does NOT prove SMA100 is optimal.
It means only that, inside the preregistered search grid, slower BTC recovery confirmation ranked better under the chosen robustness objective.

The result is also mixed:
- reproduction performance can be very strong;
- opened-2026 return is materially better than LOW_VOL;
- but full-history DD remains worse than LOW_VOL;
- only 2/13 robustness windows improve BOTH return and DD for each top-3 candidate.

Therefore there is no validated winner.

## Why this is still useful

The BTC idea behaves very differently from the shadow-token SMA25/50 test.

Shadow-token SMA25/50:
- full-history +611.48%
- DD -79.20%
- failed.

BTC development candidates:
- can recover substantial return;
- have materially better DD than the shadow-token SMA rule;
- but still do not robustly dominate LOW_VOL.

This supports the user's idea that BTC may be a better market-recovery reference than the individual target token.

## Frozen future validation contract

When older, previously unused history is made available:

Test exactly:
1. BTC SMA25 / SMA100
2. BTC SMA30 / SMA100
3. BTC SMA12 / SMA100

Do not:
- change lengths;
- add candidates;
- change the ranking rule;
- tune on the holdout.

Compare against:
- BASELINE
- frozen LOW_VOL
- frozen-timing CASH

The older-history test is a reverse-time historical holdout, not prospective OOS.

## Verdict

`BTC_SMA_REENTRY_DEVELOPMENT_V1 = PROMISING_DEVELOPMENT_SHORTLIST / NOT_VALIDATED / HOLDOUT_REQUIRED`

No production promotion.

## Runtime impact

Production changed: NONE
Paper-live changed: NONE
Migration required: NO
Automatic execution authorized: NO
Manual confirmation remains required.
