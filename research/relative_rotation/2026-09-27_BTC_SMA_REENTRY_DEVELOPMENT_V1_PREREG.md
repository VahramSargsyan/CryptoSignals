# BTC SMA REENTRY DEVELOPMENT V1 — Tuning Plan

Date: 2026-09-27
Branch: `research/btc-sma-reentry-development-v1`
Mode: STRESS_TEST_ONLY + ECOSYSTEM_PLANNING
Status: DEVELOPMENT_TUNING / FROZEN_SEARCH_SPACE_BEFORE_EXECUTION / NO PRODUCTION CHANGE

## Purpose

Deliberately tune a BTC-based cash re-entry signal on the currently known development history.

This is NOT validation.

The future validation plan is:
1. tune on the current development interval only;
2. freeze the selected candidate set;
3. later load older BTC/crypto history that has not been used in this search;
4. evaluate the frozen candidates there without changing SMA lengths or ranking rules.

The older holdout history is intentionally not downloaded or inspected in this experiment.

## Strategy state machine

Entry to cash remains frozen:
- universe: ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK;
- breadth = number above own causal SMA200;
- crisis entry after 3 consecutive closes breadth <=3;
- actual capital moves to CASH_PROXY at next daily open;
- shadow relative router continues virtually;
- 0.1% actual transition cost.

Cash exit is BTC-based:

For each SMA pair (fast, slow):
- calculate BTCUSDT daily-close SMA(fast) and SMA(slow);
- bullish crossover on date D:
  - fast(D) > slow(D)
  - fast(D-1) <= slow(D-1)
- only a fresh crossover after cash entry can trigger re-entry;
- at D+1 open move actual capital directly from CASH_PROXY into the current shadow-router target;
- resume normal relative rotations;
- another cash defense in the same crisis is forbidden;
- re-arm future crisis defense only after breadth>=5 for 3 closes.

BTC is used only as a market-recovery signal.
BTC is not added to the 8-node relative router.

## Development data boundary

Do NOT use repository `data/BTCUSDT.csv` because its content appears mislabeled/inconsistent with BTC pricing.

Use the existing Binance historical downloader to obtain BTCUSDT only for the current development range:
- start: 2023-05-05 UTC
- end: 2026-09-26 UTC

No earlier BTC history may be downloaded in this development run.

Crypto evaluation:
- reproduction period 2025-03-29 -> 2026-03-28
- already-open 2026 period 2026-03-29 -> 2026-09-26
- full eligible 8-asset history from first SMA200/VOL30-ready date through 2026-09-26
- non-overlapping complete 180d and 120d windows from the same existing anchor.

## Frozen SMA search grid

Fast SMA candidates:
- 5
- 7
- 10
- 12
- 15
- 20
- 25
- 30

Slow SMA candidates:
- 20
- 25
- 30
- 40
- 50
- 60
- 75
- 100

Test every pair where:
- fast < slow.

No EMA, slope threshold, minimum wait, confirmation days, price-above-SMA rule, or secondary filter is allowed in this pass.

## Required outputs for every SMA pair

- reproduction median return / DD
- opened-2026 median return / DD
- full-history median return / DD
- worst start
- positive starts
- cash exposure
- cash entries
- re-entry count
- median and maximum cash wait
- unresolved cash episodes
- 180d robustness:
  - return wins vs frozen LOW_VOL
  - DD wins vs frozen LOW_VOL
  - both wins
- 120d robustness:
  - same metrics

## Frozen development ranking

Do not choose the visually highest full-history return.

Rank all SMA pairs lexicographically by:

1. total number of robustness windows (180d + 120d) beating frozen LOW_VOL on BOTH return and drawdown — higher is better;
2. total robustness DD wins vs LOW_VOL — higher is better;
3. total robustness return wins vs LOW_VOL — higher is better;
4. reproduction-period median max drawdown — less negative is better;
5. reproduction-period median return — higher is better;
6. full-history median return — higher is better.

Keep the TOP 3 development candidates.

The three candidates are a shortlist, not validated winners.

## Future holdout rule

When older historical data becomes available:
- do not change the TOP 3 SMA lengths;
- do not add new candidates;
- do not change ranking criteria;
- do not tune on the holdout;
- evaluate all three candidates plus BASELINE and frozen LOW_VOL;
- preserve negative results.

Because the holdout is earlier in calendar time than the development set, it is a reverse-time external historical holdout, not a prospective OOS test. It can test regime generalization but cannot replace future forward validation.

## Parallel untuned research

Macro / official Fed-liquidity recovery work continues separately.

Results from that branch must not change this SMA search grid or development ranking.

## Runtime impact

Production changed: NONE
Paper-live changed: NONE
Migration required: NO
Automatic execution authorized: NO
Manual confirmation remains required.

TEST_LEVEL: PREREGISTRATION_ONLY
