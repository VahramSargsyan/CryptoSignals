# Grid Unseen Top-10 Validation — XRP / TRX V1

Date: **2026-09-26**  
Strategy: **VAHRAM_LINK_LEVEL_GRID_V1**  
Workflow mode: **STRESS_TEST_ONLY**  
Decision status: **CROSS-ASSET SANITY CHECK PASSED / NOT OOS-PURE / NOT PROMOTED**  
Runtime impact: **NONE**  
Migration required: **NO**

## Purpose

Test whether the existing grid behavior survives on two additional large-cap crypto assets that were not part of the original five-asset research universe.

The two assets were chosen **before looking at strategy results**:

- XRPUSDT
- TRXUSDT

Selection rationale:

- both were in the current/recent top-10 by market capitalization;
- stablecoins were excluded;
- both have enough Binance daily history to provide a full 1095-day prehistory before the evaluation start;
- neither XRP nor TRX participated in the prior WIDE / capital / H-L / MA / volatility optimization work.

No parameter was changed for either asset.

## Data boundary

The accessible XRP/TRX daily CSV source was continuous through **2026-01-05**. Its 2026-01-06 candle was incomplete compared with the canonical Binance artifact, so that date was deliberately excluded.

For fairness, all seven assets were therefore evaluated on the same window:

- evaluation start: **2023-09-25**
- evaluation end: **2026-01-05**
- H/L prehistory: **1095 prior daily candles**
- H/L refresh: **30 candles**
- fee: **10 bps**
- slippage: **5 bps**

The external XRP/TRX source was sanity-checked against canonical BTC candles on several dates. Values matched through 2026-01-05; the incomplete 2026-01-06 row was the reason for the cutoff.

## Frozen strategy configurations

### BASE

- p=1 linear-depth allocation
- Micro +1
- Mid +10 recovery
- existing Mid percent targets
- 100% positive-profit reinvestment
- no permanent runner

### WIDE

- p=1 linear-depth allocation
- Micro +6
- Mid +18 recovery
- existing Mid percent targets
- 100% positive-profit reinvestment
- no permanent runner

No XRP/TRX-specific tuning.

## New-asset results

| Asset | WIDE return | WIDE max DD | WIDE closed exits | BASE return | BASE max DD | Buy & Hold | B&H max DD |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| XRP | **+196.29%** | **32.45%** | 297 | +171.45% | 30.95% | **+366.52%** | 49.09% |
| TRX | **+43.82%** | **8.79%** | 124 | +35.99% | 7.91% | **+248.69%** | 51.11% |

Two-asset geometric return:

- WIDE: **+106.43%**
- BASE: **+92.13%**
- Buy & Hold: **+303.33%**

Median max drawdown across the two new assets:

- WIDE: **20.62%**
- BASE: **19.43%**
- Buy & Hold: **50.10%**

## What generalized

The main positive result is not that the grid beat passive holding.

It did **not**.

What generalized out of the original five-asset research set:

1. WIDE remained profitable on both new assets.
2. BASE remained profitable on both new assets.
3. WIDE again outperformed BASE on both:
   - XRP: +196.29% vs +171.45%
   - TRX: +43.82% vs +35.99%
4. Grid drawdown remained much lower than Buy & Hold on both assets.

This reduces the probability that the WIDE improvement was purely an artifact of the original five assets.

It does **not** prove universal superiority.

## Same-window original five

For comparison, on the same shortened 2023-09-25 through 2026-01-05 window:

| Asset | WIDE | WIDE DD | Buy & Hold | B&H DD |
| --- | ---: | ---: | ---: | ---: |
| LINK | +426.82% | 38.67% | +98.87% | 62.65% |
| ETH | +137.99% | 29.72% | +103.51% | 63.75% |
| SOL | +860.24% | 26.50% | +610.51% | 59.77% |
| BNB | +129.80% | 8.27% | +335.93% | 36.77% |
| BTC | +75.91% | 5.00% | +256.69% | 32.02% |

Original-five geometric return on this common window:

- WIDE: **+244.71%**
- BASE: **+220.95%**
- Buy & Hold: **+238.91%**

## Seven-asset aggregate

Adding XRP and TRX to the five-asset set:

- WIDE geometric return: **+197.73%**
- BASE geometric return: **+177.19%**
- Buy & Hold geometric return: **+256.19%**

Median max drawdown:

- WIDE: **26.50%**
- BASE: **24.12%**
- Buy & Hold: **51.11%**

WIDE beat Buy & Hold on **3 of 7** assets:

- LINK
- ETH
- SOL

Buy & Hold beat WIDE on:

- BTC
- BNB
- XRP
- TRX

## Interpretation

This test weakens a simple overfitting explanation.

If WIDE had been strongly overfit to the original five assets, a plausible failure mode would be:

- negative return on new assets;
- WIDE losing to BASE;
- extreme drawdown / unstable behavior.

That did not happen.

Instead:

- both new assets were profitable;
- WIDE beat BASE on both;
- drawdown stayed controlled.

However, the test also weakens any claim that the grid is a universal return-maximizer.

On XRP and TRX, Buy & Hold captured much more of the long-term upward move.

The current evidence therefore supports a narrower characterization:

> The grid appears to generalize as a volatility-harvesting / drawdown-reducing mechanism, while its ability to beat Buy & Hold depends strongly on the asset's path structure.

## Important evidence limits

This is not clean future OOS evidence:

- the evaluation dates overlap the same market era used in prior research;
- XRP/TRX were unseen assets, but the strategy architecture was already developed before testing them;
- the XRP/TRX data source differs from the canonical saved Binance artifact source, although overlapping BTC rows were checked and the common cutoff was chosen conservatively.

Therefore status remains research-only.

## Research decision

1. Do not modify WIDE from this test.
2. Do not tune XRP/TRX separately.
3. Treat the result as **cross-asset generalization evidence**, not a strategy promotion.
4. Expand only with additional preregistered assets / future data, not by selecting winners after seeing results.
5. Keep paper-live unchanged.

## Duplicate-work guard

Future chats should not present XRP/TRX as untested assets for WIDE unless:

- the horizon is extended with a canonical fresh source;
- a clean future OOS period is used;
- strategy mechanics materially change.

## Paper-live boundary

No paper-live profile changed.

Still frozen:

- CONTROL_BASE: +1/+10
- CANDIDATE_WIDE: +6/+18

No result here authorizes live-money execution.
