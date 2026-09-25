# LINK Grid External Crowd / Market Context Research V1

Date: **2026-09-26**  
Strategy: **VAHRAM_LINK_LEVEL_GRID_V1**  
Workflow mode: **STRESS_TEST_ONLY**  
Decision status: **PARTIAL EXECUTION / NO PROMOTION / DATA BLOCKS RECORDED**  
Runtime impact: **NONE**  
Migration required: **NO**

## Research question

Can information that is not merely another transform of the token's own spot-price history improve the grid exit regime?

The grid entry engine was left unchanged. External state was allowed to select only among:

- NARROW: Micro +3 / Mid +12
- WIDE: Micro +6 / Mid +18
- EXTRA_WIDE: Micro +9 / Mid +24

Targets were frozen at entry.

## Five proposed context families

1. Funding rate
2. Open interest / delta open interest
3. Liquidations
4. Crypto Fear & Greed
5. Broad market / liquidity context

Execution status:

| Family | Status |
| --- | --- |
| Funding | BACKTEST_EXECUTED |
| Open interest | DATA_ACCESS_BLOCKED_FOR_EQUAL_5_ASSET_TEST |
| Liquidations | DATA_BLOCKED_NO_TRUSTWORTHY_EQUAL_HISTORY |
| Fear & Greed | BACKTEST_EXECUTED |
| Broad market / liquidity context | BACKTEST_EXECUTED |

No OI or liquidation result was fabricated or replaced silently by a proxy.

## Shared test window

The external pinned Fear & Greed snapshot used here ends on 2026-08-15. To keep every executed comparison on the same information horizon, all variants were compared on:

- trading start: 2023-09-25
- trading end: 2026-08-15
- 1095 prior daily candles as prehistory before the first trade
- universe: LINKUSDT, ETHUSDT, SOLUSDT, BNBUSDT, BTCUSDT
- Binance Spot daily grid candles
- 10 bps fee
- 5 bps slippage
- linear-depth capital p=1
- 100% positive-profit reinvestment
- no permanent runner
- external state for candle T used only information available no later than T-1

Because this horizon is shorter than the canonical 2026-09-24 three-year evaluation, its WIDE control is **not** the previously reported +253.03% figure.

Same-window WIDE control:

- five-asset geometric return: **+168.87%**
- median max drawdown: **38.67%**
- closed exits: **1,538**

## External data provenance

Pinned data snapshot used for executed signals:

- public repository: `olaxbt/ai-market-maker`
- repository license: AGPL-3.0
- no source code was copied into CryptoSignals
- only public, pinned research data values were used for this exploratory analysis

Underlying sources recorded by that snapshot:

- funding: Binance Vision monthly funding-rate archives
- Fear & Greed: Alternative.me
- liquidity context: DefiLlama public aggregate, already recorded as lag-1 in the source snapshot

This evidence does not adopt or incorporate AGPL implementation code.

## Signal definitions

### Funding

For each asset:

1. aggregate funding observations to a daily mean;
2. compare the previous completed day's mean with a causal trailing 252-day distribution;
3. <= 20th percentile: contrarian-positive / EXTRA_WIDE (+9/+24);
4. >= 80th percentile: crowded-long / NARROW (+3/+12);
5. otherwise: WIDE (+6/+18).

At least 100 historical daily observations were required before percentile classification.

### Fear & Greed

Using the previous completed day's index:

- value < 25: EXTRA_WIDE (+9/+24)
- value > 75: NARROW (+3/+12)
- otherwise: WIDE (+6/+18)

### Broad market / liquidity context

A conservative cross-market state used:

- previous-day 7-day return breadth across LINK, ETH, SOL, BNB, BTC;
- lagged 7-day stablecoin-supply direction from the pinned DefiLlama snapshot.

Original directional mapping:

- >= 4/5 assets positive AND stablecoin 7d supply change positive -> EXTRA_WIDE
- <= 1/5 assets positive AND stablecoin 7d supply change negative -> NARROW
- otherwise -> WIDE

This is a subset of the broader market-context idea. Historical BTC-dominance data was not added in this pass.

## Main results

| Variant | Five-asset geometric return | Median DD | Closed exits | Assets beating WIDE |
| --- | ---: | ---: | ---: | ---: |
| WIDE +6/+18 control | **+168.87%** | 38.67% | 1,538 | — |
| Funding regime | **+170.62%** | **37.31%** | 1,566 | 2/5 |
| Fear & Greed regime | +165.96% | 38.72% | 1,615 | 1/5 |
| Broad market/liquidity regime | +160.15% | 39.32% | 1,634 | 3/5 |
| Stablecoin-only diagnostic | +159.78% | 39.08% | 1,365 | 3/5 |
| 2-of-3 consensus: funding + F&G + market | +167.73% | 38.50% | 1,567 | 2/5 |

### Funding by asset

| Asset | WIDE | Funding regime |
| --- | ---: | ---: |
| LINK | +359.72% | **+385.75%** |
| ETH | +73.05% | **+75.28%** |
| SOL | **+516.06%** | +514.26% |
| BNB | **+87.16%** | +83.69% |
| BTC | **+53.19%** | +51.07% |

Funding improves the aggregate primarily through LINK and ETH; it is not a broad five-asset win.

### Fear & Greed by asset

- LINK: +352.72%
- ETH: +73.64%
- SOL: +510.38%
- BNB: +82.30%
- BTC: +52.12%

Only ETH exceeded its same-window WIDE control.

### Broad market/liquidity by asset

- LINK: +286.19%
- ETH: +67.83%
- SOL: +519.98%
- BNB: +93.41%
- BTC: +53.32%

It beat WIDE on SOL, BNB and BTC but gave up too much on LINK/ETH.

## Independent-window stress check

The common external-data horizon was split into three equal 352-day windows. Each window restarted the strategy with fresh normalized capital and its own full 1095-day prehistory.

Results versus constant WIDE across 15 asset-window comparisons:

| Signal | Wins vs WIDE | Mean return difference | Median return difference |
| --- | ---: | ---: | ---: |
| Funding | 7/15 | -0.37 pp | -0.09 pp |
| Fear & Greed | 6/15 | -0.27 pp | -0.19 pp |
| Broad market/liquidity | 9/15 | +0.52 pp | +0.51 pp |
| 2-of-3 consensus | 4/15 | -0.67 pp | -0.02 pp |

Important interpretation:

- Funding's slightly better full-period compound did **not** reproduce as a stable window advantage.
- Broad market/liquidity had the better window breadth, but its full-period compound was materially below WIDE because some large-trend periods were weakened.
- Combining the three available external signals did not improve robustness.

## Direction sensitivity probes

To avoid assuming that the initial economic interpretation was automatically correct, each single signal was also inverted as a sensitivity check.

Full-period results:

| Sensitivity variant | Geometric return | Median DD | Assets beating WIDE |
| --- | ---: | ---: | ---: |
| Funding inverted | +155.54% | 39.14% | 2/5 |
| Fear & Greed inverted | +168.27% | 39.10% | 3/5 |
| Market context inverted | **+172.00%** | 37.88% | 3/5 |

The inverted market-context result looks attractive on the single compound period, but it failed the equal-window check:

- wins: 7/15
- mean difference vs WIDE: -0.51 pp
- median difference: -0.33 pp

Therefore it is explicitly classified as **POST_HOC_INTERESTING / NOT PROMOTED**.

A post-hoc 2-of-3 combination using the inverted market orientation reached about +170.74%, but also failed the window robustness check (7/15 wins; negative mean and median differences). It is not promoted.

## Open interest status

Open interest was not assigned a performance number in this pass.

Reasons:

- Binance live historical-open-interest REST access is limited to a recent window.
- Binance Vision does publish historical futures metrics archives containing `sum_open_interest`, but equal five-asset history was not materialized into this runtime during this pass.
- A public Hugging Face Binance futures dataset was identified with per-symbol `*_metrics.parquet` files containing open interest, but it was not imported into the reproducible CryptoSignals evidence pipeline here.

Decision: **DATA_ACCESS_BLOCKED_FOR_THIS_PASS**, not a negative strategy result.

Future OI research should use a pinned, checksummed local snapshot before testing.

## Liquidation status

No valid three-year, five-asset liquidation result was produced.

The public Binance liquidation stream is suitable for forward collection, while accessible historical force-order endpoints concern user/account history and recent windows rather than a clean public three-year market liquidation archive.

Decision: **DATA_BLOCKED / DO NOT FABRICATE PROXY**.

A future test may use:

- a licensed historical liquidation dataset with timestamp-level provenance; or
- forward collection from the public liquidation stream beginning from a frozen start date.

OI/taker-flow liquidation proxies must be labelled as proxies and tested as a separate hypothesis, not reported as actual liquidation data.

## Research decision

1. Do not change grid entries.
2. Do not promote Fear & Greed as an exit selector.
3. Do not promote the current broad-market regime rule.
4. Funding is the first external crowd signal in this family to slightly exceed WIDE on the common full period, but it fails robustness; classify as **INTERESTING / NOT PROMOTED**.
5. The simple multi-signal consensus did not fix the instability.
6. Do not conclude that external context is useless: broad-market context beat WIDE in 9/15 independent asset-windows, suggesting information exists but the current mapping into fixed exit widths is too crude.
7. Open interest and liquidations remain untested until trustworthy data is materialized.
8. Do not tune dozens of thresholds on the already-seen history.

## Duplicate-work guard

Before repeating funding, Fear & Greed, market breadth, stablecoin-liquidity, OI, or liquidation ideas for this grid:

- read this file;
- state exactly what data source, knowledge-time convention, and mechanism differ;
- do not call OI/liquidation tested based on this pass;
- do not promote the +170.62% funding result without new validation;
- preserve the 2026-09-26 forward boundary for genuinely unseen evidence where applicable.

## Paper-live boundary

No existing paper-live profile changed.

Still frozen:

- CONTROL_BASE: +1/+10
- CANDIDATE_WIDE: +6/+18

No result here authorizes live-money execution.
