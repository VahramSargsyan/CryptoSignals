# Grid Universe Quality / Suitability Screen V1

Date: **2026-09-26**  
Strategy: **VAHRAM_LINK_LEVEL_GRID_V1**  
Workflow mode: **STRESS_TEST_ONLY**  
Decision status: **RESEARCH UNIVERSE SCREENED / NO LIVE PROMOTION**  
Runtime impact: **NONE**  
Migration required: **NO**

## Objective

Build a research universe using two separate gates:

1. **QUALITY / SURVIVAL** — an asset must be old, liquid, large enough, and have a clear reason to exist before the grid is allowed to own it for years;
2. **GRID SUITABILITY** — among assets that pass survival screening, identify price paths where staged limit buying and repeated recovery are historically productive.

This is a research-candidate screen, not a buy list.

## Admission baseline

All executed Grid assets satisfy:

- at least 1095 daily candles before the first simulated trade;
- established Binance Spot history;
- large-cap / long-lived status in the current crypto market;
- no symbol-specific tuning.

The current market-cap snapshot places the 15-asset research set approximately within the top ~31 non-stable crypto assets, with ranks naturally moving intraday.

## Universe

- BTC
- ETH
- BNB
- SOL
- XRP
- TRX
- DOGE
- ADA
- LINK
- XLM
- LTC
- HBAR
- AVAX
- BCH
- UNI

## Functional purpose screen

| Asset | Primary token / network role | Research interpretation |
| --- | --- | --- |
| BTC | monetary / settlement asset and network security incentive | strongest survival control; not assumed to be Grid-optimal |
| ETH | gas + proof-of-stake security + settlement for Ethereum apps | core infrastructure candidate |
| BNB | gas / governance across BNB Chain ecosystem + burn mechanics | strong ecosystem utility, but exchange/ecosystem concentration matters |
| SOL | native asset for Solana transactions / staking / app economy | core L1 candidate |
| XRP | native XRP Ledger settlement asset / liquidity bridge | mature payments/settlement candidate |
| TRX | fees, staking, resources and governance on TRON; large stablecoin-payment role | mature operational network candidate |
| DOGE | peer-to-peer digital currency | very long survival history but weaker differentiated infrastructure role |
| ADA | fees, staking and governance on Cardano | mature L1 candidate |
| LINK | payment / staking / cryptoeconomic security for Chainlink oracle services | core infrastructure candidate |
| XLM | fees, rent/minimum balances and native asset for Stellar payment network | mature payments infrastructure candidate |
| LTC | long-lived digital-money / payments asset | strong age/liquidity, simpler differentiated utility |
| HBAR | network fees + proof-of-stake security on Hedera | infrastructure candidate with governance-model caveat |
| AVAX | native asset for Avalanche network / staking / L1 ecosystem | infrastructure candidate |
| BCH | peer-to-peer electronic cash | mature payments asset |
| UNI | governance token; since UNIfication, protocol-fee flows can burn UNI | protocol-linked candidate; token value-capture mechanics require monitoring |

## Common Grid evaluation

To avoid symbol-specific periods, all 15 assets were evaluated on:

- **2023-09-25 through 2026-03-28**
- H/L lookback: **1095 prior daily candles**
- H/L refresh: **30 candles**
- fee: **10 bps**
- slippage: **5 bps**
- capital: **p=1 linear-depth**
- WIDE: **Micro +6 / Mid +18**
- BASE: **Micro +1 / Mid +10**
- no permanent runner
- full positive-profit reinvestment

BTC prehistory was reconstructed from two overlapping Binance Spot daily sources because the newer pinned snapshot begins in 2021; evaluation-period prices were unchanged.

## Grid results

| Asset | WIDE | BASE | Buy & Hold | WIDE DD | WIDE vs B&H |
| --- | ---: | ---: | ---: | ---: | ---: |
| SOL | +564.49% | +508.31% | +329.04% | 42.67% | +235.45 pp |
| HBAR | +442.19% | +431.16% | +82.15% | 67.49% | +360.03 pp |
| LINK | +316.06% | +300.82% | +23.19% | 38.67% | +292.87 pp |
| ADA | +253.19% | +223.28% | +2.79% | 54.04% | +250.40 pp |
| DOGE | +250.70% | +275.06% | +52.26% | 52.48% | +198.44 pp |
| UNI | +195.96% | +198.87% | -19.42% | 52.23% | +215.38 pp |
| XLM | +186.71% | +160.21% | +52.47% | 43.34% | +134.24 pp |
| XRP | +128.44% | +113.59% | +167.76% | 32.45% | -39.32 pp |
| BCH | +109.86% | +117.99% | +135.37% | 43.18% | -25.50 pp |
| AVAX | +105.58% | +156.46% | +0.77% | 69.17% | +104.81 pp |
| BNB | +87.21% | +73.03% | +195.13% | 24.50% | -107.92 pp |
| ETH | +80.62% | +70.93% | +27.57% | 32.19% | +53.05 pp |
| BTC | +59.89% | +50.33% | +152.24% | 15.67% | -92.35 pp |
| TRX | +48.30% | +39.75% | +278.88% | 8.79% | -230.58 pp |
| LTC | +42.56% | +74.80% | -14.54% | 43.25% | +57.10 pp |

Aggregate across 15 assets:

- WIDE geometric return: **+160.98%**
- BASE geometric return: **+159.01%**
- Buy & Hold geometric return: **+72.67%**
- WIDE median max DD: **43.18%**
- Buy & Hold median max DD: **70.26%**
- WIDE positive assets: **15/15**
- WIDE beats Buy & Hold: **10/15**
- WIDE beats BASE: **10/15**

## Path-structure diagnostics

Exploratory cross-sectional relationships between WIDE's excess return over Buy & Hold and descriptive price features:

- annualized realized volatility correlation: approximately **+0.84**
- deep lower-wick share correlation: approximately **+0.84**
- path efficiency correlation: approximately **-0.74**
- 10-day recovery frequency after a >=5% intraday shock: approximately **+0.26**

Interpretation:

- higher volatility and more deep lower wicks are associated with stronger Grid-vs-Hold excess in this survivor sample;
- lower path efficiency — more distance travelled relative to net displacement — also aligns strongly with Grid advantage;
- recovery frequency by itself is weaker than expected.

These are exploratory correlations on only 15 preselected survivors and are **not predictive proof**. The prior TWT/DOGE/AVAX/SHIB stress test already showed that high volatility can still be harmful during persistent directional decline.

## Research priority tiers

### Tier A — primary Grid research candidates

These combine a relatively strong survival/function case with favorable Grid evidence:

- **LINK**
- **SOL**
- **ETH**
- **ADA**
- **XLM**

Rationale: clear network/infrastructure role, sufficient age/liquidity, positive WIDE, and WIDE historically exceeded Buy & Hold on the common window.

### Tier B — high-value research candidates with a caveat

- **HBAR** — exceptional historical Grid return but very high WIDE drawdown (~67.5%) and lower market-cap rank than core assets.
- **UNI** — strong protocol relevance and improved fee-to-token burn mechanics since UNIfication, but protocol success and UNI value capture must be monitored separately.
- **DOGE** — excellent Grid price behavior and extraordinary longevity/liquidity, but weaker differentiated fundamental utility.
- **AVAX** — clear infrastructure role and Grid beat Hold, but BASE materially beat WIDE and WIDE drawdown was high.
- **LTC** — extremely long-lived and liquid; Grid beat Hold, but BASE beat WIDE and historical Grid return was modest.

### Tier C — strong survival assets / useful controls, but not first-choice WIDE Grid candidates

- **BTC**
- **BNB**
- **XRP**
- **TRX**
- **BCH**

These remain important benchmark / ownership candidates, but Buy & Hold exceeded WIDE on the common window. They should not be forced into Grid merely because they pass the survival gate.

## Important interpretation

The screen suggests that asset selection may create a larger improvement than micro-tuning exit distances.

The emerging architecture is:

```text
QUALITY / SURVIVAL PASS
        ↓
PRICE-PATH / GRID-SUITABILITY PASS
        ↓
choose execution style
        ├─ GRID
        ├─ HOLD / trend
        └─ reject / no capital
```

The target is not to make every good asset a Grid asset.

## Survivorship-bias warning

This 15-asset universe was intentionally selected from old, large, still-existing assets.

Therefore the impressive 15/15 positive WIDE result is **survivor-conditioned evidence**.

A later survival-gate validation must reconstruct historical universes and include assets that:

- had sufficient age / rank at the historical decision date;
- later collapsed, lost liquidity, or were delisted.

Otherwise the quality gate can appear stronger than it really is.

## Research decision

1. Freeze the 15-asset set as the first **QUALITY + GRID research universe**.
2. Promote no asset directly to live capital.
3. Use Tier A as the first deep-fundamental research batch.
4. Keep Tier B as targeted follow-up.
5. Use Tier C as controls and possible Hold/trend candidates.
6. Do not optimize Grid parameters per symbol during the quality study.
7. The next research layer should collect usage, tokenomics, supply/unlocks, concentration, protocol economics, liquidity/exchange breadth and material structural risks for Tier A before any operational eligibility decision.

## Paper-live boundary

No current paper-live profile or universe changed.

This document does not authorize live-money execution.
