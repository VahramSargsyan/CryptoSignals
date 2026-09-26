# Grid Micro / Mid Redundancy Stress Test V1

Date: **2026-09-26**  
Strategy: **VAHRAM_LINK_LEVEL_GRID_V1**  
Workflow mode: **STRESS_TEST_ONLY**  
Decision status: **REDUNDANCY TESTED / NO SIMPLIFICATION PROMOTED YET**  
Runtime impact: **NONE**  
Migration required: **NO**

## Question

Do the current two capital engines add real value:

- **Micro** — 64 sublevel entries with short recovery exits;
- **Mid** — 16 main-level entries with longer percentage / recovery exits;

or does splitting capital into two independent pools merely add complexity without improving capital efficiency?

The owner preference is explicit: if one engine consistently dominates, or both are effectively equivalent, simplify to one capital engine rather than preserve complexity for its own sake.

## Structural fact

In the current implementation Micro and Mid are financially independent:

- separate starting capital;
- separate reserved cash;
- separate open lots;
- separate exit targets;
- separate profit reinvestment.

They do not share cash and one layer does not improve the fills or targets of the other.

Therefore the combined portfolio has **no nonlinear execution synergy**. It is a weighted blend / diversification of two independent engines.

## Canonical capital-split evidence

Existing canonical p=1 / 3y H/L / WIDE research already tested:

| Micro / Mid | Geometric return | Median DD |
| --- | ---: | ---: |
| 20 / 80 | **+257.23%** | 39.90% |
| 35 / 65 | +255.13% | 39.63% |
| 50 / 50 | +253.03% | 38.67% |
| 65 / 35 | +250.92% | 37.70% |
| 80 / 20 | +248.80% | **36.73%** |

Interpretation:

- increasing Mid share increased full-period return modestly;
- increasing Micro share reduced drawdown;
- 50/50 is not a special optimum; it is simply a middle risk/return point;
- the effect is modest compared with other structural parameters.

With p=0, changing Micro/Mid split had almost no effect on geometric return, further supporting that the two-layer split is primarily a risk/turnover blend rather than a unique source of edge.

## Supplementary extreme-layer stress test

A separate same-data research harness was used to compare:

- 100% Micro-only;
- 100% Mid-only;
- current 50/50 combination;

with equal total starting capital, same p=1 allocation, same fees/slippage, same WIDE/BASE mechanics and 100% positive-profit reinvestment.

Important: the reconstructed public D1 data snapshot did **not** reproduce the old canonical absolute aggregate return exactly. Therefore these numbers are used only for **relative redundancy / regime behavior**, not as a replacement canonical benchmark.

### WIDE regime winner

Across three independent one-year reset windows:

- Year 1: **Micro-only** aggregate return > Mid-only;
- Year 2: **Mid-only** > Micro-only;
- Year 3: **Micro-only** > Mid-only.

On the difficult final year Micro also had lower aggregate drawdown.

Full-period per-asset leadership was mixed:

- Micro stronger: LINK, ETH, SOL;
- Mid stronger: BNB, BTC.

### BASE regime winner

Across the same annual windows:

- Year 1: **Micro-only** slightly stronger aggregate;
- Year 2: **Mid-only** stronger;
- Year 3: **Micro-only** stronger.

Full-period leadership was again mixed by asset.

## Key finding

There is **no evidence that one layer universally dominates the other**.

But there is also **no evidence of special return synergy from holding both pools simultaneously**.

The current two-pool architecture behaves as diversification:

- Mid tends to favor higher long-horizon return in the canonical p=1 split scan;
- Micro tends to favor lower drawdown and can be stronger in adverse / recent windows;
- combined 50/50 sits between the two.

Therefore the architectural question is not:

> “Do we need two layers to make the strategy work?”

The evidence says **no**.

The real question is:

> “Is the risk diversification benefit worth the extra conceptual and operational complexity?”

## Current decision

1. Do **not** remove Micro or Mid yet.
2. Treat the current 50/50 split as **not structurally special**.
3. Do not claim two pools increase raw capital efficiency; they mainly blend two return/risk profiles.
4. Before any simplification patch, run the exact 100/0 and 0/100 endpoints on the same frozen canonical six-year artifacts used by the +253.03% WIDE benchmark.
5. If one endpoint is superior on both return and drawdown, remove the dominated layer candidate from future architecture.
6. If the tradeoff remains “Mid = more return / Micro = less drawdown,” simplification becomes a product preference rather than an empirical dominance decision.
7. A shared single-wallet architecture with both order types is a **different hypothesis** and is not tested here.

## Duplicate-work guard

Already known:

- 20/80 through 80/20 Micro/Mid split;
- two pools have no nonlinear capital interaction;
- 50/50 is not a special optimum;
- annual relative leadership can switch between Micro and Mid;
- neither layer currently has robust universal dominance.

Do not present these as new findings without a materially different dataset or shared-wallet mechanism.

## Paper-live boundary

No strategy code changed.

No paper-live capital split changed.

No result here authorizes live-money execution.


---

## Expanded 15-asset universe test

The redundancy test was expanded to the same 15-asset universe used by the quality / Grid-suitability screen:

BTC, ETH, BNB, SOL, XRP, TRX, DOGE, ADA, LINK, XLM, LTC, HBAR, AVAX, BCH, UNI.

Common evaluation:

- 2023-09-25 through 2026-03-28
- 1095 daily-bar H/L
- refresh 30 bars
- WIDE +6/+18
- p=1
- 10 bps fee
- 5 bps slippage
- 100% positive-profit reinvestment
- equal total starting capital for MICRO_ONLY and MID_ONLY

The combined 50/50 control reproduced the prior 15-asset screening values. BTC was re-run using the same reconstructed BTC source used by that screen.

### All 15 assets

| Mode | Geometric return | Median DD | Closed exits |
| --- | ---: | ---: | ---: |
| **MICRO_ONLY** | **+170.91%** | **41.39%** | 2,303 |
| MID_ONLY | +147.17% | 42.77% | **721** |
| current 50/50 | +160.98% | 43.18% | 3,024 |

Return winner count:

- Micro: **9/15**
- Mid: **6/15**

Mid reduced closed exits by about **68.7%** versus Micro, but the fee/slippage-adjusted aggregate return was materially lower.

This is important: lower Mid turnover is already reflected in the result because the simulator charges the same fee/slippage model to both layers. Micro still produced the higher 15-asset net aggregate despite more turnover.

### Original canonical five vs ten additional assets

#### Original five: BTC / ETH / BNB / SOL / LINK

| Mode | Geometric return | Median DD | Closed exits |
| --- | ---: | ---: | ---: |
| Micro | +167.35% | 33.21% | 1,259 |
| **Mid** | **+176.92%** | **32.60%** | **266** |

On the original five, Mid is slightly stronger on both aggregate return and median DD while using about **78.9% fewer exits**.

Return winner count:

- Mid 3/5
- Micro 2/5

#### Ten additional assets

XRP / TRX / DOGE / ADA / XLM / LTC / HBAR / AVAX / BCH / UNI.

| Mode | Geometric return | Median DD | Closed exits |
| --- | ---: | ---: | ---: |
| **Micro** | **+172.70%** | **46.47%** | 1,044 |
| Mid | +133.52% | 51.20% | **455** |

Return winner count:

- Micro **7/10**
- Mid 3/10

Here the result is materially in Micro's favor.

Micro dominated Mid on both return and DD for:

- XRP
- DOGE
- LTC
- AVAX
- BCH
- UNI

Mid dominated Micro on both return and DD only for:

- ADA

### Tier A quality/Grid candidates

Tier A from the prior universe screen:

- LINK
- SOL
- ETH
- ADA
- XLM

This is the most decision-relevant subset if future capital is restricted to higher-quality Grid candidates.

| Mode | Geometric return | Closed exits |
| --- | ---: | ---: |
| Micro-only | +244.93% | 884 |
| **Mid-only** | **+245.61%** | **278** |
| Current 50/50 | +247.36% | 1,162 |

Median DD:

- Micro: ~43.46%
- Mid: ~42.71%
- Combined: ~42.67%

Interpretation:

- Micro and Mid are essentially tied on Tier A return;
- Mid has slightly lower median DD;
- Mid uses **68.6% fewer exits** than Micro;
- adding the entire Micro layer to Mid raises geometric return only from +245.61% to +247.36% on this sample, while total exits increase from 278 to 1,162.

This is the strongest evidence so far in favor of a future **Mid-only simplification for a quality-filtered Grid universe**.

It is not universal across all crypto assets: the broader ten-asset extension strongly favored Micro.

## Revised interpretation

The question is now universe-dependent.

- Broad old/large-cap universe: Micro has the stronger aggregate return.
- Original canonical five: Mid is slightly stronger and much quieter.
- Tier A quality/Grid candidates: Micro and Mid are almost identical in return, while Mid is dramatically quieter.

Therefore the evidence does **not** justify saying Mid universally dominates Micro.

However, if the production universe is intentionally restricted by the new QUALITY/SURVIVAL + GRID-SUITABILITY gates, Mid-only becomes a serious simplification candidate.

## Updated decision

1. Do not delete Micro from the canonical strategy yet.
2. Preserve **MID_ONLY on Tier A quality assets** as a high-priority simplification candidate.
3. The next promotion-grade test should use the frozen canonical artifacts / forward paper evidence and compare:
   - current 50/50;
   - Mid-only;
   - optionally Micro-only as control.
4. Evaluate not just return/DD but order count, fee paid, turnover, capital utilization and operational noise.
5. Do not generalize the Tier A result to arbitrary volatile tokens; the ten-asset extension shows Micro can be materially stronger there.
