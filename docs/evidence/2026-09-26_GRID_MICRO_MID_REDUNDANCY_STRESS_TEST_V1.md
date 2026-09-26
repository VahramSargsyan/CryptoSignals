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
