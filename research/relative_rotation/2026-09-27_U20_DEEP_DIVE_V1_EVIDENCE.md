# U20 Relative Rotation Deep Dive v1 — Evidence

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Branch: research/relative-rotation-universe-scale-v1
GitHub Actions run: 36317133596
Result: PASS
TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST

## Scope and limits

U20 is the fixed 20-token graph:
ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, BTC, ETH, XRP, DOGE, ADA, AVAX, DOT, LTC, BCH, NEAR, UNI, FIL.

Signal/router baseline:
- Binance Spot 1D closed candles
- rolling median 180d
- ARM 15%
- reversal confirmation 3%
- next-open execution
- strongest confirmed max-dislocation conflict router
- 0.1% transition cost

Common data begins 2023-05-05. The first fully mature 180-row date is 2023-10-31. Latest closed candle is 2026-09-26. Mature span is 1062 days / about 34.86 months. Therefore a mature 36-month test is not available yet and is not fabricated.

All U8-vs-U20 cross-universe medians use the same original eight starting assets. Rolling windows overlap and are diagnostic, not statistically independent samples. U20 had already been identified as interesting before this deep dive; this is not untouched validation.

## Long-cycle U8 vs U20

| Metric | U8 | U20 |
|---|---:|---:|
| Mature span median return | +922.31% | +550.64% |
| Mature span ATOM-start return | +875.75% | +466.95% |
| Mature span median max DD | -71.23% | -64.50% |
| Mature span worst start return | +721.05% | +407.36% |
| Mature span starts positive | 8/8 | 8/8 |
| Mature span beat same-asset HODL | 8/8 | 8/8 |
| Mature span beat equal-weight U20 | 8/8 | 8/8 |
| Median transitions | 21 | 26 |
| Median conflict days producing candidates | 6 | 8 |

Interpretation: over the almost-three-year mature span U8 compounded substantially more, while U20 had a smaller but still severe drawdown. U20 does not dominate U8 on terminal long-cycle return.

## Rolling 12-month windows — 23 monthly-start windows

| Metric | U8 | U20 |
|---|---:|---:|
| Median of window median returns | +52.82% | +106.41% |
| Worst window median return | -41.90% | -36.85% |
| Positive median-return windows | 69.57% | 82.61% |
| Median ATOM-start return | +50.26% | +70.71% |
| Worst ATOM-start return | -56.60% | -47.97% |
| ATOM positive windows | 65.22% | 73.91% |
| Positive median excess vs same-asset HODL | 91.30% | 78.26% |
| Positive median excess vs equal-weight U20 | 91.30% | 65.22% |

U20 has a better absolute 12-month median and a less bad worst 12-month result, but U8 more consistently beats the benchmarks on the 12-month horizon.

U20 worst 12-month median window was 2024-04-01 -> 2025-03-31: -36.85% median and -47.97% from ATOM.

## Rolling 24-month windows — 11 monthly-start windows

| Metric | U8 | U20 |
|---|---:|---:|
| Median of window median returns | +108.99% | +207.81% |
| Worst window median return | -16.92% | +64.35% |
| Positive median-return windows | 90.91% | 100.00% |
| Median ATOM-start return | +111.35% | +164.87% |
| Worst ATOM-start return | -26.89% | +35.40% |
| ATOM positive windows | 90.91% | 100.00% |
| Positive median excess vs same-asset HODL | 100.00% | 100.00% |
| Positive median excess vs equal-weight U20 | 100.00% | 100.00% |

Every available U20 24-month window was positive both on the eight-start median and from ATOM. The weakest U20 24-month window, 2024-04-01 -> 2026-03-31, still returned +64.35% median and +35.40% from ATOM. This is the strongest long-horizon robustness result in this run.

## Node ablation — remove one U20 addition

Baseline U20 mature return: +550.64% median / +466.95% ATOM.

| Removed node | Mature median | ATOM | Effect |
|---|---:|---:|---|
| FIL | +222.31% | +180.86% | severe degradation |
| UNI | +224.36% | +242.77% | severe degradation |
| ADA | +480.96% | +406.24% | moderate degradation |
| NEAR | +542.12% | +459.53% | small degradation |
| DOT | +550.64% | +466.95% | no realized-route effect |
| BTC | +550.64% | +466.95% | no realized-route effect |
| ETH | +550.64% | +466.95% | no realized-route effect |
| DOGE | +550.64% | +466.95% | no realized-route effect |
| AVAX | +572.13% | +485.68% | small improvement when removed |
| LTC | +684.13% | +428.51% | mixed: median improves, ATOM worsens |
| BCH | +1228.98% | +1047.62% | major improvement when removed |
| XRP | +1271.89% | +1084.68% | major improvement when removed |

Important: the XRP/BCH result is post-hoc ablation. It identifies topology-pollution candidates; it does not justify deleting them from production without a preregistered follow-up.

FIL and UNI are the strongest evidence of beneficial network interaction: removing either collapses U20 long-cycle performance and turns the worst ATOM 24-month result negative (-27.32% without FIL; -18.14% without UNI), whereas canonical U20 worst ATOM 24-month result is +35.40%.

## Marginal admission — U8 plus one new node

Selected results on the mature span:

| Added alone to U8 | Median return | ATOM return |
|---|---:|---:|
| ADA | +1176.57% | +1163.15% |
| AVAX | +967.71% | +891.60% |
| DOT | +821.68% | +887.70% |
| DOGE | +860.58% | +816.83% |
| LTC | +808.36% | +632.60% |
| FIL | +622.63% | +589.72% |
| UNI | +609.09% | +377.93% |
| NEAR | +359.77% | +423.70% |
| BTC | +443.66% | +418.90% |
| BCH | +534.06% | +505.19% |
| XRP | +183.41% | +214.62% |

U8 baseline is +922.31% median / +875.75% ATOM. Thus FIL, UNI and NEAR are weak when admitted alone, yet FIL/UNI become structurally valuable inside U20. This is direct evidence for the user's chain/bridge hypothesis rather than a simple ranking of standalone pairs or nodes.

ADA is the strongest single-node addition in this historical mature sample, but that is not an independent forward result.

## Full-cycle U20 node centrality

Across eight original starting assets over the mature span:

| Node | Entries | Exits | Occupancy share | Chosen from conflicts |
|---|---:|---:|---:|---:|
| NEAR | 32 | 32 | 9.98% | 8 |
| FIL | 16 | 16 | 6.31% | 0 |
| LTC | 15 | 15 | 7.36% | 14 |
| UNI | 14 | 14 | 5.20% | 5 |
| XRP | 8 | 8 | 12.41% | 0 |
| BCH | 9 | 9 | 10.59% | 0 |
| ADA | 8 | 8 | 2.82% | 0 |
| AVAX | 8 | 8 | 1.69% | 0 |
| BTC | 0 | 0 | 0.00% | 0 |
| ETH | 0 | 0 | 0.00% | 0 |
| DOGE | 0 | 0 | 0.00% | 0 |
| DOT | 0 | 0 | 0.00% | 0 |

NEAR is the busiest added bridge. LTC is unusually conflict-router dependent: 14 of its 15 entries were chosen from multi-candidate conflicts. XRP and BCH have relatively few transitions but large occupancy, consistent with stale-hold/topology-pollution risk in this sample.

Frequently used chain edges include NEAR->TWT (16), UNI->LTC (14), and eight-use corridors XRP->AVAX->FIL->ATOM, ATOM->NEAR, NEAR<->PEPE, NEAR->UNI, LTC->NEAR, ATOM->FIL->PEPE->ATOM, ATOM->ADA->TWT, and AAVE->BCH->ATOM.

## Router sensitivity

| Router | Mature median | ATOM | Median DD | Median transitions | Worst ATOM 24m |
|---|---:|---:|---:|---:|---:|
| strongest max-dislocation (canonical) | +550.64% | +466.95% | -64.50% | 26 | +35.40% |
| weakest max-dislocation | +786.11% | +705.78% | -59.21% | 37 | +92.43% |
| skip conflicts | +557.24% | +569.53% | -64.92% | 26.5 | +59.89% |
| deterministic first | +98.62% | +94.24% | -78.18% | 25 | -53.61% |

Interpretation: U20 is highly routing-sensitive. The canonical strongest rule is not uniquely best in this historical sample, and an arbitrary deterministic router can destroy much of the performance. Do not tune to the visible weakest-router winner; the correct conclusion is that conflict resolution is a first-class risk surface.

## Parameter sensitivity

| Variant | Mature median | ATOM | Median DD | Worst ATOM 24m |
|---|---:|---:|---:|---:|
| 180/15%/3% canonical | +550.64% | +466.95% | -64.50% | +35.40% |
| 120/15%/3% | +411.94% | +409.05% | -71.74% | -50.43% |
| 240/15%/3% | +25.97% | -7.35% | -71.20% | -43.85% |
| 180/12.5%/3% | +316.28% | +302.80% | -69.92% | +35.40% |
| 180/20%/3% | +440.92% | +375.91% | -70.11% | -55.47% |
| 180/15%/2% | +275.58% | +235.32% | -64.44% | +8.79% |
| 180/15%/5% | +195.35% | +90.10% | -65.71% | -27.76% |

The inherited 180/15/3 setting is the strongest of this fixed local sensitivity set and keeps every available ATOM 24-month window positive. However, nearby parameter changes can materially degrade results, so the strategy is not parameter-flat.

## Cost sensitivity

| Per-transition cost | Mature median | ATOM | Worst ATOM 24m |
|---|---:|---:|---:|
| 0.10% | +550.64% | +466.95% | +35.40% |
| 0.25% | +525.71% | +446.05% | +32.18% |
| 0.50% | +486.19% | +412.85% | +26.98% |
| 1.00% | +414.22% | +352.16% | +17.14% |

At up to 1% modeled cost per transition, every available ATOM 24-month window remains positive. Cost is not the primary fragility in this historical run; topology/router/parameter choice is much more important.

## Main conclusions

1. U20 is not simply 'better than U8'. U8 wins terminal return over the 34.86-month mature span (+922% vs +551%).
2. U20 is materially stronger on available rolling 24-month robustness: 11/11 positive median windows and 11/11 positive ATOM windows; worst median +64.35%, worst ATOM +35.40%.
3. The user's bridge-node hypothesis is supported. FIL/UNI/NEAR can be weak alone yet useful inside the larger chain.
4. The graph contains clear topology-pollution candidates. XRP and BCH are the strongest current suspects, but removal must be tested prospectively/preregistered rather than adopted from this post-hoc ablation.
5. BTC/ETH/DOGE/DOT were inert in the realized mature U20 routes under the canonical router.
6. U20 is highly router-sensitive and materially parameter-sensitive. This is the largest unresolved model risk.
7. Drawdown remains severe. U20's mature median max DD is about -64.5%; this is not a low-volatility strategy.
8. Transaction-cost robustness is comparatively good; even 1% modeled transition cost did not break the available 24-month ATOM windows.

## Decision / next research gate

Keep live at 8 nodes. Treat U20 as RESEARCH_CANDIDATE, not production.

The next clean experiment should be preregistered before outcomes and should test topology sanitation rather than parameter tuning: U20 canonical versus causal NEW_NODE_PROBATION and/or a rule that demotes persistently harmful/inert nodes without using future performance. XRP/BCH may be included as diagnostic ablations but must not be hard-blacklisted solely from this sample.

No production/live files changed.