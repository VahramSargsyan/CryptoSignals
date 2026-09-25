# LINK Grid Moving-Average Exit Research Decision Log V1

Date: **2026-09-26**  
Strategy: **VAHRAM_LINK_LEVEL_GRID_V1**  
Decision status: **TESTED / NOT PROMOTED / DO NOT REPEAT WITHOUT NEW EVIDENCE**  
Runtime impact: **NONE**  
Migration required: **NO**

## Purpose

This document is durable research memory for future chats and maintainers.

Before proposing another moving-average exit, moving-average regime switch, or moving-average runner for `VAHRAM_LINK_LEVEL_GRID_V1`, read this document first. Do not repeat a recorded experiment unless the new work is materially different or uses genuinely new unseen evidence.

The current research direction remains:

- the grid determines **where to buy**;
- fixed recovery targets remain the strongest shared exit baseline found so far;
- moving averages have been explored as exit rules, BASE/WIDE regime selectors, and runner exits;
- none of those MA families is promoted into canonical or paper-live behavior from the evidence below.

## Shared research baseline

Universe:

- LINKUSDT
- ETHUSDT
- SOLUSDT
- BNBUSDT
- BTCUSDT

Common methodology used for comparison:

- Binance Spot daily candles;
- 6 years total data;
- first 1095 daily candles used as prehistory only;
- final 1096 daily candles used for trading/evaluation;
- causal H/L range using prior candles only;
- 10 bps fees;
- 5 bps slippage;
- linear-depth capital allocation, p=1;
- 100% positive-profit reinvestment unless a runner experiment explicitly changes the treatment of realized profit;
- no production or paper-live behavior changed.

Reference results:

| Exit profile | Five-asset geometric return |
| --- | ---: |
| BASE: Micro +1 / Mid +10 | +226.4% |
| WIDE: Micro +6 / Mid +18 | **+253.0%** |

The WIDE profile is a research candidate, not an accepted live rule.

## Evidence provenance boundary

The corrected canonical five-asset datasets and the BASE/WIDE research engine are repository-backed and have reproducible GitHub evidence.

The MA experiments recorded below were exploratory local backtests performed against those same canonical data semantics during research conversations. Their exact experiment harness was not persisted as a standalone repository script/artifact at the time of testing.

Therefore:

- use these results as a **decision-memory / duplicate-work guard**;
- do not present them as an independently reproducible repository artifact;
- before any MA variant is promoted, frozen, or used in paper-live, first encode that exact hypothesis in repository-owned code and produce a reproducible artifact.

## Experiment family A — MA directly replaces the fixed exit

Hypothesis:

- keep grid entries unchanged;
- replace Micro +6 and/or Mid +18 recovery exits with moving-average-based sale levels/rules;
- use prior closed-candle MA information only, with no look-ahead.

Reported five-asset geometric returns:

| Direct MA exit variant | Geometric return |
| --- | ---: |
| WIDE +6 / +18 control | **+253.0%** |
| BASE +1 / +10 | +226.4% |
| nearest of the tested 25/50 and 100/200 MA pairs | +215.8% |
| SMA25 / SMA100 | +212.8% |
| farther of the tested MA-pair choices | +208.5% |
| SMA25 / SMA200 | +207.4% |
| SMA50 / SMA100 | +203.6% |
| SMA50 / SMA200 | +198.4% |

No tested direct-MA-exit variant beat WIDE on the reported comparison.

Illustrative asset comparisons from the best direct-MA family:

- LINK: WIDE +516.8% vs best reported MA variant about +416.0%;
- ETH: WIDE +137.0% vs best reported MA variant about +131.8%;
- BTC: WIDE +82.3% vs best reported MA variant about +74.0%.

Trade churn also increased materially:

- WIDE: about 1,595 closed exits in the reported comparison;
- MA direct-exit variants: roughly 3,400–5,400 exits.

Interpretation: using MA directly as the sale mechanism caused earlier/frequent recycling of positions and did not improve the shared five-asset result.

**Decision: NOT PROMOTED.**

## Experiment family B — MA selects BASE or WIDE

This is a different mechanism from direct MA selling.

Hypothesis:

- grid still determines entries;
- at entry, an MA trend state selects either BASE (+1/+10) or WIDE (+6/+18);
- the selected target is then frozen for that lot.

Reported five-asset geometric returns:

| MA regime selector | Geometric return | Notes |
| --- | ---: | --- |
| constant WIDE +6/+18 | **+253.03%** | control |
| MA25/50 | +246.1% | initially promising, below WIDE |
| MA50/100 | +250.7% | strongest simple round-number pair |
| MA40/90 | **+254.95%** | slightly above WIDE in aggregate, post-hoc |
| MA45/90 | +253.94% | slightly above WIDE in aggregate, post-hoc |
| MA50/90 | +253.31% | near WIDE |
| MA100/200 | +240.0% | below WIDE |

The best aggregate result, MA40/90, was not robust enough to promote:

- it improved LINK and SOL but lost to WIDE on ETH, BNB, and BTC;
- across 15 asset-year windows it beat WIDE in only 6/15;
- mean annual difference versus WIDE was about -1.6 percentage points;
- median annual difference was about -1.1 percentage points;
- it was discovered after inspecting the same historical development period.

Interpretation: MA state contains some useful trend information, but the apparent aggregate winner is vulnerable to post-hoc selection and does not show broad enough stability.

**Decision: INTERESTING / NOT PROMOTED.**

## Experiment family C — fixed recovery plus MA-managed runner

Hypothesis:

- grid determines entries;
- +6/+18 returns working capital;
- some position/profit remains as a runner;
- MA crossover closes the runner.

### C1 — runner taken from the whole position

Micro runner used a faster MA concept; Mid runner used a slower MA concept.

| Whole-position runner | Geometric return | Median max drawdown |
| --- | ---: | ---: |
| 0% / WIDE control | **+253.03%** | 38.67% |
| 10% | +247.37% | 37.66% |
| 25% | +241.02% | 36.41% |
| 50% | +234.46% | **34.98%** |

This reduced drawdown but also withheld working capital from the grid and reduced return.

### C2 — runner taken only from realized profit

This version fully returns original capital to the grid and leaves only a fraction of profit exposed as a token runner.

| Profit sent to MA runner | Geometric return | Median max drawdown |
| --- | ---: | ---: |
| 0% / WIDE control | **+253.03%** | 38.67% |
| 10% | +250.28% | 38.31% |
| 25% | +246.46% | 37.79% |
| 50% | +240.76% | 37.01% |
| 100% | +231.42% | **35.75%** |

The closest configuration was 10% of profit into the MA runner.

Reported 10% profit-runner comparison:

| Asset | WIDE | 10% profit runner |
| --- | ---: | ---: |
| LINK | **+516.80%** | +503.10% |
| ETH | **+137.02%** | +136.19% |
| SOL | **+818.79%** | +808.04% |
| BNB | **+123.98%** | +123.43% |
| BTC | +82.26% | **+82.48%** |

A Mid runner using 40/90 was also checked and did not improve the shared result versus the 50/100-style runner family.

Stability check for the best reported MA-runner candidate:

- wins versus WIDE: 7/15 asset-year windows;
- median difference: approximately -0.007 percentage points;
- mean difference: approximately -0.21 percentage points.

Interpretation: MA-managed runners behaved mainly as a mild risk reducer, not a return enhancer. Full reinvestment after WIDE exits remained stronger historically.

**Decision: NOT PROMOTED.**

## Current research decision

For `VAHRAM_LINK_LEVEL_GRID_V1`:

1. **Do not modify the grid entry logic with MA based on this research.**
2. **Do not replace +6/+18 with direct MA exits.**
3. **Do not add the tested MA-runner structures to paper-live.**
4. Keep BASE (+1/+10) and WIDE (+6/+18) as the existing forward-observation profiles.
5. Treat MA50/100 and the 40–50 / 90–100 area as research observations only, not canonical parameters.
6. A future MA experiment must state what is materially new compared with the three tested families above.

## Duplicate-work guard for future chats

If a future request proposes MA25/50, MA50/100, MA100/200, MA40/90, MA-based direct selling, MA-based BASE/WIDE switching, or an MA-managed runner, the assistant should first say that related experiments already exist and summarize the relevant results from this document.

Repeat testing is justified only when at least one of these is true:

- new **future unseen** evidence is available after the 2026-09-26 forward boundary;
- the mechanism is materially different from direct MA exit, BASE/WIDE regime selection, and MA runner;
- a bug or semantic error is found in the earlier experiment;
- the hypothesis is preregistered and implemented in repository-owned reproducible code before examining its validation data.

Do not tune increasingly specific MA pairs such as 39/87, 42/93, or similar merely to maximize the already-seen development history.

## Paper-live boundary

This decision log does not change the frozen paper-live profiles:

- `CONTROL_BASE`: Micro +1 / Mid +10;
- `CANDIDATE_WIDE`: Micro +6 / Mid +18.

Paper start remains:

`2026-09-26T00:00:00Z`

No MA result in this document authorizes live-money execution or a strategy-status promotion.
