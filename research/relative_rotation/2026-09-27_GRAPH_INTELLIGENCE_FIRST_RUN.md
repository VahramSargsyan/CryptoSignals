# Relative Rotation Graph Intelligence — First OSS Run

Date: 2026-09-27  
Branch: `research/relative-graph-intelligence-v1`  
GitHub Actions run: `36294086699`  
Source SHA: `561ba5f43a09339e7190cb8a1b70de7feeef2220`  
Artifact: `relative-graph-intelligence-v1` / ID `10923825124`

Workflow mode: PATCH_FIX + ECOSYSTEM_PLANNING  
Status: RESEARCH_ONLY / NO PRODUCTION CHANGE

## Research question

Can mature pairwise-ranking methods improve conflict routing in the existing 8-node / 28-pair relative-rotation graph without changing pair signal generation?

Frozen pair engine:

- universe: ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK;
- 1D closed candles;
- 180d rolling median;
- 15% ARM;
- 3% reversal;
- next-open execution;
- 0.1% transition cost.

The graph layer is allowed to act only when the currently held asset has multiple simultaneously confirmed outbound transitions.

## OSS methods

- Evalica 0.4.2 / Apache-2.0:
  - Bradley-Terry;
  - PageRank.
- choix 0.4.1 / MIT:
  - Rank Centrality.

No upstream source code was copied into CryptoSignals; both were used as pinned research dependencies through a local adapter.

## Baseline reproduction gate

Documented highlighted OOS period:

`2025-03-29 -> 2026-03-28`

Documented prior median result:

`+41.6%`

Fresh Binance reconstruction in the new runner:

`+43.82%`

Difference:

`+2.22 percentage points`

Predeclared tolerance:

`+/- 5 percentage points`

Gate:

`PASS`

Therefore the highlighted OOS graph-ranking comparison is considered sufficiently comparable to the prior one-year research result.

## Main one-year result

| Conflict router | Median return | Worst start | Median max DD | Median transitions | Positive starts |
|---|---:|---:|---:|---:|---:|
| strongest-extreme baseline | **+43.82%** | +5.03% | -61.57% | 8 | 8/8 |
| Bradley-Terry | -26.89% | -28.51% | -62.36% | 6 | 1/8 |
| PageRank | +10.94% | -18.98% | -70.36% | 7 | 7/8 |
| Rank Centrality | +10.94% | -18.98% | -70.36% | 7 | 7/8 |
| 3-method ordinal consensus | -26.89% | -28.51% | -62.36% | 6 | 1/8 |

Decision:

`DIRECT_GLOBAL_GRAPH_RANK_OVERRIDE = REJECTED_V1`

The mature ranking methods did not improve the highlighted OOS router. The existing pair-specific extreme ranking remained materially stronger in this test.

## Failure attribution

### Bradley-Terry degeneracy in this representation

On the 365 daily snapshots in the highlighted OOS year, Bradley-Terry ranked ATOM first on all 365 days.

This is not accepted as evidence that ATOM was always globally strongest.

The V1 input gives the model one deterministic win/loss observation for each of 28 pairs at each daily snapshot. That representation is poorly suited to an unregularized Bradley-Terry maximum-likelihood interpretation and can become degenerate / non-identifiable under strongly separated tournament states.

A key damaging conflict:

`2025-05-18`

Held asset:

`TRX`

Confirmed outbound candidates:

- ATOM, strength ~0.1878
- LINK, strength ~0.2293
- SOL, strength ~0.1765

Baseline chose:

`TRX -> LINK`

Bradley-Terry chose:

`TRX -> ATOM`

That single topology change caused most starting paths to leave the productive LINK route and materially damaged the one-year result.

The 3-method consensus inherited this failure because Bradley-Terry's degenerate rank entered the ordinal sum.

Decision:

`BRADLEY_TERRY_SINGLE_SNAPSHOT_BINARY_TOURNAMENT = REJECTED_REPRESENTATION`

Do not retune Bradley-Terry on this same period.

### PageRank / Rank Centrality

PageRank and Rank Centrality agreed on the top-ranked node on approximately **98.6%** of the 365 OOS days.

This is useful independent evidence that the two graph-centrality methods were reading nearly the same network structure.

They also agreed on the main damaging OOS override:

`2025-09-21`

Held asset:

`TWT`

Seven confirmed outbound candidates existed.

Baseline strongest extreme chose:

`TWT -> ATOM`

PageRank / Rank Centrality chose:

`TWT -> PEPE`

The graph-centrality route finished the full year with only ~+10.9% median and a worse median max drawdown (~-70.4%).

Decision:

`PAGERANK / RANK_CENTRALITY AS DIRECT ROUTER OVERRIDE = NOT_SUPPORTED`

This does not reject these methods as diagnostics or future confidence features.

## Sequential 180-day diagnostic

These rows use the new reproducible strongest-extreme baseline and therefore must not be silently compared to the previously documented sequential baseline where **causal pair-specific percentile** was used for conflict ranking.

| Window | Strongest-extreme | Bradley-Terry | PageRank / Rank Centrality |
|---|---:|---:|---:|
| 2023-10-31 -> 2024-04-27 | +374.23% | +393.18% | **+455.77%** |
| 2024-04-28 -> 2024-10-24 | -34.46% | -34.46% | -34.21% |
| 2024-10-25 -> 2025-04-22 | +93.67% | +134.64% | **+138.31%** |
| 2025-04-23 -> 2025-10-19 | **+20.08%** | +4.00% | +9.90% |
| 2025-10-20 -> 2026-03-28 | -47.05% | **-39.14%** | -47.05% |

Interpretation:

- graph centrality can improve some regimes substantially;
- it can also destroy a previously productive route;
- therefore it is regime/path dependent and not a safe unconditional replacement;
- the result reinforces the earlier finding that **routing topology matters more than a generic global ranking**.

## Cycle-consistency diagnostic

Across the 365 OOS daily network snapshots:

- 310 / 365 days (~84.9%) had zero cyclic triples;
- mean cycle ratio: ~0.46%;
- maximum cycle ratio: ~8.93% (5 cyclic triples out of 56 possible triples);
- all four dates that generated logged one-year routing conflicts had cycle ratio = 0.

Decision:

`CYCLE_RATIO_V1 = LOW_INFORMATION_FOR_OBSERVED_CONFLICTS`

Do not promote cycle inconsistency as a trade gate from this representation.

## What survives from the OSS idea

Rejected:

- unconditional graph-ranking override;
- Bradley-Terry on one binary comparison per pair per day;
- 3-method consensus that treats the degenerate Bradley-Terry rank as equal evidence.

Still worth preserving as research features:

1. PageRank / Rank Centrality **agreement**;
2. global graph rank as metadata attached to a pair-specific transition;
3. graph centrality as a future confidence / attribution feature rather than a replacement router;
4. a future Bradley-Terry experiment only if the input becomes a meaningful trailing history of repeated weighted pair observations and is preregistered before a new validation period.

## Validation boundary

No data after `2026-03-28` was used.

The frozen untouched period for `DEFENSIVE_LOW_VOL_CRYPTO` remains unobserved by this graph experiment.

## Files / runtime impact

Added research-only:

- `scripts/research_relative_rotation_graph_intelligence.py`
- `tests/test_relative_rotation_graph_intelligence.py`
- `.github/workflows/relative-rotation-graph-intelligence.yml`
- `research_requests/run_relative_rotation_graph_intelligence_v1.trigger`
- `research/relative_rotation/2026-09-27_GRAPH_INTELLIGENCE_OSS_REUSE.md`
- this evidence file.

Production / paper-live behavior changed:

`NONE`

Migration required:

`NO`

## Test level

`GITHUB_ACTIONS_BACKTEST_EXECUTED + BASELINE_REPRODUCTION_GATE + HISTORICAL_GRAPH_ROUTING_STRESS_TEST`

Residual risks:

- the reconstructed strongest-extreme baseline is close to but not numerically identical to the prior one-year result;
- sequential historical evidence used a different conflict-ranking baseline and is diagnostic only here;
- all graph methods were evaluated after the historical router problem was already known;
- this experiment does not provide clean new OOS evidence for graph ranking;
- binary daily pair preference may discard useful magnitude/state information;
- fresh independent validation would be required before any graph-derived routing rule could be promoted.
