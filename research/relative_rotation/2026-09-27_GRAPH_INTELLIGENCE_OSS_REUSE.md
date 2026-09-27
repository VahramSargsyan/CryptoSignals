# Graph Intelligence OSS Reuse Record V1

Date: 2026-09-27  
Workflow mode: PATCH_FIX + ECOSYSTEM_PLANNING  
Status: RESEARCH_ONLY / NOT_PRODUCTION_APPROVED

## Objective

Test whether mature pairwise-ranking algorithms can improve conflict resolution in the existing 8-node / 28-pair relative-rotation router without replacing the frozen pair signal engine.

The existing router remains authoritative for signal generation:

- 1D closed candles;
- 180d rolling median;
- 15% ARM;
- 3% reversal confirmation;
- next-open execution;
- 0.1% transition cost.

The OSS layer is consulted only when the currently held asset has more than one simultaneously confirmed outbound transition.

## Adopted dependencies

### Evalica

Repository: https://github.com/dustalov/evalica  
Reviewed upstream commit: `c3c89eb81fda382ebd96458e376e515d912e1815`  
Version used: `0.4.2`  
License: Apache-2.0  
Reuse classification: ADAPT / DEPENDENCY

Used methods:

- Bradley-Terry
- PageRank

No source code was copied into CryptoSignals. The library is installed as a pinned research dependency in the GitHub Actions experiment.

### choix

Repository: https://github.com/lucasmaystre/choix  
Reviewed upstream commit: `491d8ed9d59035f31049b047ff0d7b820f1c5b59`  
Version used: `0.4.1`  
License: MIT  
Reuse classification: ADAPT / DEPENDENCY

Used method:

- Rank Centrality

No source code was copied into CryptoSignals. The library is installed as a pinned research dependency in the GitHub Actions experiment.

## Local adapter

Local file:

`scripts/research_relative_rotation_graph_intelligence.py`

The adapter converts the 28 current pairwise relative-value preferences into pairwise comparisons, computes global ranks, and uses those ranks only to break real multi-signal conflicts.

Variants:

1. BASELINE — strongest confirmed relative extreme;
2. BRADLEY_TERRY;
3. PAGERANK;
4. RANK_CENTRALITY;
5. CONSENSUS_3 — sum of ordinal ranks from the three OSS methods.

## Pairwise preference semantics

For pair A/B with ratio B/A:

- if B/A is above its causal 180d median, A is the relative-value winner;
- if B/A is below its causal 180d median, B is the relative-value winner.

This is a diagnostic aggregation layer. It does not itself create a trade.

## Graph consistency diagnostic

Every daily complete 8-node tournament also records the fraction of cyclic triples.

Example cycle:

`ATOM > TWT > LINK > ATOM`

Cycle ratio is diagnostic only in V1. It does not block or trigger trades.

## Acceptance gate

Before interpreting any graph-ranking return:

1. the locally reconstructed BASELINE must reproduce the documented 2025-03-29 through 2026-03-28 median result (+41.6%) within a fixed +/-5 percentage-point tolerance;
2. if baseline reproduction fails, graph results are marked non-comparable and must not be promoted;
3. no parameter tuning is allowed after seeing graph results in this pass.

## Validation boundary

This experiment intentionally stops at 2026-03-28.

The post-2026-03-28 period remains untouched for the separately frozen `DEFENSIVE_LOW_VOL_CRYPTO` validation.

## Runtime impact

NONE.

No paper-live profile, production strategy, Telegram behavior, wallet logic, or real execution path is changed.
