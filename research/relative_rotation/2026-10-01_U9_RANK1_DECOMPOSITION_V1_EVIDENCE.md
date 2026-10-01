# U9 ROBUST RANK #1 DECOMPOSITION V1 — EVIDENCE

Date: 2026-10-01  
Workflow mode: STRESS_TEST_ONLY  
Production/live changes: NONE

## Runtime identity

- Research branch: `research-run/u9-rank1-decomposition-v1`
- GitHub Actions run: `36811462195`
- Source commit: `e687f9e8538e68fa3b73995299f6b097fbca47fe`
- Artifact ID: `11139613961`
- Artifact SHA256: `abe84906dd3f668705001765208edc4db8683e0cd6a1ffcc4ca10e53ba1c656c`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Research question

Why did the exhaustive robust U9 rank #1 composition differ from the historically higher-return U9 rank #4 composition, and which network nodes/routes actually caused the difference?

This is a causal path-decomposition study, not a new universe search.

## Compared universes

### Robust U9 rank #1

`TWT, PEPE, SOL, AAVE, LINK, AVAX, FIL, ALGO, HBAR`

### Robust U9 rank #4 / BNB+TRX variant

`TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, HBAR`

### Shared core

`TWT, PEPE, AAVE, AVAX, FIL, ALGO, HBAR`

Rank #1-only nodes:
- SOL
- LINK

Rank #4-only nodes:
- BNB
- TRX

## Important ranking interpretation

Robust rank #1 does **not** mean highest MATURE return.

In this live-public-data rerun:

| Universe | 1Y median | 2Y median | MATURE median | MATURE median DD |
|---|---:|---:|---:|---:|
| Rank #1: SOL+LINK | +191.1% | +1593.9% | +4998.1% | -62.4% |
| Rank #4: BNB+TRX | +169.2% | +1958.8% | +6985.7% | -62.4% |

Rank #1 is the more robust exhaustive composition across ranking dimensions, while BNB+TRX produced the larger MATURE return.

The original frozen exhaustive artifact from 2026-09-28 remains canonical for its exact values (+4881.4% vs +6904.0% MATURE). This rerun is a separate causal decomposition and must not overwrite the frozen ranking evidence.

## Exact 2-of-4 replacement matrix

Shared core plus every two-node combination from SOL, LINK, BNB, TRX:

| Added pair | 1Y median | 2Y median | MATURE median | MATURE DD |
|---|---:|---:|---:|---:|
| BNB + TRX | +169.2% | +1958.8% | **+6985.7%** | -62.4% |
| SOL + LINK | **+191.1%** | +1593.9% | +4998.1% | -62.4% |
| SOL + TRX | +172.9% | +1511.1% | +4686.4% | -62.4% |
| LINK + TRX | +171.8% | +1451.1% | +4660.4% | -62.4% |
| LINK + BNB | +37.7% | +1970.8% | +4479.8% | -62.4% |
| SOL + BNB | +176.1% | **+2011.7%** | +4416.7% | -62.4% |

This shows that performance is not explained by a simple single-token ranking. The pair/network context materially changes the route graph.

## Key structural finding: SOL and LINK were not physically used

Across the seven shared MATURE starting assets, Rank #1 path usage recorded:

- SOL held days: **0**
- LINK held days: **0**
- SOL entries/exits: **0 / 0**
- LINK entries/exits: **0 / 0**

Therefore the difference between Rank #1 and Rank #4 is not that SOL or LINK themselves generated return.

Their presence — and, critically, the absence of BNB/TRX — changes which routes are available from the shared nodes.

This is a network-topology effect.

## First path divergence

Across the seven common start assets:

- 2023-11-14 execution: 4 starts
- 2023-11-15 execution: 1 start
- 2023-12-07 execution: 2 starts

For the typical TWT/AAVE/AVAX starts, the decisive signal is on 2023-11-13 and executes on 2023-11-14.

### Rank #1

From AVAX:

`AVAX -> PEPE`

max_dislocation: **41.14%**

### Rank #4

From AVAX:

`AVAX -> BNB`

max_dislocation: **58.25%**

Because BNB exists in Rank #4, the stronger eligible BNB route wins. Rank #1 cannot take that branch and goes to PEPE instead.

## Early fork routes

Representative Rank #1 path:

`AVAX -> PEPE -> TWT`

Representative Rank #4 path:

`AVAX -> BNB -> TRX -> TWT`

Rank #4 sequence:

- 2023-11-13 signal / 2023-11-14 execution: AVAX -> BNB, max dislocation 58.25%
- 2023-12-29 signal / 2023-12-30 execution: BNB -> TRX, max dislocation 17.57%
- 2024-01-23 signal / 2024-01-24 execution: TRX -> TWT, max dislocation 15.63%

Both systems reconverge on TWT on 2024-01-24 for the shared paths.

## Capital already differs at reconvergence

Rank1 / Rank4 capital ratio at first reconvergence:

- TWT start: 0.8793
- PEPE start: 0.6567
- AAVE start: 0.8793
- AVAX start: 0.8793
- FIL start: 0.8913
- ALGO start: 1.0264
- HBAR start: 0.6567

Thus BNB/TRX already created a substantial advantage for most shared starts before the paths reconverged.

ALGO is the main exception at the first reconvergence.

## The first fork is not the whole story

A second counterfactual equalized capital at the first reconvergence and restarted both universes from the same holding.

From 2024-01-24 to the end:

`Rank1 / Rank4 final capital ratio = 0.8227359426`

So even after deleting the early capital advantage, the BNB+TRX universe later earns about **21.5% more** from the same capital base.

The final common-start Rank1 / Rank4 capital ratios range roughly from:

- 0.5403 to 0.8445

Equivalently, Rank #4 finishes with about:

- 1.18x to 1.85x Rank #1 capital

depending on the common start.

Therefore this is **not** an XRP-style single-fork story. The early fork is important, but later route differences also matter.

## Contrast with XRP node interference

The previously documented XRP case showed:

- a harmful added node;
- one dominant 2024 fork;
- later reconvergence;
- most of the final capital gap already locked in by that early fork.

The BNB/TRX case is the opposite pattern:

- added BNB/TRX routes are historically beneficial;
- the early 2023-2024 fork creates part of the advantage;
- later route differences add further advantage;
- the effect is not reducible to one token's own price appreciation.

Together these cases show that adding a node to the RR universe can either improve or damage the capital route graph.

## Research interpretation

The evidence supports the following mechanism statement:

> Relative Rotation universe composition is a route-graph design problem, not merely an asset-selection problem.

A token may matter even when it is never physically held, because its inclusion or exclusion changes eligible transitions and therefore future path topology.

Conversely, a token can be useful because it opens a beneficial intermediate route (BNB -> TRX -> TWT), not because the token is intrinsically the strongest buy-and-hold asset.

No ex-ante rule has yet been demonstrated that reliably distinguishes beneficial route nodes (BNB/TRX in this history) from harmful route nodes (XRP in the previously studied history).

## No-repeat rule

Do not repeat this exact U9 Rank #1 vs Rank #4 decomposition on the same known historical period merely to rediscover the same result.

A rerun is justified only by:

1. materially new unseen market data;
2. a documented route-engine semantic correction;
3. changed cost/execution assumptions;
4. a different universe comparison;
5. a new causal hypothesis, such as an ex-ante fundamental/network filter;
6. a forward-validation protocol.

## Production boundary

- No paper/live strategy files changed.
- No universe promotion is authorized by this analysis.
- No Telegram/execution behavior changed.
- BNB/TRX historical benefit must not be converted into a future hardcoded preference without forward evidence.

## Residual risks

- Known-history causal decomposition is not unseen OOS evidence.
- Large compounded returns remain sensitive to route timing.
- This rerun used current live-public historical data and therefore is not byte-identical to the 2026-09-28 frozen exhaustive dataset.
- The current analysis establishes historical mechanism, not a future-return guarantee.
