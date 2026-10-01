# Relative Rotation U9 Rank #1 Decomposition — Decision Log V1

Date: 2026-10-01  
Status: ACTIVE RESEARCH MEMORY  
Workflow mode: STRESS_TEST_ONLY  
Production change: NONE

## Compared universes

Robust Rank #1:

`TWT, PEPE, SOL, AAVE, LINK, AVAX, FIL, ALGO, HBAR`

BNB+TRX / robust Rank #4:

`TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, HBAR`

Canonical evidence:

`research/relative_rotation/2026-10-01_U9_RANK1_DECOMPOSITION_V1_EVIDENCE.md`

Runtime:

- GitHub Actions run: `36811462195`
- source commit: `e687f9e8538e68fa3b73995299f6b097fbca47fe`
- artifact ID: `11139613961`
- artifact SHA256: `abe84906dd3f668705001765208edc4db8683e0cd6a1ffcc4ca10e53ba1c656c`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Decision facts

1. Robust Rank #1 is not the highest-return MATURE universe.
   - Rank #1 SOL+LINK: +4998.1% in this rerun.
   - Rank #4 BNB+TRX: +6985.7%.

2. SOL and LINK were held **0 days** across the seven shared MATURE starts.
   Their role is topological, not direct return generation.

3. The first major fork is caused by BNB being available:
   - Rank #1: AVAX -> PEPE at 41.14%.
   - Rank #4: AVAX -> BNB at 58.25%.

4. Rank #4 then follows the historically beneficial intermediate chain:
   `BNB -> TRX -> TWT`.

5. At first reconvergence on 2024-01-24, Rank #4 already has more capital for most starts.

6. Equalizing capital at that reconvergence does **not** erase the advantage.
   From reconvergence to the end:
   `Rank1 / Rank4 = 0.8227`.

7. Therefore this is not a one-fork-only phenomenon. Later BNB/TRX-enabled route differences also matter.

8. This case is the useful opposite of XRP node interference:
   - XRP historically opened a harmful path.
   - BNB/TRX historically opened beneficial paths.

## Frozen interpretation

`UNIVERSE_NODE_VALUE_IS_TOPOLOGICAL_NOT_JUST_ASSET_RETURN`

and

`NODE_ADDITION_CAN_HELP_OR_HURT_ROUTE_GRAPH`

No reliable ex-ante discriminator has yet been demonstrated.

## Research use

Use this case together with the XRP interference case as a paired benchmark for future filters:

- fundamental destination filter;
- network support filter;
- route-quality filter;
- forward shadow validation.

A proposed filter is interesting only if it can distinguish both cases causally using information available before the outcome.

## No-repeat rule

Do not rerun the same decomposition on the same history unless there is new data, corrected semantics, changed execution assumptions, or a new preregistered causal hypothesis.

## Production boundary

No change to production universe, paper/live routing, Telegram, or execution is authorized by this decision log.
