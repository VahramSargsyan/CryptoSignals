# U10 SOL-to-ETH substitution v1 — Evidence

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Branch: research/u10-sol-to-eth-v1
GitHub Actions run: 36329221448
Result: PASS
TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST

## Derivation

The bull-run confound audit found SOL to be:
- PRIMARY explosive under a preregistered price-path rule;
- non-essential in U10 node ablation.

ETH was the only PRIMARY-clean optional token in the original candidate pool that could preserve the SMART_CONTRACT_L1 primary niche.

Therefore SOL -> ETH was tested as a mechanically derived substitution.

## Fair-comparison starters

Cross-universe medians use the same seven starters available in every tested universe:
ATOM, TWT, PEPE, BNB, TRX, AAVE, LINK.

ATOM-start is separately preserved.

## Universes

BASE_U10:
ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

DROP_SOL_U9:
ATOM, TWT, PEPE, BNB, TRX, AAVE, LINK, FIL, HBAR

SOL_TO_ETH_U10:
ATOM, TWT, PEPE, BNB, ETH, TRX, AAVE, LINK, FIL, HBAR

DROP_HBAR_U9 (context):
ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL

## Results

| Universe | Mature ~34.9m | Latest 1Y | Latest 2Y | Mature median DD | PEPE-bull-neutralized mature | Worst rolling 24m |
|---|---:|---:|---:|---:|---:|---:|
| BASE_U10 | +2356.75% | +90.61% | +713.78% | -71.04% | +819.35% | +85.30% |
| DROP_SOL_U9 | +2356.75% | +90.61% | +713.78% | -71.04% | +819.35% | +85.30% |
| SOL_TO_ETH_U10 | +942.80% | +88.91% | +226.76% | -71.04% | +290.23% | -30.03% |
| DROP_HBAR_U9 | +655.53% | +39.83% | +150.26% | -71.04% | +182.73% | -41.31% |

## Key finding 1 — SOL is removable

For the seven common starters, removing SOL causes no measurable change in:
- mature terminal return;
- latest one-year return;
- latest two-year return;
- drawdown;
- PEPE-bull-neutralized return;
- worst rolling 24-month return.

Therefore SOL is redundant in the observed useful routing for these starts.

Given that SOL also satisfies the PRIMARY explosive price-path rule, the evidence supports dropping SOL from the research baseline.

## Key finding 2 — do not force a niche replacement

Replacing SOL with ETH preserves the L1 label but materially worsens the graph:
- mature median falls from +2356.75% to +942.80%;
- latest two-year return falls from +713.78% to +226.76%;
- PEPE-neutralized mature return falls from +819.35% to +290.23%;
- worst rolling 24-month result falls from +85.30% to -30.03%.

Therefore economic-niche diversity is useful as a screen, but maximizing niche count mechanically is not a valid rule.

Network topology and behavioral complementarity dominate label completeness.

## Key finding 3 — HBAR is different from SOL

Removing HBAR:
- mature median drops to +655.53%;
- latest year drops to +39.83%;
- latest two years drop to +150.26%;
- worst rolling 24m becomes -41.31%.

Combined with the prior bull-run attribution result showing zero direct contribution from HBAR's strongest historical bull interval, HBAR remains consistent with a structural bridge role.

## Updated research baseline candidate

U9_CLEANER:
ATOM, TWT, PEPE, BNB, TRX, AAVE, LINK, FIL, HBAR

Properties:
- removes the redundant PRIMARY-explosive SOL node;
- retains FIL/HBAR complementarity;
- does not force a harmful ETH replacement;
- still contains mandatory PRIMARY-explosive PEPE, so it is not literally bull-run free;
- retains strong performance even after PEPE strongest-bull-interval positive-P&L neutralization.

## Interpretation

The current evidence favors token deletion over one-for-one replacement when a niche representative does not add useful routing.

This changes the working rule:

NOT:
one token per maximum number of niches.

BETTER:
maximize useful economic/behavioral complementarity subject to avoiding redundant or stale nodes.

No live files changed.
