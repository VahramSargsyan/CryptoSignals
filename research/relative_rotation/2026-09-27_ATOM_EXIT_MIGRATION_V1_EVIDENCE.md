# ATOM Exit Migration v1 — Evidence

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Branch: research/atom-exit-migration-v1
Corrected GitHub Actions run: 36342319500
Result: PASS
TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST

## Research question

How can ATOM be retired from the current rotation graph gradually, without a forced immediate sale and without blindly replacing it with another token?

No live files were changed.

## Baselines

BASE_U10:
ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

U9_CLEANER:
ATOM, TWT, PEPE, BNB, TRX, AAVE, LINK, FIL, HBAR

For TWT/PEPE common-start comparisons these two baselines produce identical paths over the evaluated windows, confirming again that SOL is inactive in the relevant routes.

## Direct removal of ATOM

DROP_ATOM_U8_CLEANER:
TWT, PEPE, BNB, TRX, AAVE, LINK, FIL, HBAR

TWT/PEPE common-start median:

| Window | With ATOM | Drop ATOM |
|---|---:|---:|
| Mature | +2550.45% | +6163.71% |
| Latest 2Y | +849.67% | +1901.44% |
| Latest 1Y | +52.30% | +15.96% |
| Worst rolling 12m | +9.83% | +22.40% |

Interpretation:
- ATOM removal strongly improves the long-cycle graph in this sample;
- the exact latest one-year endpoint is weaker without ATOM;
- rolling one-year floor is nevertheless better without ATOM;
- therefore ATOM is not uniformly harmful or uniformly useful: its effect is regime dependent.

## One-token ATOM replacements

All candidates were fixed mechanically from the 15-token pool.

### AVAX

Replace ATOM with AVAX:

- mature: +6854.00%
- latest 2Y: +2140.23%
- latest 1Y: +29.79%
- worst rolling 12m: +11.99%

AVAX improves long-cycle terminal return versus simple ATOM removal, but reduces the rolling-12m floor versus DROP_ATOM (+22.40% -> +11.99%).

Therefore AVAX is a SHADOW_CANDIDATE, not an automatic replacement.

### ALGO

- mature: +5756.85%
- 2Y: +1901.44%
- 1Y: +15.96%
- worst 12m: +22.40%

ALGO is effectively inactive in the relevant TWT/PEPE paths and adds no value over simple removal.

### SOL

In U9_CLEANER, SOL replacement is identical to simple removal:
- mature +6163.71%
- 2Y +1901.44%
- 1Y +15.96%
- worst 12m +22.40%

Again no reason to force a replacement.

### ETH

- mature +2585.95%
- 2Y +766.18%
- 1Y +14.93%
- worst 12m -22.87%

Materially worse robustness.

### ADA

- mature +5730.97%
- 2Y +1438.61%
- 1Y -10.86%
- worst 12m +14.20%

Latest-year result is negative.

### XRP

- mature +2826.38%
- 2Y +1819.44%
- 1Y +11.21%
- worst 12m +22.40%

No clear advantage over simple removal.

## EXIT_ONLY_ATOM migration

Definition:
- existing ATOM may remain as the current held state;
- no new transitions into ATOM are allowed;
- wait for the first canonical confirmed outbound ATOM signal to a member of the no-ATOM target graph;
- execute that outbound transition;
- after the exit, ATOM is permanently retired and cannot be re-entered.

Target:
TWT, PEPE, BNB, TRX, AAVE, LINK, FIL, HBAR

Historical ATOM-start outcomes:

| Start window | Wait in ATOM | First exit | Exit-only result | Legacy ATOM-start result |
|---|---:|---|---:|---:|
| Mature 2023-10-31 | 28 days | BNB | +5640.05% | +2142.74% |
| Post-PEPE 2024-06-11 | 168 days | BNB | +505.03% | +453.50% |
| Latest 2Y 2024-09-27 | 60 days | BNB | +784.83% | +709.46% |
| Latest 1Y 2025-09-27 | 23 days | FIL | +171.67% | +116.90% |

In every tested start, the one-way ATOM retirement path outperformed the corresponding legacy route that allowed later re-entry into ATOM.

This is strong historical support for EXIT_ONLY_ATOM as a migration mechanism.

## Current-state diagnostic

On the final fully closed daily candle:
2026-09-26

Confirmed outbound ATOM events eligible for the target no-ATOM graph:
NONE.

Therefore the frozen strategy does not currently justify a forced ATOM exit.

The migration rule would wait for a future confirmed outbound ATOM signal.

## Operational implication for other Cosmos assets

The research supports treating ATOM as a temporary one-way staging asset, not as a destination.

If other Cosmos-ecosystem positions are later consolidated into ATOM, the low-exposure interpretation is:
1. do not consolidate them into ATOM far in advance merely because ATOM is the bridge;
2. consolidate a position into ATOM when that position is liquid and an actual outbound migration step is ready;
3. route the resulting ATOM onward to the current eligible destination;
4. do not reopen ATOM after exit.

Exact asset-specific unstaking / liquid-staking / swap routing is outside this audit and should be handled separately per Cosmos asset.

## Recommended staged research migration

PHASE 0 — NOW
- keep current ATOM position unchanged;
- mark ATOM as proposed EXIT_ONLY in shadow research;
- stop considering ATOM as a future destination in the target graph;
- live production code remains unchanged until explicitly approved.

PHASE 1 — FIRST OUTBOUND SIGNAL
- when canonical logic produces confirmed ATOM -> X where X belongs to:
  TWT, PEPE, BNB, TRX, AAVE, LINK, FIL, HBAR
- execute/observe the normal outbound transition;
- from that point the research route is permanently no-ATOM.

PHASE 2 — NO-ATOM BASELINE
Target baseline:
TWT, PEPE, BNB, TRX, AAVE, LINK, FIL, HBAR

Do not force a replacement token.

PHASE 3 — SHADOW AVAX
Track:
BASE_NO_ATOM
versus
BASE_NO_ATOM + AVAX

AVAX has the strongest mechanical replacement evidence but is not robust enough to promote immediately.

## Main conclusion

The cleanest low-disruption path is not:

ATOM -> choose a replacement now.

It is:

ATOM -> EXIT_ONLY -> first natural outbound signal -> no-ATOM U8 -> evaluate AVAX in shadow.

This separates portfolio execution from universe redesign and avoids creating a new hindsight-selected mandatory node.

No production/live files changed.
