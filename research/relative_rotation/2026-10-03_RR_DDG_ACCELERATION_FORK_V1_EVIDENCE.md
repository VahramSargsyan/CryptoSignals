# RR DDG + ACCELERATION FORK V1 — EVIDENCE

Date: 2026-10-03
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Runtime identity

- Research branch: `research/rr-ddg-acceleration-fork-v1`
- Draft PR: #114
- GitHub Actions run: `37139574258`
- Job: `111251028530`
- Artifact ID: `11279532437`
- Artifact: `rr-ddg-acceleration-fork-v1-2`
- Artifact SHA256: `47fef5b01a60594a910102ee7aae664b20f8fa0fba0adf4d501e9d30bb568bc5`
- Fixed as-of: `2026-10-03T16:30:00Z`
- Latest common closed D1: `2026-10-02T00:00:00Z`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_STRESS_TEST`

## Purpose

1. Reconstruct the last month with the currently accepted RR + Destination
   Dominance 1.5x router.
2. Re-test post-route acceleration against the **corrected** destination.
3. Test the preregistered hypothesis:
   `NEXT RR SIGNAL vs ACCELERATION FORK`.
4. Reconstruct the 2026-09-28 LINK case with AAVE compared against TRX rather
   than the obsolete intermediate ALGO branch.

## Corrected LINK case

At the 2026-09-28 close:

Old baseline route:
`LINK -> ALGO`

Current accepted DDG route:
`LINK -> TRX`

Evidence:
- DDG override: YES
- competing destination: TRX
- strength ratio: **2.078x**
- threshold: 1.50x

Thus the production patch addresses the original unnecessary LINK -> ALGO hop
for an equivalent future topology.

### AAVE versus the corrected TRX destination

Assume corrected LINK -> TRX execution at the 2026-09-29 open.

| Observation | Strongest accelerator | AAVE relative impulse vs TRX | AAVE rank |
|---:|---|---:|---:|
| Sep-29 close / day 1 | AAVE | +10.93% | 1 |
| Sep-30 close / day 2 | AAVE | +6.10% | 1 |
| Oct-01 close / day 3 | AAVE | +14.93% | 1 |

AAVE therefore crossed the frozen +10% acceleration threshold **causally on
day 1**, even against the corrected TRX destination.

Hypothetical decision:
- detect AAVE: Sep-29 close;
- switch TRX -> AAVE: Sep-30 open;
- AAVE was the strongest acceleration candidate;
- AAVE relative excess versus remaining in TRX through the latest available
  closed candle in V1: **+8.55%**.

This episode is real and causally detectable. It is not proof that the rule
generalizes.

## Full-history Test B — acceleration after corrected RR+DDG route

Corrected RR+DDG route opportunities:
- **3429**

+10% first-3-bar acceleration triggers:
- **1499**
- trigger rate: **43.8%**

| Horizon | N | Candidate beats corrected destination | Median relative excess | Mean relative excess | >=20% continuation |
|---:|---:|---:|---:|---:|---:|
| 3d | 1492 | 45.8% | -0.57% | +0.49% | 7.0% |
| 7d | 1475 | 43.7% | -2.18% | +3.14% | 11.3% |
| 14d | 1458 | 46.5% | -1.66% | +4.37% | 15.0% |
| 30d | 1435 | 43.7% | -4.37% | +5.26% | 19.3% |

Classification:

`NAIVE_ACCELERATION_OVERRIDE_AFTER_DDG = NOT_SUPPORTED`

The positive mean and negative median again show a right-tailed distribution:
a minority of large winners coexist with a majority of mediocre/negative
switches.

## Full-history Test C — NEXT RR SIGNAL vs ACCELERATION FORK

Short-hop cases where the corrected destination generated another RR+DDG route
within 1-3 signal closes:
- **265**

Cases where a different acceleration candidate exceeded +10% at that next RR
signal:
- **76**
- conflict rate: **28.7%**

| Horizon | N | Acceleration candidate beats next RR destination | Median relative excess | Mean relative excess | >=20% continuation |
|---:|---:|---:|---:|---:|---:|
| 3d | 76 | 47.4% | -0.12% | +5.11% | 10.5% |
| 7d | 76 | 44.7% | -1.82% | +9.93% | 15.8% |
| 14d | 76 | **39.5%** | **-2.81%** | +12.09% | 21.1% |
| 30d | 76 | 39.5% | -6.42% | +19.41% | 21.1% |

The preregistered hypothesis does **not** generalize as a direct switching rule.

Classification:

`NEXT_RR_SIGNAL_VS_ACCELERATION_OVERRIDE = NOT_SUPPORTED`

Again the distribution is extremely right-skewed. A few very large continuation
winners lift the mean while most conflicts favor the RR branch.

## Recent month: 2026-09-03 onward

Corrected source/date route opportunities:
- **113**

Eligible routes with enough first-3-bar data for the acceleration audit:
- **104**

Acceleration triggers:
- **52**

Among the subset with outcomes already observable by the fixed as-of:

| Horizon | N with result | Acceleration beats corrected destination | Median relative excess |
|---:|---:|---:|---:|
| 3d | 45 | 37.8% | -6.65% |
| 7d | 28 | 14.3% | -8.26% |
| 14d | 11 | 27.3% | -6.11% |

The recent-month sample is right-censored and must not be treated as a complete
14/30-day test, but the available outcomes are strongly against indiscriminate
acceleration switching.

### Recent short-hop conflict hypothesis

Recent 1-3 day short-hop cases:
- **14**

Recent conflicts meeting the exact preregistered +10% alternative condition:
- **0**

Thus the LINK -> ALGO historical branch is not a recent example of the
full `next RR signal vs acceleration fork` after applying the corrected
router, because the corrected router would already have entered TRX.

The AAVE question belongs instead to Test B:
`corrected TRX position -> post-route AAVE acceleration`.

## One-month path convergence diagnostic

Using the corrected route ledger from Sep-03 onward and starting from each
possible asset, the route chains converge to TRX by Sep-23 or earlier:

- LINK -> FIL -> AVAX -> TRX
- TWT -> FIL -> AVAX -> TRX
- SOL -> AVAX -> TRX
- AAVE -> HBAR -> TRX
- XRP -> AVAX -> TRX
- ALGO -> TRX
- AVAX -> TRX
- FIL -> AVAX -> TRX
- HBAR -> TRX
- ATOM -> HBAR -> TRX
- PEPE -> AVAX -> TRX
- BNB -> AVAX -> TRX
- TRX -> TRX

This is a descriptive routing-topology result, not a return claim.

It shows that during this month the corrected RR graph had a strong common
destination attractor: **TRX**.

## Interpretation

Three statements can simultaneously be true:

1. the old LINK -> ALGO hop was avoidable under the accepted DDG patch;
2. AAVE became detectably stronger than the corrected TRX position one day
   later and has so far rewarded a hypothetical Sep-30 switch;
3. blindly applying the same acceleration switch to every historical case would
   reduce the typical outcome relative to staying with RR+DDG.

Therefore the unresolved research question is not:

`SHOULD WE SWITCH TO THE FASTEST TOKEN?`

It is:

`WHICH +10% ACCELERATION EVENTS BELONG TO THE SMALL EXTREME-CONTINUATION TAIL?`

AAVE currently appears to be in that tail, but V1 does not yet provide a
general causal classifier that separates those events at detection time.

## Decision

No production change.

Frozen classifications:

- `LINK_2026_09_28_CURRENT_DDG_DESTINATION = TRX`
- `AAVE_DAY1_VS_TRX_ACCELERATION = CAUSALLY_DETECTED`
- `AAVE_CURRENT_EPISODE = POSITIVE_SO_FAR`
- `NAIVE_ACCELERATION_OVERRIDE_AFTER_DDG = NOT_SUPPORTED`
- `NEXT_RR_SIGNAL_VS_ACCELERATION_OVERRIDE = NOT_SUPPORTED`
- `EXTREME_CONTINUATION_CLASSIFIER = OPEN_RESEARCH_QUESTION`

## Next admissible research

A separately preregistered classifier should use features available at the
+10% detection close to distinguish the extreme continuation tail from the
majority that mean-reverts.

Promising fixed feature families:
- persistence: candidate remains rank #1 on a second close;
- acceleration shape: impulse expands again after the first trigger;
- candidate's own RR topology against several TARGET assets;
- cross-sectional breadth: how many assets the candidate is beating;
- volume/liquidity expansion if point-in-time data are available.

No threshold from V1 should be retuned on the same history.

## Artifact files

- `all_effective_routes.csv`
- `recent_month_effective_routes.csv`
- `post_route_acceleration.csv`
- `recent_month_acceleration.csv`
- `next_signal_acceleration_forks.csv`
- `recent_month_forks.csv`
- `acceleration_summary.csv`
- `fork_summary.csv`
- `link_case_corrected_vs_aave.csv`
- `link_case.json`
- `summary.json`
- `report.md`
