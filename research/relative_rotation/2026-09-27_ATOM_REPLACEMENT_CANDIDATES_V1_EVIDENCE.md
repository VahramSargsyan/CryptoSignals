# ATOM Replacement Candidates v1 — Evidence

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Branch: research/atom-replacement-candidates-v1
GitHub Actions run: 36343934773
Result: PASS
TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST

## Fixed base

U9_NO_ATOM:

TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

## Fixed candidate replacements

AVAX, ETH, ALGO, ADA, XRP

Primary comparison uses the same nine U9 starters for every variant.

## Baseline U9_NO_ATOM

- mature 2023-10-31 -> 2026-09-26: +5639.09%
- latest 2Y: +1718.15%
- latest 1Y: +18.55%
- worst rolling 12m median: +14.27%
- worst rolling 24m median: +286.34%

The absolute long-horizon numbers remain historically inflated by known opened-history effects and must not be treated as future-return expectations.

## Candidate results

| Candidate | Mature | Delta vs U9 | Latest 2Y | Delta | Latest 1Y | Delta | Worst 12m | Worst 24m |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| AVAX | +5594.47% | -44.62 pp | +1563.59% | -154.56 pp | +8.47% | -10.08 pp | +4.55% | +253.50% |
| ETH | +2958.17% | -2680.93 pp | +694.13% | -1024.01 pp | +17.49% | -1.06 pp | -14.36% | +67.16% |
| ALGO | +5563.65% | -75.44 pp | +1718.15% | 0.00 pp | +18.55% | 0.00 pp | +14.27% | +286.34% |
| ADA | +4666.08% | -973.01 pp | +1297.71% | -420.44 pp | -8.87% | -27.41 pp | +6.62% | +325.41% |
| XRP | +2447.37% | -3191.72 pp | +1643.66% | -74.49 pp | +13.69% | -4.86 pp | -3.17% | +293.05% |

No candidate improves the base U9 mature result.

No candidate improves the exact latest one-year result.

ALGO is the closest to neutral but adds essentially no practical value.

## Endpoint sensitivity

Trailing 365-day endpoint increments versus U9_NO_ATOM:

### AVAX
- 2026-05-31: -10.43 pp
- 2026-06-30: -13.44 pp
- 2026-07-31: -14.29 pp
- 2026-08-31: -13.74 pp
- 2026-09-26: -10.08 pp

AVAX is consistently harmful across all five endpoints.

### ETH
- +0.00
- +3.42
- -1.46
- -1.46
- -1.06 pp

ETH is near-neutral recently but materially harmful on the mature and two-year horizons.

### ALGO
- 0.00 pp at all five endpoints

ALGO is inert in the U9_NO_ATOM topology.

### ADA
- -8.22
- -10.58
- -12.86
- -48.79
- -27.41 pp

ADA is consistently harmful.

### XRP
- +0.00
- -61.26
- -6.72
- -6.54
- -4.86 pp

XRP is not stable and is mostly harmful.

## Topology

### AVAX
- occupancy: 4.06%
- entries/exits: 13 / 13
- no mature routes end in AVAX

It is active but degrades the graph.

### ETH
- occupancy: 3.16%
- entries/exits: 22 / 22

Active but harmful over long horizons.

### ALGO
- occupancy: 0.29%
- entries/exits: 3 / 3

Essentially inert.

### ADA
- occupancy: 14.85%
- entries/exits: 30 / 30

Highly active but degrades recent and long-horizon results.

### XRP
- occupancy: 15.88%
- entries/exits: 23 / 23

Highly active but strongly damages mature performance.

## Bull-run confound control

Candidate own-bull-neutralized mature results do not rescue any candidate relative to U9_NO_ATOM.

The candidate ranking is therefore not being rejected merely because a candidate had an old bull run; the graph itself is weaker after adding these nodes.

## Conclusion

Within the original 15-token research pool:

BEST ATOM REPLACEMENT = NONE

Working base:

U9_NO_ATOM =
TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

Do not force a tenth node.

This result does not prove that no external replacement exists. The next gate is an expanded one-at-a-time candidate screen using the documented U20/U30/U40 asset universe.

No live files changed.
