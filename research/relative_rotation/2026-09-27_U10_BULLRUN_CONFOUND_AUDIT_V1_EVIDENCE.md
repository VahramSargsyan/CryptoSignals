# U10 Bull-Run Confound Audit v1 — Evidence

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Branch: research/u10-bullrun-confound-audit-v1
GitHub Actions run: 36328897681
Result: PASS
TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST

## Purpose

Test whether U10's large historical result is mostly an artifact of including tokens that rose explosively from depressed prices.

No live universe was changed.

## Frozen PRIMARY explosive definition

A token is PRIMARY explosive if either:
- maximum low-to-high drawup inside any rolling 90-day window >= +200% (>=3x), or
- maximum low-to-high drawup inside any rolling 180-day window >= +400% (>=5x).

This definition was fixed before viewing results.

## Primary explosive assets in the 15-token candidate pool

- ADA
- ALGO
- AVAX
- HBAR
- PEPE
- SOL
- XRP

U10 PRIMARY explosive members:
- PEPE
- SOL
- HBAR

Mandatory-user-held PRIMARY explosive member:
- PEPE

Therefore a strictly zero-bull-run universe is impossible while PEPE remains mandatory. The useful question is whether the strategy's P&L depends on harvesting those bull runs.

## Token bull metrics relevant to U10

| Asset | Max 90d low-to-high drawup | Max 180d low-to-high drawup | PRIMARY |
|---|---:|---:|---|
| PEPE | +1155.46% | +1798.86% | YES |
| HBAR | +780.42% | +780.42% | YES |
| SOL | +535.37% | +960.75% | YES |
| AAVE | +199.00% | +390.45% | NO |
| TRX | +193.83% | +284.63% | NO |
| LINK | +190.36% | +267.10% | NO |
| ATOM | +183.11% | +183.11% | NO |
| BNB | +164.95% | +208.48% | NO |
| FIL | +148.15% | +278.45% | NO |
| TWT | +132.82% | +144.05% | NO |

## U10 baseline

Mature window:
2023-10-31 -> 2026-09-26

- median return: +2249.75%
- ATOM-start: +2142.74%

## Bull-run P&L neutralization

For each token, the strategy path was kept unchanged, but positive daily equity gains were neutralized while the held asset was inside that token's single strongest 90-day drawup interval.

This is an attribution stress test, not a tradable counterfactual.

### U10 PRIMARY tokens

PEPE strongest 90d interval:
2024-02-23 -> 2024-05-23

Neutralizing positive U10 P&L while PEPE was held in this interval:
- median falls from +2249.75% to +779.31%
- ATOM falls from +2142.74% to +739.26%

PEPE therefore explains a large part of the magnitude.

HBAR strongest 90d interval:
2024-11-04 -> 2025-01-17

Neutralization effect:
- no measurable change to U10 terminal return

SOL strongest 90d interval:
2023-09-26 -> 2023-12-25

Neutralization effect:
- no measurable change to U10 terminal return

Interpretation:
HBAR and SOL satisfy the price-path bull-run definition, but U10 did not earn its result by being held during their strongest historical bull-run intervals.

### Simultaneous PRIMARY neutralization

Neutralizing positive held-asset P&L during the strongest 90d interval of PEPE, SOL and HBAR simultaneously:

- median: +779.31%
- ATOM: +739.26%

This is identical to PEPE-only neutralization because SOL/HBAR's strongest intervals contributed no positive held-route P&L under the canonical U10 path.

Conclusion:
the headline +2249.75% is materially inflated by PEPE's historical explosive episode, but the strategy does not collapse when that episode is removed from P&L attribution. It remains strongly positive in this diagnostic.

## Node ablation: price explosion versus structural usefulness

| Removed from U10 | PRIMARY explosive | Mature median | ATOM | Effect |
|---|---|---:|---:|---|
| HBAR | YES | +622.63% | +589.72% | severe degradation |
| FIL | NO | +1357.01% | +1199.05% | severe degradation |
| AAVE | NO | +1393.36% | +1465.03% | degradation |
| TRX | NO | +1456.57% | +1320.98% | degradation |
| BNB | NO | +2025.49% | +1751.85% | moderate degradation |
| LINK | NO | +2142.74% | +2142.74% | small / start-dependent |
| SOL | YES | +2356.75% | +2142.74% | improves median when removed |

This produces two different cases:

### SOL

SOL is both:
- PRIMARY explosive;
- structurally unnecessary in this U10 sample.

Removing SOL improves median return by about +107 percentage points while leaving ATOM-start unchanged.

SOL is therefore the first rational replacement/drop candidate.

### HBAR

HBAR is PRIMARY explosive, but:
- U10 does not profit from its strongest bull-run interval;
- removing HBAR destroys much of the U10 network effect.

HBAR's evidence is consistent with a structural bridge role, not simply bull-run harvesting.

It should not be rejected solely because its own price once exploded.

## PEPE constraint

PEPE is both:
- mandatory under the user's current portfolio/rotation constraint;
- the most explosive token in the candidate pool;
- a material source of U10's historical P&L magnitude.

Therefore any claim that the current research universe is “bull-run free” would be false.

A better standard is:
- mandatory PEPE stays;
- optional assets should avoid unnecessary bull-run dependence;
- strategy robustness must be reported after PEPE bull-period neutralization.

## Optional PRIMARY-explosive assets

After keeping mandatory ATOM/TWT/PEPE, optional PRIMARY-explosive assets are:
- SOL
- AVAX
- ALGO
- ADA
- XRP
- HBAR

PRIMARY-clean optional assets are:
- BNB
- TRX
- AAVE
- LINK
- FIL
- ETH

There are exactly six ways to choose five of those six clean optional assets for a bull-clean U8.

## Bull-clean U8 distribution

Among all 792 mandatory ATOM/TWT/PEPE U8s, grouping by the number of optional PRIMARY-explosive members:

| Optional explosive members | Sets | Median last-year return | Positive last-year sets | Median two-year return |
|---:|---:|---:|---:|---:|
| 0 | 6 | +42.35% | 83.33% | +146.99% |
| 1 | 90 | +12.53% | 71.11% | +166.33% |
| 2 | 300 | +9.10% | 61.67% | +224.21% |
| 3 | 300 | +1.81% | 50.33% | +249.60% |
| 4 | 90 | -14.68% | 33.33% | +244.58% |
| 5 | 6 | -15.11% | 16.67% | +225.17% |

This is a strong last-year relationship:
adding more optional historically explosive assets is associated with worse recent one-year robustness.

The two-year relationship is not monotonic; explosive assets can still contribute to longer-cycle upside.

Therefore “remove every token that ever bull-ran” is too crude, but avoiding unnecessary explosive optional nodes looks useful for recent-regime robustness.

## Bull-clean structural candidates

Using only training data through 2025-03-28 and the already frozen structural concepts (niche count, relative volatility, low correlation, low PCA1 concentration), the six available bull-clean U8 candidates are:

1. ATOM/TWT/PEPE/BNB/TRX/AAVE/LINK/ETH
2. ATOM/TWT/PEPE/BNB/TRX/AAVE/LINK/FIL
3. ATOM/TWT/PEPE/BNB/TRX/AAVE/FIL/ETH
4. ATOM/TWT/PEPE/TRX/AAVE/LINK/FIL/ETH
5. ATOM/TWT/PEPE/BNB/TRX/LINK/FIL/ETH
6. ATOM/TWT/PEPE/BNB/AAVE/LINK/FIL/ETH

The top structural candidate does not use SOL or HBAR.

However, because HBAR has already shown strong bridge value without harvesting its own bull run, a strict price-path ban may throw away useful topology.

## Current conclusion

The user's confound concern is valid.

- The original +2249.75% U10 headline materially benefits from PEPE's historical explosion.
- After PEPE/SOL/HBAR strongest-90d positive-P&L neutralization, U10 still returns about +779% median / +739% ATOM.
- SOL is the clearest token to drop or replace: explosive and non-essential.
- HBAR should not be dropped merely because it bull-ran: its own bull-run P&L contribution is zero in the canonical route while its topology contribution is large.
- PEPE remains the unavoidable known bull-run confound under the user's mandatory-asset constraint.

## Next clean substitution test

A natural post-audit candidate is:
U10_SOL_TO_ETH = U10 - SOL + ETH

Rationale:
- SOL is PRIMARY explosive and redundant under ablation;
- ETH is not PRIMARY explosive under the frozen threshold;
- ETH preserves the smart-contract-L1 niche;
- the substitution is derived from this audit, so its future test must be labeled post-hoc / hypothesis generation.

No live files changed.
