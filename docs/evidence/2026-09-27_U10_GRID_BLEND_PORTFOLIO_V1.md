# U10 + Grid Blend Portfolio v1 — Evidence

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Validated branch: research/u10-grid-blend-v1
GitHub Actions run: 36341663411
Source commit: 03bcbfc0ebed738ab8a2814a1ceb9751ff3bc005
Artifact ID: 10939046493
Artifact digest: sha256:e389b26b8d7a3b9e9ac9eb712b6dcc5861a8c3917a6f488d4f0e1a2d525ff89a
Result: PASS
TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST

## Objective

Test fixed start-of-period blends of frozen U10 and frozen Grid without changing either strategy's signals.

The portfolio is split once at the start.

No rebalancing or capital transfers occur afterward.

Example:

70% U10 / 30% Grid on 10,000 USDT:
- 7,000 enters U10;
- 3,000 enters Grid;
- each sleeve compounds independently;
- combined daily equity is the sum of the two sleeve equities.

## Window

2023-10-31 -> 2026-09-26

Starting capital:

10,000 USDT

## Controls

| Control | Final equity | Return | Max DD |
|---|---:|---:|---:|
| 100% U10 | 219,485.20 | +2,094.85% | -71.04% |
| 100% GRID_TIER_A_5 BASE | 33,530.59 | +235.31% | -43.48% |
| 100% GRID_TIER_A_B_10 BASE | 35,613.98 | +256.14% | -44.97% |
| 100% GRID_FULL_15 BASE | 29,674.06 | +196.74% | -41.26% |

## Blend results

| Grid sleeve | U10 / Grid | Final equity | Return | Max DD | DD reduction vs U10 | Terminal equity retained vs U10 |
|---|---:|---:|---:|---:|---:|---:|
| Tier A 5 | 80/20 | 182,294.28 | +1,722.94% | -67.60% | 3.44 pp | 83.06% |
| Tier A 5 | 70/30 | 163,698.82 | +1,536.99% | -65.41% | 5.63 pp | 74.58% |
| Tier A 5 | 60/40 | 145,103.36 | +1,351.03% | -62.78% | 8.26 pp | 66.11% |
| Tier A 5 | 50/50 | 126,507.89 | +1,165.08% | -59.59% | 11.46 pp | 57.64% |
| Tier A+B 10 | 80/20 | 182,710.96 | +1,727.11% | -68.04% | 3.00 pp | 83.25% |
| Tier A+B 10 | 70/30 | 164,323.83 | +1,543.24% | -66.16% | 4.88 pp | 74.87% |
| Tier A+B 10 | 60/40 | 145,936.71 | +1,359.37% | -63.94% | 7.10 pp | 66.49% |
| Tier A+B 10 | 50/50 | 127,549.59 | +1,175.50% | -61.33% | 9.71 pp | 58.11% |
| Full 15 | 80/20 | 181,522.97 | +1,715.23% | -68.00% | 3.05 pp | 82.70% |
| Full 15 | 70/30 | 162,541.86 | +1,525.42% | -66.06% | 4.99 pp | 74.06% |
| Full 15 | 60/40 | 143,560.74 | +1,335.61% | -63.73% | 7.31 pp | 65.41% |
| Full 15 | 50/50 | 124,579.63 | +1,145.80% | -60.89% | 10.16 pp | 56.76% |

## Highest terminal blend

Tier A+B 10 BASE / 80% U10 + 20% Grid

Final equity:

182,710.96 USDT

Return:

+1,727.11%

Max drawdown:

-68.04%

Compared with 100% U10:
- terminal equity lower by 36,774.25 USDT;
- 83.25% of U10 terminal equity retained;
- max drawdown improved by only 3.00 percentage points.

## Shallowest blend drawdown

Tier A 5 BASE / 50% U10 + 50% Grid

Final equity:

126,507.89 USDT

Return:

+1,165.08%

Max drawdown:

-59.59%

Compared with 100% U10:
- terminal equity lower by 92,977.31 USDT;
- only 57.64% of U10 terminal equity retained;
- max drawdown improved by 11.46 percentage points.

This is a substantial drawdown reduction, but it is expensive in lost terminal growth.

## Growth-versus-drawdown trade-off

For the Tier A 5 sleeve:

- 80/20 sacrificed 16.94% of U10 terminal equity for 3.44 pp drawdown reduction.
- 70/30 sacrificed 25.42% for 5.63 pp.
- 60/40 sacrificed 33.89% for 8.26 pp.
- 50/50 sacrificed 42.36% for 11.46 pp.

On this history, the blend did not create a free diversification benefit.

More Grid steadily reduced drawdown, but terminal growth fell much faster.

## Correlation / drawdown overlap

Daily close-to-close return correlation with U10:

- Tier A 5 BASE: +0.512
- Tier A+B 10 BASE: +0.553
- Full 15 BASE: +0.557

These are moderate positive correlations, not hedge-like negative correlations.

Using the descriptive definition "below each sleeve's own running peak":

- U10 and Tier A 5 were simultaneously in drawdown on about 89.17% of common daily observations;
- U10 and Tier A+B 10: about 89.92%;
- U10 and Full 15: about 90.02%.

Conditional on at least one sleeve being below its peak, simultaneous drawdown overlap was roughly 90.9% to 92.1%.

Interpretation:

Grid did not usually move opposite U10 during stress.

The blend's lower percentage drawdown is primarily a lower-volatility/lower-growth diversification effect, not evidence that Grid reliably earns positive returns during U10 drawdowns.

## Important absolute-equity nuance

U10's absolute strongest mature-path drawdown trough was 2024-09-07.

At that date:
- 100% U10 equity was about 19,365.54 USDT;
- Tier A 5 80/20 blend: 18,574.33;
- Tier A 5 70/30: 18,178.72;
- Tier A 5 60/40: 17,783.12;
- Tier A 5 50/50: 17,387.51;
- Tier A+B 10 80/20: 18,515.39;
- Full 15 80/20: 18,348.74.

Thus the blends had a smaller percentage peak-to-trough drawdown but did not have a higher absolute equity floor at this particular early U10 trough.

Why:

U10 had already accumulated a much larger pre-trough gain.

Reducing the U10 sleeve reduced both its peak and the capital carried into the trough.

This is why percentage drawdown and absolute capital at a stress date must both be reported.

## Original-capital minimum

Blend minimum versus the original 10,000 USDT remained mild in all tested cases:

- Tier A 5: from -2.59% at 80/20 to -0.44% at 50/50;
- Tier A+B 10: from -2.73% to -1.08%;
- Full 15: from -2.94% to -1.02%.

100% U10 long-path minimum was -4.30%.

So Grid did modestly improve protection of original principal during the early path.

## Main findings

1. A simple fixed U10/Grid split works mechanically and reduces U10 percentage drawdown.
2. The reduction is not large enough to be free: terminal growth declines substantially as Grid weight increases.
3. Grid is moderately positively correlated with U10 and spends most stress periods in drawdown at the same time.
4. Therefore Grid is not a strong historical hedge for U10; it is primarily a lower-growth, lower-volatility sleeve.
5. Tier A 5 gave the strongest drawdown reduction at equal Grid weights.
6. Tier A+B 10 preserved slightly more terminal growth but reduced U10 drawdown slightly less.
7. Full 15 did not produce a clear diversification advantage over the selected smaller Grid universes.
8. The blend's smaller percentage drawdown can coexist with lower absolute equity at a particular U10 trough because U10 had accumulated more capital before that trough.

## Research implication

The simple static blend is less compelling than expected if the objective is to preserve most of U10's historical upside while materially reducing its severe drawdowns.

More promising follow-up mechanisms are those that activate protection conditionally rather than permanently allocating a large Grid sleeve.

Candidates include:
- U10 profit-lock / cash overlay already under study;
- dynamic transfer of realized U10 profit into a Grid sleeve after predefined milestones;
- a small permanent Grid sleeve plus U10 cash protection;
- drawdown-triggered rather than fixed Grid allocation.

Any dynamic mechanism requires a new preregistration because it changes capital-flow rules.

## Guardrail

No live/paper allocation change is authorized.

## Residual risks

- one historical common path;
- Tier A / Tier A+B are development-selected;
- Full 15 is survivor-conditioned;
- U10 and Grid have different friction models;
- daily-close correlation does not capture intraday hedging behavior;
- no periodic or conditional rebalancing was tested;
- historical performance does not establish future performance.
