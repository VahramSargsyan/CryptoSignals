# Reinvestment and token-runner exploration v1

Date: 2026-09-25  
Strategy: `VAHRAM_LINK_LEVEL_GRID_V1`  
Status: **EXPLORATORY RESEARCH / NOT VALIDATION / NO CANONICAL CHANGE**

## Question

Two owner ideas were tested:

1. how much realized profit should be reinvested into the same grid slot;
2. whether part of a position should remain unsold as a long-term token runner.

## Important baseline clarification

The existing backtest already reinvested **100% of positive realized profit** into the same slot.

In other words, the previously reported LINK / ETH / SOL / BNB / BTC results already included per-slot compounding.

This experiment makes that assumption explicit and configurable.

## Research definitions

### Profit reinvestment

Tested values:

```text
0% / 50% / 100%
```

Only positive realized profit is split.

- reinvested profit returns to that slot's next grid cycle;
- non-reinvested profit stays as reserve cash and remains part of portfolio equity;
- realized losses are fully carried by the slot.

### Token runner

Tested values:

```text
0% / 10% / 25% / 50%
```

At a normal strategy exit, the runner fraction is not sold. It is accumulated as token inventory and marked to market through the end of the test.

The active grid slot is allowed to continue cycling with the sold portion.

This is only one interpretation of "leave part in tokens." It does not yet use a second target, trailing exit, or H/L-reset exit for the runner.

## Dataset and shared assumptions

Assets:

- LINKUSDT
- ETHUSDT
- SOLUSDT
- BNBUSDT
- BTCUSDT

For each asset:

```text
6 years daily history
first 3 years = H/L prehistory only
final 3 years = trading/evaluation
fees = 10 bps
slippage = 5 bps
capital curve = p=1 linear-depth
```

Two exit profiles were explored:

- `BASE`: Micro +1 sublevel, Mid +10-sublevel recovery;
- `WIDE`: Micro +6 sublevels, Mid +18-sublevel recovery.

Total candidates: 24.

## Finding 1 — reinvestment is a major return driver

With no token runner, increasing profit reinvestment increased return on **all five assets**.

### BASE exits, runner = 0%

| Asset | 0% reinvest | 50% reinvest | 100% reinvest |
|---|---:|---:|---:|
| LINK | +197.86% | +305.93% | **+482.22%** |
| ETH | +82.49% | +99.05% | **+119.81%** |
| SOL | +325.11% | +481.26% | **+736.06%** |
| BNB | +68.21% | +84.75% | **+104.17%** |
| BTC | +49.81% | +59.01% | **+69.60%** |

The gain is not free: drawdown also rises materially.

### BASE exits, runner = 0% — max drawdown

| Asset | 0% reinvest | 50% reinvest | 100% reinvest |
|---|---:|---:|---:|
| LINK | 24.33% | 29.82% | **35.98%** |
| ETH | 14.50% | 24.97% | **39.00%** |
| SOL | 12.23% | 25.93% | **51.57%** |
| BNB | 7.41% | 15.10% | **26.31%** |
| BTC | 5.86% | 9.86% | **16.26%** |

Therefore reinvestment behaves like a risk/compounding dial, not a free optimization.

## Finding 2 — a permanent runner is strongly asset-dependent

With 100% profit reinvestment and the current BASE exits:

| Asset | 0% runner | 10% runner | 25% runner | 50% runner |
|---|---:|---:|---:|---:|
| LINK | **+482.22%** | +296.12% | +193.44% | +120.29% |
| ETH | **+119.81%** | +89.66% | +75.18% | +66.03% |
| SOL | **+736.06%** | +525.47% | +432.57% | +398.75% |
| BNB | +104.17% | +118.96% | +125.86% | **+131.76%** |
| BTC | +69.60% | +80.86% | +90.61% | **+100.65%** |

The same pattern remains under the WIDE exit profile:

| Asset | 0% runner | 10% runner | 25% runner | 50% runner |
|---|---:|---:|---:|---:|
| LINK | **+516.80%** | +397.64% | +271.22% | +153.01% |
| ETH | **+137.02%** | +115.41% | +93.71% | +73.71% |
| SOL | **+818.79%** | +640.85% | +504.54% | +426.48% |
| BNB | +123.98% | +129.71% | +131.80% | **+133.12%** |
| BTC | +82.26% | +86.16% | +91.86% | +100.17% |

This is a strong qualitative split:

- LINK / ETH / SOL benefited from recycling the whole position through the grid;
- BTC / BNB benefited from retaining a substantial long-term token component during this historical period.

## Finding 3 — runners materially increase drawdown

A larger passive token inventory increases exposure to the asset's market drawdowns.

Example under BASE exits with full reinvestment:

| Asset | DD at 0% runner | DD at 50% runner |
|---|---:|---:|
| LINK | 35.98% | 66.39% |
| ETH | 39.00% | 57.21% |
| SOL | 51.57% | 72.36% |
| BNB | 26.31% | 48.66% |
| BTC | 16.26% | 40.36% |

So the higher BTC/BNB return from runners came with a large risk increase.

## Cross-asset result

The best single configuration shared by all five assets remained:

```text
WIDE exits
100% profit reinvestment
0% permanent runner
```

Cross-asset geometric-mean return: **+253.03%**  
Median max drawdown: **38.67%**

The permanent-runner rule did **not** improve the shared five-asset configuration.

## Asset-specific exploratory winners

Within this small search:

- LINK: WIDE / 100% reinvest / 0% runner → +516.80%
- ETH: WIDE / 100% reinvest / 0% runner → +137.02%
- SOL: WIDE / 100% reinvest / 0% runner → +818.79%
- BNB: WIDE / 100% reinvest / 50% runner → +133.12%
- BTC: BASE / 100% reinvest / 50% runner → +100.65%

These are **not accepted per-asset settings**. Selecting them after looking at the same history is optimization bias.

## Interpretation

Two conclusions are already useful:

1. full profit reinvestment is a major contributor to the strategy's historical compounding, but it also amplifies drawdown;
2. retaining token runners is not universally good or bad — it behaved very differently across assets.

The BTC/BNB behavior supports researching a hybrid grid + long-term-hold architecture.

The LINK/ETH/SOL behavior shows why a universal fixed runner percentage would be dangerous.

## Better runner designs to test next

A permanent runner is intentionally simple. More realistic variants should include:

- keep 10–25% only after deeper entries;
- runner exits at a second higher target;
- runner exits on H/L range reset;
- runner uses a trailing rule after a strong trend;
- runner percentage depends on trend/regime rather than token name;
- cap total runner inventory as a percentage of portfolio equity.

These rules should be tested with walk-forward or additional unseen assets before any promotion.

## Runtime evidence

GitHub Actions run: `36148233904`  
Artifact: `10869948166`  
Candidates: 24  
Repository test suite: passed
