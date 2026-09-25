# Level-grid optimizer v1 — first parameter search

Date: 2026-09-25  
Strategy: `VAHRAM_LINK_LEVEL_GRID_V1`  
Status: **RESEARCH ONLY / NO CANONICAL PARAMETER CHANGE**

## Question

Can the current strategy improve by changing:

1. capital distribution by depth;
2. Micro profit-taking distance;
3. Mid recovery distance;
4. Mid percentage-target aggressiveness?

## Anti-overfit structure

Assets:

- LINKUSDT
- ETHUSDT
- SOLUSDT
- BNBUSDT
- BTCUSDT

For every asset:

```text
6 years total daily data
first 3 years = H/L prehistory only
final 3 years = evaluation
```

The final evaluation period was split again:

```text
first ~2 years = parameter-selection/training segment
last 365 days  = temporal holdout
```

Candidates were ranked using the training segment only. The final 365-day result was reported afterward and was not used by the script to rank candidates.

Fees and slippage remained at the existing research assumptions.

## Capital-allocation search

Capital curve:

```text
weight(level) proportional to level^p
```

Separate values of `p` were tested for Micro and Mid:

```text
0, 0.5, 1, 1.5, 2, 2.5, 3
```

Interpretation:

- `p=0` = equal capital on all main levels;
- `p=1` = current linear-depth research baseline;
- larger `p` = progressively more capital reserved for deeper levels.

49 Micro/Mid combinations were evaluated across all five assets.

### Training winner

```text
Micro p = 0
Mid p   = 0
```

Cross-asset geometric-mean return:

- training segment: **+283.91%**
- full 3-year period: **+260.06%**
- temporal holdout: **-5.76%**
- median full-period max drawdown: **44.52%**

This configuration maximized historical training profit but failed the temporal holdout.

### Current linear baseline

```text
Micro p = 1
Mid p   = 1
```

Cross-asset geometric-mean return:

- training segment: **+208.09%**
- full 3-year period: **+226.41%**
- temporal holdout: **+5.50%**
- median full-period max drawdown: **35.98%**

### More bottom-heavy comparison

```text
Micro p = 2
Mid p   = 2
```

Cross-asset geometric-mean return:

- training segment: **+153.23%**
- full 3-year period: **+187.43%**
- temporal holdout: **+12.61%**
- median full-period max drawdown: **28.92%**

### Capital finding

There is a clear profit/robustness tradeoff in this first search.

Equal allocation looked strongest in the historical selection segment but deteriorated in the unseen final year and had materially larger drawdown.

The existing linear-depth allocation gave up some historical return while improving holdout behavior.

A still more bottom-heavy curve reduced historical return further but had the strongest temporal-holdout result and the smallest median drawdown among these three reference points.

This means the current capital rule is **not proven optimal**, but the evidence does not support replacing it with the simple historical-profit winner.

## Exit-rule search

The initial exit sweep tested:

- Micro recovery: 1, 2, 3, 4 sublevels;
- Mid recovery: 6, 8, 10, 12, 14 sublevels;
- Mid percentage-target scale: 0.75, 1.0, 1.25.

The training winner landed on the upper search boundary, so a second refinement sweep was run:

- Micro recovery: 3, 4, 5, 6 sublevels;
- Mid recovery: 10, 14, 18, 22 sublevels;
- Mid percentage-target scale: 1.0, 1.25, 1.5.

### Current exit baseline

```text
Micro recovery = 1 sublevel
Mid recovery   = 10 sublevels
Mid target     = 1.00 x current percentages
capital powers = 1 / 1
```

Cross-asset geometric-mean return:

- training: **+208.09%**
- full period: **+226.41%**
- holdout: **+5.50%**
- median max drawdown: **35.98%**

### Refined training winner

```text
Micro recovery = 6 sublevels
Mid recovery   = 18 sublevels
Mid target     = 1.00 x current percentages
capital powers = 1 / 1
```

Cross-asset geometric-mean return:

- training: **+233.79%**
- full period: **+253.03%**
- holdout: **+5.32%**
- median max drawdown: **38.67%**

Full-period asset returns for this candidate:

| Asset | Current baseline | 6 / 18 exit candidate |
|---|---:|---:|
| LINK | +482.22% | +516.80% |
| ETH | +119.81% | +137.02% |
| SOL | +736.06% | +818.79% |
| BNB | +104.17% | +123.98% |
| BTC | +69.60% | +82.26% |

The candidate improved the full-period return on all five assets and used materially fewer closed trades.

However, its holdout geometric mean was essentially unchanged/slightly lower than the current baseline. Therefore it is **promising, not accepted as an improvement**.

### Refined robust-score winner

```text
Micro recovery = 6 sublevels
Mid recovery   = 22 sublevels
Mid target     = 1.50 x current percentages
capital powers = 1 / 1
```

Cross-asset geometric-mean return:

- training: **+233.40%**
- full period: **+253.07%**
- holdout: **+5.73%**
- median max drawdown: **39.71%**

This also improves historical return materially, but the drawdown is higher and the holdout advantage over the baseline is small.

## Current conclusion

The first optimizer pass found evidence that the exit rules have more obvious room for improvement than the capital-allocation rule.

Specifically, slower profit-taking / wider recovery distances improved three-year historical returns across every tested asset.

The capital-allocation search showed a stronger tradeoff: more capital near upper levels increases historical profit but also raises drawdown and produced worse temporal-holdout behavior; deeper reservation sacrifices some historical return but improved recent holdout behavior.

No optimizer candidate is promoted to the canonical strategy.

## Next research step

Do not optimize further against the same final 365-day holdout.

That holdout has now been observed and is no longer clean.

The next stage should use one or both of:

1. older rolling walk-forward windows that were not used by this selector;
2. additional large-cap assets not used in the five-asset optimizer.

Then compare a small preregistered set:

- current baseline;
- wider-exit candidate;
- more bottom-heavy capital candidate;
- one combined candidate.

Only after that should any parameter replace the current research baseline.

## Runtime evidence

Full optimizer:

- GitHub Actions run: `36143551633`
- artifact: `10868347311`
- capital candidates: 49
- exit candidates: 60
- repository tests: passed

Refined exit optimizer:

- GitHub Actions run: `36144431011`
- artifact: `10868473322`
- exit candidates: 48
- repository tests: passed
