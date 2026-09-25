# VAHRAM_LINK_LEVEL_GRID_V1 — strategy specification draft

Status: **EXPERIMENTAL / SPECIFICATION DRAFT**  
Workflow mode: PATCH_FIX (documentation only)  
Runtime changes: NONE  
Backtest status: NOT BACKTESTED IN THIS REPOSITORY  
Primary current example: LINK/USDT

## 1. Purpose

Document Vahram's manually developed level-based accumulation and sell strategy for crypto assets.

This document reconstructs the strategy from:

- the private trading workbook (`trader's diary`, especially `Link level`);
- the private trade journal;
- current TradingView level annotations supplied by Vahram.

Private workbook links, account data and personal trading records are intentionally **not committed** to this public repository.

This specification is not yet an acceptance claim. It must be treated as a research hypothesis until the remaining rules are confirmed and reproducible tests exist.

## 2. Strategy identity

- `strategy_id`: `VAHRAM_LINK_LEVEL_GRID_V1`
- `strategy_name`: `VAHRAM_LINK_LEVEL_GRID`
- `strategy_version`: `1.0.0-draft`
- `status`: `EXPERIMENTAL`
- initial symbol used for reconstruction: `LINK/USDT`
- direction: spot / long accumulation and staged selling
- leverage: none assumed
- timeframe: not frozen yet

## 3. Core idea

The strategy does not buy one fixed amount at arbitrary dips.

It creates a predefined price ladder between a selected cycle high `H` and cycle low `L`, then allocates more capital as price moves deeper into the range.

The range is divided into:

1. **16 main levels**;
2. **4 sublevels inside each main level**;
3. separate slower and faster trading layers observed in the workbook (`Mid` and `Micro`).

The intended behavior is asymmetric:

- upper levels use relatively little capital;
- deeper levels receive progressively more capital;
- lower-price inventory is expected to be reduced during rebounds according to predefined sell rules.

## 4. Price geometry

Let:

```text
H = selected range high
L = selected range low
R = H - L

MAIN_STEP = R / 16
SUB_STEP  = R / 64
```

For main level `i`, where `i = 1..16`, the lower boundary is:

```text
LEVEL_PRICE(i) = L + ((16 - i) / 16) * R
```

Therefore the normalized main boundaries are:

| Level | Normalized boundary |
|---:|---:|
| 1 | 0.9375 |
| 2 | 0.8750 |
| 3 | 0.8125 |
| 4 | 0.7500 |
| 5 | 0.6875 |
| 6 | 0.6250 |
| 7 | 0.5625 |
| 8 | 0.5000 |
| 9 | 0.4375 |
| 10 | 0.3750 |
| 11 | 0.3125 |
| 12 | 0.2500 |
| 13 | 0.1875 |
| 14 | 0.1250 |
| 15 | 0.0625 |
| 16 | 0.0000 |

The current TradingView layout also shows extension levels above the previous high, including:

```text
1.0625
1.1250
1.1875
```

Their exact operational role still needs confirmation.

## 5. Workbook reconstruction example

The current `Link level` workbook example uses approximately:

```text
H = 31.03
L = 8.17

MAIN_STEP ≈ 1.43
SUB_STEP  ≈ 0.36
```

The four sublevels inside a main band are labelled in the workbook as:

```text
d -> c -> b -> a
```

with `a` being the lowest sublevel of the band.

Example for Level 1:

```text
d ≈ 30.67
c ≈ 30.32
b ≈ 29.96
a ≈ 29.60
```

Level 2 then continues below it:

```text
d ≈ 29.24
c ≈ 28.89
b ≈ 28.53
a ≈ 28.17
```

## 6. Micro layer — confirmed workbook behavior

The `Micro` layer is a denser sublevel trading grid.

Observed rule:

- buy at a sublevel;
- place the intended sell approximately one sublevel above;
- recycle the released USDT into future qualifying lower-grid entries.

The current workbook's Micro capital schedule is:

| Main level | Capital allocation |
|---:|---:|
| 1 | 1% |
| 2 | 2% |
| 3 | 3% |
| 4 | 4% |
| 5 | 4% |
| 6 | 4% |
| 7 | 6% |
| 8 | 6% |
| 9 | 6% |
| 10 | 6% |
| 11 | 8% |
| 12 | 10% |
| 13 | 11% |
| 14 | 11% |
| 15 | 11% |
| 16 | 7% |

This schedule sums to 100% of the Micro capital pool.

Most levels are split equally across four sublevels. The first level is a special case in the current sheet: only the lower two sublevels are funded.

## 7. Mid / main-level layer — reconstructed behavior

The workbook also contains a slower layer corresponding to larger spacing than the Micro grid.

The current sheet allocates approximately 100% of this layer's capital across the 16 main levels.

Observed allocation from the formulas:

| Main level | Capital allocation |
|---:|---:|
| 1 | 0.50% |
| 2 | 2.00% |
| 3 | 3.00% |
| 4 | 4.00% |
| 5 | 4.00% |
| 6 | 5.00% |
| 7 | 5.50% |
| 8 | 5.50% |
| 9 | 6.40% |
| 10 | 7.89% |
| 11 | 8.87% |
| 12 | 9.86% |
| 13 | 10.35% |
| 14 | 11.34% |
| 15 | 11.34% |
| 16 | 4.46% |

The tiny deviation from exactly 100% is rounding in the workbook.

Important: the exact entry/exit mechanics of this slower layer changed across depth in the current workbook and must be confirmed before implementation. The spreadsheet should not be treated as a clean final algorithm merely because formulas exist.

## 8. Evidence from real manual use

The private trade journal contains actual LINK trades tagged with level codes such as:

```text
6A
6B
7A
7B
7C
8A
8B
8C
8D
9A
9B
9C
9D
10A
10B
10C
10D
11B
11C
11D
12A
12B
12C
12D
13A
13C
13D
```

This confirms that the level/sublevel notation was used operationally rather than being only a theoretical chart.

The journal also distinguishes at least:

- `Micro`;
- `Mid`.

Historical manual use is evidence that the strategy existed, **not** evidence that it is profitable or validated.

## 9. Current TradingView annotations

The newer TradingView representation shows the same 16 normalized main levels and annotates deeper levels with increasing capital percentages.

From approximately Level 7 downward, annotations also contain language of the form:

```text
X% or 10 levels from the level
```

This rule is materially important but is not explicit enough in the workbook to encode safely without Vahram's confirmation.

## 10. Capital-management principle

The strategy intentionally keeps substantial reserve capital for deeper prices.

This avoids the common failure mode:

```text
price falls
-> trader buys heavily
-> price falls again
-> trader has no dry powder
```

Instead, the allocation curve increases with depth and is fully planned before execution.

This is one of the defining characteristics of the strategy and must be preserved in any future automated implementation.

## 11. What is NOT yet frozen

The following items are deliberately unresolved:

1. **Range anchors** — exact rule for choosing and later resetting `H` and `L`.
2. **Capital pools** — whether `Mid` and `Micro` each use independent 100% capital pools, or are sub-allocations of one shared portfolio.
3. **Main/Mid entry and exit mechanics** — whether current workbook formulas are canonical or contain historical experiments.
4. **"X% or 10 levels" rule** — exact meaning, precedence and exit trigger.
5. **Upper extension levels** — when 1.0625 / 1.125 / 1.1875 become active sell targets.
6. **Range breakout behavior** — what happens if price moves above `H` or below `L`.
7. **Re-entry after sell** — whether released capital returns to the same level budget, global cash reserve, or is reallocated dynamically.
8. **Fees/slippage/minimum-order rules** — required before backtesting.
9. **Timeframe** — the price ladder is geometric, but the decision timeframe is not yet frozen.

## 12. Safety / research status

This strategy must remain:

```text
EXPERIMENTAL
```

until:

- the unresolved rules are frozen;
- a deterministic implementation exists;
- fees and slippage are included;
- historical backtests are reproducible;
- held-out validation is performed;
- results are compared against benchmark behavior;
- drawdown and capital lock-up are measured.

No claim of profitability is made by this document.

## 13. Next implementation boundary

After Vahram confirms the unresolved rules:

1. freeze `VAHRAM_LINK_LEVEL_GRID_V1` specification;
2. create deterministic level-generation functions;
3. represent Mid/Micro as explicit portfolio layers;
4. add fixtures reproducing the private workbook example without committing private workbook data;
5. implement the strategy through the shared strategy contract;
6. add a portfolio-aware backtest capable of multiple simultaneous open lots;
7. test LINK first;
8. only then generalize to other assets.
