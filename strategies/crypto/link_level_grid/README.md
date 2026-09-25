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
- current TradingView level annotations supplied by Vahram;
- Vahram's direct clarification of the intended rules.

Private workbook links, account data and personal trading records are intentionally **not committed** to this public repository.

This specification is not yet an acceptance claim. It must be treated as a research hypothesis until the remaining rules are frozen and reproducible tests exist.

## 2. Strategy identity

- `strategy_id`: `VAHRAM_LINK_LEVEL_GRID_V1`
- `strategy_name`: `VAHRAM_LINK_LEVEL_GRID`
- `strategy_version`: `1.0.0-draft`
- `status`: `EXPERIMENTAL`
- initial symbol used for reconstruction: `LINK/USDT`
- direction: spot / long accumulation and staged selling
- leverage: none assumed
- canonical candle timeframe for range construction: **1D only**

## 3. Core idea

The strategy does not buy one fixed amount at arbitrary dips.

It constructs a dynamic price ladder from a broad market range and pre-plans how capital may be deployed as price moves down through that range.

The range is divided into:

1. **16 main levels**;
2. **4 sublevels inside each main level**;
3. therefore **64 sublevels** across the full range;
4. two intended independent capital layers:
   - `Micro`;
   - `Mid`.

The intended behavior is asymmetric:

- upper levels use relatively little capital;
- deeper levels are allowed to use more capital;
- capital must remain available for lower levels;
- inventory accumulated low is reduced during rebounds according to predefined exit rules.

## 4. Dynamic range anchors — confirmed rule

The level geometry is based on **daily candles only**.

For a selected historical lookback of up to approximately **3 years**, identify the broad price extremes:

```text
H = highest relevant daily-candle high in the selected lookback
L = lowest relevant daily-candle low in the selected lookback
R = H - L
```

The anchors are **dynamic, not permanent**.

However, they are not intended to be recomputed on every candle. The range is refreshed only when the broad market structure has changed enough to justify rebuilding the ladder.

Important implementation consequence:

> range recalculation is a separate event from ordinary daily signal processing.

The exact mechanical trigger for when an existing range should be replaced by a newer range is still not frozen.

## 5. Price geometry

Given:

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

| Main level | Normalized boundary |
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

The 64-sublevel grid should be numbered from top to bottom for deterministic implementation:

```text
sublevel 1  = highest sublevel near H
...
sublevel 64 = lowest sublevel near L
```

A move upward in price therefore means the sublevel index decreases.

Example:

```text
64 -> 54 = rise of 10 sublevels
```

## 6. Upper extension levels

The current TradingView layout also shows extension levels above the previous high:

```text
1.0625
1.1250
1.1875
```

These are preserved as observed strategy context.

Their exact operational role is not yet frozen and must not be implemented by assumption.

## 7. Workbook reconstruction example

The current private `Link level` workbook example uses approximately:

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

This workbook is useful historical evidence, but its capital allocation must not be treated as the final canonical allocation because the live manual experiment suffered from capital exhaustion during a sharp decline.

## 8. Independent capital layers — confirmed design intent

The intended architecture is:

```text
MICRO CAPITAL POOL = independent 100% pool
MID CAPITAL POOL   = independent 100% pool
```

They should not accidentally consume one another's reserved capital.

The historical workbook does **not** cleanly represent this ideal design because the capital split was created during live experimentation and was not sized correctly before the market declined.

Therefore:

- the historical Micro percentages are provenance;
- the historical Mid percentages are provenance;
- neither schedule is automatically canonical for a future implementation;
- a new capital-allocation specification must be designed and frozen before backtesting the intended strategy.

## 9. Micro layer — observed historical behavior

The `Micro` layer is the denser trading layer operating on the 64-sublevel grid.

Observed workbook behavior:

- buy at a sublevel;
- sell after a relatively small rebound to a higher grid point;
- recycle released USDT into future qualifying entries.

Historical Micro allocation observed in the workbook:

| Main level | Historical allocation |
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

This sums to 100%, but it is retained only as **historical evidence**, not as the final desired allocation curve.

## 10. Mid layer — observed historical behavior

The `Mid` layer is the slower, larger-move layer.

Historical Mid allocation reconstructed from the workbook:

| Main level | Historical allocation |
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

The tiny deviation from exactly 100% is rounding.

This schedule is also **historical, not canonical**.

During the original manual deployment, price began in the upper part of the ladder and then fell sharply. Capital had not been reserved correctly for the deeper levels, so later entries became constrained by lack of remaining cash.

That historical path explains irregular workbook behavior and must not be reverse-engineered as an intentional rule.

## 11. Exit rule — percentage target OR 10-sublevel recovery

A key confirmed rule for deeper entries is a dual exit condition.

For an entry made at a sufficiently deep level, a position may be sold when **either**:

1. price appreciation reaches the percentage target assigned to that level; **or**
2. price rises by **10 sublevels** on the 64-sublevel grid.

The practical intent is to avoid waiting for a very large percentage rebound from very low prices when a meaningful grid recovery has already occurred.

Example:

```text
entry at sublevel 64
percentage target = 61% (illustrative level target)

Do not require the full +61% if price has already recovered
10 sublevels.

Grid alternative:
64 -> 54

At sublevel 54, the 10-sublevel recovery condition is satisfied.
```

Operational interpretation for future implementation:

```text
exit when FIRST of:
- percentage target reached
- +10 sublevels upward reached
```

For a sublevel index `k`:

```text
grid_exit_index = k - 10
```

when this alternative is applicable.

The exact boundary for when the 10-sublevel alternative becomes active is currently represented by the TradingView annotations from approximately main Level 7 downward.

## 12. Current TradingView percentage targets

The current chart annotates the main levels with approximately the following percentage targets:

| Main level | Current annotated target |
|---:|---:|
| 1 | 26% |
| 2 | 28% |
| 3 | 31% |
| 4 | 35% |
| 5 | 43% |
| 6 | 53% |
| 7 | 61% or 10 sublevels |
| 8 | 70% or 10 sublevels |
| 9 | 80% or 10 sublevels |
| 10 | 92% or 10 sublevels |
| 11 | 102% or 10 sublevels |
| 12 | 117% or 10 sublevels |
| 13 | 130% or 10 sublevels |
| 14 | 150% or 10 sublevels |
| 15 | 175% or 10 sublevels |
| 16 | 212% or 10 sublevels |

These percentages are recorded from the current TradingView representation.

Before executable implementation, the project should still freeze whether these values are:

- fixed constants;
- derived from a formula;
- or regenerated whenever `H/L` changes.

## 13. Evidence from real manual use

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

The journal distinguishes at least:

- `Micro`;
- `Mid`.

Historical manual use is evidence that the strategy existed, **not** evidence that it is profitable or validated.

## 14. Capital-management principle

The defining risk-control idea is to preserve dry powder for deeper prices.

The historical failure mode was:

```text
upper levels consumed too much capital
-> price fell much deeper than expected
-> intended lower-level buys became underfunded
```

The future strategy specification must explicitly prevent this.

A valid capital plan must guarantee, before any entry is placed, that each lower level retains its reserved budget unless a separately defined rebalance rule permits otherwise.

This is more important than reproducing the accidental historical allocation percentages.

## 15. What is now confirmed

Confirmed by Vahram:

1. **1D candles only** for defining the broad range.
2. **Dynamic H/L anchors** derived from broad extremes over a lookback of up to roughly 3 years.
3. H/L are **changed infrequently**, not recalculated every day.
4. Range is divided into **16 main levels x 4 = 64 sublevels**.
5. `Micro` and `Mid` should ideally have **independent capital pools**.
6. Historical capital allocation became distorted because too much capital was consumed before the deeper decline.
7. Deep-position exit logic can use **percentage target OR 10-sublevel recovery**.
8. Example of grid recovery: **64 -> 54**.

## 16. Remaining unresolved rules

The remaining decisions should be frozen before implementation:

1. **Range refresh trigger** — exactly when a new 3-year/broad H/L range replaces the existing one.
2. **Exact extreme definition** — whether `H/L` use candle High/Low, Close extremes, or another confirmed daily-candle field.
3. **Canonical Micro allocation curve** — redesign from clean capital assumptions.
4. **Canonical Mid allocation curve** — redesign from clean capital assumptions.
5. **Canonical Mid entry rule** — exact relationship between main levels and the four sublevels.
6. **Percentage target generation** — fixed table vs formula vs H/L-dependent calculation.
7. **10-sublevel applicability** — exact first main/sublevel where the alternative becomes active.
8. **Upper extension levels** — role of 1.0625 / 1.125 / 1.1875.
9. **Range breakout behavior** — what happens above `H` or below `L`.
10. **Re-entry after sell** — where released capital returns.
11. **Fees/slippage/minimum-order rules** — required before backtesting.

## 17. Historical mistake is not strategy logic

A critical research rule for this strategy:

> Do not encode Vahram's historical capital shortage as intended algorithm behavior.

The original live experiment began while price was in the upper levels. The subsequent decline was deeper than expected, and the available capital was depleted before all intended lower entries could be funded.

That is an execution/capital-planning failure in the experiment, not a desired feature of the strategy.

Future implementation must model the intended strategy from a clean starting capital state.

## 18. Safety / research status

This strategy remains:

```text
EXPERIMENTAL
```

until:

- unresolved rules are frozen;
- independent Micro/Mid capital pools are specified;
- deterministic implementation exists;
- fees and slippage are included;
- historical backtests are reproducible;
- held-out validation is performed;
- benchmark comparison is available;
- drawdown and capital lock-up are measured.

No profitability claim is made by this document.

## 19. Next implementation boundary

Next work should be specification-first:

1. define the exact H/L refresh algorithm;
2. redesign clean Micro capital allocation;
3. redesign clean Mid capital allocation;
4. freeze percentage-target derivation;
5. freeze the exact 10-sublevel activation boundary;
6. freeze re-entry/recycling rules;
7. only then implement deterministic level generation and portfolio logic;
8. add fixtures reproducing the LINK geometry without committing private account data;
9. backtest LINK first;
10. generalize only after evidence exists.
