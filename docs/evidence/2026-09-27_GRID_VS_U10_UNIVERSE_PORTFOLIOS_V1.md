# Grid vs U10 Universe Portfolios v1 — Evidence

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Validated branch: research/grid-vs-u10-universes-v1
GitHub Actions run: 36338060414
Source commit: 5987467e876ea7cd076119ae2832f7642c1d2f3e
Artifact ID: 10937927834
Artifact digest: sha256:be27f67e19b78fe9f18f58895570a83709093fdbf7a701441bda2083724fcf2b
Result: PASS
TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST

## Objective

Compare frozen research U10 with actual multi-token Grid portfolios on the same starting capital and primary evaluation window, then compare Grid across several fixed token universes.

No live or paper-live configuration was changed.

## Critical architecture difference

U10:
- one portfolio;
- capital is concentrated in the currently selected asset;
- 23 rotations on the primary mature path.

Grid portfolio in this study:
- equal initial capital split across tokens;
- every token runs its own independent Grid;
- each token sleeve has 50/50 Micro/Mid capital;
- portfolio equity is the daily sum of all token-sleeve equities;
- no cross-token rebalancing.

Therefore this study compares two portfolio architectures, not two identical execution mechanisms.

## Primary evaluation

Window:

2023-10-31 -> 2026-09-26

Elapsed:

1061 days

Initial capital for every portfolio:

10,000 USDT

### U10 frozen mechanics

Universe:

ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

- Binance Spot 1D
- rolling median 180d
- ARM 15%
- reversal confirmation 3%
- strongest-confirmed max-dislocation router
- next-open execution
- 0.1% modeled transition cost

### Grid frozen mechanics

- Binance Spot 1D
- 16 main levels x 4 sublevels
- 1095 prior daily candles required
- 1095-candle H/L lookback
- H/L refresh every 30 candles
- linear_depth_reserved
- 100% positive-profit reinvestment
- no permanent runner
- fee 10 bps
- adverse slippage 5 bps
- no end liquidation
- BASE = Micro +1 / Mid +10
- WIDE = Micro +6 / Mid +18

## Primary portfolio results

| Strategy / universe | Profile | Final equity | Return | Minimum equity | Min vs initial | Max DD | Activity |
|---|---|---:|---:|---:|---:|---:|---:|
| U10 | rotation | 219,485.20 | +2,094.85% | 9,570.48 | -4.30% | -71.04% | 23 transitions |
| GRID_TIER_A_5 | BASE | 33,530.59 | +235.31% | 10,000.00 | 0.00% | -43.48% | 3,642 closed lots |
| GRID_TIER_A_5 | WIDE | 33,500.33 | +235.00% | 10,000.00 | 0.00% | -44.52% | 1,129 closed lots |
| GRID_TIER_A_B_10 | BASE | 35,613.98 | +256.14% | 9,998.52 | -0.01% | -44.97% | 5,727 closed lots |
| GRID_TIER_A_B_10 | WIDE | 35,149.27 | +251.49% | 9,998.52 | -0.01% | -46.75% | 1,824 closed lots |
| GRID_FULL_15 | BASE | 29,674.06 | +196.74% | 10,000.00 | 0.00% | -41.26% | 9,049 closed lots |
| GRID_FULL_15 | WIDE | 29,688.21 | +196.88% | 10,000.00 | 0.00% | -42.83% | 3,035 closed lots |
| GRID_U10_MATURE_8 | BASE | 27,953.25 | +179.53% | 10,000.00 | 0.00% | -40.15% | 4,218 closed lots |
| GRID_U10_MATURE_8 | WIDE | 29,453.57 | +194.54% | 10,000.00 | 0.00% | -41.38% | 1,463 closed lots |

## U10 versus Grid

Highest terminal Grid portfolio in this fixed comparison:

GRID_TIER_A_B_10 / BASE

Final:

35,613.98 USDT

U10 final:

219,485.20 USDT

U10 terminal equity was about 6.16x the highest tested Grid portfolio terminal equity.

This large terminal difference came with much larger peak-to-trough risk:

- U10 max DD: -71.04%
- GRID_TIER_A_B_10 BASE max DD: -44.97%
- GRID_FULL_15 BASE max DD: -41.26%

Long-path original-capital floor was very different from peak drawdown for both strategies.

U10:
- minimum equity vs original 10,000: 9,570.48 / -4.30%.

Most primary Grid portfolios:
- minimum equity remained approximately at or above the original 10,000;
- Tier A+B briefly reached 9,998.52 / about -0.01%.

Thus the Grid portfolios historically protected original capital more smoothly on this path while U10 generated much larger compounded terminal growth.

## Grid universe sensitivity

### Tier A 5

LINK, SOL, ETH, ADA, XLM

BASE:
- final 33,530.59
- +235.31%
- max DD -43.48%
- 5/5 profitable
- median token return +252.89%

WIDE:
- final 33,500.33
- +235.00%
- max DD -44.52%
- 5/5 profitable
- median token return +251.74%

Result:
BASE and WIDE were effectively tied at portfolio level, with BASE slightly ahead and slightly lower drawdown.

### Tier A+B 10

LINK, SOL, ETH, ADA, XLM, HBAR, UNI, DOGE, AVAX, LTC

BASE:
- final 35,613.98
- +256.14%
- max DD -44.97%
- 10/10 profitable
- median token return +253.81%

WIDE:
- final 35,149.27
- +251.49%
- max DD -46.75%
- 10/10 profitable
- median token return +237.75%

Result:
adding Tier B increased terminal return versus Tier A 5 on this window, but also increased portfolio drawdown.

### Full 15

BTC, ETH, BNB, SOL, XRP, TRX, DOGE, ADA, LINK, XLM, LTC, HBAR, AVAX, BCH, UNI

BASE:
- final 29,674.06
- +196.74%
- max DD -41.26%
- 15/15 profitable

WIDE:
- final 29,688.21
- +196.88%
- max DD -42.83%
- 15/15 profitable

Result:
broader diversification lowered terminal return compared with the selected 5/10-token sets but also slightly lowered portfolio drawdown.

The broader universe diluted high-return Grid sleeves with lower-return control assets such as BTC, TRX, BNB and BCH.

### U10 Mature-8

ATOM, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

BASE:
- final 27,953.25
- +179.53%
- max DD -40.15%
- 6/8 profitable

WIDE:
- final 29,453.57
- +194.54%
- max DD -41.38%
- 6/8 profitable

This set is important because it shows that assets useful inside a rotation strategy are not necessarily good permanent Grid sleeves.

WIDE token returns:

| Asset | Return | Token max DD |
|---|---:|---:|
| SOL | +444.74% | -51.60% |
| HBAR | +439.21% | -64.48% |
| AAVE | +411.52% | -55.64% |
| LINK | +251.74% | -41.81% |
| BNB | +110.38% | -27.59% |
| TRX | +41.36% | -8.15% |
| FIL | -69.21% | -93.95% |
| ATOM | -73.45% | -89.17% |

ATOM and FIL were severe Grid failures on this fixed long window, yet both can still be useful nodes inside U10 because U10 does not hold them permanently and uses relative-rotation exits.

This is evidence that universe suitability is strategy-specific.

## BASE versus WIDE

WIDE was not uniformly superior at portfolio level.

- Tier A 5: BASE slightly higher return and lower DD.
- Tier A+B 10: BASE higher return and lower DD.
- Full 15: WIDE higher return by only about 14 USDT, while BASE had lower DD.
- U10 Mature-8: WIDE materially higher terminal return, with slightly higher DD.

WIDE used far fewer closed Grid lots:

- Tier A+B 10 BASE: 5,727
- Tier A+B 10 WIDE: 1,824
- Full 15 BASE: 9,049
- Full 15 WIDE: 3,035

This confirms that exit spacing changes operational intensity substantially even when terminal portfolio results are close.

## Exact U10-token Grid diagnostic

Exact U10 universe:

ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

Canonical Grid requires 1095 daily candles before first trade.

PEPE is the limiting asset.

First date when all ten U10 assets satisfy Grid prehistory:

2026-05-04

Diagnostic window:

2026-05-04 -> 2026-09-26

Elapsed:

145 days

| Strategy | Profile | Final equity | Return | Minimum equity | Min vs initial | Max DD |
|---|---|---:|---:|---:|---:|---:|
| U10 | rotation | 18,326.57 | +83.27% | 7,560.75 | -24.39% | -36.68% |
| Grid exact U10 | BASE | 12,158.84 | +21.59% | 8,617.57 | -13.82% | -20.74% |
| Grid exact U10 | WIDE | 12,307.00 | +23.07% | 8,598.09 | -14.02% | -20.96% |

The exact-token diagnostic points in the same qualitative direction:
- U10 produced much higher return;
- Grid had substantially shallower drawdown.

But 145 days is too short to treat this as equivalent to the primary 1061-day test.

## Operational intensity

Primary U10:
- 23 asset transitions.

Grid:
- from roughly 1,129 WIDE closed lots on Tier A 5
- to 9,049 BASE closed lots on Full 15.

This is a major structural difference.

Grid's historical return requires far more execution events and is therefore more sensitive to:
- transaction friction;
- implementation reliability;
- exchange constraints;
- order-management complexity.

## Friction-model boundary

The comparison preserves existing frozen research assumptions.

U10:
- 0.1% modeled cost per rotation.

Grid:
- 10 bps fee;
- 5 bps adverse slippage on fills.

These are not identical.

Therefore the terminal return gap cannot be described as pure strategy alpha.

However, the size of the observed gap is much larger than the small difference in nominal modeled friction alone.

## Selection and survivorship caveats

Tier A and Tier A+B:
- selected using previous development evidence;
- not untouched out-of-sample universes.

Full 15:
- intentionally old, large, surviving assets;
- materially exposed to survivorship bias.

U10:
- FIL/HBAR additions also have residual selection leakage documented in U10 research.

No universe here is a clean historically reconstructed investable universe.

## Main findings

1. U10 generated dramatically higher terminal growth than every tested equal-weight Grid portfolio on the same 1061-day window.
2. Grid portfolios had materially shallower peak-to-trough drawdowns.
3. Grid universe composition matters substantially.
4. The selected 10-token Tier A+B Grid portfolio had the highest terminal Grid result in this test.
5. Expanding from 10 selected Grid tokens to all 15 survivor-quality tokens reduced terminal return but modestly improved drawdown.
6. U10 tokens are not automatically Grid-suitable: ATOM and FIL were strongly negative as permanent Grid sleeves.
7. BASE vs WIDE leadership depends on universe; WIDE is not a universal portfolio winner.
8. Grid requires orders of magnitude more trade events than U10.

## Research implication

The evidence supports treating Grid and U10 as potentially complementary architectures rather than interchangeable copies.

A future combined-allocation study may be more useful than forcing one strategy to replace the other.

Candidate next study:

U10 + GRID BLEND PORTFOLIO

Examples to test without optimization:
- 80% U10 / 20% Grid
- 70% U10 / 30% Grid
- 60% U10 / 40% Grid
- 50% U10 / 50% Grid

Grid sleeve candidates should include at least:
- Tier A+B 10 BASE
- Full 15 BASE
- Tier A 5 BASE

Required metrics:
- terminal equity;
- max drawdown;
- minimum vs initial;
- correlation of daily returns;
- drawdown overlap;
- contribution during U10 stress periods.

## Guardrail

No live/paper promotion or allocation change is authorized by this study.

## Residual risks

- historical performance does not establish future performance;
- all results depend on current frozen research mechanics;
- Grid selected universes have development-selection leakage;
- Full 15 has survivorship bias;
- U10 has its own universe-selection leakage;
- friction models differ;
- Grid exact-U10 comparison is only 145 days;
- no clean historical universe reconstruction has yet been performed.
