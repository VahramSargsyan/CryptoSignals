# U10 + Grid Blend Portfolio v1 — preregistration

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Scope: research-only portfolio construction; no live/paper strategy changes

## Objective

Test whether a fixed capital split between frozen U10 and frozen Grid can reduce U10 drawdown while preserving a meaningful portion of U10 terminal growth.

This is not a new signal strategy.

The two strategy sleeves remain completely independent.

## Common evaluation window

2023-10-31 -> 2026-09-26

Starting portfolio capital:

10,000 USDT

## Sleeve mechanics

### U10 sleeve

Frozen U10:
ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

Frozen mechanics:
- Binance Spot 1D closed candles
- rolling median 180 days
- ARM 15%
- reversal confirmation 3%
- strongest-confirmed max-dislocation router
- next-open execution
- 0.1% modeled transition cost

### Grid sleeve

Use the exact Grid portfolio mechanics already validated in Grid vs U10 Universe Portfolios v1:

- independent token Grid sleeves
- equal initial allocation across tokens inside the Grid sleeve
- 50/50 Micro/Mid inside each token sleeve
- 16 main levels x 4 sublevels
- 1095 prior daily candles
- 1095-candle H/L lookback
- 30-candle range refresh
- linear_depth_reserved
- 100% positive-profit reinvestment
- no permanent runner
- 10 bps fee
- 5 bps adverse slippage
- no forced end liquidation

Grid profile frozen for this first blend study:

BASE = Micro +1 / Mid +10

Reason:
BASE was the stronger or lower-drawdown profile on Tier A 5 and Tier A+B 10 in the immediately preceding fixed-universe comparison. No further tuning is allowed inside this study.

## Grid universes tested

1. GRID_TIER_A_5
   LINK, SOL, ETH, ADA, XLM

2. GRID_TIER_A_B_10
   LINK, SOL, ETH, ADA, XLM, HBAR, UNI, DOGE, AVAX, LTC

3. GRID_FULL_15
   BTC, ETH, BNB, SOL, XRP, TRX, DOGE, ADA, LINK, XLM, LTC, HBAR, AVAX, BCH, UNI

Selection caveats remain inherited:
- Tier A / Tier A+B are development-selected;
- Full 15 is survivor-conditioned.

## Capital splits

For each Grid universe, test:

- 80% U10 / 20% Grid
- 70% U10 / 30% Grid
- 60% U10 / 40% Grid
- 50% U10 / 50% Grid

Controls:
- 100% U10
- 100% corresponding Grid portfolio

## No rebalancing

Capital is split once at the start.

Example:

70/30 on 10,000 USDT:
- 7,000 USDT enters U10
- 3,000 USDT enters Grid

After that:
- U10 compounds only inside U10;
- Grid compounds only inside Grid;
- there are no transfers between sleeves;
- there is no monthly/annual rebalancing;
- combined daily equity = U10 sleeve equity + Grid sleeve equity.

This avoids creating artificial buy-low/sell-high effects from rebalancing.

## Metrics

For every blend:
- final equity;
- total return;
- minimum equity and minimum vs initial;
- max drawdown from prior portfolio peak;
- max-DD peak/trough dates and values;
- terminal equity retained versus 100% U10;
- drawdown reduction versus 100% U10;
- daily-return correlation between U10 and Grid sleeves;
- drawdown overlap:
  fraction of days when both sleeves are below their own prior peaks;
- equity at the known U10 long-path trough dates.

## Interpretation

Do not identify an "optimal" allocation from a single historical path.

This study is descriptive:
- how much U10 growth is retained;
- how much U10 drawdown is reduced;
- whether Grid diversifies U10 in time, not only by token count.

No live/paper allocation change is authorized.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
