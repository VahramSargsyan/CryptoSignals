# Grid vs U10 Universe Portfolios v1 — preregistration

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Scope: research-only comparison; no live/paper strategy changes

## Questions

1. How does the current U10 relative-rotation strategy compare with the current Grid research family when both start from the same 10,000 USDT portfolio capital?
2. How sensitive is Grid performance to the token universe itself?
3. Does Grid look stronger on a quality-selected subset than on a broader survivor-conditioned universe?
4. What happens if Grid is restricted to the mature members of U10?
5. As a secondary diagnostic, how does Grid behave on the exact U10 ten-token universe once every token has the required 1095-day Grid prehistory?

## Important architecture difference

U10:
- one portfolio;
- capital is concentrated in one currently selected asset;
- rotation between assets is determined by the frozen relative-rotation graph.

Grid:
- independent Grid strategy per token;
- a multi-token Grid portfolio is defined here by equal initial capital allocation across tokens;
- each token keeps its own independent Micro/Mid pools;
- portfolio equity is the sum of all token-level Grid equities.

This portfolio definition is explicit and research-only.

## U10 frozen mechanics

U10 assets:
ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

- Binance Spot 1D closed candles
- rolling median: 180 days
- ARM: 15%
- reversal confirmation: 3%
- strongest-confirmed max-dislocation router
- next-open execution
- modeled transition cost: 0.1%
- fresh capital starts in ATOM

## Grid frozen mechanics

Strategy family:
VAHRAM_LINK_LEVEL_GRID_V1

Common:
- Binance Spot 1D
- 16 main levels x 4 sublevels
- 1095 prior daily candles required before first trade
- H/L lookback: 1095 candles
- range refresh: 30 candles
- linear_depth_reserved capital allocation
- 100% positive-profit reinvestment
- no permanent runner
- fee: 10 bps
- slippage: 5 bps
- equal initial capital split between Micro and Mid inside each token sleeve
- equal initial portfolio allocation across tokens
- open positions marked to market at end; no forced end liquidation

Profiles:
- BASE: Micro +1 sublevel / Mid +10 sublevels
- WIDE: Micro +6 sublevels / Mid +18 sublevels

## Primary common window

Target:
2023-10-31 -> 2026-09-26

This matches the first fully mature U10 capital date.

Every Grid token in a primary universe must independently have at least 1095 prior daily candles before 2023-10-31.

If a token fails that gate, the runner must fail that primary universe rather than silently shorten the Grid warm-up.

Starting portfolio capital for every strategy/universe:
10,000 USDT

## Primary Grid universes

### GRID_TIER_A_5

LINK, SOL, ETH, ADA, XLM

This is the previously documented Tier A quality/grid research subset.

Selection-leakage warning:
Tier A was identified using prior research evidence and is therefore not an untouched universe.

### GRID_TIER_A_B_10

LINK, SOL, ETH, ADA, XLM, HBAR, UNI, DOGE, AVAX, LTC

This combines prior Tier A and Tier B research candidates.

Selection-leakage warning:
also development-selected.

### GRID_FULL_15

BTC, ETH, BNB, SOL, XRP, TRX, DOGE, ADA, LINK, XLM, LTC, HBAR, AVAX, BCH, UNI

This is the previously frozen 15-asset quality/survival research universe.

Survivorship-bias warning:
the set intentionally contains old, large, surviving assets.

### GRID_U10_MATURE_8

ATOM, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

These are the U10 members expected to have sufficient history for the primary 2023-10-31 Grid start.

TWT and PEPE are excluded from this primary Grid universe only because the canonical Grid requires 1095 prior daily candles.

## Secondary exact-U10 Grid diagnostic

Universe:
ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

For each token:
- identify the first candle having 1095 earlier daily candles.

Exact-U10 Grid start:
- maximum of those per-token eligible dates.

Run:
- Grid BASE exact U10;
- Grid WIDE exact U10;
- fresh U10 ATOM-start from the same exact date.

This is a short-window diagnostic if the resulting history is short. It must not be treated as equivalent to the primary nearly-three-year comparison.

## Portfolio construction

For an N-token Grid universe:

- each token receives 10,000 / N USDT;
- each token sleeve splits that capital 50/50 between Micro and Mid;
- no rebalancing between token sleeves;
- portfolio equity = sum of all token-level total_equity values.

This is different from averaging token percentage returns.

## Metrics

For every portfolio:
- start/end date;
- elapsed days;
- final equity;
- total return from 10,000;
- minimum equity;
- minimum equity vs initial;
- max drawdown from prior portfolio peak;
- drawdown peak/trough dates and values;
- number of closed Grid trades or U10 transitions;
- positive/negative token sleeves for Grid;
- per-token final return and max drawdown for Grid.

For Grid universe comparison also report:
- concentration of contribution to terminal profit;
- median token return;
- worst/best token return;
- fraction of tokens profitable.

## Fairness / friction boundary

Primary comparison preserves each strategy's existing research execution model:

U10:
- 0.1% modeled cost per rotation.

Grid:
- 10 bps fee plus 5 bps adverse slippage on Grid fills.

These are not identical friction models.

Do not describe a return difference as pure strategy alpha without noting this modeling difference.

## Guardrails

- no live promotion;
- no parameter tuning after seeing results;
- no symbol-specific Grid tuning;
- no changing U10 mechanics;
- no changing Grid 1095-day prehistory requirement;
- no replacing portfolio-level results with geometric averages.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
