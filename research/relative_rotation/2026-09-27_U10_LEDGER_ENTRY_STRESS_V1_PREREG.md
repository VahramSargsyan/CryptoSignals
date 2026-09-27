# U10 Ledger + Entry Stress v1 — preregistration

Date: 2026-09-27
Mode: PATCH_FIX
Scope: research/backtest instrumentation only

## Objective

Run the current research U10 through the same accounting and principal-risk framework already used for canonical U8.

U10 is frozen for this test as:

ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

No live strategy promotion or live monitor change is allowed by this test.

## Frozen mechanics

- Binance Spot 1D closed candles
- rolling median: 180 days
- ARM threshold: 15%
- reversal confirmation: 3%
- strongest confirmed max-dislocation router
- next-open execution
- transition cost: 0.1% per executed transition
- fresh portfolio starts in ATOM
- normalized initial capital: 10,000 USDT

## Long-path test

1. Download the full common U10 panel through the latest closed 2026-09-26 candle.
2. Keep the full common panel for indicator warm-up.
3. Start capital only on the first fully mature 180-row date.
4. Record every executed transition:
   - signal date
   - execution date
   - from asset / quantity / price
   - gross USDT
   - 0.1% transition cost
   - to asset / quantity / price
   - cumulative fee drag versus a zero-cost shadow route
5. Record:
   - final equity / return
   - minimum equity versus original 10,000
   - days below original capital
   - max peak-to-trough drawdown
   - exact peak and trough values/dates
   - route and transition count

## U10-specific drawdown anchor

The runner must discover U10's own strongest historical peak-to-trough drawdown on the mature long path.

Do not reuse U8 drawdown dates unless U10 independently produces the same dates.

## Fresh-entry scenarios

Using U10's own max-drawdown peak/trough:

1. ONE_YEAR_BEFORE_PEAK = one calendar year before U10 max-DD peak
2. PEAK_YEAR_START = January 1 of the U10 max-DD peak year
3. AT_PEAK_DATE = U10 max-DD peak date
4. TROUGH_YEAR_START = January 1 of the U10 max-DD trough year

Each scenario:
- starts a fresh 10,000 USDT ATOM position;
- retains historical market/monitor state before the capital start date;
- reports minimum equity versus initial capital separately from peak-to-trough drawdown.

## Interpretation rule

Never use a max-drawdown percentage as shorthand for loss of original capital.

Always show:
- MAX DRAWDOWN FROM PRIOR PEAK
- MINIMUM EQUITY VS INITIAL CAPITAL

## Validation

A parallel zero-cost shadow must follow the exact same route.

After N transitions:

actual_value / zero_cost_value = (1 - 0.001)^N

The runner must fail if the identity is violated beyond floating-point tolerance.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_BACKTEST
