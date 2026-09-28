# RR U10 DAY-0 PRICE RESET COUNTERFACTUAL V1 — PREREG

Date: 2026-09-28
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## User hypothesis

Run an intentionally abstract counterfactual.

At the first evaluation day, freeze the open price of every U10 token.
After every later rotation, when the destination token is bought, substitute
that frozen day-0 token price instead of the destination token's actual market
price on the rotation date.

Even the final rotation uses the same frozen day-0 purchase price.

The purpose is to isolate the mathematical effect requested by the user.
This is NOT an executable trading/backtest assumption and must never be
presented as realizable historical P&L.

## Frozen U10 route

Universe:
`RR_TARGET_U10_CANDIDATE_HBAR_V1`

Assets:
`TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR`

Signal/routing semantics remain canonical:
- Binance Spot D1
- lookback 180d
- ARM 15%
- reversal 3%
- strongest confirmed max-dislocation router
- signal close T
- transition on next available daily open
- all 10 possible starting assets
- MATURE window 2023-10-31 through 2026-09-26

The sequence and dates of rotations MUST remain exactly the same as canonical
U10 for each starting asset.

## Day-0 price snapshot

Snapshot timestamp:
- first available canonical evaluation bar on/after 2023-10-31.

For every U10 token freeze:

`DAY0_PRICE[token] = open[token, start_i]`

Persist the snapshot table.

Initial normalized capital:
- 100 USDT per starting path.
- initial quantity = 100 / DAY0_PRICE[start_asset].

## Counterfactual rotation execution

For a canonical rotation from current asset A into destination B on execution
date E:

1. Value current holding using A's REAL historical execution-date open:
   `value_before = qty_A * real_open[A,E]`

2. Charge the same frozen 0.1% transition cost:
   `value_after_cost = value_before * (1 - 0.001)`

3. Ignore B's real historical execution-date purchase price.

4. Buy B using B's frozen day-0 price:
   `qty_B = value_after_cost / DAY0_PRICE[B]`

5. Hold B through its REAL historical price path until the next canonical
   rotation.

6. Repeat the same substitution at every later destination purchase, including
   the final rotation before cutoff.

Final mark-to-market at cutoff:
- quantity of the final held token multiplied by its REAL historical close at
  cutoff.

This is the literal requested counterfactual.

## Required comparison

Run in parallel:

### BASELINE_CANONICAL
Normal canonical next-open execution at real destination price.

### DAY0_RESET
Destination buy price is always the frozen day-0 price.

Both variants:
- use identical canonical signal dates;
- use identical from/to route;
- charge 0.1% per transition;
- differ only in destination purchase price.

Fail the test if route dates/from/to differ between the two variants.

## Per-transition output

For every transition persist:
- start_asset
- transition_index
- signal_date
- execute_date
- from_asset
- to_asset
- from_real_open
- to_real_open
- to_day0_frozen_open
- destination_price_substitution_ratio =
  real_to_open / frozen_to_open
- baseline capital before and after cost
- counterfactual capital before and after cost
- baseline destination quantity
- counterfactual destination quantity
- capital ratio DAY0_RESET / BASELINE immediately after the price substitution
  when both are marked at the real destination open
- canonical market regime if available

## Outputs

Persist:
- `day0_price_snapshot.csv`
- `transition_counterfactual_ledger.csv`
- `start_path_summary.csv`
- `route_equivalence.csv`
- `summary.json`
- `report.md`

## Required summary

For each start path:
- transition count
- baseline final capital from 100 USDT
- DAY0_RESET final capital from 100 USDT
- multiple versus baseline
- whether every counterfactual transition-to-transition capital point increased
- maximum and minimum counterfactual capital

Across starts:
- median baseline final capital
- median DAY0_RESET final capital
- median multiple versus baseline
- route-equivalence status

## Interpretation boundary

The result is intentionally synthetic.

It must NOT be described as:
- achievable historical return;
- executable market P&L;
- evidence that stale prices can be traded;
- proof of arbitrage.

It may be used only to inspect the mathematical effect of repeatedly resetting
destination token prices to the first-day snapshot while preserving real
historical holding paths.

## Production boundary

No live/paper, Telegram, exchange, universe, allocation or execution behavior
may change.

Target test level:

`TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_COUNTERFACTUAL_STRESS_TEST`
