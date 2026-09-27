# U10 20% Cash-Out + Re-entry v1 — Evidence

Date: 2026-09-27
Mode: PATCH_FIX / research-only capital-management experiment
Branch validated: research/u10-20pct-cash-reentry-v1
GitHub Actions run: 36333838940
Source commit: 197b6a7afb1b8b5f590a154afb33e9dc5b35c5e6
Result: PASS
TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_BACKTEST

## Question

What happens if a 10,000 USDT mature U10 portfolio first reaches 10x capital, then locks 20% in cash while 80% continues to follow frozen U10?

Variants tested:

1. NO_CASH_OUT baseline
2. CASH_FOREVER
3. REENTER_6M
4. REENTER_12M

## Frozen U10

ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

Frozen mechanics:

- Binance Spot 1D closed candles
- rolling median 180 days
- ARM 15%
- reversal confirmation 3%
- strongest confirmed max-dislocation router
- next-open execution
- 0.1% modeled transition cost on U10 rotations
- 0.1% modeled transaction cost on cash-out
- 0.1% modeled transaction cost on re-entry
- parked cash = USDT, 0% yield

## 10x trigger

Initial capital:

10,000 USDT

10x threshold:

100,000 USDT

First daily close >= 100,000:

2025-09-19

Cash-out executed at next daily open:

2025-09-20

Asset sold:

TWT

Portfolio value at cash-out open before sale:

104,451.46 USDT

Gross 20% sleeve:

20,890.29 USDT

Cash-out cost:

20.89 USDT

Net cash parked:

20,869.40 USDT

Remaining 80% continued to follow frozen U10.

## Baseline

No cash-out final equity on 2026-09-26:

219,485.20 USDT

Return from original 10,000:

+2,094.85%

## Results

| Scenario | Re-entry | Post-cashout minimum | Post-cashout max DD | Final equity | Delta vs baseline |
|---|---|---:|---:|---:|---:|
| NO_CASH_OUT | none | about 70,805.10 USDT | -53.73% | 219,485.20 | baseline |
| CASH_FOREVER | never | 77,513.48 USDT | -47.41% | 196,457.56 | -23,027.64 (-10.49%) |
| REENTER_6M | 2026-03-20 | 77,513.48 USDT | -51.66% | 207,891.85 | -11,593.35 (-5.28%) |
| REENTER_12M | 2026-09-20 | 77,513.48 USDT | -47.41% | 197,881.52 | -21,603.68 (-9.84%) |

The approximate NO_CASH_OUT minimum after the same cash-out date is reconstructed from the proportional 80% sleeve before any re-entry:
(77,513.48 - 20,869.40) / 0.8 = 70,805.10 USDT.

## CASH_FOREVER

Net cash parked:

20,869.40 USDT

Post-cashout minimum total equity:

77,513.48 USDT on 2025-11-04

Compared with the approximate baseline minimum over the same period:

70,805.10 USDT

Cash floor improved the observed minimum by about:

6,708.38 USDT / +9.47%

Post-cashout max drawdown:

-47.41%

Final equity:

196,457.56 USDT

Return from original 10,000:

+1,864.58%

Opportunity cost versus no-cash baseline:

-23,027.64 USDT / -10.49%

## REENTER_6M

Cash-out:

2025-09-20

Re-entry target and execution:

2026-03-20

Asset bought:

TWT

Gross cash re-entered:

20,869.40 USDT

Re-entry cost:

20.87 USDT

Post-cashout minimum total equity:

77,513.48 USDT on 2025-11-04

Post-cashout max drawdown:

-51.66%

Final equity:

207,891.85 USDT

Return from original 10,000:

+1,978.92%

Opportunity cost versus baseline:

-11,593.35 USDT / -5.28%

Interpretation:

In this historical path, a six-month cash parking period preserved the early drawdown protection while recovering more of the later upside than keeping the 20% cash sleeve parked for the rest of the test.

## REENTER_12M

Cash-out:

2025-09-20

Re-entry:

2026-09-20

Asset bought:

ATOM

Post-cashout minimum total equity:

77,513.48 USDT

Post-cashout max drawdown:

-47.41%

Final equity:

197,881.52 USDT

Return from original 10,000:

+1,878.82%

Opportunity cost versus baseline:

-21,603.68 USDT / -9.84%

The 12-month re-entry occurs only six days before the test ends, so it behaves almost like CASH_FOREVER over this historical window.

## Main finding

A 20% profit-lock after U10 first reached 10x capital materially improved the observed capital floor during the next decline.

In this one historical path:

- baseline post-trigger minimum: about 70.8k USDT;
- 20% cash variants: 77.5k USDT;
- baseline post-trigger max drawdown: -53.73%;
- cash kept through the trough: -47.41%.

The protection was not free.

By 2026-09-26:

- no cash-out finished at 219.5k;
- six-month re-entry finished at 207.9k;
- twelve-month re-entry finished at 197.9k;
- cash forever finished at 196.5k.

Thus the overlay exchanged some terminal upside for a higher capital floor.

## Important interpretation

This does not show that 6 months is generally optimal.

It only shows that, on this specific historical path after the first 10x trigger, six-month re-entry produced a smaller terminal opportunity cost than twelve-month re-entry or permanent cash while still protecting the early decline.

A robust decision requires testing:
- multiple cash-out thresholds;
- multiple cash fractions;
- multiple re-entry delays;
- rolling-entry / different historical paths.

## Residual risks

- one historical 10x trigger only;
- cash modeled as USDT with 0% yield;
- stablecoin/counterparty risk not modeled;
- real spread/slippage may exceed the frozen 0.1% transaction-cost assumption;
- historical performance does not establish future performance.
