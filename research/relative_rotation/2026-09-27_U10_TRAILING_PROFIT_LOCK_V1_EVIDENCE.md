# U10 Trailing Profit Lock v1 — Evidence

Date: 2026-09-27
Mode: PATCH_FIX / research-only capital-management overlay
Validated branch: research/u10-trailing-profit-lock-v1
GitHub Actions run: 36335596026
Source commit: 721c76d7e370696994b2e5c31d5d4043e186874b
Result: PASS
TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_BACKTEST

## Question

Instead of selling immediately when U10 first reaches 10x original capital, use 10x only to activate a trailing profit monitor.

After activation:
- track the frozen-U10 running peak;
- when frozen-U10 daily-close equity falls at least 2x original capital (20,000 USDT) below that running peak, sell part of the active position on the next open;
- independently test 20%, 30%, 40%, and 50% cash-out fractions;
- lock the peak at the cash-out trigger;
- re-enter parked cash only after frozen-U10 daily-close equity falls at least 50% below that locked peak;
- re-enter on the next open into the asset currently held by frozen U10.

No U10 signal logic was changed.

## Frozen U10

ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

Frozen mechanics:
- Binance Spot 1D closed candles
- rolling median 180 days
- ARM 15%
- reversal confirmation 3%
- strongest confirmed max-dislocation router
- next-open execution
- 0.1% modeled U10 transition cost
- 0.1% modeled cash-out transaction cost
- 0.1% modeled re-entry transaction cost

## Shared trigger path

Original capital:

10,000 USDT

10x activation threshold:

100,000 USDT

First close at/above 10x:

2025-09-19

Running peak after activation:

127,668.80 USDT on 2025-09-20

Trailing giveback threshold:

20,000 USDT = 2x original capital

Cash-out trigger close:

102,424.14 USDT on 2025-09-22

Actual decline from locked peak at trigger:

25,244.66 USDT / -19.77%

The trigger exceeds exactly 20,000 because only daily closes are evaluated.

Cash-out execution:

2025-09-23 while holding ATOM

The same trigger dates were verified for all 20/30/40/50% cash-out fractions.

## Re-entry threshold

Locked peak:

127,668.80 USDT

50%-below-peak re-entry threshold:

63,834.40 USDT

The frozen-U10 reference equity never closed at or below 63,834.40 USDT after cash-out before the historical test ended on 2026-09-26.

Therefore:

**NO 50%-FROM-PEAK RE-ENTRY OCCURRED IN ANY 20/30/40/50% SCENARIO.**

All protected cash remained parked through the end of this historical window.

## Results

No-overlay baseline final equity:

219,485.20 USDT

Prior immediate-20%-at-10x / cash-forever final equity:

196,457.56 USDT

| Cash-out fraction | Net cash parked | Minimum total equity after cash-out | Max DD after cash-out | Final equity | Delta vs no-overlay |
|---:|---:|---:|---:|---:|---:|
| 20% | 20,464.34 | 77,108.42 | -47.52% | 196,052.50 | -23,432.70 / -10.68% |
| 30% | 30,696.52 | 80,260.08 | -43.89% | 184,336.16 | -35,149.04 / -16.01% |
| 40% | 40,928.69 | 83,411.75 | -39.84% | 172,619.81 | -46,865.39 / -21.35% |
| 50% | 51,160.86 | 86,563.41 | -35.28% | 160,903.46 | -58,581.74 / -26.69% |

Approximate no-overlay minimum after the comparable trigger region from prior validated evidence:

about 70,805 USDT

No-overlay post-trigger max drawdown:

about -53.73%

## Protection / opportunity-cost trade-off

Each extra 10 percentage points moved to cash:
- raised the historical post-cashout minimum by about 3.15k USDT;
- reduced terminal equity by about 11.72k USDT because the 50%-drawdown re-entry never happened.

Observed floor progression:

- no overlay: about 70.8k
- 20% cash: 77.1k
- 30% cash: 80.3k
- 40% cash: 83.4k
- 50% cash: 86.6k

Observed post-cashout max-drawdown progression:

- no overlay: about -53.73%
- 20% cash: -47.52%
- 30% cash: -43.89%
- 40% cash: -39.84%
- 50% cash: -35.28%

Observed terminal equity progression:

- no overlay: 219.5k
- 20% cash: 196.1k
- 30% cash: 184.3k
- 40% cash: 172.6k
- 50% cash: 160.9k

## Immediate 20% at 10x vs trailing 20%

Immediate cash-out after first 10x:
- executed 2025-09-20;
- final equity: 196,457.56 USDT.

Trailing 20%:
- waited for peak 127,668.80;
- trigger after fall to 102,424.14;
- executed 2025-09-23;
- final equity: 196,052.50 USDT.

Trailing 20% finished about 405 USDT below immediate-20%-cash-forever on this historical path.

Reason:
the trailing rule waited for a higher peak, but by the time the 20,000-USDT giveback trigger fired, the sell execution value was slightly below the immediate-10x execution value. Since the 50%-from-peak re-entry never occurred, the delayed sale did not recover that difference later.

## Main finding

The user's proposed rule successfully converts a growing U10 portfolio into a trailing capital-protection regime rather than forcing a sale exactly at 10x.

However, on this historical path:
- the trailing trigger fired quickly after a 127.7k peak;
- the 50%-from-peak re-entry level was never reached;
- therefore cash-out percentage became primarily a protection-versus-opportunity-cost choice.

Higher cash fractions materially raised the capital floor and reduced drawdown, but permanently reduced terminal equity over the observed window.

## Research implication

The next important parameter is not only cash-out fraction.

The **re-entry drawdown threshold** must be stress-tested.

Candidate sweep:
- re-enter at -25% from locked peak;
- -30%;
- -35%;
- -40%;
- -45%;
- -50%.

This can show whether -50% is too conservative and whether a shallower re-entry restores more upside without giving back most of the protection.

A separate sweep should later test the trailing giveback amount itself:
- 1x original capital;
- 2x;
- 3x;
- percentage-based peak drawdown thresholds.

## Residual risks

- one historical activation/peak path only;
- cash modeled as non-yielding USDT;
- stablecoin/counterparty risk not modeled;
- real spread/slippage may exceed 0.1%;
- historical performance does not establish future performance;
- no rolling trigger-path distribution has yet been tested.
