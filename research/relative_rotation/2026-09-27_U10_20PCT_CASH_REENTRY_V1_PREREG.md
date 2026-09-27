# U10 20% Cash-Out + Re-entry v1 — preregistration

Date: 2026-09-27
Mode: PATCH_FIX
Scope: research-only capital-management experiment

## Question

What happens if a fresh 10,000 USDT U10 portfolio first grows to at least 10x its starting capital, then locks 20% of the portfolio in cash, while the remaining 80% continues to follow U10?

Test three variants:

1. CASH_FOREVER — keep the withdrawn 20% in USDT through the end of the historical period.
2. REENTER_6M — re-enter the cash sleeve after 6 calendar months.
3. REENTER_12M — re-enter the cash sleeve after 12 calendar months.

Baseline:
4. NO_CASH_OUT — canonical U10 capital path with no partial cash-out.

## Frozen U10

ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

Frozen strategy mechanics:
- Binance Spot 1D closed candles
- rolling median 180 days
- ARM 15%
- reversal confirmation 3%
- strongest confirmed max-dislocation router
- next-open strategy execution
- 0.1% modeled transition cost on each executed rotation

## Starting capital

10,000 USDT at the first fully mature U10 date.

10x trigger threshold:
100,000 USDT total portfolio equity.

## Cash-out convention

The first daily close at or above 100,000 USDT creates a pending cash-out instruction.

At the next daily open:
1. execute any already-pending U10 rotation first;
2. value the invested portfolio at that open;
3. sell 20% of the invested portfolio into USDT;
4. apply 0.1% transaction cost to the withdrawn sleeve;
5. keep the remaining 80% invested in the asset currently held by U10.

The cash sleeve does not participate in subsequent U10 rotations while parked.

## Re-entry convention

For REENTER_6M and REENTER_12M:
- target date = cash-out execution date + 6 or 12 calendar months;
- on the first available daily open on or after the target date:
  1. execute any pending U10 strategy rotation first;
  2. buy the asset currently held by U10 with the entire cash sleeve;
  3. apply 0.1% modeled transaction cost to the re-entry;
  4. merge the re-entered quantity into the active U10 position.

This avoids inventing a second route for the parked capital.

## Cash assumption

Parked cash is modeled as USDT with:
- 0% yield;
- no price volatility;
- no staking yield;
- no additional counterparty-risk model.

## Required outputs

Per scenario:
- cash-out trigger date;
- cash-out execution date;
- asset sold;
- gross 20% sleeve value;
- cash-out transaction cost;
- net cash parked;
- target re-entry date;
- actual re-entry date;
- asset bought on re-entry;
- re-entry cost;
- total equity immediately before and after cash-out;
- minimum total equity after cash-out;
- max drawdown after cash-out;
- terminal equity and return;
- terminal difference versus NO_CASH_OUT;
- cash sleeve value through time;
- invested sleeve value through time.

## Interpretation

This test is not a new trading strategy.
It measures a capital-protection overlay on top of frozen U10.

No live/paper promotion is authorized by this research.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_BACKTEST
