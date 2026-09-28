# U10 Monthly Surge P1 Cash-50 Re-entry-25 v1 — preregistration

Date: 2026-09-29
Mode: STRESS_TEST_ONLY
Scope: one-parameter research change only; no live/paper changes

## Purpose

Re-run the previously frozen P1 monthly-surge overlay changing only the
protected cash fraction from 30% to 50%.

## Frozen control

The prior canonical P1 rule remains the control:

- single-month selected-extreme surge: +100%
- after arming, track post-surge running peak
- cash-out after >=5% pullback from that peak
- cash fraction: 30%
- execute next daily open
- 0.1% modeled cash-out cost
- lock the running peak at cash-out
- re-enter the full parked cash sleeve when frozen-U10 reference close is
  >=25% below the locked peak
- execute re-entry next daily open
- 0.1% modeled re-entry cost
- one cycle per surge month
- cash is non-yielding USDT

Frozen control canonical result from run 36343997067:

- baseline U10: 219,485.20 USDT
- P1 cash-30: 270,653.52 USDT
- P1 vs baseline: +23.31%
- max drawdown: -66.36%
- 3 cash-outs / 3 re-entries
- 66 cash days

## Candidate

Change exactly one parameter:

`cash_fraction = 0.50`

Everything else stays identical to P1 cash-30.

Candidate therefore is:

- surge +100%
- sell 50% after >=5% pullback from post-surge running peak
- re-enter the full 50% parked cash sleeve at -25% from the locked peak
- same next-open execution
- same 0.1% cash-out and re-entry costs
- same canonical U10 and same historical panel
- same ATOM start
- same mature start
- same event dates

## Primary comparison

Report:

- baseline ordinary U10
- P1 cash-30 frozen control
- P1 cash-50 candidate
- candidate delta vs baseline
- candidate delta vs cash-30 in USDT and %
- max drawdown for all three
- cash days
- cash-out / re-entry counts
- fee totals

## Topology robustness

Use the exact same frozen 792 U10 topology set from the original study:

Mandatory:
- ATOM
- TWT
- PEPE

Pool:
ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, AVAX, FIL, ETH, ALGO, ADA, XRP, HBAR

Across the 791 non-canonical alternatives report:

- fraction where cash-50 beats ordinary baseline
- fraction where cash-50 beats cash-30
- fraction where cash-50 improves max DD vs cash-30
- fraction where both terminal equity and max DD improve vs cash-30
- median / q25 / q75 terminal delta vs cash-30
- unfinished cash-cycle rate

No parameter sweep is allowed in this test.

## Interpretation boundary

This test answers only:

`What historically happens if P1 protects 50% instead of 30%?`

It does not establish an optimal cash fraction.

No 40/60/70% sweep is allowed after results are seen in this run.

## Production boundary

No live/paper, Telegram, exchange, universe, allocation or execution behavior
may change.

Target test level:

`TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`
