# CASH CRISIS QUARANTINE V1 — Preregistration

Date: 2026-09-27
Branch: `research/cash-crisis-quarantine-v1`
Mode: STRESS_TEST_ONLY + ECOSYSTEM_PLANNING
Status: PREREGISTERED / FROZEN_BEFORE_EXECUTION / NO PRODUCTION CHANGE

## User hypothesis

When frozen crypto stress is confirmed, leave crypto for cash, but do NOT remain in cash until the existing breadth recovery exit.

Instead:
1. enter CASH_PROXY on the same frozen next-open crisis entry;
2. stay in cash for a fixed preregistered quarantine duration;
3. while in cash, do not evaluate a new crisis-entry trigger;
4. the shadow relative router continues normally;
5. after the quarantine expires, re-enter the current shadow router target at the next daily open;
6. reset the crisis streak state;
7. from that re-entry day onward, resume normal router behavior and begin looking for the next crisis from scratch.

This test changes defensive DURATION only.
It does not use macro confirmation, Fed liquidity, or the breadth >=5 frozen recovery rule for exit from cash.

## Frozen crisis-entry rule

Unchanged:
- universe: ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK;
- own causal SMA200;
- breadth = count above own SMA200;
- stress confirmed after 3 consecutive closes with breadth <=3;
- cash entry executes at next daily open;
- 0.1% transition cost;
- shadow router continues while actual capital is in cash.

## Preregistered quarantine durations

All durations are tested and reported together:

- 14 days
- 21 days
- 30 days
- 45 days
- 60 days

No duration will be selected after seeing results and called validated.

Definition:
- if cash entry executes at open on date D, that is cash day 1;
- for a quarantine of N days, cash is held through the close of D+N-1;
- re-entry occurs at the open of D+N into the then-current shadow router target;
- because crypto is 7-day daily data, these are calendar/daily-bar days.

## Crisis state while in cash

While cash quarantine is active:
- breadth may still be logged for diagnostics;
- low/high crisis streaks are NOT accumulated;
- no second crisis entry can be armed;
- no breadth >=5 exit is used;
- the fixed quarantine cannot be shortened or extended.

At cash exit:
- low/high streaks reset to zero;
- normal crisis detection resumes from that day's close;
- therefore the earliest possible next cash entry requires a fresh 3-close breadth<=3 confirmation after re-entry.

## Cash model

- label: CASH_PROXY;
- modeled yield: 0%;
- no crypto price exposure during quarantine;
- entry and exit each pay 0.1% transition cost;
- no depeg, counterparty, tax, custody, or venue risk is modeled.

## Comparators

Report:
- baseline relative router;
- frozen LOW_VOL defensive V1;
- frozen-timing CASH destination from the previous ablation;
- CASH_QUARANTINE_14D;
- CASH_QUARANTINE_21D;
- CASH_QUARANTINE_30D;
- CASH_QUARANTINE_45D;
- CASH_QUARANTINE_60D.

The previous frozen-timing CASH comparator remains useful because it isolates the effect of changing duration.

## Evaluation boundaries

Use the same real Binance 1D panel through 2026-09-26.

Required:
1. reproduction period: 2025-03-29 -> 2026-03-28;
2. already-open 2026 period: 2026-03-29 -> 2026-09-26;
3. full eligible history from first valid SMA200/VOL30 date;
4. consecutive non-overlapping complete 180-day windows from that anchor;
5. consecutive non-overlapping complete 120-day windows from the same anchor.

State resets at the start of each robustness window.

## Required metrics

For every variant and window:
- median return;
- median and worst max drawdown;
- worst starting-asset return;
- positive starts;
- actual transition count;
- cash entries;
- cash days and cash exposure;
- re-entry count;
- episode count.

Across durations:
- return ranking is descriptive only, not a winner-selection rule;
- drawdown ranking is descriptive only;
- count how many 180d/120d windows each duration beats frozen LOW_VOL on return and drawdown;
- count how many windows each duration beats frozen-timing CASH;
- report whether any duration improves BOTH return and drawdown versus frozen LOW_VOL in the same window.

## Interpretation discipline

Possible descriptive outcomes:
- FIXED_QUARANTINE_PROMISING
- LOW_VOL_STILL_DOMINANT
- DURATION_SENSITIVE
- MIXED

No duration may be promoted from this sample.
Any future single-duration strategy requires a new validation boundary.

## Runtime impact

Production changed: NONE
Paper-live changed: NONE
Migration required: NO
Automatic execution authorized: NO
Manual confirmation remains required.

TEST_LEVEL: PREREGISTRATION_ONLY
