# Current U10 -> Conservative Target U9 Migration v1 — preregistration

Date: 2026-09-28
Mode: STRESS_TEST_ONLY

## Source universe

ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

## Frozen target candidate

TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP

Derived by the bull-neutralized exhaustive U6..U15 search.

## Membership delta

KEEP:
TWT, PEPE, BNB, TRX, AAVE, FIL

SUNSET / EXIT:
ATOM, SOL, LINK, HBAR

ADD:
AVAX, ALGO, XRP

## Transition router

At migration start:

- if held asset is already in TARGET, route only inside TARGET;
- if held asset is one of SUNSET assets, keep holding it until the first confirmed outbound signal whose target belongs to TARGET;
- execute next-open under the frozen strategy;
- after leaving a SUNSET asset, never enter any SUNSET asset again;
- no forced one-for-one mapping between EXIT and ADD tokens.

Thus capital converges to TARGET via strategy signals rather than immediate portfolio rebalance.

## Historical starts

Monthly migration starts:
2024-01-01 through 2026-03-01.

For each start date and every CURRENT_U10 starting asset:
- compare staying on CURRENT_U10 through 2026-09-26;
- compare TRANSITION router through 2026-09-26.

Report:
- median terminal return across source starts;
- worst start;
- transition beats source rate;
- each SUNSET asset completion rate;
- days from migration start to first exit;
- first destination frequencies.

## Interpretation

This is a migration-risk test, not a new universe-selection test.

No live change in this run.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
