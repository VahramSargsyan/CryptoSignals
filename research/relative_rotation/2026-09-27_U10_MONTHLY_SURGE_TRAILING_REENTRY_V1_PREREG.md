# U10 Monthly Surge Pullback + Trailing Re-entry Peak v1 — preregistration

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Scope: research-only completion of P1/P2 return cycle; no live/paper changes

## Rule

Freeze P1/P2 exactly as previously tested.

After cash-out:
- keep 30% in USDT;
- initialize re-entry running peak to the locked cash-out peak;
- while cash is parked, if frozen-U10 reference daily-close equity makes a new high, update the re-entry running peak upward;
- re-enter all cash when reference daily-close equity falls at least 25% from that latest running peak;
- execute next daily open;
- apply 0.1% re-entry cost.

No timeout.
No peak-reclaim immediate entry.
No new percentage parameter.

This keeps the already frozen -25% re-entry depth but allows the reference peak to follow a continuing uptrend.

## Comparison

Across canonical U10 and all 791 alternative U10s compare:
1. baseline U10
2. P1/P2 with frozen cash-out peak
3. P1/P2 + 65d timeout
4. P1/P2 + exact old-peak reclaim
5. P1/P2 + trailing re-entry peak

## Required outputs

- final equity
- max drawdown
- delta vs baseline
- delta vs original no-fallback P1/P2
- delta vs 65d
- delta vs peak reclaim
- number of completed re-entries
- unfinished cycles
- days in cash
- cycle durations
- how often trailing peak was raised before re-entry
- initial locked peak versus final re-entry peak

Cross-topology:
- rate final > baseline
- rate DD improves
- rate both improve
- rate trailing version beats no-fallback
- rate trailing version beats 65d
- rate trailing version beats peak reclaim
- median/q25/q75 delta vs no-fallback

## Guardrail

This is the final return-rule candidate to be selected using 2023-2026 data.

Do not add another timeout, reclaim buffer, MA, or percentage sweep after viewing this result.

If coherent, freeze P1/P2 + trailing re-entry peak before opening 2020-2022 historical validation.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
