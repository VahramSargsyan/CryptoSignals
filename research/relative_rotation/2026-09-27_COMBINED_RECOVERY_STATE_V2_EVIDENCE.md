# COMBINED RECOVERY STATE V2 — Evidence

Date: 2026-09-27
Branch: `research/combined-recovery-state-v2`
Mode: STRESS_TEST_ONLY + ECOSYSTEM_PLANNING
Status: EXECUTED / RETROSPECTIVE / DO NOT PROMOTE

## Run identity

- GitHub Actions run: `36309021430`
- source commit: `d9833d9c8d8c208aba2015de8cebcbb1cad2c912`
- artifact: `combined-recovery-state-v2`
- artifact ID: `10928223513`
- artifact digest: `sha256:7f265333617ffbc58c5c855dca20d84f6c4cf989aa158471d68700c7e765cfd5`
- compile: PASS
- unit tests: PASS
- current-8 backtest: PASS
- retrospective LEGACY-7 backtest: PASS
- artifact upload: PASS

## Single change from V1

V1:
`fresh BTC bullish crossover today`

V2:
`BTC is already in bullish state: fast SMA > SMA100`

Everything else stayed frozen:
- CASH entry breadth<=3 x3;
- official M2 3m>0 AND 6m>0 AND 12m>0;
- breadth recovery >=4;
- candidates 25/100, 30/100, 12/100;
- next-open CASH -> current shadow target;
- no second cash entry inside same crisis;
- re-arm breadth>=5 x3;
- 0.1% transition cost;
- cash yield 0%.

No new parameter search.

# Current 8-asset result

Frozen LOW_VOL:
- reproduction: +49.05% / DD -47.13%
- opened 2026: +32.92% / DD -18.34%
- full eligible history: +887.02% / DD -47.13%

## 25/100 V2

Reproduction:
- V1: -30.06% / -31.97%, wait 185d
- V2: **+18.61% / -43.20%, wait 40d**
- LOW_VOL: +49.05% / -47.13%
- V2 cash exposure: 51.23%
- V2 positive starts: 7/8
- V2 unresolved cash starts at period end: 8/8

Opened 2026:
- V1: +25.44% / -18.34%, wait 146d
- V2: **+25.44% / -18.34%, wait 146d**
- LOW_VOL: +32.92% / -18.34%

Full eligible history:
- V1: +163.62% / -60.73%, median wait 296d
- V2: **+381.74% / -64.04%, median wait 34.5d**
- V2 max wait: 296d
- V2 cash exposure: 42.71%
- V2 median exits: 6
- LOW_VOL: +887.02% / -47.13%

Robustness vs LOW_VOL:
- both return+DD wins: 2/13
- return wins: 3/13
- DD wins: 5/13

V2 vs V1 robustness:
- return wins: 2/13
- DD wins: 0/13

## 30/100 V2

Reproduction:
- V1: -27.05% / -31.04%, wait 187d
- V2: **+18.61% / -43.20%, wait 40d**

Opened 2026:
- V1 = V2:
  +25.44% / -18.34%, wait 146d

Full:
- V1: +136.56% / -64.76%, wait 205.5d
- V2: **+381.74% / -64.04%, wait 34.5d**
- V2 max wait 296d
- LOW_VOL: +887.02% / -47.13%

Robustness vs LOW_VOL:
- both wins: 2/13
- return wins: 3/13
- DD wins: 5/13

V2 vs V1:
- return wins: 3/13
- DD wins: 0/13

## 12/100 V2

Reproduction:
- V1: +2.18% / -43.20%, wait 166d
- V2: **+18.61% / -43.20%, wait 40d**

Opened 2026:
- V1 = V2:
  +20.99% / -18.34%, wait 144d

Full:
- V1: +245.01% / -48.61%, wait 294d
- V2: **+362.48% / -64.21%, wait 41d**
- V2 max wait 294d
- LOW_VOL: +887.02% / -47.13%

Robustness vs LOW_VOL:
- both wins: 2/13
- return wins: 3/13
- DD wins: 5/13

V2 vs V1:
- return wins: 2/13
- DD wins: 0/13

# Main current-history finding

The event-semantics diagnosis was correct.

V1 full-history median waits:
- 25/100: 296d
- 30/100: 205.5d
- 12/100: 294d

V2:
- 25/100: 34.5d
- 30/100: 34.5d
- 12/100: 41d

Therefore replacing a one-day crossover event with a persistent BTC bullish state removes most of the artificial delay.

But it also removes much of V1's cash-quarantine protection.

V2 full-history DD:
- 25/100: -64.04%
- 30/100: -64.04%
- 12/100: -64.21%

LOW_VOL:
- -47.13%

Thus faster re-entry restores some return but reintroduces large market risk.

# State diagnostics

Current full panel:

25/100:
- BTC bullish-state days: 619
- blocked by M2: 196
- blocked by breadth after M2 pass: 120
- all-recovery-state days: 303

30/100:
- BTC bullish-state days: 618
- blocked by M2: 196
- blocked by breadth: 113
- all-state days: 309

12/100:
- BTC bullish-state days: 643
- blocked by M2: 195
- blocked by breadth: 130
- all-state days: 318

The three candidates become very similar under state semantics.
This implies the combined gate is dominated more by M2 + breadth state than by small differences among the frozen fast-SMA lengths.

# Retrospective LEGACY-7

This period is already known and is NOT untouched validation.

Full old period:

LOW_VOL:
- -40.08% / DD -76.24%

V1 event gate:
- +30.17% / -37.06%
- but no re-entry; 7/7 unresolved cash at end.

V2 state gate, all three BTC candidates:
- **-18.79% / -62.52%**
- median cash wait for resolved exit: 1d
- median re-entry exits: 1
- cash exposure: 80.45%
- unresolved at end: 7/7
- positive starts: 0/7

Interpretation:
V2 does re-enter at least once, unlike V1.
However some all-state configuration can already be true almost immediately after a cash entry, producing a very early re-entry in the old regime.

This is the opposite edge of the V1 problem:
- V1 could be far too late;
- V2 can sometimes be too early.

LEGACY-7 robustness vs LOW_VOL:
- both wins: 5/8
- return wins: 5/8
- DD wins: 7/8

These wins remain retrospective and are affected by unresolved terminal cash states.

# Critical 2022 window

2022-02-10 -> 2022-08-08.

LOW_VOL:
- +2.34% / -36.79%

Standalone BTC previously:
- approximately -49% to -61% return
- approximately -79% to -80% DD.

V1 event gate:
- -7.36% / -7.36%
- no re-entry.

V2 state gate, all three candidates:
- **-7.36% / -7.36%**
- no re-entry
- cash exposure ~98.33%.

Therefore V2 preserves the most important benefit of V1:
the catastrophic false BTC recovery in the 2022 bear window remains blocked.

# What V2 established

Supported:

1. V1's 200-400d waits were largely an event-synchronization artifact.
2. Persistent BTC bullish state is mechanically superior to requiring a new crossover after breadth recovery.
3. M2 + breadth can still block the known catastrophic 2022 false recovery.
4. Small fast-SMA differences matter much less once BTC is represented as a state.

Not supported:

1. V2 as a production cash-exit rule.
2. V2 dominance over frozen LOW_VOL.
3. The idea that M2 + BTC bullish state + breadth>=4 is sufficient to control full-cycle drawdown.
4. Retuning SMA lengths or M2 horizons on the same histories.

# Verdict

`COMBINED_RECOVERY_STATE_V2 = EVENT_SEMANTICS_FIX_WORKS / WAIT_NORMALIZED / 2022_FALSE_RECOVERY_STILL_BLOCKED / FULL_CYCLE_RISK_TOO_HIGH / PARTIAL_IMPROVEMENT / DO_NOT_PROMOTE`

The unresolved design problem is now narrower:

`How to distinguish a durable breadth/BTC recovery from an early temporary recovery WITHOUT returning to V1's multi-month cash quarantine.`

That is a new hypothesis and should not be solved by post-hoc threshold tuning inside V2.

## Runtime impact

Production changed: NONE
Paper-live changed: NONE
Migration required: NO
Automatic execution authorized: NO
Manual confirmation remains required.

TEST_LEVEL: GITHUB_ACTIONS_EXECUTED + UNIT_TESTS + REAL_BINANCE_1D + OFFICIAL_H6_M2_SNAPSHOT + CURRENT8_FULL_HISTORY + RETROSPECTIVE_LEGACY7 + 180D_120D_ROBUSTNESS
