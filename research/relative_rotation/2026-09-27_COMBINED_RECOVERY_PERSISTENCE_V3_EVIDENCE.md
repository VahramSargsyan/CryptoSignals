# COMBINED RECOVERY PERSISTENCE V3 — Evidence

Date: 2026-09-27
Branch: `research/combined-recovery-persistence-v3`
Mode: STRESS_TEST_ONLY + ECOSYSTEM_PLANNING
Status: EXECUTED / RETROSPECTIVE / DO NOT PROMOTE

## Run identity

- GitHub Actions run: `36310096438`
- source commit: `cf7a310cfd66a53d239ebf3f0a97ce9b39569117`
- artifact: `combined-recovery-persistence-v3`
- artifact ID: `10928512309`
- artifact digest: `sha256:147c9768d07cad6a8266f5aa0329e08cb681494599cacc5cf4144d5eac2ee889`
- compile: PASS
- unit tests: PASS
- current-8 backtest: PASS
- retrospective LEGACY-7 backtest: PASS
- artifact upload: PASS

TEST_LEVEL: GITHUB_ACTIONS_EXECUTED + UNIT_TESTS + REAL_BINANCE_1D + OFFICIAL_H6_M2_SNAPSHOT + CURRENT8_FULL_HISTORY + RETROSPECTIVE_LEGACY7 + 180D_120D_ROBUSTNESS

## Single change from V2

V2:
- exit CASH on the first close where:
  - M2 3m/6m/12m > 0;
  - BTC fast SMA > SMA100;
  - breadth >=4.

V3:
- require those same V2 recovery states to remain true for **3 consecutive closes while actually in CASH**;
- then exit at the next daily open.

The confirmation length is the existing frozen `CONFIRM_DAYS=3`.
No new numeric parameter was introduced.

## Current 8-asset history

Frozen LOW_VOL:
- reproduction: +49.05% / DD -47.13%
- opened 2026: +32.92% / DD -18.34%
- full eligible history: +887.02% / DD -47.13%

### 25/100

Reproduction:
- V2: +18.61% / -43.20%, median wait 40d
- V3: **+21.37% / -43.20%, median wait 50d**
- V3 cash exposure: 53.97%
- median recovery-streak resets: 2

Opened 2026:
- V2: +25.44% / -18.34%, wait 146d
- V3: **+26.27% / -18.34%, wait 148d**

Full eligible history:
- V2: +381.74% / -64.04%, median wait 34.5d
- V3: **+401.35% / -62.01%, median wait 41d**
- V3 cash exposure: 45.49%
- median recovery-streak resets: 5

Robustness vs LOW_VOL:
- return wins: 3/13
- DD wins: 5/13
- both wins: **2/13**

Robustness V3 vs V2:
- return wins: 5/13
- DD wins: 4/13
- both wins: 3/13

### 30/100

Results are effectively identical to 25/100 under V3:

Reproduction:
- V3: **+21.37% / -43.20%, wait 50d**

Opened 2026:
- V3: **+26.27% / -18.34%, wait 148d**

Full:
- V3: **+401.35% / -62.01%, wait 41d**
- V2: +381.74% / -64.04%

Robustness vs LOW_VOL:
- both wins 2/13.

### 12/100

Reproduction:
- V2: +18.61% / -43.20%, wait 40d
- V3: **+21.37% / -43.20%, wait 50d**

Opened 2026:
- V2: +20.99% / -18.34%, wait 144d
- V3: **+25.44% / -18.34%, wait 146d**

Full:
- V2: +362.48% / -64.21%, wait 41d
- V3: **+387.03% / -62.85%, wait 48d**
- cash exposure: 46.64%
- median streak resets: 2

Robustness vs LOW_VOL:
- return wins 3/13
- DD wins 5/13
- both wins 2/13.

V3 vs V2:
- return wins 6/13
- DD wins 5/13
- both wins 4/13.

## Current-history interpretation

V3 behaves exactly as intended mechanically:

- V2 full-history wait: ~34.5-41d;
- V3: ~41-48d;
- delay increase is moderate, not a return to V1's 200-400d quarantine.

At the same time V3 modestly improves full-history return and drawdown versus V2.

Best current V3 full-history rows:
- 25/100 and 30/100: +401.35% / -62.01%.

But frozen LOW_VOL remains much stronger:
- +887.02% / -47.13%.

Thus three-close persistence helps, but does not solve the full-cycle risk/return trade-off.

## Retrospective LEGACY-7

This period is already known and is not untouched validation.

Full old period:

LOW_VOL:
- -40.08% / -76.24%.

V2, all three candidates:
- -18.79% / -62.52%
- median resolved cash wait 1d.

V3, all three candidates:
- **-28.46% / -66.99%**
- median resolved cash wait 3d
- unresolved cash starts at end: 7/7.

Thus V3 is worse than V2 on the full old period, though still better than LOW_VOL on median return and DD in that particular retrospective sample.

LEGACY robustness vs LOW_VOL:
- return wins: 5/8
- DD wins: 7/8
- both wins: 5/8.

But V3 vs V2:
- return wins: 0/8
- DD wins: 0/8.

So the added persistence does not generalize as an improvement to the old retrospective regime.

## Critical 2022 window

2022-02-10 -> 2022-08-08.

LOW_VOL:
- +2.34% / -36.79%.

V2:
- -7.36% / -7.36%
- no re-entry.

V3:
- **-7.36% / -7.36%**
- no re-entry
- unresolved cash: 7/7.

Therefore V3 preserves the most important protective behavior:
the catastrophic standalone-BTC false recovery remains blocked.

## Main finding

V3 confirms that a short persistence requirement is directionally useful on the current 8-asset history:

- it does not recreate V1's multi-month artificial delay;
- it improves V2 full-history return modestly;
- it improves V2 full-history DD modestly;
- it preserves the 2022 false-recovery block.

But the improvement is not robust enough:

- only 2/13 current robustness windows beat LOW_VOL on both return and DD;
- full-history DD remains roughly -62% to -63%;
- full-history return remains far below LOW_VOL;
- old LEGACY-7 results are worse than V2.

## Verdict

`COMBINED_RECOVERY_PERSISTENCE_V3 = PERSISTENCE_FILTER_HELPS_CURRENT_SAMPLE / WAIT_ACCEPTABLE / 2022_BLOCK_PRESERVED / LEGACY_WORSE_THAN_V2 / NO_ROBUSTNESS_BREAKTHROUGH / DO_NOT_PROMOTE`

Supported:
- 3-close persistence is a mechanically sensible refinement of V2;
- it slightly improves the current-history V2 trade-off;
- it does not cause V1-style over-waiting.

Not supported:
- production promotion;
- superiority to frozen LOW_VOL;
- further tuning of confirmation length on these same histories.

The unresolved problem remains:
how to reduce the large full-cycle drawdown after re-entry without either:
- returning too early, or
- staying in cash for hundreds of days.

## Runtime impact

Production changed: NONE
Paper-live changed: NONE
Migration required: NO
Automatic execution authorized: NO
Manual confirmation remains required.
