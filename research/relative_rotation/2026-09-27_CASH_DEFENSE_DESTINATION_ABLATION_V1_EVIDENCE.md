# CASH DEFENSE DESTINATION ABLATION V1 — Evidence

Date: 2026-09-27  
Branch: `research/cash-defense-destination-ablation-v1`  
Mode: STRESS_TEST_ONLY  
Status: EXECUTED / RESEARCH_ONLY / NO PRODUCTION CHANGE

## Run identity

- GitHub Actions run: `36300363884`
- Source commit: `25f0b97e7ffce27daede34d7166427e674bc1056`
- Artifact: `cash-defense-destination-ablation-v1`
- Artifact ID: `10924184874`
- Run ID: `CASH_DEFENSE_DESTINATION_ABLATION_V1_25f0b97e7ffc`
- Workflow conclusion: SUCCESS

TEST_LEVEL: GITHUB_ACTIONS_EXECUTED + UNIT_TESTS + FROZEN_REPRODUCTION_GATE + REAL_BINANCE_1D + FULL_HISTORY + NON_OVERLAPPING_180D_120D_ROBUSTNESS

## Question

Holding defensive timing fixed, is it better to:
- remain inside crypto in the frozen lowest-VOL30 token; or
- leave crypto entirely into a 0%-yield CASH_PROXY?

Only destination changed.

Unchanged:
- 8-asset universe;
- own SMA200;
- enter after 3 consecutive closes breadth <=3;
- exit after 3 consecutive closes breadth >=5;
- next-open execution;
- 0.1% actual transition cost;
- shadow relative router continues;
- no macro confirmation used.

## Reproduction gate

Historical reproduction period:
`2025-03-29 -> 2026-03-28`

Frozen LOW_VOL reproduction:
- baseline median return: +43.82%
- baseline median max DD: -61.57%
- LOW_VOL median return: +49.05%
- LOW_VOL median max DD: -47.13%

Expected prior values were matched within the preregistered +/-5pp tolerance.

`REPRODUCTION_GATE = PASS`

### Destination result in reproduction period

LOW_VOL:
- median return: **+49.05%**
- median max DD: **-47.13%**
- worst start: **+41.84%**
- positive starts: 8/8
- defensive exposure: ~69.86%

CASH:
- median return: **+5.93%**
- median max DD: **-43.20%**
- worst start: **+0.81%**
- positive starts: 8/8
- defensive exposure: ~69.86%

Cash effect:
- return: **-43.12 percentage points** versus LOW_VOL
- DD: only **+3.93pp improvement** versus LOW_VOL

Interpretation:
Cash bought only modest additional drawdown reduction while sacrificing most of the LOW_VOL return.

## Previously opened 2026 period

Period:
`2026-03-29 -> 2026-09-26`

This period is not untouched for the cash hypothesis because it had already been opened by the frozen LOW_VOL validation.

LOW_VOL:
- median return: **+32.92%**
- median max DD: **-18.34%**
- worst start: **+9.49%**
- positive starts: 8/8
- defensive exposure: ~79.12%

CASH:
- median return: **+20.99%**
- median max DD: **-18.34%**
- worst start: **-0.34%**
- positive starts: 7/8
- defensive exposure: ~79.12%

Cash effect:
- return: **-11.93pp**
- median DD improvement: effectively **0pp**

Interpretation:
In this recovery-heavy period cash delivered no median drawdown benefit at all and reduced return materially.

## Full-history result

Eligible start:
`2023-11-20`

End:
`2026-09-26`

Eight defensive episodes.

BASELINE:
- median return: **+646.96%**
- median max DD: **-71.23%**

LOW_VOL:
- median return: **+887.02%**
- median max DD: **-47.13%**
- worst start: **+864.26%**
- positive starts: 8/8
- defensive exposure: ~53.93%

CASH:
- median return: **+258.06%**
- median max DD: **-67.56%**
- worst start: **+249.80%**
- positive starts: 8/8
- defensive exposure: ~53.93%

Cash minus LOW_VOL:
- median return difference: **-628.97pp**
- median max-DD difference: **-20.43pp**

Important:
For drawdown, a more negative number is worse. Therefore full-history CASH had materially worse max drawdown than LOW_VOL despite having no crypto-price exposure during defensive periods.

Mechanism:
Cash often locked in the pre-defense loss and then remained flat while the frozen LOW_VOL token, mostly TRX, appreciated during long defensive episodes. The LOW_VOL appreciation rebuilt equity and lifted the running peak/trough path; cash did not.

## Episode-level destination comparison

Episode return definition:
portfolio return from the close immediately before defensive execution to the last defensive close before exit execution. This includes entry execution effects but excludes the exit-day post-exit crypto move.

LOW_VOL defensive asset was TRX in all eight full-history episodes.

| Episode | Entry | Exit | LOW_VOL return | CASH return | CASH - LOW_VOL |
|---|---|---|---:|---:|---:|
| 1 | 2024-04-20 | 2024-05-20 | +10.26% | +0.01% | -10.26pp |
| 2 | 2024-06-14 | 2024-07-17 | +14.49% | -0.09% | -14.57pp |
| 3 | 2024-08-07 | 2024-08-26 | +34.68% | -0.12% | -34.80pp |
| 4 | 2024-08-30 | 2024-09-29 | -3.14% | -0.14% | +3.00pp |
| 5 | 2024-10-03 | 2024-10-14 | +5.14% | -0.10% | -5.24pp |
| 6 | 2024-11-05 | 2024-11-09 | -1.33% | -0.10% | +1.23pp |
| 7 | 2025-02-27 | 2025-07-18 | +38.71% | -0.10% | -38.81pp |
| 8 | 2025-11-02 | 2026-08-23 | +15.54% | -0.13% | -15.68pp |

Cash beat LOW_VOL during only **2/8** defensive episodes.
LOW_VOL beat cash during **6/8**.

The two cash wins were precisely episodes where TRX itself was negative.

## Robustness windows

Frozen robustness contract:
- anchor: first fully SMA200/VOL30-ready date = 2023-11-20;
- non-overlapping complete windows only;
- state reset at each window;
- 5 complete 180-day windows;
- 8 complete 120-day windows.

Across all 13 windows:

Return comparison:
- CASH better: **3**
- LOW_VOL better: **9**
- equal: **1**

Drawdown comparison:
- CASH better: **6**
- LOW_VOL better: **2**
- equal: **5**

Important trade-off:
Cash sometimes improved drawdown, but the return penalty was much more persistent.

180-day windows:
- CASH return beat LOW_VOL: **0/5**
- LOW_VOL return beat CASH: **5/5**

## Verdict

`CASH_DEFENSE_DESTINATION_ABLATION_V1_VERDICT = LOW_VOL_DOMINANT_UNDER_FROZEN_TIMING`

Supported:
- cash can reduce drawdown in some stress windows;
- cash wins when even the selected low-vol crypto asset declines;
- a true crypto-vs-cash layer remains a plausible regime-specific tool.

Not supported:
- replacing frozen LOW_VOL with cash under the existing breadth 3/5 timing;
- assuming cash is automatically safer merely because price exposure is zero;
- promoting cash as the default defensive destination.

The key issue is timing:
the existing defensive state often lasts long enough that a resilient asset such as TRX can compound meaningfully. A flat cash destination then suffers large opportunity cost and can even retain a deeper portfolio drawdown because it never rebuilds equity during defense.

## Next research implication

Do NOT retune the destination test.

If cash is researched again, it should be a separate timing hypothesis:
- earlier systemic-stress entry and/or
- materially earlier recovery exit,
- preregistered before execution.

That future test must not be described as validation of this cash destination candidate.

Macro BROAD_RISK_OFF and official liquidity/recovery context may motivate such a timing hypothesis, but neither is used here.

## Runtime impact

Production changed: NONE  
Paper-live changed: NONE  
Migration required: NO  
Automatic execution authorized: NO  
Manual confirmation remains required.
