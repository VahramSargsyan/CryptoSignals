# Relative Rotation U10 P1 Cash50 Reentry25 Decision Log V1

Date: 2026-09-29
Status: ACTIVE RESEARCH MEMORY
Workflow mode: STRESS_TEST_ONLY
Production change: NONE

## Canonical evidence

`research/relative_rotation/2026-09-29_U10_SURGE_P1_CASH50_REENTRY25_V1_EVIDENCE.md`

Runtime:
- primary run: `36476916640`
- primary source SHA: `ae55a3107b65768ae9027d0fffa4a77242f9a9c6`
- artifact: `10993483988`
- artifact SHA256: `50e8545f2f7d754a6a8e08d0f0ab66f246557e5c545975fd71ae823c25031c11`
- independent canonical verification: `36477154823` PASS
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Frozen conclusion

`CASH50_DOMINATES_CASH30_ON_TESTED_HISTORY_BUT_IS_NOT_PROVEN_OPTIMAL`

Canonical U10:
- baseline: 219,485.20 USDT
- Cash30: 270,653.52 USDT
- Cash50: 308,829.05 USDT
- Cash50 vs Cash30: +38,175.53 USDT / +14.10%
- max DD: Cash30 -66.36% -> Cash50 -62.74%

791 alternative U10s:
- Cash50 > Cash30: 96.21%
- Cash50 < Cash30: 3.79%
- Cash50 DD better: 73.32%
- both final and DD better: 69.53%
- median terminal delta vs Cash30: +14.18%

Do not call 50% optimal. This exact test only compares 30% and 50%.

No live/paper promotion is authorized.
