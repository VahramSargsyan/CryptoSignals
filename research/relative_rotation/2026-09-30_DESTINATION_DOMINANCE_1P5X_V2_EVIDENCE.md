# Destination Dominance 1.50x V2 — Threshold Decision Record

Date: 2026-09-30
Mode before production change: STRESS_TEST_ONLY
Production promotion: user-approved PATCH_FIX

## Rule

A baseline route `S -> A` must already be `CONFIRMED`.

An automatic destination-dominance override to `S -> B` is eligible only when:

1. `S -> B` is `ARMED` or `CONFIRMED`;
2. `max_dislocation(S -> B) >= 1.50 × max_dislocation(S -> A)`;
3. the actual direct pair `A -> B` is `ARMED` or `CONFIRMED` toward B.

The strongest qualifying B by max_dislocation is selected.

## Why 1.50x was chosen

A threshold sweep on the frozen historical Binance D1 cache showed:

| Threshold | 1Y median | 2Y median | Mature ~3Y median |
|---:|---:|---:|---:|
| baseline | +155.75% | +1850.89% | +3167.27% |
| >1.0x | +170.78% | +1945.56% | +3359.32% |
| 1.15x | +188.71% | +2102.32% | +3588.36% |
| 1.50x | +188.71% | +2102.32% | +3588.36% |
| 1.75x | +188.71% | +2102.32% | +3588.36% |
| 2.00x | +178.81% | +2026.80% | +3461.89% |
| 3.00x | +155.75% | +1850.89% | +3167.27% |

Observed Mature-path override anatomy:
- 1.108x: PEPE -> FIL instead of PEPE -> TWT; continuation delta about -5.59 pp;
- 1.877x: AVAX -> TRX instead of AVAX -> BNB; continuation delta about +3.32 pp;
- 2.031x: FIL -> BNB instead of FIL -> XRP; continuation delta about +299.65 pp;
- 2.699x: AAVE -> PEPE instead of AAVE -> XRP; continuation delta about +17.01 pp.

The 1.50x threshold therefore:
- rejects the observed weak 1.108x negative shortcut;
- preserves the observed useful 1.877x shortcut;
- avoids the stricter 2.0x rule discarding that useful 1.877x case.

## Rolling evidence

Daily 365-day rolling windows for 1.50x:
- better than baseline: 240;
- equal: 367;
- worse: 91.

This is not uniform dominance.

## Overfit warning

The 1.50x threshold was chosen after viewing the historical threshold sweep.

`POST_SELECTED_THRESHOLD = TRUE`

Therefore:
- historical improvement is not proof of future superiority;
- do not retune 1.50x casually from forward outcomes;
- collect every real override as forward evidence;
- any threshold change requires a new version and a separate frozen stress-test.

## Execution-cost context

Real use showed that unnecessary intermediate conversions can have materially
higher all-in cost than the historical 0.1% modeled transition cost. This gives
a second economic reason to avoid weak intermediate hops, but it does not prove
that 1.50x is the universal optimal threshold.

## Production decision

User explicitly requested the 1.50x minimum on 2026-09-30 after reviewing the
threshold sweep.

Runtime identity:
`RELATIVE_ROTATION_TARGET_U10_FORWARD_V1_4_DESTINATION_DOMINANCE_1P5X`

TEST_LEVEL for the historical evidence:
`LOCAL_REPLAY_ON_FROZEN_BINANCE_D1_CACHE + THRESHOLD_SWEEP + DAILY_ROLLING_WINDOWS + EXECUTION_COST_SENSITIVITY`
