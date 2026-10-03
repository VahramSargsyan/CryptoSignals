# RR LOOKBACK + BASELINE FAMILY V1 — EVIDENCE

Date: 2026-10-03
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Runtime identity

- Research branch: `research/rr-lookback-25-50-45-90-180-v1`
- Draft PR: #119
- GitHub Actions run: `37143852151`
- Job: `111263655301`
- Artifact ID: `11281496106`
- Artifact: `rr-lookback-baseline-family-v1-2`
- Artifact SHA256: `4f445362dbdc2bc6940c41a9044bd5bdecfd52cd632fcb7c8dc106f5be7ae66a`
- Fixed as-of: `2026-10-03T16:30:00Z`
- Latest common closed D1: `2026-10-02T00:00:00Z`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_PATH_DEPENDENT_STRESS_TEST`

## Question

Test Relative Rotation reference periods 25/45/50/90/180 in both:
- rolling MEDIAN form;
- true rolling arithmetic SMA form.

All other mechanics frozen:
- ARM = 15%;
- reversal = 3%;
- accepted DDG = 1.50x;
- next-open execution;
- 0.10% modeled transition cost;
- same TARGET/sunset semantics.

Canonical benchmark:
`MEDIAN_180`.

## MATURE results

| Variant | MATURE median return | Median DD | Median transitions | Rolling 365 beat canonical | Classification |
|---|---:|---:|---:|---:|---|
| MEDIAN_25 | -5.43% | -89.71% | 25 | 0.0% | NOT_SUPPORTED |
| MEDIAN_45 | +61.96% | -83.25% | 28 | 8.3% | NOT_SUPPORTED |
| MEDIAN_50 | +78.94% | -85.26% | 28 | 12.5% | NOT_SUPPORTED |
| MEDIAN_90 | +1368.92% | -68.34% | 24 | 29.2% | NOT_SUPPORTED |
| MEDIAN_180 | **+3506.80%** | -62.43% | 23 | canonical | CANONICAL |
| SMA_25 | +230.04% | -73.60% | 32 | 12.5% | NOT_SUPPORTED |
| SMA_45 | -54.86% | -85.26% | 22 | 0.0% | NOT_SUPPORTED |
| SMA_50 | -59.25% | -86.66% | 24 | 0.0% | NOT_SUPPORTED |
| SMA_90 | +439.44% | -80.88% | 23 | 16.7% | NOT_SUPPORTED |
| SMA_180 | +912.36% | **-58.15%** | 20 | 29.2% | NOT_SUPPORTED |

No 25/50 variant passed the preregistered robustness gate.

## Important 25 / 50 result

The prior usefulness of SMA25/SMA50 in other market-regime research does not
transfer to using 25/50 as the **RR pair reference baseline**.

Here:
- MEDIAN_25 loses money over MATURE;
- MEDIAN_50 is only +78.94% vs canonical +3506.80%;
- SMA_25 reaches +230.04%;
- SMA_50 loses -59.25%;
- all have materially worse drawdowns than canonical except SMA180's DD.

Therefore:
`RR_SHORT_REFERENCE_25_50 = NOT_SUPPORTED`.

## Why shorter references fail structurally

The likely mechanism visible in the signal audit is:

A short reference adapts rapidly toward the new pair ratio.

That reduces the measured dislocation from the reference and can prevent or
delay the 15% ARM condition exactly when one asset is strongly separating from
another.

Thus "shorter lookback" does not necessarily mean "earlier RR signal".
For an extreme-dislocation mean-reversion engine it can mean the opposite:
the baseline chases price and erases the deviation needed to arm.

This interpretation is supported by the current September case below.

## 2026-09-28 LINK case

### Canonical MEDIAN_180

On Sep-28:
- baseline LINK -> ALGO;
- DDG effective route LINK -> TRX;
- strength ratio 2.078x.

Thus canonical current mechanics produce the corrected direct route on Sep-28.

### SMA_180

Also on Sep-28:
- baseline LINK -> ALGO;
- DDG effective route LINK -> TRX;
- strength ratio 2.623x.

### All tested 25/45/50/90 short variants

For:
- MEDIAN_25 / 45 / 50 / 90;
- SMA_25 / 45 / 50 / 90;

there was **no LINK route on Sep-28**.

They did not produce LINK -> TRX until Sep-29.

So the shorter baseline was one day **later**, not earlier, in the exact live
episode that motivated the research.

## AAVE question

Across every tested variant on Sep-28 through Oct-01:

- there was **no TRX -> AAVE confirmed route**;
- on Sep-28 and Sep-30 the RR direction for AAVE itself was AAVE -> TRX.

Therefore neither 25 nor 50 solved the current "would RR identify AAVE as the
better destination?" problem.

The AAVE continuation remains a different mechanism from the frozen RR
mean-reversion route.

## Window robustness

Examples:

### MEDIAN_50
- MATURE: +78.94%
- LAST_2Y: -52.02%
- LAST_1Y: -60.27%
- rolling windows beating canonical: 12.5%

### SMA_25
- MATURE: +230.04%
- LAST_2Y: +5.28%
- LAST_1Y: -57.45%
- rolling beat rate: 12.5%

### MEDIAN_90
- MATURE: +1368.92%
- LAST_2Y: +388.39%
- LAST_1Y: +70.94%
- rolling beat rate: 29.2%

Even the strongest shorter lookback, MEDIAN_90, remains materially below
MEDIAN_180 on full-path return and rolling robustness.

## SMA versus MEDIAN at 180

SMA_180:
- MATURE +912.36%;
- LAST_2Y +735.10%;
- LAST_1Y +102.90%;
- median DD -58.15%;
- median transitions 20.

MEDIAN_180:
- MATURE +3506.80%;
- LAST_2Y +2296.64%;
- LAST_1Y +162.93%;
- median DD -62.43%;
- median transitions 23.

SMA180 slightly improves drawdown/turnover but sacrifices a very large amount of
return. It is not supported as a replacement.

## Decision

No production change.

Frozen labels:

- `MEDIAN_180 = CANONICAL_RETAINED`
- `RR_SHORT_REFERENCE_25_50 = NOT_SUPPORTED`
- `RR_LOOKBACK_45_90 = NOT_SUPPORTED_AS_REPLACEMENT`
- `RR_SMA_BASELINE_FAMILY = NOT_SUPPORTED_AS_REPLACEMENT`
- `TRX_TO_AAVE_NOT_CAUGHT_BY_SHORT_RR = TRUE`

## Research implication

The 25/50 success in SMA-based market/regime work and the 180-day success in RR
are not contradictory.

They answer different questions:

- a short SMA can react quickly to a trend/regime;
- RR requires a relatively stable anchor so that an extreme relative
  dislocation remains visible long enough to ARM and later confirm reversal.

Future work should therefore not shrink the RR anchor merely to make it
"faster". If faster information is desired, it should be introduced as a
separate confirmation/overlay layer rather than replacing the 180-day RR
reference.

## Artifact files

- `primary_path_detail.csv`
- `primary_summary.csv`
- `rolling_365_detail.csv`
- `rolling_365_summary.csv`
- `classifications.csv`
- `sep28_oct01_signal_audit.csv`
- `sep28_link_path_traces.csv`
- `variant_scorecard.csv`
- `route_stats.csv`
- `routes_median_25.csv`
- `routes_median_45.csv`
- `routes_median_50.csv`
- `routes_median_90.csv`
- `routes_median_180.csv`
- `routes_sma_25.csv`
- `routes_sma_45.csv`
- `routes_sma_50.csv`
- `routes_sma_90.csv`
- `routes_sma_180.csv`
- `summary.json`
- `report.md`
