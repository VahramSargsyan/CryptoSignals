# RR MEDIAN CASCADE 25/50/90/180 V1 — EVIDENCE

Date: 2026-10-03
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Runtime identity

- Research branch: `research/rr-median-cascade-25-50-90-180-v1`
- Draft PR: #120
- GitHub Actions run: `37144512916`
- Job: `111265582141`
- Artifact ID: `11281986761`
- Artifact: `rr-median-cascade-25-50-90-180-v1-2`
- Artifact SHA256: `4905ed758dfbdc5cbd9f14eb75503995a24e43ea66c1d53583659e079f65121c`
- Fixed as-of: `2026-10-03T16:30:00Z`
- Latest common closed D1: `2026-10-02T00:00:00Z`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_STRESS_TEST`

## Mechanism

Canonical RR remains unchanged at MEDIAN_180.

The new diagnostic layer uses the ordered ratio:

`candidate C / baseline B`

and tracks:

- M25
- M50
- M90
- M180

Frozen upward sequence:

- S1_FAST_CROSS: M25 crosses above M50;
- S2_MID_STACK: M25 > M50 > M90;
- S3_FULL_STACK: M25 > M50 > M90 > M180.

## Formal preregistered classification

`CASCADE_STRONG_RESEARCH_SIGNAL`

This classification follows the frozen V1 gate, but must be interpreted
carefully because the absolute edge is small and calendar-year behavior is not
stable.

## Pair-event forward results

### S1_FAST_CROSS

14d:
- N = 1197
- candidate beats baseline = 50.4%
- median relative excess = +0.05%
- mean = +2.42%
- >=20% continuation = 10.6%

30d:
- win rate = 49.2%
- median = -0.44%

Interpretation:
S1 alone is essentially neutral.

### S2_MID_STACK — primary

14d:
- N = 817
- candidate beats baseline = **50.7%**
- median relative excess = **+0.24%**
- mean = +1.68%
- >=20% continuation = **11.0%**

This narrowly clears the preregistered primary gate.

30d:
- win rate = 48.5%
- median = -0.67%

Interpretation:
S2 has a small short/intermediate diagnostic edge, not a strong standalone
trading edge.

### S3_FULL_STACK

14d:
- N = 535
- win rate = 50.3%
- median = +0.19%
- mean = +0.90%
- >=20% continuation = 9.5%

30d:
- win rate = 45.7%
- median = -1.96%

Interpretation:
waiting for the full stack does not improve generic forward performance and is
often too late.

## Cascade timing

- median S1 -> S2: **11 days**
- median S2 -> S3: **1 day**
- median S1 -> S3: **14 days**
- S1 reaching S2 within 30d: **64.1%**
- S1 reaching S3 within 60d: **41.1%**

This confirms that full S3 is commonly a substantially lagged state.

## Calendar-year stability

S2 14d:

- 2023: N36, win 61.1%, median +0.96%
- 2024: N295, win 49.2%, median -0.55%
- 2025: N264, win 57.6%, median +2.48%
- 2026: N222, win 42.8%, median -1.33%

The frozen strong gate passes because at least two years independently show
positive median and >50% win rate.

However, the sign flips in 2024 and 2026.

Therefore:

`PAIR_CASCADE_EDGE = REGIME_UNSTABLE`

The formal classification should not be interpreted as production readiness.

## RR post-entry overlay result

When a fresh cascade event is used after an RR+DDG entry to select an
alternative candidate, the practical result is poor.

### S2 after RR entry

14d:
- N = 2648
- candidate beats RR destination = **43.9%**
- median relative excess = **-0.67%**
- mean = -0.65%

### S3 after RR entry

14d:
- N = 2837
- candidate beats RR destination = **39.5%**
- median relative excess = **-3.19%**
- mean = -1.22%

Therefore:

`POST_ENTRY_CASCADE_SWITCH = NOT_SUPPORTED`

This is consistent with prior full-path research: chasing a newly detected
trend after entry can damage the RR path.

## AAVE / TRX — exact current sequence

For ratio:

`AAVE / TRX`

the latest upward cascade is:

- **2026-08-31: S1_FAST_CROSS**
- **2026-09-01: S2_MID_STACK**
- **2026-09-12: S3_FULL_STACK**

Thus AAVE reached the full median stack **16 days before** the corrected
LINK -> TRX RR signal on Sep-28.

On Sep-28:

- AAVE/TRX ratio = 445.381406
- M25 = 401.488182
- M50 = 377.287299
- M90 = 296.811503
- M180 = 282.048975
- ordering = `M25 > M50 > M90 > M180`
- state = `S3_FULL_STACK`
- MOM10 = +7.99%

As of Oct-02:

- AAVE/TRX ratio = 536.570063
- M25 = 412.415016
- M50 = 381.921120
- M90 = 304.458084
- M180 = 282.048975
- state remains `S3_FULL_STACK`.

## Critical Sep-28 cross-sectional observation

At the Sep-28 LINK -> TRX decision, the pre-entry cascade audit ranked:

- best alternative candidate: **AAVE**
- AAVE state: **S3_FULL_STACK**

Every other tested TARGET alternative versus TRX was only S2 at that date:

- HBAR: S2
- ALGO: S2
- AVAX: S2
- FIL: S2
- PEPE: S2
- TWT: S2
- XRP: S2
- BNB: S2

Thus AAVE was structurally unique **before** the corrected RR execution.

This is the most important new finding of V1.

## Interpretation

The useful mechanism is probably NOT:

`enter RR destination -> wait for a new cascade -> chase the candidate`.

That version fails.

The materially new hypothesis suggested by V1 is:

> At the moment RR is about to enter destination B, inspect whether another
> candidate C already has a mature median-cascade structure versus B.

The current AAVE case would have been flagged because AAVE was already the only
S3_FULL_STACK alternative to TRX on Sep-28.

This is different from the rejected post-entry acceleration/cascade rules.

## Decision

No production change.

Frozen labels:

- `MEDIAN_180_RR_CORE = UNCHANGED`
- `S1_STANDALONE = WEAK_NEUTRAL`
- `S2_PAIR_DIAGNOSTIC = SMALL_EDGE_REGIME_UNSTABLE`
- `S3_GENERIC_FORWARD_EDGE = NOT_IMPROVED`
- `POST_ENTRY_CASCADE_SWITCH = NOT_SUPPORTED`
- `AAVE_TRX_PRE_ENTRY_S3_ON_2026_09_28 = TRUE`
- `AAVE_UNIQUE_FULL_STACK_VS_TRX_ON_2026_09_28 = TRUE`
- `PRE_ENTRY_CASCADE_GATE = OPEN_RESEARCH_CANDIDATE`

## Next admissible research

A separate preregistered V2 should test the **PRE-ENTRY CASCADE GATE**:

At every canonical RR+DDG signal SOURCE -> B:
1. before next-open execution, inspect every alternative TARGET C versus B;
2. rank by cascade state S3 > S2 > S1 > NONE;
3. test whether a unique S3 candidate should:
   - replace B;
   - delay B;
   - or merely veto/flag the route;
4. compare full path-dependent outcomes with CORE RR+DDG;
5. preserve current AAVE case as one observation, not a tuning target.

This V2 must be frozen before evaluating the historical outcome.

## Residual risks

- pair-event observations are highly correlated;
- the formal strong gate is permissive relative to the small economic edge;
- 2024 and 2026 S2 results are negative;
- pre-entry unique-S3 usefulness has not yet been backtested as a trading rule;
- no real execution-cost/path-dependent promotion test was run for the
  pre-entry gate.

## Artifact files

- `all_pair_daily_cascade_states.csv`
- `upward_cascade_events.csv`
- `downward_cascade_events.csv`
- `stage_forward_summary.csv`
- `cascade_timing_detail.csv`
- `s2_calendar_year_summary.csv`
- `rr_pre_entry_cascade_audit.csv`
- `rr_overlay_cascade_events.csv`
- `rr_overlay_summary.csv`
- `aave_trx_daily_trace.csv`
- `audit_dates_all_pair_states.csv`
- `recent_month_upward_events.csv`
- `canonical_rr_ddg_routes.csv`
- `summary.json`
- `report.md`
