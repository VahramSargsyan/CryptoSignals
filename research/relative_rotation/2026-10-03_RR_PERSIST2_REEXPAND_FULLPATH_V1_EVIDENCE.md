# RR PERSIST2_REEXPAND FULL-PATH V1 — EVIDENCE

Date: 2026-10-03
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Runtime identity

- Research branch: `research/rr-persist2-reexpand-fullpath-v1`
- Draft PR: #117
- GitHub Actions run: `37142306053`
- Job: `111259101935`
- Artifact ID: `11280648313`
- Artifact: `rr-persist2-reexpand-fullpath-v1-2`
- Artifact SHA256: `6e31fc7dcddc871bc457d0e29ee6f418537594c5ede740a8303a90966fca0d37`
- Evaluation start: `2023-10-31T00:00:00Z`
- Evaluation end: `2026-10-02T00:00:00Z`
- Fixed as-of: `2026-10-03T16:30:00Z`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_PATH_DEPENDENT_STRESS_TEST`

## Frozen overlay

The full-path test used the already frozen V2 rule unchanged:

`PERSIST2_REEXPAND`

- corrected RR+DDG entry first;
- strongest alternative reaches >= +10% in first 1-3 closed bars;
- same candidate stays #1 on next two closes;
- second confirmation impulse > original trigger impulse;
- switch next open;
- one transition cost;
- acceleration overlay disarmed after its own switch and may re-arm only after
  the next core RR+DDG transition;
- an earlier core RR route cancels an unconfirmed acceleration setup.

No threshold was retuned.

## Primary full-period result — 0.10% transition cost

Across all 10 TARGET starting assets:

- baseline median normalized capital: **3675.57**
- overlay median normalized capital: **2742.25**
- overlay capital is about **25.4% lower** than baseline median capital
- overlay better starts: **0 / 10**
- overlay worse starts: **10 / 10**
- baseline median max DD: **-62.43%**
- overlay median max DD: **-62.43%**
- median extra transitions: **+11**
- median acceleration switches: **6**

Classification:

`FULL_PATH_WORSE`

Important: the report's raw "median return delta -989.34%" is the difference
between very large normalized total-return percentages, not a -989% loss of
capital. The economically clearer comparison is 2742.25 versus 3675.57, or
about **-25.4% final capital relative to baseline**.

## All 10 primary starts

At 0.10% transition cost every frozen TARGET start finished worse with the overlay:

- AAVE: 3883.82 -> 3153.98
- ALGO: 3227.42 -> 2620.94
- AVAX: 3817.79 -> 3100.36
- BNB: 2872.95 -> 1995.66
- FIL: 3526.19 -> 2863.56
- HBAR: 4224.65 -> 2934.60
- PEPE: 3606.80 -> 2505.42
- TRX: 3744.35 -> 2214.87
- TWT: 3590.82 -> 2124.05
- XRP: 4444.51 -> 3087.32

All paths still converged to TRX by the evaluation end, but the overlay reached
that endpoint with materially less capital.

## Secondary sunset starts

The secondary legacy/sunset starts also all worsened at 0.10%:

- ATOM: 3307.10 -> 2297.23
- SOL: 3249.85 -> 2087.77
- LINK: 3707.31 -> 2192.96

Approximate overlay capital loss versus baseline:
- ATOM: -30.5%
- SOL: -35.8%
- LINK: -40.8%

Thus the result is not driven by a single TARGET starting asset.

## Cost sensitivity

Median normalized capital across the 10 TARGET starts:

| Cost / transition | Baseline | Overlay | Overlay vs baseline |
|---:|---:|---:|---:|
| 0.10% | 3675.57 | 2742.25 | about -25.4% |
| 0.50% | 3344.75 | 2392.57 | about -28.5% |
| 1.00% | 2971.19 | 2016.10 | about -32.1% |
| 2.00% | 2340.40 | 1463.12 | about -37.5% |
| 3.00% | 1839.07 | 1059.01 | about -42.4% |

At every predeclared cost level:
- better starts = 0 / 10
- worse starts = 10 / 10

The extra switching burden becomes more damaging as execution friction rises.

## Rolling 365-day robustness

Monthly-spaced 365-day paired observations:
- **240**

Overall:
- overlay better: **32**
- equal: **70**
- worse: **138**
- better rate: **13.3%**
- median return delta: **-28.22 percentage points**
- mean return delta: **-40.53 percentage points**
- median DD delta: approximately 0
- median extra transitions: +3

### BTC_BULL_START

- paired observations: 220
- better: 32
- equal: 70
- worse: 118
- better rate: 14.5%
- median return delta: -9.45 percentage points

### BTC_BEAR_START

- paired observations: 20
- better: 0
- equal: 0
- worse: 20
- better rate: **0%**
- median return delta: **-112.22 percentage points**
- median DD delta: +2.83 percentage points

The overlay sometimes reduced drawdown in bear-start windows, but the return
penalty was much larger. It did not provide a useful bear-market tradeoff in
this frozen form.

## Current LINK -> TRX -> AAVE semantic check

The path-dependent engine reproduced the intended current case:

Baseline:
- Sep-28 signal
- Sep-29 open: LINK -> TRX

Overlay:
- Sep-28 signal
- Sep-29 open: LINK -> TRX
- Oct-01 close: frozen PERSIST2_REEXPAND confirms AAVE
- Oct-02 open: TRX -> AAVE

So the full-path engine correctly preserves the current AAVE example.

That example remains a real positive local case. The failure is in generalizing
that local edge to the whole strategy path.

## Why isolated V2 looked better but full path got worse

V2 measured each confirmed acceleration switch as an isolated candidate-vs-held
trade after the confirmation point.

The full-path test additionally captures:

1. changed future held asset;
2. changed future RR signal source;
3. interrupted / delayed canonical rotations;
4. extra transition costs;
5. additional route count;
6. compounding of earlier path mistakes.

The local V2 precision improvement therefore does **not** survive path dependence.

This is the main research conclusion.

## Decision

Frozen classification:

- `PERSIST2_REEXPAND_ISOLATED_PRECISION = IMPROVED`
- `PERSIST2_REEXPAND_FULL_PATH = REJECTED`
- `AAVE_CURRENT_CASE = VALID_LOCAL_EXCEPTION`
- `PRODUCTION_PROMOTION = NOT_AUTHORIZED`

No production/live/Telegram/exchange change.

## No-repeat rule

Do not rerun the same full-switch PERSIST2_REEXPAND overlay on the same history
with slightly changed thresholds.

Any future continuation research must be materially different, for example:

- partial allocation / capped sleeve rather than full portfolio switch;
- shadow signal only, used to delay a core transition rather than replace the held asset;
- a separately preregistered topology-confirmed continuation subset;
- materially new forward data.

Such work requires a new version and new preregistration.

## Artifact files

- `full_period_start_comparison.csv`
- `cost_sensitivity_summary.csv`
- `rolling_365d_pairs.csv`
- `rolling_365d_summary.csv`
- `current_link_case_transitions.csv`
- `current_link_case_daily.csv`
- `full_period_transition_ledger_primary_cost.csv`
- `summary.json`
- `report.md`
