# U10 Monthly Surge Fractal Buy-Stop Re-entry v1 — Evidence

Date: 2026-09-28
Mode: STRESS_TEST_ONLY
Validated branch: research/u10-monthly-surge-fractal-buy-stop-v1

Primary GitHub Actions run:
36375093832

Primary source commit:
371fbf4cb872426fbb4b4059f0c3b087803d5dbf

Primary artifact:
10950478533

Artifact digest:
sha256:3811945e829916ee14c8d22191b2722a8e1ce6798fddedaba1a27339ab3646be

Independent canonical verification run:
36375212278

Canonical verification result:
PASS

Primary result:
PASS

TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST

## Hypothesis

Replace a fixed -25% re-entry target with a causal market-structure buy-stop.

Frozen P1/P2 cash-out:

P1:
- +100% monthly surge arm
- sell 30% after >=5% pullback from post-arm running peak

P2:
- same but sell after >=10% pullback

Fractal re-entry:

- after cash-out, wait until U10 reference equity is at least -15% below the latest post-surge reference running peak;
- then search the currently held U10 asset for a confirmed 5-bar swing high;
- pivot high uses two bars on the left and two on the right;
- the pivot becomes known only after both right-side bars close;
- buy-stop becomes executable only from the following session;
- a new lower confirmed swing high ratchets the stop downward;
- a higher pivot does not raise the existing lower stop;
- U10 asset rotation cancels the old asset stop and resets the structure for the new current asset;
- if session open gaps above the stop, fill at open;
- otherwise if daily high touches the stop, fill at the stop;
- re-invest the entire parked cash sleeve;
- 0.1% re-entry cost.

No fixed -25% re-entry.
No timeout.
No moving average.
No additional tuning.

## Canonical U10 result

Baseline U10:

- final equity: 219,485.20 USDT
- max DD: -71.04%

### P1-FRACTAL

- final equity: 258,557.42 USDT
- delta vs baseline: +39,072.22 / +17.80%
- max DD: -68.36%
- max-DD improvement: +2.69 pp
- cash-outs: 3
- fractal re-entries: 3
- unfinished cycles: 0
- daily closes with cash: 145
- confirmed pivots considered: 16
- lower-stop replacements: 11
- gap-open fills: 0
- stop-price fills: 3

Controls:

- fixed old-peak -25% final: 270,653.52
- moving-reference-peak -25% final: 252,642.73
- fractal final: 258,557.42

Thus on canonical U10:

- fractal was -4.47% below the high-return fixed -25% control;
- fractal was +2.34% above the moving-peak -25% control;
- fractal completed all cycles without a timeout.

### P2-FRACTAL

- final equity: 253,741.70 USDT
- delta vs baseline: +34,256.50 / +15.61%
- max DD: -68.36%
- cash-outs: 3
- re-entries: 3
- unfinished cycles: 0
- cash days: 142
- confirmed pivots: 16
- stop replacements: 11
- gap-open fills: 0
- stop-price fills: 3

Controls:

- fixed -25%: 265,751.95
- moving-peak -25%: 248,067.33
- fractal: 253,741.70

P1 remained stronger than P2 on this canonical path.

## Canonical P1 cycles

### 2024-02 surge

Cash-out:
- signal: 2024-02-29
- execution: 2024-03-01
- asset: ATOM
- initial locked reference peak: 56,284.29

Reference later made a higher peak:
66,881.36

Fractal search:
- activated: 2024-03-16
- activation reference drawdown: -18.70%

Structure:
- 6 confirmed pivots considered
- 4 lower-stop replacements
- 2 U10 asset resets while cash was parked

Re-entry:
- execution: 2024-05-05
- asset: ATOM
- pivot center: 2024-05-02
- pivot confirmation: 2024-05-04
- buy-stop: 9.10 USDT
- fill: 9.10 USDT
- fill type: STOP_PRICE
- reference drawdown at re-entry close: -26.37%
- cash duration: 65 days

### 2025-11 surge

Cash-out:
- execution: 2025-11-09
- asset: PEPE
- locked reference peak: 175,691.95

Search:
- activated: 2025-11-11
- reference drawdown: -16.63%

Structure:
- 3 confirmed pivots
- 2 lower-stop replacements
- no U10 asset reset

Re-entry:
- execution: 2025-12-04
- asset: PEPE
- pivot center: 2025-11-28
- confirmation: 2025-11-30
- fill type: STOP_PRICE
- reference drawdown at re-entry close: -31.79%
- cash duration: 25 days

### 2026-01 surge

P1 cash-out:
2026-01-16

P2 cash-out:
2026-01-19

Search:
2026-01-20

Reference search drawdown:
-16.18%

Structure:
- 7 confirmed pivots
- 5 lower-stop replacements
- 1 U10 asset reset

Re-entry:
- execution: 2026-03-12
- asset: TWT
- pivot center: 2026-03-09
- confirmation: 2026-03-11
- stop/fill: 0.4999 USDT
- reference drawdown at re-entry close: -29.61%

Cash duration:
- P1: 55 days
- P2: 52 days

## 791 alternative U10 topology stress

Every alternative U10 used:
- the same fixed 15-asset candidate pool;
- mandatory ATOM/TWT/PEPE;
- 7 additional assets selected exhaustively from the remaining 12;
- same frozen U10 mechanics;
- same fractal mechanics.

### P1-FRACTAL

Across 791 non-canonical U10s:

- final equity above ordinary baseline: 100.00%
- max DD improved: 90.14%
- BOTH terminal equity and DD improved: 90.14%
- unfinished cash cycles: 0.00%

Terminal delta vs baseline:

- minimum: +8.27%
- q25: +17.55%
- median: +21.39%
- q75: +25.39%
- maximum: +30.13%

Compared with fixed old-peak -25%:
- fractal better: 260 / 791 = 32.87%
- fractal worse: 531 / 791
- median effect: -0.33%

Compared with moving-reference-peak -25%:
- fractal better: 668 / 791 = 84.45%
- fractal worse: 123 / 791
- median effect: +3.60%

Median cash days:
122

Median fractal re-entries:
3

### P2-FRACTAL

Across 791 alternatives:

- final above baseline: 100.00%
- max DD improved: 79.01%
- BOTH improved: 79.01%
- unfinished cycles: 0.00%

Terminal delta vs baseline:

- minimum: +8.35%
- q25: +15.61%
- median: +17.54%
- q75: +22.94%
- maximum: +23.97%

Compared with fixed -25%:
- fractal better: 296 / 791 = 37.42%
- median effect: -0.50%

Compared with moving-reference-peak -25%:
- fractal better: 619 / 791 = 78.26%
- median effect: +3.30%

Median cash days:
117

Again P1 showed the stronger topology robustness.

## Actual re-entry depth

The key objective was to stop assuming that the correct re-entry depth must equal -25%.

Across the 2,227 alternative-U10 fractal cycles:

Reference drawdown from the latest reference peak at actual re-entry close:

- shallowest: about -8.81%
- 10th percentile: about -13.20%
- 25th percentile: about -19.45%
- median: about -29.61%
- 75th percentile: about -39.29%
- deepest: about -40.43%

This confirms the user's core hypothesis:

A structural reversal entry naturally produces re-entry depths both shallower and deeper than a fixed -25% level.

The fixed value is not the observed structural outcome.

## Cash-cycle duration

P1 alternative cycles:

- minimum: 14 days
- q25: 25 days
- median: 47 days
- q75: 55 days
- maximum: 74 days

P2:

- minimum: 6 days
- q25: 25 days
- median: 27 days
- q75: 52 days
- maximum: 73 days

No cycle required an arbitrary timeout.

## Asset rotation interaction

Fractal re-entry frequently had to survive U10 asset changes.

P1:
- about 65.1% of alternative cash cycles had at least one U10 asset reset while cash was parked.

P2:
- about 54.4%.

The stop reset rule therefore materially matters.

Most frequent re-entry assets across alternative cycles included:
- TWT
- PEPE
- ATOM
- ALGO
- HBAR
- AVAX

This confirms that re-entry cannot safely be modeled as a static order on the asset held at cash-out.

## Fill integrity

All 2,227 P1 and all 2,227 P2 alternative re-entry cycles filled at the stop price rather than through an overnight gap-open in this historical panel.

This does not mean future gaps cannot occur; the runner still preserves gap-open execution semantics.

## Main result

The fractal buy-stop rule is materially more coherent than a fixed time fallback or old-peak reclaim.

It:

- uses observable price structure;
- introduces no new re-entry percentage target;
- adapts actual entry depth;
- completed every cash cycle across all 791 alternative U10s;
- beat ordinary U10 terminal equity in 100% of alternatives;
- for P1 improved both terminal equity and max DD in about 90%;
- beat the previously tested moving-peak -25% rule in about 84% of alternatives.

However:

- the original frozen old-peak -25% control still had slightly higher median terminal equity;
- fractal beat that fixed control in only about one-third of alternatives.

Therefore fractal is not a historical dominance result over every fixed-price control.

Its strongest evidence is **structural robustness and adaptive completion**, not maximum hindsight return.

## Interpretation

P1-FRACTAL is the more promising of the two variants on 2023-2026.

The result directly supports the user's proposed idea that the re-entry level should emerge from a local reversal structure rather than a universal -25% number.

The next research question should not be another percentage sweep.

A cleaner next validation would examine:
- the already-consumed 2020-2022 OLD10 history only as secondary evidence;
- future paper events;
- possibly a stricter distinction between fractal confirmation and actual order trigger behavior.

Do not tune this rule against 2020-2022 and then call that period out-of-sample.

## Live guardrail

No live/paper behavior changed.

No promotion authorized.

TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST

## Residual risks

- same market dates across all 792 U10 topologies;
- 15% search activation was user/development selected;
- 5-bar pivot is one structural definition among many;
- daily OHLC cannot determine intraday ordering when multiple levels are touched on the same candle beyond the explicit stop-fill rules;
- current candidate pool has selection leakage;
- transaction cost model remains simplified;
- historical performance does not establish future performance.
