# RR EXTREME CONTINUATION FULL-PATH V3 — EVIDENCE

Date: 2026-10-03
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Runtime identity

- Research branch: `research/rr-extreme-continuation-fullpath-v3`
- Draft PR: #118
- GitHub Actions run: `37142338585`
- Job: `111259198456`
- Artifact ID: `11280229617`
- Artifact: `rr-extreme-continuation-fullpath-v3-2`
- Artifact SHA256: `2f57e2baaaa6811729f84c858cdca53b3c007e0378a7d8def2c2235b9c3eb8f5`
- Fixed as-of: `2026-10-03T16:30:00Z`
- Latest common closed D1: `2026-10-02T00:00:00Z`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_FULL_PATH_STRESS_TEST`

## Frozen overlay tested

No V2 rule was changed.

`PERSIST2_REEXPAND`:

- after an ordinary corrected RR+DDG entry;
- strongest alternative reaches >= +10% relative impulse within first 1-3 D1 closes;
- same candidate remains #1 on next close;
- remains #1 again on the following close;
- second confirmation impulse must exceed original trigger impulse;
- hypothetical switch occurs next open.

Full-path safeguards:
- RR+DDG has priority over an unfinished overlay;
- same-close RR+DDG and overlay conflict -> RR+DDG wins;
- no acceleration-on-acceleration cascade;
- after overlay entry, wait for a subsequent ordinary RR+DDG transition before
  starting a new acceleration monitor.

## Baseline sanity check

At 0.10% cost, MATURE baseline:

- median return: **+3592.78%**
- median max DD: **-62.43%**
- median transitions: **23.5**

This is close to the previously frozen RR+DDG 1.5x MATURE historical level
(~+3588%), with the small difference expected from the later V3 cutoff.

This supports semantic continuity of the baseline simulation.

## Primary full-path result

### 0.10% per-transition cost

| Window | Baseline median | Overlay median | Delta | Baseline median DD | Overlay median DD |
|---|---:|---:|---:|---:|---:|
| LAST_1Y | +122.01% | +122.01% | +0.00 pp | -64.01% | -64.01% |
| LAST_2Y | +2332.74% | +1901.84% | **-430.90 pp** | -62.43% | -62.43% |
| MATURE | +3592.78% | +2686.67% | **-906.11 pp** | -62.43% | -62.43% |

MATURE:
- baseline median transitions: 23.5
- overlay median transitions: 34.0
- overlay transitions across 10 start paths: 56

The overlay reduced MATURE median return by about **906 percentage points**
without improving median drawdown.

Frozen classification:

`FULLPATH_REJECTED`

## All 10 MATURE starts were worse

At 0.10% cost, overlay minus baseline total-return delta:

| Start asset | Baseline return | Overlay return | Delta |
|---|---:|---:|---:|
| TRX | +3583.84% | +2079.07% | -1504.76 pp |
| TWT | +3513.37% | +2037.39% | -1475.98 pp |
| HBAR | +4213.00% | +2895.97% | -1317.03 pp |
| XRP | +4184.40% | +2876.10% | -1308.30 pp |
| PEPE | +3601.71% | +2471.35% | -1130.37 pp |
| BNB | +2795.81% | +1911.54% | -884.27 pp |
| AAVE | +3890.64% | +3140.73% | -749.91 pp |
| AVAX | +3761.59% | +3035.94% | -725.66 pp |
| FIL | +3452.91% | +2785.26% | -667.65 pp |
| ALGO | +3210.10% | +2588.08% | -622.02 pp |

There was no hidden subset of U10 starting states where the full-path overlay
improved the mature result.

## Cost stress

### MATURE

| Cost | Baseline | Overlay | Delta |
|---:|---:|---:|---:|
| 0.10% | +3592.78% | +2686.67% | -906.11 pp |
| 0.50% | +3260.54% | +2331.33% | -929.22 pp |
| 1.00% | +2885.38% | +1949.43% | -935.95 pp |
| 3.00% | +1748.26% | +977.02% | -771.23 pp |

The result is not a 0.10%-cost artifact.

Higher execution friction does not rescue the overlay.

## Rolling stability

At 0.10% cost:

### Daily rolling 365-day windows
- N = 704
- better = **118**
- equal = 170
- worse = **416**
- median delta = **-29.19 pp**
- worst delta = -225.10 pp
- best delta = +87.11 pp

### Daily rolling 730-day windows
- N = 339
- better = **0**
- equal = 0
- worse = **339**
- median delta = **-139.77 pp**
- worst delta = -593.97 pp
- best delta = -29.58 pp

The 730-day result is especially decisive:
the overlay was worse in every evaluated rolling two-year window.

## Why V2 event-study looked promising but V3 fails

V2 evaluated every causal source/date route opportunity independently.

That produced:
- 1502 base +10% events;
- 500 PERSIST2_REEXPAND passes;
- 478 complete 14d outcomes;
- 53.6% 14d win rate;
- +1.14% median local relative excess.

But a real portfolio is not simultaneously holding every possible source asset.

Along the actual 10 MATURE paths:
- only a small subset of those counterfactual route opportunities is ever
  encountered;
- only 8 unique overlay switch events appeared in the deduplicated MATURE trace;
- those switches change the held asset and therefore change every later RR path.

This is the core lesson:

`POSITIVE_LOCAL_EVENT_EDGE != POSITIVE_PATH_DEPENDENT_STRATEGY_EDGE`

The overlay can win locally yet move the strategy onto a worse subsequent
sequence of holdings.

## Unique executed overlay events in the MATURE path sample

Deduplicated by confirmation date / from asset / to asset:

| Confirm | From | To | 14d local relative excess |
|---|---|---|---:|
| 2023-11-05 | AVAX | TWT | -51.39% |
| 2023-11-12 | BNB | AVAX | +18.53% |
| 2023-11-17 | BNB | AVAX | +6.79% |
| 2023-11-24 | BNB | AAVE | -2.22% |
| 2024-01-02 | TRX | FIL | -18.15% |
| 2024-11-23 | TWT | XRP | +21.72% |
| 2024-11-28 | BNB | AAVE | +69.90% |
| 2025-03-05 | PEPE | TRX | -10.21% |

Even spectacular local wins such as BNB -> AAVE (+69.9% over 14d versus
remaining in BNB) do not imply a better final portfolio path after subsequent
RR state changes.

## BTC SMA200 diagnostic

Unique overlay switches with complete 14d data:

### BTC_BULL
- switches: 5
- win rate: 40.0%
- median local excess: -2.22%
- mean: +12.21%
- >=20% continuation: 40.0%

### UNKNOWN
- switches: 3
- win rate: 66.7%
- median: +6.79%
- sample predates availability of a causal BTC SMA200 within this dataset.

No BTC_BEAR overlay switch was encountered in the deduplicated MATURE path.

The sample is too small for a regime rule.

## LAST_1Y nuance

For the 10 standard U10 start paths:
- overlay switches in LAST_1Y = **0**
- baseline and overlay paths are therefore identical.

This does **not** contradict the current AAVE diagnostic.

The user's live BOOK_2 entered TRX recently from legacy LINK. LINK is sunset /
exit-only and is not one of the 10 standard U10 aggregate start assets.

The separate LINK diagnostic correctly tests that live-like path.

## LINK -> TRX -> AAVE diagnostic

Start:
- 2026-09-28 with LINK held

Frozen corrected path:

1. 2026-09-28 signal:
   `LINK -> TRX`
2. 2026-09-29 execution:
   enter TRX
3. 2026-09-29:
   AAVE base acceleration trigger
4. 2026-10-01:
   PERSIST2_REEXPAND confirmation
5. 2026-10-02:
   overlay execution
   `TRX -> AAVE`

No RR+DDG signal preempted the overlay.

Through the latest closed D1:
- final asset = AAVE
- total transitions = 2
- RR transitions = 1
- overlay transitions = 1
- short diagnostic path return = **+4.32%**

Thus the current AAVE episode remains a valid positive forward-style example,
while the historical full-path rule is rejected.

## Decision

No production change.

Frozen labels:

- `PERSIST2_REEXPAND_LOCAL_EVENT_FILTER = PRECISION_IMPROVEMENT`
- `PERSIST2_REEXPAND_FULL_PATH_OVERLAY = REJECTED`
- `AAVE_2026_10_02_OVERLAY_EXECUTION_COUNTERFACTUAL = POSITIVE_SO_FAR`
- `PRODUCTION_PROMOTION = NOT_AUTHORIZED`

## Practical implication

Do not add a generic automatic TRX -> acceleration-candidate switch to the
current RR+DDG engine.

The AAVE episode should remain a **forward shadow observation** rather than a
new production rule.

If research continues, it must explain why the rare current AAVE case should
be separated from the historically harmful full-path overlay without tuning a
rule around this single observed example.

## No-repeat rule

Do not rerun the same V2 classifier as a full-path overlay with the same
precedence/path semantics merely to search for a favorable threshold.

A new experiment requires:
- new unseen forward evidence;
- a materially different state architecture;
- a documented implementation correction;
- or a separately preregistered causal classifier not tuned to the AAVE case.

## Artifact files

- `window_variant_summary.csv`
- `window_start_asset_results.csv`
- `baseline_vs_overlay_comparison.csv`
- `rolling_detail.csv`
- `rolling_summary.csv`
- `overlay_regime_events.csv`
- `overlay_regime_summary.csv`
- `mature_overlay_transition_trace.csv`
- `link_aave_diagnostic_trace.csv`
- `link_aave_diagnostic.json`
- `summary.json`
- `report.md`
