# ALGO Deep Audit v1 — Consolidated Evidence

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Branch: research/algo-deep-audit-v1

Runs:
- ALGO Deep Audit v1: 36337534018 — PASS
- ALGO Feeder Interaction v1: 36337923910 — PASS
- ALGO Endpoint Sensitivity v1: 36338123976 — PASS

TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST

## Executive finding

ALGO's appearance in 10/10 latest-year top U8 and 10/10 top U10 is NOT explained by harvesting ALGO's historical bull run.

However, ALGO is also NOT a stable universal upgrade.

Its usefulness is highly regime/end-date dependent. It is best classified as a context-dependent routing node that became unusually attractive near the September 2026 endpoint.

Current recommendation:
- do not add ALGO to U9_CLEANER;
- keep ALGO on the research watchlist;
- require forward/endpoint-stable evidence before promotion.

Live universe remains unchanged.

---

## 1. ALGO price history versus strategy performance

### Full Binance history — context only

Strongest 90d low-to-high drawup:
- +568.66%
- 2020-11-15 -> 2021-02-12

Strongest 180d low-to-high drawup:
- +635.04%
- 2020-11-03 -> 2021-02-12

These episodes predate the strategy dataset and therefore cannot inflate the 2023-2026 backtests.

### Strategy-common history

Strongest 90d / 180d drawup:
- +365.36%
- 2024-11-04 -> 2024-12-07

ALGO HODL:
- common history: -33.18%
- mature 2023-10-31 -> 2026-09-26: +5.44%
- latest 2Y: -17.69%
- post-bull 2024-12-08 -> 2026-09-26: -76.79%
- latest 1Y: -43.31%

This is important:
ALGO was a poor buy-and-hold asset after its 2024 bull run, while ALGO-containing graph combinations could still rank highly.

Therefore ALGO's graph effect is not equivalent to ALGO price appreciation.

---

## 2. Direct bull-run P&L attribution

For the latest-year hindsight top-1 U8:

TWT, PEPE, SOL, AAVE, AVAX, FIL, ETH, ALGO

Mature raw median:
+987.28%

After neutralizing every positive strategy-equity day while:
- held asset = ALGO
- date = 2024-11-04 -> 2024-12-07

Median:
+987.28%

No change.

For latest-year hindsight top-1 U10:

TWT, PEPE, SOL, AAVE, LINK, AVAX, FIL, ETH, ALGO, XRP

Mature raw median:
+391.33%

ALGO-bull-neutralized:
+391.33%

Again no change.

Conclusion:
the specific historical ALGO bull run contributed no measurable positive held-route P&L to these top graph results.

ALGO is not a bull-run-harvesting confound in the same way PEPE was.

---

## 3. Latest-year matched marginal value

The latest closed year was:
2025-09-27 -> 2026-09-26

### U8 contexts

ALGO was tested in every possible seven-asset base context:
3,432 contexts.

Against every alternative possible eighth token:

- pairwise win rate: 66.12%
- pairwise loss rate: 32.74%
- ties: 1.14%
- median return advantage vs median alternative: +5.20 percentage points
- mean advantage: +11.82 pp
- ALGO was best extension in 15.79% of contexts
- top-3 extension in 58.36%
- median-or-better in 77.97%
- worst extension in only 1.14%

### U10 contexts

2,002 nine-asset base contexts.

- pairwise win rate: 72.50%
- loss rate: 27.05%
- median advantage vs median alternative: +8.93 pp
- mean advantage: +15.46 pp
- best extension: 27.97%
- top-3 / median-or-better: 83.62%
- worst extension: 1.10%

On the exact 2025-09-27 -> 2026-09-26 endpoint ALGO therefore looked broadly useful, not merely lucky in one top combination.

---

## 4. Companion dependence

ALGO's latest-year marginal value was positive across many companion structures.

Strongest U8 median marginal uplift when the seven-asset base contained:

- FIL: +12.18 pp
- AAVE: +8.10 pp
- AVAX: +7.29 pp
- PEPE: +6.58 pp
- XRP: +6.23 pp
- SOL: +6.18 pp
- ETH: +5.96 pp

Strongest U10 companion contexts:

- FIL: +13.24 pp
- AAVE: +11.51 pp
- PEPE: +10.26 pp
- SOL: +10.15 pp
- AVAX: +10.03 pp
- XRP: +9.90 pp
- ETH: +9.52 pp

FIL is the strongest repeated companion.

ALGO's median marginal delta remained positive even in contexts with few historically explosive peer assets, so the result is not explained solely by clustering ALGO with other past bull-run tokens.

---

## 5. What ALGO actually does in hindsight top sets

### Top-10 U8 aggregation

Across 80 routes:

- ALGO occupancy: only 3.55% of route-days
- entries: 82
- exits: 44
- routes ending in ALGO: 48 / 80 = 60%
- median ALGO holding streak: 5 days
- mean streak: 11.27 days
- maximum streak: 23 days

Incoming transitions:
- AVAX -> ALGO: 57
- SOL -> ALGO: 9
- ETH -> ALGO: 8
- XRP -> ALGO: 8

Outgoing:
- ALGO -> FIL: 44

The dominant corridor is:

AVAX -> ALGO -> FIL

But 60% of top-U8 routes end in ALGO, which creates a meaningful end-of-sample concern.

### Top-10 U10 aggregation

Across 100 routes:

- occupancy: 2.39%
- entries: 54
- exits: 54
- routes ending in ALGO: 10%
- median holding streak: 18 days
- max: 23 days

Incoming:
- AVAX -> ALGO: 30
- ETH -> ALGO: 9
- SOL -> ALGO: 8
- XRP -> ALGO: 7

Outgoing:
- ALGO -> FIL: 44
- ALGO -> TRX: 10

In U10 ALGO looks more like a balanced bridge than a terminal sink.

---

## 6. ALGO in the current U9_CLEANER

Current cleaner research graph:

ATOM, TWT, PEPE, BNB, TRX, AAVE, LINK, FIL, HBAR

Adding ALGO gives:

ATOM, TWT, PEPE, BNB, TRX, AAVE, LINK, FIL, HBAR, ALGO

Using the mandatory ATOM/TWT/PEPE starters:

### Post-ALGO-bull

U9_CLEANER:
+254.42%

U9_CLEANER + ALGO:
+254.42%

### Latest 1Y

U9_CLEANER:
+109.95%

+ ALGO:
+109.95%

### Latest 2Y

U9_CLEANER:
+822.08%

+ ALGO:
+822.08%

ATOM latest 1Y:
+116.90% in both.

Rolling post-bull results are also identical.

Nine rolling 12m windows:
- median: +128.21%
- worst median window: +27.36%
- 9/9 positive

Three rolling 18m windows:
- median: +255.81%
- worst median: +200.25%
- 3/3 positive

ALGO therefore adds no measurable value to the current cleaner graph on the relevant post-bull horizons.

---

## 7. Replacement tests inside U9_CLEANER

Replacing a current U9 node with ALGO:

### Replace AAVE

Latest 1Y:
+52.05%

vs baseline:
+109.95%

Bad.

### Replace FIL

Latest 1Y:
+12.71%

Bad.

### Replace HBAR

Latest 1Y:
+59.85%

Bad.

### Replace BNB

Latest 1Y remains +109.95%, but:
- post-bull falls from +254.42% to +238.84%
- latest 2Y falls from +822.08% to +750.18%

No improvement.

### Replace TRX

Post-bull / latest 1Y / latest 2Y are unchanged, but mature performance is materially lower.

No evidence for promotion.

### Replace LINK

Post-bull / latest 1Y / latest 2Y are unchanged, while mature performance is lower.

No evidence for promotion.

Conclusion:
ALGO does not improve the practical cleaner baseline by addition or one-for-one replacement.

---

## 8. Feeder interaction test

Because top-set topology showed ALGO entries mainly from:
AVAX, ETH, SOL and XRP,

the following fixed interactions were tested:

BASE + feeder
versus
BASE + feeder + ALGO

Feeders:
AVAX, ETH, SOL, XRP.

For the mandatory ATOM/TWT/PEPE starts, ALGO incremental return was exactly 0.00 pp for:
- AVAX
- ETH
- SOL
- XRP

on:
- post-bull period
- latest 2Y
- latest 1Y

Latest-year ALGO occupancy:
0%

Entries:
0

Exits:
0

for all four feeder tests.

Therefore simply adding a known feeder to U9_CLEANER is not sufficient.

The current graph contains stronger competing routes that prevent the feeder -> ALGO corridor from being selected.

ALGO's usefulness is therefore highly topology-dependent.

---

## 9. Endpoint sensitivity — critical result

Because 60% of top-U8 routes ended in ALGO, the full 6,435-U8 exhaustive search was rerun on five trailing 365-day windows.

### Window ending 2026-05-31

ALGO HODL:
-34.16%

ALGO frequency:
- top 10: 8/10
- top 50: 22/50
- top 100: 55/100

Matched pairwise win rate:
69.20%

Median marginal delta:
+7.41 pp

### Window ending 2026-06-30

ALGO HODL:
-55.26%

Frequency:
- top 10: 4/10
- top 50: 25/50
- top 100: 57/100

Pairwise win:
47.60%

Median marginal delta:
0.00 pp

### Window ending 2026-07-31

ALGO HODL:
-67.66%

Frequency:
- top 10: 0/10
- top 50: 7/50
- top 100: 28/100

Pairwise win:
17.74%

Median marginal delta:
-1.12 pp

### Window ending 2026-08-31

ALGO HODL:
-63.02%

Frequency:
- top 10: 6/10
- top 50: 23/50
- top 100: 45/100

Pairwise win:
32.61%

Median marginal delta:
-0.07 pp

### Window ending 2026-09-26

ALGO HODL:
-43.31%

Frequency:
- top 10: 10/10
- top 50: 50/50
- top 100: 98/100

Pairwise win:
66.12%

Median marginal delta:
+5.20 pp

This is the decisive limitation.

ALGO shifts from:
- 0/10 top U8 and 17.7% pairwise win in the July endpoint
to
- 10/10 top U8 and 66.1% pairwise win by September 26.

Therefore ALGO's latest top ranking is highly regime/end-date sensitive.

It is not a stable universal winner.

---

## 10. Final classification

### Bull-run confound?

NO, not materially.

Evidence:
- ALGO's 2024 bull interval contributes zero measured positive held-route P&L to top-1 U8/U10;
- ALGO fell -76.8% after that bull run;
- ALGO still often improved relative-rotation graphs.

### Structural value?

YES, in specific graph regimes.

Observed structural role:
AVAX / SOL / ETH / XRP -> ALGO -> FIL
and sometimes
ALGO -> TRX.

### Universal candidate for U9_CLEANER?

NO.

ALGO is inert in U9_CLEANER and remains inert even when the identified feeder tokens are added one at a time.

### Endpoint stable?

NO.

Its ranking varies dramatically across nearby one-year endpoints.

## Research status

ALGO = WATCHLIST_CONTEXTUAL_BRIDGE

Not:
CORE_NODE

Not:
LIVE_CANDIDATE

Not:
BULL_RUN_ARTIFACT

## Recommended future gate

Do not add ALGO now.

Keep it as a monitored candidate and require one of:

1. forward evidence after 2026-09-27 showing ALGO becomes active in the cleaner graph;
2. a causal admission rule that detects the relevant graph state before ALGO becomes attractive;
3. repeated rolling-window evidence that its marginal contribution stays positive across multiple non-overlapping regimes.

No live/paper-live files changed.
