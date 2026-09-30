# U10 Relative Rotation — Strategy Lab Evidence

Date: 2026-09-27
Family: RELATIVE_ROTATION_GRAPH
Candidate: U10 = canonical U8 + FIL + HBAR
Status: RESEARCH / NOT LIVE-PROMOTED

## Purpose

Consolidate the current U10 research in one Strategy Lab entry.

U10 universe:

ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

Canonical live/paper U8 remains unchanged unless a later explicit promotion decision is made.

## Technical evidence

### U10 vs U8 / exhaustive U8 distribution

- prereg: [U10 Last-Year vs U8 Universes v1](../../research/relative_rotation/2026-09-27_U10_LAST_YEAR_VS_U8_UNIVERSES_V1_PREREG.md)
- evidence: [U10 Last-Year vs U8 Universes v1 Evidence](../../research/relative_rotation/2026-09-27_U10_LAST_YEAR_VS_U8_UNIVERSES_V1_EVIDENCE.md)
- runner: `scripts/research_u10_last_year_vs_u8_universes_v1.py`
- workflow: `.github/workflows/u10-last-year-vs-u8-universes-v1.yml`
- GitHub Actions run: `36327701598`
- result: PASS
- test level: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`

### U10 transition ledger / principal-risk stress

- prereg: [U10 Ledger + Entry Stress v1](../../research/relative_rotation/2026-09-27_U10_LEDGER_ENTRY_STRESS_V1_PREREG.md)
- evidence: [U10 Ledger + Entry Stress v1 Evidence](../../research/relative_rotation/2026-09-27_U10_LEDGER_ENTRY_STRESS_V1_EVIDENCE.md)
- runner: `scripts/research_u10_ledger_entry_stress_v1.py`
- workflow: `.github/workflows/u10-ledger-entry-stress-v1.yml`
- GitHub Actions run: `36328079690`
- result: PASS
- test level: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_BACKTEST`

## Latest one-year comparison

Window:

2025-09-27 -> 2026-09-26

| Metric | Canonical U8 | Research U10 |
|---|---:|---:|
| Median return | -24.10% | +90.21% |
| ATOM-start return | -14.58% | +116.90% |
| Median max drawdown | -71.50% | -53.73% |
| Median transitions | 6.0 | 9.0 |

U10 median-return percentile-equivalent versus all 792 mandatory-ATOM/TWT/PEPE U8 universes:

97.98%

U10 ATOM-start percentile-equivalent:

100.00%

## Two-year comparison

Window:

2024-09-27 -> 2026-09-26

| Metric | Canonical U8 | Research U10 |
|---|---:|---:|
| Median return | +256.13% | +718.68% |
| ATOM-start return | +252.17% | +709.46% |
| Median max drawdown | -71.23% | -53.73% |

## Long mature U10 path

Common panel:

2023-05-05 -> 2026-09-26

First mature capital date:

2023-10-31

Normalized capital:

10,000 USDT

Final equity:

219,485.20 USDT

Return:

+2,094.85%

Transitions:

23

Modeled transition-cost drag versus the exact same zero-cost route:

2.2749%

Zero-cost terminal equity:

224,594.44 USDT

Cost-adjusted terminal equity:

219,485.20 USDT

## Long-path risk

Absolute minimum versus original 10,000:

9,570.48 USDT

Worst original-capital loss on the long path:

-4.30%

Absolute strongest peak-to-trough drawdown:

-71.04%

That drawdown was:

66,881.36 USDT on 2024-03-11
->
19,365.54 USDT on 2024-09-07

The trough was still +93.66% above the original 10,000 USDT.

Therefore:

`MAX DRAWDOWN FROM PEAK` and `LOSS VS ORIGINAL CAPITAL` must remain separate.

## Fresh-entry principal-risk stress

The absolute strongest U10 drawdown occurs too early to provide a full mature year before its peak.

For a true one-year-before test, the runner selected the strongest U10 drawdown whose peak occurs after at least one full mature year:

2026-01-13 peak -> 2026-06-06 trough

Peak-to-trough drawdown:

-53.73%

### Fresh starts

| Fresh start | Minimum equity | Worst loss vs original | Final equity | Final return |
|---|---:|---:|---:|---:|
| 2025-01-13 — one year before peak | 4,891.68 | -51.08% | 40,317.86 | +303.18% |
| 2026-01-01 — peak-year start | 6,319.77 | -36.80% | 15,318.54 | +53.19% |
| 2026-01-13 — at peak date | 4,906.34 | -50.94% | 11,892.51 | +18.93% |

The 2026-01-13 fresh start recovered above 10,000 on 2026-08-21.

## Current interpretation

U10 is historically stronger than canonical U8 on the latest one-year, two-year and long mature tests currently recorded.

At the same time, U10 is not low-risk for a fresh entrant.

A fresh 10,000 USDT entry one year before the later stress peak historically fell to about 4,892 USDT before eventually reaching about 40,318 USDT.

A fresh entry directly at the stress peak fell to about 4,906 USDT before later recovering.

Therefore the current research picture is:

- stronger historical terminal performance than U8;
- smaller latest-window drawdown than U8;
- still capable of roughly 50% principal drawdowns for badly timed fresh entries;
- FIL/HBAR materially alter the route;
- selection leakage around the choice of FIL/HBAR remains unresolved.

## 20% cash-out after first 10x capital milestone

Research files:

- [Prereg](../../research/relative_rotation/2026-09-27_U10_20PCT_CASH_REENTRY_V1_PREREG.md)
- [Evidence](../../research/relative_rotation/2026-09-27_U10_20PCT_CASH_REENTRY_V1_EVIDENCE.md)
- runner: `scripts/research_u10_20pct_cash_reentry_v1.py`
- workflow: `.github/workflows/u10-20pct-cash-reentry-v1.yml`
- GitHub Actions run: `36333838940`
- result: PASS
- test level: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_BACKTEST`

Rule tested:

- when U10 first closes at or above 10x the original 10,000 USDT capital, sell 20% into USDT on the next daily open;
- keep the remaining 80% following frozen U10;
- compare keeping cash forever versus re-entering after 6 or 12 months;
- cash and re-entry each pay 0.1% modeled transaction cost.

Observed trigger:

- first close >= 100,000 USDT: 2025-09-19;
- cash-out execution: 2025-09-20;
- pre-sale portfolio: 104,451.46 USDT;
- gross 20% sleeve: 20,890.29 USDT;
- net parked cash after cost: 20,869.40 USDT.

| Scenario | Post-cashout minimum | Post-cashout max DD | Final equity | Delta vs no-cash baseline |
|---|---:|---:|---:|---:|
| No cash-out | about 70,805.10 | -53.73% | 219,485.20 | baseline |
| Cash forever | 77,513.48 | -47.41% | 196,457.56 | -10.49% |
| Re-enter after 6 months | 77,513.48 | -51.66% | 207,891.85 | -5.28% |
| Re-enter after 12 months | 77,513.48 | -47.41% | 197,881.52 | -9.84% |

Current interpretation:

- locking 20% after the first 10x milestone materially improved the observed capital floor during the next decline;
- keeping the cash through the drawdown reduced peak-to-trough damage from about -53.73% to -47.41%;
- the protection cost some later upside;
- in this single historical path, six-month re-entry recovered more upside than twelve-month re-entry or permanent cash;
- this does **not** establish 6 months as a generally optimal waiting period.

Next robustness step should sweep:
- profit-lock threshold;
- cash percentage;
- re-entry delay;
- repeated/rolling historical trigger paths.

## Trailing profit-lock after 10x activation

Research files:

- [Prereg](../../research/relative_rotation/2026-09-27_U10_TRAILING_PROFIT_LOCK_V1_PREREG.md)
- [Evidence](../../research/relative_rotation/2026-09-27_U10_TRAILING_PROFIT_LOCK_V1_EVIDENCE.md)
- runner: `scripts/research_u10_trailing_profit_lock_v1.py`
- workflow: `.github/workflows/u10-trailing-profit-lock-v1.yml`
- GitHub Actions run: `36336178272`
- result: PASS
- test level: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_BACKTEST`

Rule:

- reaching 10x capital only activates monitoring;
- keep tracking the frozen-U10 running peak;
- after a decline of at least 2x original capital (20,000 USDT) from that peak, sell 20%, 30%, 40%, or 50%;
- lock that peak;
- sweep re-entry at -5%, -10%, -15%, -20%, -25%, -30%, -35%, -40%, and -45% from the locked peak.

Observed shared trigger path:

- 10x activation: 2025-09-19;
- running peak: 127,668.80 USDT on 2025-09-20;
- cash-out trigger: 102,424.14 USDT on 2025-09-22;
- actual giveback at trigger: 25,244.66 USDT / -19.77%;
- cash-out execution: 2025-09-23.

Re-entry timing by threshold:

| Requested drawdown | Re-entry execution |
|---:|---|
| -5% | 2025-09-25 |
| -10% | 2025-09-25 |
| -15% | 2025-09-25 |
| -20% | 2025-09-26 |
| -25% | 2025-10-11 |
| -30% | 2025-10-11 |
| -35% | 2025-10-11 |
| -40% | 2025-10-11 |
| -45% | not reached |

Because decisions use daily closes, several nominal thresholds collapse to the same real execution date.

Key observations:

- -5% to -15% re-entry was so shallow that the overlay effectively canceled itself almost immediately; terminal equity stayed essentially equal to baseline minus extra transaction costs.
- -20% re-entry produced only a small uplift.
- -25% through -40% all re-entered on 2025-10-11 and materially increased terminal equity in this single path.
- -45% never triggered, so it behaved as permanent cash protection and maximized the capital floor while sacrificing terminal upside.

Highest terminal equity observed in the 36-scenario sweep:

**264,646.11 USDT**

Scenario:

**50% cash-out + re-entry at any threshold from -25% through -40%**

This was:

**+20.58% versus the no-overlay U10 baseline terminal equity of 219,485.20 USDT.**

Highest post-cashout floor:

**86,563.41 USDT**

Scenario:

**50% cash-out + -45% re-entry threshold not reached**

Shallowest post-cashout max drawdown:

**-35.28%**

Same 50% / -45% no-re-entry scenario.

Important limitation:

This is one historical trigger path. It does **not** establish 50% cash-out or a -25% to -40% re-entry zone as generally optimal.

Required robustness before accepting any rule:

- rolling historical trigger paths;
- alternative activation multiples;
- alternative trailing giveback amounts;
- more than one market cycle where data permit;
- sensitivity to daily-close threshold clustering.

## Monthly surge -> pullback overlay stress

Research:

- prereg: `research/relative_rotation/2026-09-27_U10_MONTHLY_SURGE_PULLBACK_V1_PREREG.md`
- runner: `scripts/research_u10_monthly_surge_pullback_v1.py`
- workflow: `.github/workflows/u10-monthly-surge-pullback-v1.yml`
- evidence: [U10 Monthly Surge -> Pullback Overlay v1](2026-09-27_U10_MONTHLY_SURGE_PULLBACK_V1.md)
- GitHub Actions run: `36343997067`
- result: PASS
- test level: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`

Hypothesis:

after an unusually large monthly U10 equity surge, a substantial pullback may occur within roughly the next one to two months often enough to justify testing a partial profit-lock and lower re-entry overlay.

Canonical direct event study:

- +100% selected-month surge events: 3;
- -25% running-pullback hit within 31 days: 3/3;
- -25% hit within 62 days: 3/3;
- median worst 31d pullback: -38.03%;
- median worst 62d pullback: -40.66%.

Primary P1:

- arm after +100% monthly surge;
- wait for 5% pullback from running post-arm peak;
- sell 30%;
- re-enter at -25% from locked peak.

Canonical result:

- baseline final: 219,485.20 USDT;
- P1 final: 270,653.52 USDT;
- delta: +23.31%;
- max DD: -66.36% vs baseline -71.04%;
- 3 cash-outs / 3 re-entries;
- 66 daily closes with cash parked.

Primary P2:

- same but sell after 10% pullback;
- final: 265,751.95 USDT;
- delta: +21.08%;
- max DD: -66.36%;
- 63 daily closes with cash parked.

Exhaustive topology robustness:

- candidate pool: 15 assets;
- mandatory ATOM/TWT/PEPE;
- choose 7 of remaining 12;
- total U10 universes: 792;
- non-canonical alternatives: 791.

Across 2,227 +100%-surge events in the 791 alternatives:

- -25% running pullback within 31 days: 88.86%;
- within 62 days: 88.95%;
- median worst 31d DD: -38.03%;
- median worst 62d DD: -40.66%.

P1 across 791 alternatives:

- terminal equity improved: 761/791 = 96.21%;
- max DD improved: 715/791 = 90.39%;
- both improved: 685/791 = 86.60%;
- median terminal delta: +23.44%;
- q25/q75 terminal delta: +18.97% / +25.81%;
- median max-DD improvement: +4.68 pp.

P2 across 791 alternatives:

- terminal equity improved: 761/791 = 96.21%;
- max DD improved: 713/791 = 90.14%;
- both improved: 683/791 = 86.35%;
- median terminal delta: +20.10%;
- median max-DD improvement: +3.41 pp.

Important counterexample / regime dependence:

- July 2025 produced 120 alternative-U10 +100% surge events;
- none reached a -25% running pullback within 31 or 62 days.

Therefore:

- the effect is strong enough for continued research;
- it is not deterministic;
- 792 topologies share the same market dates and are not 792 independent OOS histories;
- no overlay is promoted to live or paper-live.

Next validation priority:

**temporal holdout / rolling-start validation of frozen P1 and P2**, not further parameter hunting.

## Surge-return completion and old-history validation

### Fixed 65-day fallback

Evidence:

- `docs/evidence/2026-09-27_U10_MONTHLY_SURGE_TIMEOUT65_V1.md`
- run: `36345356154`
- result: PASS

Finding:

- canonical U10 never needed the 65d fallback;
- among 791 alternatives, P1 timeout was invoked in 41.85%;
- when a natural -25% re-entry eventually arrived after day 65, the timeout was historically worse every time;
- when natural -25% never arrived before the dataset ended, the timeout was historically better every time;
- therefore fixed elapsed time does not separate slow corrections from genuinely invalidated pullback expectations.

### Exact old-peak reclaim fallback

Evidence:

- `docs/evidence/2026-09-27_U10_MONTHLY_SURGE_PEAK_RECLAIM_V1.md`
- run: `36345775861`
- result: PASS

Finding:

- exact reclaim of the original locked peak was too eager;
- canonical February/March 2024 reclaimed the old peak in six days, then made a higher peak and still suffered the later deep correction;
- canonical P1 final fell from 270,653.52 to 238,128.62 under old-peak reclaim;
- median alternative effect versus no-fallback was negative.

### Trailing re-entry peak

Evidence:

- `docs/evidence/2026-09-27_U10_MONTHLY_SURGE_TRAILING_REENTRY_V1.md`
- run: `36346146398`
- result: PASS

Rule:

- retain the frozen -25% re-entry depth;
- while cash is parked, move the re-entry peak upward whenever frozen-U10 reference equity makes a new high;
- re-enter after a -25% close from the latest running peak.

Canonical:

- P1 final: 252,642.73
- max DD: -68.60%
- no unfinished cash cycle
- about -6.65% terminal equity versus original P1.

Across 791 alternative U10s:

- final > baseline: 100.00%
- both final and DD improved: 90.39%
- unfinished cycles: 0%
- median terminal effect vs original P1: -6.65%.

Interpretation:

the trailing rule is a complete mechanical cycle, but completion has an opportunity cost and it is not superior to original P1 on the discovery period.

### Older temporal validation: 2020-2022

Evidence:

- prereg: `research/relative_rotation/2026-09-27_U10_OLD_HISTORY_2020_2022_V1_PREREG.md`
- evidence: `docs/evidence/2026-09-27_U10_OLD_HISTORY_2020_2022_V1.md`
- runner: `scripts/research_u10_old_history_2020_2022_v1.py`
- workflow: `.github/workflows/u10-old-history-2020-2022-v1.yml`
- run: `36346584553`
- result: PASS
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`

Old survivor universe construction:

- current survivor superset only;
- first Binance daily USDT candle on/before 2020-01-01;
- valid through 2022-12-31;
- 13 eligible assets;
- ATOM mandatory;
- exhaustive 10-token combinations;
- 220 old U10-like universes;
- common mature start: 2020-05-25.

Direct effect:

- 477 +100% surge events;
- -25% running pullback within 31d: 88.26%;
- within 62d: 99.58%;
- median worst 31d DD: -33.47%;
- median worst 62d DD: -46.17%.

This closely matches the discovery-period 31d hit rate of 88.86%.

P1_ORIGINAL on old history:

- final > baseline: 88.64%
- DD better: 60.45%
- both better: 49.09%
- median terminal delta: +12.36%
- q25/q75: +5.56% / +14.85%
- unfinished cash cycles: 11.82%.

P1_TRAILING on old history:

- final > baseline: 61.36%
- DD better: 42.27%
- both better: 33.18%
- median terminal delta: +6.16%
- unfinished: 0%.

Current conclusion:

- the +100% surge -> substantial pullback phenomenon has now replicated in a non-overlapping older period and different token topologies;
- P1_ORIGINAL has meaningful cross-period terminal-return evidence;
- drawdown improvement is less stable than terminal-return improvement;
- trailing re-entry solves mechanical completion but did not replicate strongly enough to call it a superior fallback;
- do not tune another fallback using the now-open 2020-2022 results;
- next evidence should be forward/paper rather than more retrospective fallback optimization.

## Promotion guardrail

Do not automatically replace canonical U8 with U10.

Before any promotion, at minimum add:

1. rolling monthly fresh-entry stress;
2. parameter/selection robustness around FIL/HBAR;
3. forward paper-live evidence;
4. explicit promotion decision.

## Next recommended U10 research

`U10_ROLLING_ENTRY_STRESS_V1`

Start a fresh 10,000 USDT ATOM portfolio every month across the mature history and record:

- minimum equity versus initial capital;
- max drawdown;
- days below initial;
- time to recovery;
- final return;
- route;
- transition count.

This would show whether the current fresh-entry results are typical or dependent on a few selected dates.

## Residual risks

- historical performance does not establish future performance;
- FIL/HBAR selection may contain historical selection leakage;
- real trading spread/slippage may exceed the frozen 0.1% transition-cost model;
- current U10 fresh-entry stress is not yet a full rolling-entry distribution.
