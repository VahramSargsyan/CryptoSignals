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
