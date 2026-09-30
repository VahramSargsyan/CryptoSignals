# U8 Transition Ledger v1 — Evidence

Date: 2026-09-27
Mode: PATCH_FIX / research reporting only
Branch: research/u8-transition-ledger-v1
GitHub Actions run: 36322314111
Source commit: b96ee6fa812025b38374bc52934e37d38a96646b
Result: PASS
TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_BACKTEST

## Scope

This patch adds exact per-transition accounting to the frozen canonical U8 backtest.

No live strategy logic, universe, router, signal parameter, or held-asset configuration was changed.

Frozen U8:
ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK

Frozen mechanics:
- Binance Spot 1D closed candles
- rolling median 180d
- ARM 15%
- reversal 3%
- strongest confirmed max-dislocation router
- next-open execution
- 0.1% cost on every executed transition

## Data boundary

Common panel: 2023-05-05 -> 2026-09-26
Mature evaluation after 180-row warm-up: 2023-10-31 -> 2026-09-26
Mature span: 1062 days / 34.89 months

A full mature 36-month span is not available and was not fabricated.

## Normalized accounting

Initial capital at first open: 10,000.00 USDT
Initial ATOM open: 8.10200000 USDT
Initial position: 1,234.2631449 ATOM

Executed transitions: 21

Final position: 51,147.2493732 ATOM
Final equity after transition costs: 95,491.91 USDT
Return from initial 10,000 USDT at first open: +854.92%

Legacy research-runner baseline:
first-day close equity = 9,786.47 USDT
legacy-compatible return = final / first-close equity - 1 = +875.75%

This exactly reconciles the older U8 evidence. The older runner did apply transition costs; the apparent discrepancy came from its return denominator convention.

## Risk clarification: drawdown is not loss of initial capital

The previously reported `-71.23%` is a **peak-to-trough drawdown**, not a loss of 71.23% of the original 10,000 USDT.

Exact ATOM-start equity path:

- peak before max drawdown: **141,044.55 USDT** on **2025-09-20** while holding TWT;
- trough: **40,576.26 USDT** on **2026-06-06** while holding AAVE;
- decline from that peak: **-100,468.29 USDT / -71.23%**;
- the trough was still **+305.76% above the original 10,000 USDT**.

The lowest equity relative to the original capital occurred much earlier:

- lowest equity: **9,570.48 USDT** on **2023-11-03**;
- loss versus original capital: **-4.30%**;
- daily closes below 10,000 USDT: **5** total, all at the start of the mature test.

After the strategy recovered above the initial 10,000 USDT, it never again closed below the original capital during the tested mature period.

Therefore future U8 evidence must keep these two risk measures separate:

1. **MAX DRAWDOWN FROM PRIOR PEAK** — path risk / give-back from accumulated gains;
2. **LOWEST EQUITY VS INITIAL CAPITAL** — actual loss relative to starting capital.

A `-71.23%` max drawdown must never be described as `-71.23% of starting capital`.

## Cost shadow

Same route with zero transition cost: 97,519.47 USDT
Zero-cost return from first-open capital: +875.19%

Terminal fee drag: 2,027.56 USDT
Terminal fee drag vs no-cost shadow: 2.0791%

Accounting identity:
actual/no-cost = 0.979208675965
(1 - 0.001)^21 = 0.979208675965

PASS: exact accounting invariant matched.

## Every transition

| # | Signal | Execute | From qty / asset | Gross USDT | Fee | To qty / asset | Net USDT | Cumulative drag |
|---:|---|---|---|---:|---:|---|---:|---:|
| 1 | 2023-11-27 | 2023-11-28 | 1,234.2631 ATOM | 11,487.29 | 11.49 | 50.487461 BNB | 11,475.80 | 0.1000% |
| 2 | 2023-12-29 | 2023-12-30 | 50.487461 BNB | 15,837.92 | 15.84 | 149,816.1016 TRX | 15,822.08 | 0.1999% |
| 3 | 2024-01-23 | 2024-01-24 | 149,816.1016 TRX | 16,061.78 | 16.06 | 14,920.7016 TWT | 16,045.72 | 0.2997% |
| 4 | 2024-02-07 | 2024-02-08 | 14,920.7016 TWT | 17,185.66 | 17.19 | 17,699,462,266.8156 PEPE | 17,168.48 | 0.3994% |
| 5 | 2024-02-29 | 2024-03-01 | 17,699,462,266.8156 PEPE | 48,496.53 | 48.50 | 4,295.7998 ATOM | 48,448.03 | 0.4990% |
| 6 | 2024-03-07 | 2024-03-08 | 4,295.7998 ATOM | 59,703.03 | 59.70 | 40,256.0222 TWT | 59,643.32 | 0.5985% |
| 7 | 2024-04-20 | 2024-04-21 | 40,256.0222 TWT | 45,819.40 | 45.82 | 5,275.2778 ATOM | 45,773.59 | 0.6979% |
| 8 | 2024-11-25 | 2024-11-26 | 5,275.2778 ATOM | 41,785.48 | 41.79 | 65.618224 BNB | 41,743.69 | 0.7972% |
| 9 | 2025-02-01 | 2025-02-02 | 65.618224 BNB | 42,844.76 | 42.84 | 42,917.7967 TWT | 42,801.92 | 0.8964% |
| 10 | 2025-02-20 | 2025-02-21 | 42,917.7967 TWT | 44,806.18 | 44.81 | 9,088.6038 ATOM | 44,761.37 | 0.9955% |
| 11 | 2025-03-02 | 2025-03-03 | 9,088.6038 ATOM | 44,606.87 | 44.61 | 5,040,979,690.6297 PEPE | 44,562.26 | 1.0945% |
| 12 | 2025-05-14 | 2025-05-15 | 5,040,979,690.6297 PEPE | 70,069.62 | 70.07 | 254,821.7986 TRX | 69,999.55 | 1.1934% |
| 13 | 2025-05-18 | 2025-05-19 | 254,821.7986 TRX | 68,292.24 | 68.29 | 4,296.2185 LINK | 68,223.95 | 1.2922% |
| 14 | 2025-07-23 | 2025-07-24 | 4,296.2185 LINK | 78,105.25 | 78.11 | 97,974.8206 TWT | 78,027.15 | 1.3909% |
| 15 | 2025-09-21 | 2025-09-22 | 97,974.8206 TWT | 119,774.22 | 119.77 | 27,194.1918 ATOM | 119,654.44 | 1.4895% |
| 16 | 2025-11-24 | 2025-11-25 | 27,194.1918 ATOM | 68,012.67 | 68.01 | 14,900,144,958.5739 PEPE | 67,944.66 | 1.5881% |
| 17 | 2026-01-05 | 2026-01-06 | 14,900,144,958.5739 PEPE | 104,450.02 | 104.45 | 44,027.6650 ATOM | 104,345.57 | 1.6865% |
| 18 | 2026-02-04 | 2026-02-05 | 44,027.6650 ATOM | 87,835.19 | 87.84 | 123,832.0019 TWT | 87,747.36 | 1.7848% |
| 19 | 2026-05-18 | 2026-05-19 | 123,832.0019 TWT | 59,612.73 | 59.61 | 665.621024 AAVE | 59,553.11 | 1.8830% |
| 20 | 2026-06-29 | 2026-06-30 | 665.621024 AAVE | 60,897.67 | 60.90 | 172,293.3158 TWT | 60,836.77 | 1.9811% |
| 21 | 2026-08-02 | 2026-08-03 | 172,293.3158 TWT | 64,868.43 | 64.87 | 51,147.2494 ATOM | 64,803.56 | 2.0791% |

## Route

ATOM -> BNB -> TRX -> TWT -> PEPE -> ATOM -> TWT -> ATOM -> BNB -> TWT -> ATOM -> PEPE -> TRX -> LINK -> TWT -> ATOM -> PEPE -> ATOM -> TWT -> AAVE -> TWT -> ATOM

## Conclusion

The 0.1% modeled transition cost is not a one-time deduction from starting capital. Separately, the -71.23% risk figure is peak-to-trough drawdown from accumulated gains, not a 71.23% loss of original capital.

It is applied independently at every executed transition. Across 21 transitions, the exact multiplicative cost factor is 0.979208675965, equivalent to a 2.0791% terminal drag versus an otherwise identical zero-cost route.

Residual risk:
real exchange spread/slippage and any fee different from the frozen 0.1% model are not separately represented.
