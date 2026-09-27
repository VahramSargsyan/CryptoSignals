# U10 Monthly Extreme Path v1

Date: 2026-09-27
Mode: DIAGNOSTIC_ONLY
Strategy: research U10 relative rotation
Source daily equity: validated U10 control / mature path
Source window: 2023-10-31 -> 2026-09-26
Starting capital: 10,000 USDT
Runtime/live impact: NONE

## Objective

Build a monthly path that does **not** use month-end equity.

For each calendar month, select one daily-close equity extreme:

- the monthly MAX or monthly MIN,
- whichever is farther in absolute percentage terms from the previously selected monthly extreme.

For the first partial month, the comparison anchor is the original 10,000 USDT.

The percentage shown for each row is:

`selected_month_extreme / previous_selected_extreme - 1`

Therefore the table is a sequential extreme-to-extreme path, matching the intended reading:

`10,000 -> 9,786 (-2.14%) -> 12,234 (+25.01%) -> ...`

## Important data boundary

These are **daily-close equity extrema**, not intraday high/low equity.

The underlying validated U10 research uses daily closed candles. An intraday-capital-extreme study would require a separate high/low reconstruction and is not claimed here.

## Monthly selected extremes

| Month | Type | Date | Capital USDT | Change vs previous selected extreme |
|---|---|---|---:|---:|
| START | — | 2023-10-30 anchor | 10,000.00 | — |
| 2023-10 | MIN | 2023-10-31 | 9,786.47 | -2.14% |
| 2023-11 | MAX | 2023-11-26 | 12,234.02 | +25.01% |
| 2023-12 | MAX | 2023-12-27 | 16,362.99 | +33.75% |
| 2024-01 | MIN | 2024-01-09 | 15,396.60 | -5.91% |
| 2024-02 | MAX | 2024-02-28 | 56,284.29 | +265.56% |
| 2024-03 | MAX | 2024-03-11 | 66,881.36 | +18.83% |
| 2024-04 | MIN | 2024-04-13 | 39,684.39 | -40.66% |
| 2024-05 | MAX | 2024-05-05 | 49,244.72 | +24.09% |
| 2024-06 | MIN | 2024-06-29 | 34,853.76 | -29.22% |
| 2024-07 | MIN | 2024-07-05 | 30,090.18 | -13.67% |
| 2024-08 | MIN | 2024-08-15 | 23,791.50 | -20.93% |
| 2024-09 | MIN | 2024-09-07 | 19,365.54 | -18.60% |
| 2024-10 | MAX | 2024-10-21 | 25,838.31 | +33.42% |
| 2024-11 | MAX | 2024-11-24 | 46,195.61 | +78.79% |
| 2024-12 | MIN | 2024-12-02 | 42,508.80 | -7.98% |
| 2025-01 | MAX | 2025-01-06 | 47,863.25 | +12.60% |
| 2025-02 | MIN | 2025-02-24 | 34,972.59 | -26.93% |
| 2025-03 | MIN | 2025-03-10 | 26,629.70 | -23.86% |
| 2025-04 | MAX | 2025-04-26 | 42,887.83 | +61.05% |
| 2025-05 | MAX | 2025-05-10 | 64,004.72 | +49.24% |
| 2025-06 | MIN | 2025-06-22 | 39,524.74 | -38.25% |
| 2025-07 | MAX | 2025-07-27 | 74,538.50 | +88.59% |
| 2025-08 | MIN | 2025-08-02 | 64,162.53 | -13.92% |
| 2025-09 | MAX | 2025-09-20 | 127,668.80 | +98.98% |
| 2025-10 | MIN | 2025-10-10 | 72,516.59 | -43.20% |
| 2025-11 | MAX | 2025-11-07 | 175,691.95 | +142.28% |
| 2025-12 | MIN | 2025-12-18 | 93,462.73 | -46.80% |
| 2026-01 | MAX | 2026-01-13 | 195,693.36 | +109.38% |
| 2026-02 | MIN | 2026-02-05 | 121,279.68 | -38.03% |
| 2026-03 | MAX | 2026-03-16 | 150,358.74 | +23.98% |
| 2026-04 | MIN | 2026-04-06 | 105,425.21 | -29.88% |
| 2026-05 | MAX | 2026-05-12 | 143,007.99 | +35.65% |
| 2026-06 | MIN | 2026-06-06 | 90,550.12 | -36.68% |
| 2026-07 | MAX | 2026-07-26 | 165,061.71 | +82.29% |
| 2026-08 | MAX | 2026-08-21 | 188,449.26 | +14.17% |
| 2026-09 | MAX | 2026-09-26 | 219,485.20 | +16.47% |

## Interpretation boundary

This path deliberately compresses each calendar month into one selected extreme.

It is useful for visualizing large month-to-month swings but it discards the second extreme inside each month.

For example, a month can contain both a deep minimum and a strong maximum; only the farther one from the previous selected point is retained here.

No strategy rule, live setting, or capital-management rule is changed by this diagnostic.

TEST_LEVEL: DERIVED_FROM_VALIDATED_DAILY_EQUITY
