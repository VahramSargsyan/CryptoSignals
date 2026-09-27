# U10 Ledger + Entry Stress v1 — Evidence

Date: 2026-09-27
Mode: PATCH_FIX / research instrumentation only
Branch: research/u10-ledger-entry-stress-v1
GitHub Actions run: 36328079690
Source commit: 9c7decdbe14809b74a959be73fa58b2cec90d978
Result: PASS
TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_BACKTEST

## Scope

Run research U10 through the same capital-accounting and principal-risk framework used for U8.

No live strategy configuration or live universe was changed.

U10:

ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

Frozen mechanics:

- Binance Spot 1D closed candles
- rolling median 180 days
- ARM 15%
- reversal confirmation 3%
- strongest confirmed max-dislocation router
- next-open execution
- 0.1% transition cost per executed rotation
- fresh portfolio starts in ATOM
- normalized starting capital: 10,000 USDT

## Data boundary

Common U10 panel:

2023-05-05 -> 2026-09-26

First fully mature 180-row capital start:

2023-10-31

A full year before the absolute strongest U10 drawdown peak is not available because the strongest peak occurs on 2024-03-11, before one full mature year has elapsed.

The runner therefore preserves two separate drawdown anchors:

1. absolute strongest mature-path drawdown;
2. strongest drawdown whose peak has at least one full mature year of history before it.

## Long mature path

Initial capital:

10,000.00 USDT

Initial ATOM position:

1,234.26314490 ATOM @ 8.10200000 USDT

Final position:

117,560.363454 ATOM

Final equity:

219,485.20 USDT

Return from original 10,000 USDT:

+2,094.85%

Executed transitions:

23

Route:

ATOM -> BNB -> TRX -> TWT -> PEPE -> ATOM -> TWT -> ATOM -> BNB -> TWT -> FIL -> PEPE -> HBAR -> TWT -> ATOM -> FIL -> PEPE -> ATOM -> HBAR -> TWT -> AAVE -> HBAR -> PEPE -> ATOM

## Original-capital risk on long path

Lowest daily-close equity:

9,570.48 USDT on 2023-11-03

Worst loss versus original 10,000:

-4.30%

Daily closes below original capital:

5

First recovery above original capital after the initial dip:

2023-11-05

## Absolute strongest U10 peak-to-trough drawdown

Peak:

66,881.36 USDT on 2024-03-11 while holding TWT

Trough:

19,365.54 USDT on 2024-09-07 while holding ATOM

Peak-to-trough drawdown:

-71.04%

Trough versus original 10,000:

+93.66%

Interpretation:

The -71.04% figure is a loss from accumulated peak equity, not a 71.04% loss of original capital.

## Transition-cost shadow

Cost-adjusted final equity:

219,485.20 USDT

Same exact route with zero transition cost:

224,594.44 USDT

Terminal fee drag:

5,109.25 USDT

Terminal drag versus zero-cost shadow:

2.2749%

Accounting identity:

actual / zero_cost = 0.977251237821

(1 - 0.001)^23 = 0.977251237821

PASS: exact fee-accounting invariant matched.

## Every executed transition

| # | Signal | Execute | From quantity | Gross USDT | Fee | To quantity | Cumulative fee drag |
|---:|---|---|---|---:|---:|---|---:|
| 1 | 2023-11-27 | 2023-11-28 | ATOM 1,234.263145 | $11,487.29 | $11.49 | BNB 50.487461 | 0.1000% |
| 2 | 2023-12-29 | 2023-12-30 | BNB 50.487461 | $15,837.92 | $15.84 | TRX 149,816.101553 | 0.1999% |
| 3 | 2024-01-23 | 2024-01-24 | TRX 149,816.101553 | $16,061.78 | $16.06 | TWT 14,920.701565 | 0.2997% |
| 4 | 2024-02-07 | 2024-02-08 | TWT 14,920.701565 | $17,185.66 | $17.19 | PEPE 17,699,462,266.815563 | 0.3994% |
| 5 | 2024-02-29 | 2024-03-01 | PEPE 17,699,462,266.815563 | $48,496.53 | $48.50 | ATOM 4,295.799795 | 0.4990% |
| 6 | 2024-03-07 | 2024-03-08 | ATOM 4,295.799795 | $59,703.03 | $59.70 | TWT 40,256.022220 | 0.5985% |
| 7 | 2024-04-20 | 2024-04-21 | TWT 40,256.022220 | $45,819.40 | $45.82 | ATOM 5,275.277756 | 0.6979% |
| 8 | 2024-11-25 | 2024-11-26 | ATOM 5,275.277756 | $41,785.48 | $41.79 | BNB 65.618224 | 0.7972% |
| 9 | 2025-02-01 | 2025-02-02 | BNB 65.618224 | $42,844.76 | $42.84 | TWT 42,917.796721 | 0.8964% |
| 10 | 2025-02-09 | 2025-02-10 | TWT 42,917.796721 | $38,432.89 | $38.43 | FIL 11,599.532954 | 0.9955% |
| 11 | 2025-03-02 | 2025-03-03 | FIL 11,599.532954 | $41,340.74 | $41.34 | PEPE 4,671,877,229.778671 | 1.0945% |
| 12 | 2025-05-12 | 2025-05-13 | PEPE 4,671,877,229.778671 | $63,677.69 | $63.68 | HBAR 295,755.306873 | 1.1934% |
| 13 | 2025-07-15 | 2025-07-16 | HBAR 295,755.306873 | $68,931.69 | $68.93 | TWT 88,683.525671 | 1.2922% |
| 14 | 2025-09-21 | 2025-09-22 | TWT 88,683.525671 | $108,415.61 | $108.42 | ATOM 24,615.271482 | 1.3909% |
| 15 | 2025-10-19 | 2025-10-20 | ATOM 24,615.271482 | $79,482.71 | $79.48 | FIL 51,795.974498 | 1.4895% |
| 16 | 2025-11-08 | 2025-11-09 | FIL 51,795.974498 | $153,160.70 | $153.16 | PEPE 25,124,390,130.246922 | 1.5881% |
| 17 | 2026-01-05 | 2026-01-06 | PEPE 25,124,390,130.246922 | $176,121.97 | $176.12 | ATOM 74,238.756472 | 1.6865% |
| 18 | 2026-01-14 | 2026-01-15 | ATOM 74,238.756472 | $191,758.71 | $191.76 | HBAR 1,552,280.603345 | 1.7848% |
| 19 | 2026-02-12 | 2026-02-13 | HBAR 1,552,280.603345 | $144,672.55 | $144.67 | TWT 276,343.938202 | 1.8830% |
| 20 | 2026-05-18 | 2026-05-19 | TWT 276,343.938202 | $133,031.97 | $133.03 | AAVE 1,485.402256 | 1.9811% |
| 21 | 2026-06-28 | 2026-06-29 | AAVE 1,485.402256 | $136,226.24 | $136.23 | HBAR 1,915,951.213456 | 2.0791% |
| 22 | 2026-07-02 | 2026-07-03 | HBAR 1,915,951.213456 | $135,840.94 | $135.84 | PEPE 55,389,836,772.660004 | 2.1771% |
| 23 | 2026-08-03 | 2026-08-04 | PEPE 55,389,836,772.660004 | $160,630.53 | $160.63 | ATOM 117,560.363454 | 2.2749% |

## Strongest drawdown with a full mature year before its peak

Eligibility threshold:

2024-10-31

Peak:

195,693.36 USDT on 2026-01-13 while holding ATOM

Trough:

90,550.12 USDT on 2026-06-06 while holding AAVE

Peak-to-trough drawdown:

-53.73%

This is the drawdown anchor used for the fresh-entry stress because it allows a real full-year-before-peak test.

## Fresh-entry stress

### ONE_YEAR_BEFORE_PEAK

Start:

2025-01-13

Fresh position:

10,000 USDT -> 1,547.26907009 ATOM @ 6.463 USDT

Minimum equity:

4,891.68 USDT on 2025-03-10

Worst loss versus original capital:

-51.08%

Days below original capital:

154

Final equity on 2026-09-26:

40,317.86 USDT

Final return:

+303.18%

Transitions:

13

Route:

ATOM -> PEPE -> HBAR -> TWT -> ATOM -> FIL -> PEPE -> ATOM -> HBAR -> TWT -> AAVE -> HBAR -> PEPE -> ATOM

### PEAK_YEAR_START

Start:

2026-01-01

Fresh position:

10,000 USDT -> 5,181.34715026 ATOM @ 1.93 USDT

Minimum equity:

6,319.77 USDT on 2026-06-06

Worst loss versus original capital:

-36.80%

Days below original capital:

142

Final equity on 2026-09-26:

15,318.54 USDT

Final return:

+53.19%

Transitions:

6

Route:

ATOM -> HBAR -> TWT -> AAVE -> HBAR -> PEPE -> ATOM

### AT_PEAK_DATE

Start:

2026-01-13

Fresh position:

10,000 USDT -> 4,022.52614642 ATOM @ 2.486 USDT

Minimum equity:

4,906.34 USDT on 2026-06-06

Worst loss versus original capital:

-50.94%

Days below original capital:

233

First recovery above 10,000 after falling below it:

2026-08-21

Final equity on 2026-09-26:

11,892.51 USDT

Final return:

+18.93%

Transitions:

6

Route:

ATOM -> HBAR -> TWT -> AAVE -> HBAR -> PEPE -> ATOM

## Main findings

1. U10's long mature path is much stronger in terminal growth than canonical U8's previously tested long path, while its absolute max drawdown remains very large.
2. On the long path, original capital was only briefly below 10,000 at the beginning; the absolute -71.04% max drawdown occurred after substantial gains.
3. Fresh-entry risk remains severe. Starting one year before the later one-year-eligible drawdown still produced an interim -51.08% loss of original capital before recovering to +303.18%.
4. Starting directly at the 2026-01-13 peak produced a -50.94% loss of original capital before eventual recovery to +18.93%.
5. FIL and HBAR materially alter the route from 2025 onward and are responsible for several transitions that do not exist in canonical U8.

## Interpretation boundary

Do not describe U10 as low-risk because the long-path original capital stayed protected after early gains.

The fresh-entry tests show that a new investor can still experience roughly 37% to 51% principal drawdowns around the tested later stress period.

Do not promote U10 to live solely from this historical evidence.

## Residual risks

- historical performance does not establish future performance;
- FIL/HBAR were proposed additions and may contain selection leakage;
- a 0.1% transition-cost model does not separately represent all real spread/slippage;
- fresh-entry tests here focus on the strongest drawdown with a full mature prior year, not every possible monthly entry date.
