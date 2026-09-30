# U8 Drawdown Entry Stress v1 — Evidence

Date: 2026-09-27
Mode: PATCH_FIX / research reporting only
Branch: research/u8-drawdown-entry-stress-v1
GitHub Actions run: 36324114083
Source commit: dc0e0db7aa566db21b358685f2fb9664b347f0a6
Result: PASS
TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_BACKTEST

## Purpose

Test fresh 10,000 USDT U8 portfolios started at different dates around the historically strongest continuation-path drawdown.

This experiment separates:
- loss versus original capital;
- drawdown from an accumulated equity peak.

Live U8 behavior was not changed.

## Frozen strategy

Universe:
ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK

Mechanics:
- Binance Spot 1D closed candles
- rolling median 180 days
- ARM 15%
- reversal 3%
- strongest confirmed max-dislocation router
- next-open execution
- 0.1% transition cost on each executed rotation

Historical warm-up/state before each scenario start remains available. Each scenario starts a fresh 10,000 USDT ATOM position on its own start date.

## Results

| Scenario | Start | Final equity | Return | Minimum equity | Min vs initial | Days below 10k | Max DD from peak | DD trough vs initial | Transitions |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| ONE_YEAR_BEFORE_PEAK | 2024-09-20 | 40,163.70 | +301.64% | 8,690.93 | -13.09% | 26 | -71.23% | +70.66% | 14 |
| SAME_YEAR_START | 2025-01-01 | 22,779.50 | +127.80% | 6,854.37 | -31.46% | 73 | -71.23% | -3.21% | 14 |
| AT_PEAK_DATE | 2025-09-20 | 7,845.13 | -21.55% | 3,333.54 | -66.66% | 372 | -66.49% | -66.66% | 6 |
| TROUGH_YEAR_START | 2026-01-01 | 11,237.86 | +12.38% | 4,775.17 | -52.25% | 223 | -65.04% | -52.25% | 4 |

## Scenario interpretation

### ONE_YEAR_BEFORE_PEAK — start 2024-09-20

Fresh capital:
10,000 -> minimum 8,690.93 (-13.09%) -> final 40,163.70 (+301.64%).

During the later historically strongest continuation drawdown:
59,323.04 -> 17,066.29 (-71.23%).

But that trough was still +70.66% above the original 10,000.

Conclusion:
starting one year before the peak gave the strategy enough accumulated profit buffer that the later -71.23% drawdown did not threaten the original principal.

### SAME_YEAR_START — start 2025-01-01

Fresh capital:
10,000 -> minimum 6,854.37 (-31.46%) -> final 22,779.50 (+127.80%).

Later peak:
33,646.04 on 2025-09-20.

Historical drawdown trough:
9,679.43 on 2026-06-06.

That trough was -3.21% versus the original 10,000.

Conclusion:
starting at the beginning of 2025 was materially riskier for original capital. The same -71.23% peak-to-trough drawdown nearly erased the accumulated buffer and briefly took the portfolio below its starting principal.

### AT_PEAK_DATE — start 2025-09-20

Fresh capital:
10,000 -> minimum 3,333.54 (-66.66%) -> final 7,845.13 (-21.55%).

The portfolio never recovered above the original 10,000 during the tested period after first falling below it.

Conclusion:
a fresh entrant immediately before the strongest historical decline experienced a severe loss of original capital. In this scenario the distinction between peak drawdown and principal loss largely disappears because the portfolio had no accumulated buffer.

### TROUGH_YEAR_START — start 2026-01-01

Fresh capital:
10,000 -> minimum 4,775.17 (-52.25%) -> final 11,237.86 (+12.38%).

The scenario recovered above the original capital later, but spent 223 daily closes below 10,000.

Conclusion:
entering after the decline had already begun still exposed fresh capital to more than a 50% interim loss before eventual recovery by 2026-09-26.

## Main finding

The apparent safety of original capital in the long 2023-10-31 start is strongly path-dependent.

U8's historical max drawdown cannot be interpreted independently of entry date:
- early starts accumulated a large profit cushion before the drawdown;
- later starts did not;
- a fresh start near the 2025-09 peak lost as much as 66.66% of original capital.

Therefore future U8 risk reports must include entry-date stress tests, not only one long-history backtest.

## Residual risks

- only one historically observed major drawdown cluster was tested;
- each scenario starts in ATOM;
- slippage beyond the frozen 0.1% transition cost is not separately modeled;
- historical performance does not establish future behavior.
