# RR SEQUENTIAL LIMIT TRAILING 3-BAR V1 — EVIDENCE

Date: 2026-10-01
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Runtime identity

- Branch: `research-run/sequential-limit-recapture-v1`
- GitHub Actions run: `36857241441`
- Source commit: `7102a0de615a5073a9500ac60c383ddfe98070ce`
- Artifact ID: `11159690410`
- Artifact SHA256: `b66f766dc89d1d57e6c178ce5dcdca01b9234c574d10fc3dd4f3c0e214e8b2d3`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Research question

Can the previously rejected fixed exact-3% 7-day execution policy be improved by monotonically tightening the limit orders toward market using recent 3-day extrema, while preserving the RR route as much as possible?

## Frozen policy before results

For direct confirmed RR routes:

1. Start from the same EXACT_3PCT_LOG source SELL and destination BUY targets.
2. SELL must fill first.
3. BUY becomes eligible only from the next 1H candle after SELL.
4. Once per UTC day:
   - SELL limit may move only downward;
   - BUY limit may move only upward.
5. Tightening reference:
   - SELL = min(previous SELL limit, highest hourly HIGH from the previous 3 fully closed UTC days);
   - BUY = max(previous BUY limit, lowest hourly LOW from the previous 3 fully closed UTC days).
6. Orders may never move away from market again.
7. Hard fallback remains 168 hours.
8. At 168h, any unfinished leg is forced at hourly-open market proxy.
9. DDG/non-applicable routes remain immediate next-open market.
10. Fee model:
    - 0.1% source SELL;
    - 0.1% destination BUY.

No 2-day, 5-day, intraday, linear, or adaptive alternative was tried in this pass.

## Controls

- `CANONICAL_NEXT_OPEN_ONE_FEE`
- `NEXT_OPEN_TWO_FEE_CONTROL`
- `FIXED_EXACT3_7D_FALLBACK`
- `TRAIL_3DAILY_EXTREME_7D_FALLBACK`

## Full path-dependent return

| Window | Canonical | Two-fee next-open | Fixed 7d | 3-day trailing |
|---|---:|---:|---:|---:|
| Validation 1Y | +188.7% | +185.8% | +42.2% | **+40.8%** |
| Last 2Y | +2102.3% | +2061.9% | +1024.5% | **+957.6%** |
| MATURE | +3588.4% | +3502.7% | +1538.5% | **+1577.5%** |

MATURE drawdown:

- canonical: -62.4%
- two-fee next-open: -62.5%
- fixed 7d: -71.6%
- 3-day trailing: **-71.6%**

The trailing rule improves the MATURE median capital factor versus fixed 7d by only about:

`1.024x`

but does not improve Validation 1Y or Last 2Y.

Relative to same two-fee next-open control on MATURE:

`0.458x`

Relative to canonical one-fee on MATURE:

`0.447x`

Thus more than half of the original capital edge remains lost.

## Execution mechanics

MATURE unique direct attempts:
- 29

Fixed 7d:
- full exact-3% completion: 21 / 29 = 72.4%

3-day trailing:
- full limit completion: **24 / 29 = 82.8%**
- source SELL timeout: 3
- SELL filled but BUY timeout: 2

Therefore trailing rescued three historical cases from fallback.

### Cases rescued from timeout to full limit completion

- HBAR -> PEPE, signal 2023-11-25
- TWT -> BNB, signal 2024-12-07
- HBAR -> TWT, signal 2025-07-15

## How often did trailing actually act?

Among 29 direct attempts:

- SELL tightened at least once: **6**
- BUY tightened at least once: **4**
- either side tightened at least once: **9**
- no tightening at all: **20**

Median tightenings:
- SELL: **0**
- BUY: **0**

This means most direct trades either completed before the first useful daily tightening or the 3-day extreme rule did not move the target toward market.

The rule is therefore mechanically too slow / inactive for most rotations.

## Waiting / path pressure

MATURE median pending days:
- fixed 7d previous metric: about **61**
- trailing 3-day: **58**

The new run also separates a more meaningful metric:

- median RR opportunities observed while transfer was pending: **6**

The slight reduction in waiting is not enough to restore the lost RR path edge.

## Realized execution-price improvement

Across direct trailing attempts:

- median realized exchange-rate improvement vs immediate next-open: **+3.00%**
- 25th percentile: **-1.75%**
- 75th percentile: **+3.09%**

Interpretation:
- successful limit completions retain the expected ~3% advantage;
- timeout/fallback cases can produce worse execution than immediate next-open;
- improving fill-rate alone is not sufficient if the waiting path cost remains large.

## Important metric correction to V1 fixed-7d documentation

The previous variable named `skipped_signal_days` incremented on every daily bar while a transfer was pending.

It therefore measured:

`PENDING DAYS`

not the literal count of RR signals that appeared during those days.

This naming issue did **not** alter any historical return, drawdown, or path simulation, because the strategy correctly suppressed new transitions while pending.

This V1 trailing study adds a separate:

`RR_OPPORTUNITIES_WHILE_PENDING`

metric.

## Decision

`TRAIL_3DAILY_EXTREME_7D_FALLBACK = REJECTED_AS_OPTIMIZATION`

Reasons:

1. MATURE improves only modestly versus fixed 7d:
   - +1577.5% vs +1538.5%.
2. Validation 1Y becomes slightly worse:
   - +40.8% vs +42.2%.
3. Last 2Y becomes worse:
   - +957.6% vs +1024.5%.
4. Drawdown remains -71.6%.
5. Most trades never tighten:
   - median updates = 0;
   - only 9/29 direct attempts changed.
6. Waiting/path damage remains much larger than execution-price gain.

## What remains open

The result does **not** reject all progressive execution.

It rejects this specific slow rule:

`ONCE_DAILY + PREVIOUS_3_FULL_DAYS_EXTREME + 7D FALLBACK`

A materially different next hypothesis could use:
- shorter-bar extrema;
- more frequent tightening;
- explicit time-based convergence toward market;
- maximum allowed pending time measured in hours rather than days;
- hybrid target = favorable limit initially, then rapidly approach market.

Any such test must be preregistered separately.

## Production boundary

No production/live/paper/Telegram/exchange/order behavior changed.

## Residual risks

- Hourly wick-touch remains an optimistic fill proxy.
- Partial fill, queue priority, spread and slippage are not modeled.
- Fallback uses hourly-open proxy.
- DDG remains next-open rather than limit-optimized.
