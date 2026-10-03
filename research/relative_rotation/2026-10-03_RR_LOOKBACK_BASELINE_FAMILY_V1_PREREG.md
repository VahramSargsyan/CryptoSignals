# RR LOOKBACK + BASELINE FAMILY V1 — PREREGISTRATION

Date: 2026-10-03
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Question

Does shortening the Relative Rotation reference window — especially to 25 or
50 daily bars — improve the actual rotation path, and is any improvement caused
by the shorter lookback itself or by using a true SMA instead of the canonical
rolling median?

## Frozen variants

Two independent baseline families are tested:

### MEDIAN family
- MEDIAN_25
- MEDIAN_45
- MEDIAN_50
- MEDIAN_90
- MEDIAN_180

### SMA family
- SMA_25
- SMA_45
- SMA_50
- SMA_90
- SMA_180

Canonical benchmark:
- MEDIAN_180

No other lookback may be added after seeing V1 results.

## Shared RR mechanics

For pair ratio RIGHT / LEFT:

- reference = variant-specific rolling MEDIAN or rolling arithmetic MEAN;
- ARM threshold = +/-15% from reference;
- post-ARM extreme tracking unchanged;
- reversal confirmation = 3%;
- strongest same-day CONFIRMED baseline route;
- Destination Dominance override = accepted 1.50x rule;
- TARGET universe unchanged;
- legacy ATOM/SOL/LINK are exit-only sources;
- execution = next D1 open;
- transition cost = 0.10%.

Only baseline family and lookback change.

## Full-path evaluation

For each variant independently:
- rebuild every pair state from scratch with that exact baseline;
- rebuild DDG routes from those states;
- simulate the entire path from every supported starting asset;
- future routes use the actual current held asset.

Starting normalized capital:
- 100.

Start assets:
- TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR, ATOM, SOL, LINK.

## Fixed windows

- MATURE: 2023-10-31 through latest closed D1
- LAST_2Y: 2024-10-03 through latest closed D1
- LAST_1Y: 2025-10-03 through latest closed D1

## Rolling robustness

At monthly starts:
- full rolling 365-day windows;
- TARGET start assets only.

For every variant report:
- median rolling return;
- worst rolling return;
- share of rolling windows beating canonical MEDIAN_180;
- median rolling return delta vs canonical.

## Metrics

Per window/variant:
- median return across starts;
- worst / best return;
- positive-start rate;
- median / worst max drawdown;
- median transition count;
- median holding duration in days;
- median transition cost paid;
- end-asset concentration.

## Signal timing audit around 2026-09-28

For every variant report exact effective routes, when present, for:

- LINK on 2026-09-28;
- TRX on 2026-09-28;
- TRX on 2026-09-29;
- TRX on 2026-09-30;
- TRX on 2026-10-01;
- AAVE on the same dates.

Also simulate a LINK start from 2026-09-28 through latest closed D1 and persist
the transition trace.

This directly answers whether 25/50 would have exposed AAVE earlier than the
canonical MEDIAN_180 path.

## Interpretation rules

This is a head-to-head mechanism test, not parameter optimization.

A variant is marked ROBUST_IMPROVEMENT_CANDIDATE only if all are true:
- MATURE median return > canonical MEDIAN_180;
- LAST_2Y median return >= canonical;
- median max DD no worse than canonical by more than 10 percentage points;
- rolling 365 beat rate versus canonical >= 50%;
- median transition count <= 2x canonical.

Otherwise:
- MIXED if some primary metrics improve;
- NOT_SUPPORTED if MATURE return and rolling robustness both fail to improve.

No automatic production promotion follows from any classification.

## Fixed as-of

2026-10-03T16:30:00Z

No unclosed D1 candle may enter V1.

## No-retune rule

Do not add 20/30/60/100/120/etc. after seeing results.
Do not change ARM, reversal, DDG threshold, costs, or execution timing.
Any new period family requires V2.

TEST_LEVEL planned:
GITHUB_ACTIONS_LIVE_PUBLIC_BINANCE_D1_PATH_DEPENDENT_STRESS_TEST
