# Relative Rotation U10 Rotation Price Snapshot Audit Decision Log V1

Date: 2026-09-28
Status: ACTIVE RESEARCH MEMORY
Workflow mode: STRESS_TEST_ONLY
Production change: NONE

## Canonical evidence

`research/relative_rotation/2026-09-28_RR_U10_ROTATION_PRICE_SNAPSHOT_AUDIT_V1_EVIDENCE.md`

Runtime:
- run: `36473135756`
- source SHA: `7d665236242fc4a927b16bb5c049f003ac8f7744`
- artifact: `10992337832`
- artifact SHA256: `0242f273f98d4cf5e3fd28cdcaaa4eaea751b6805840576d187b2aea3f915c29`
- TEST_LEVEL: `GITHUB_ACTIONS_REPOSITORY_CACHE_PLUS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Frozen conclusion

Classification:

`REGIME_DEPENDENT_ROTATION_EDGE`

The strong hypothesis:

`EVERY_COMPLETED_ROTATION_LEG_INCREASES_CAPITAL`

is rejected.

Across 31 de-duplicated completed rotation legs:
- 22 positive
- 9 negative
- 0 zero
- all 10 start paths monotonic at every transition: false

## Full-period result

Normalized 100 USDT at MATURE start became median:

`3267.27 USDT`

Median total return:

`+3167.3%`

## Bull and bear rotation edge

By entry regime:

### BULL
- 18 unique legs
- positive rate 77.8%
- median net leg +27.3%
- median held rank 2.0 / 10
- median excess vs contemporaneous U10 median +16.0 pp
- median conditioned compound across starts +1427.5%
- positive conditioned starts 100%

### BEAR
- 6 unique legs
- positive rate 66.7%
- median net leg +11.6%
- median held rank 2.5 / 10
- median excess vs contemporaneous U10 median +15.6 pp
- median conditioned compound across starts +142.2%
- positive conditioned starts 100%

### MIXED
- 7 unique legs
- positive rate 57.1%
- median net leg +7.6%
- median conditioned compound across starts -21.3%
- positive conditioned starts 0%

Frozen statement:

`U10 HISTORICALLY GENERATED POSITIVE ROTATION CAPITAL IN BOTH BULL AND BEAR ENTRY REGIMES, BUT NOT ON EVERY LEG AND NOT IN MIXED REGIME AGGREGATE`

## Mechanism

The bear-regime result is explained by relative selection.

Examples:

- PEPE 2025-03-03 -> 2025-05-13:
  +54.0% net while contemporaneous U10 median was -6.3%;
  rank 1 / 10.
- PEPE 2025-11-09 -> 2026-01-06:
  +15.0% net while U10 median was -16.5%;
  rank 1 / 10.
- XRP 2026-06-29 -> 2026-07-04:
  +8.1% net.
- PEPE 2026-07-04 -> 2026-08-26:
  +37.8% net.

Bear losses also exist:
- ALGO 2024-06-20 -> 2024-07-07: -0.17%
- TWT 2026-02-13 -> 2026-05-19: -8.0%

Therefore rotation does not guarantee a positive leg.

## Reconciliation with daily bear attribution

Do not overwrite the earlier daily-regime conclusion.

Earlier:
- isolated returns occurring only on bear-labelled days were negative.

Current:
- complete rotations initiated in bear conditions compounded positively.

Both are compatible because a holding leg may begin in bear and finish after
regime transition.

Frozen synthesis:

`BULL_DEPENDENT_DAILY_ABSOLUTE_RETURNS + POSITIVE_BEAR_INITIATED_ROTATION_EDGE`

## No-repeat / production boundary

Do not rerun unchanged on identical history.

A new study requires unseen data, changed strategy semantics/universe/costs,
a documented trace correction, or separate forward validation.

No live/paper, Telegram, exchange, universe, allocation, or execution behavior changed.
