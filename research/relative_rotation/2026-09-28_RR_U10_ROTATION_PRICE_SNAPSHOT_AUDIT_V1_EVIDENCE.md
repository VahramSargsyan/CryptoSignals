# RR U10 ROTATION PRICE SNAPSHOT AUDIT V1 — EVIDENCE

Date: 2026-09-28
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Runtime identity

- Frozen universe: `RR_TARGET_U10_CANDIDATE_HBAR_V1`
- GitHub Actions run: `36473135756`
- Source commit: `7d665236242fc4a927b16bb5c049f003ac8f7744`
- Artifact ID: `10992337832`
- Artifact: `rr-u10-rotation-price-snapshot-audit-v1-1`
- Artifact SHA256: `0242f273f98d4cf5e3fd28cdcaaa4eaea751b6805840576d187b2aea3f915c29`
- TEST_LEVEL: `GITHUB_ACTIONS_REPOSITORY_CACHE_PLUS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Question

Does the frozen U10 grow because its individual rotations select relatively
strong tokens even in bear markets, rather than only because the broad market
rises?

The audit replaces broad-market abstraction with the actual open prices of all
10 U10 tokens at every rotation execution.

## Canonical equivalence

All 10 starting states passed:

- transition count match: PASS
- final asset match: PASS
- normalized final capital match: PASS

Therefore the price-snapshot trace is equivalent to the canonical
`simulate_one()` route under the frozen U10 semantics.

## Strict "always increases" hypothesis

Classification:

`REGIME_DEPENDENT_ROTATION_EDGE`

The literal hypothesis:

`EVERY_COMPLETED_ROTATION_LEG_INCREASES_CAPITAL`

is **false**.

Across 31 de-duplicated completed rotation legs:

- positive: 22
- negative: 9
- zero: 0
- all 10 start paths monotonic at every transition: NO

Worst unique leg:

- AVAX
- 2026-01-06 -> 2026-02-13
- net cycle return: -38.5%
- entry regime: MIXED

Best unique leg:

- HBAR
- 2024-11-06 -> 2024-11-20
- net cycle return: +188.2%
- entry regime: BULL

The strategy therefore does not increase on every individual rotation.

## Full-period capital

100 USDT normalized at MATURE start became, median across the 10 possible
starting states:

`3267.27 USDT`

Median total return:

`+3167.3%`

Start-state final capital range:

- BNB start: 2562.14 USDT
- HBAR start: 3816.03 USDT

All 10 starting states ended strongly positive.

## Rotation edge by entry regime

| Entry regime | Unique legs | Positive unique legs | Median net leg | Median held-token rank | Median excess vs U10 median | Median conditioned compound across starts | Positive conditioned starts |
|---|---:|---:|---:|---:|---:|---:|---:|
| BULL | 18 | 77.8% | +27.3% | 2.0 / 10 | +16.0 pp | +1427.5% | 100% |
| MIXED | 7 | 57.1% | +7.6% | 4.0 / 10 | +2.2 pp | -21.3% | 0% |
| BEAR | 6 | 66.7% | +11.6% | 2.5 / 10 | +15.6 pp | +142.2% | 100% |

This is the key result.

### Bull

Bull-entry legs are the strongest engine:

- 14 of 18 unique legs positive;
- median net leg +27.3%;
- selected token median rank 2nd of 10;
- selected token beat contemporaneous U10 median by +16.0 percentage points;
- conditioned compounding +1427.5% median across starting states.

### Bear

Bear-entry legs remain strongly positive in aggregate:

- 4 of 6 unique legs positive;
- median net leg +11.6%;
- selected token median rank 2.5 of 10;
- selected token beat contemporaneous U10 median by +15.6 percentage points;
- conditioned compounding +142.2% median across all 10 starts;
- 10/10 conditioned start paths positive.

This directly supports the narrower statement:

`U10 HISTORICALLY GENERATED POSITIVE ROTATION CAPITAL IN BOTH BULL AND BEAR ENTRY REGIMES`

It does **not** support:

`U10 ALWAYS INCREASES`

### Mixed

Mixed-entry legs are the weak regime:

- 4 of 7 unique legs positive;
- median individual leg +7.6%;
- but conditioned compounded result across starts: -21.3%;
- 0/10 conditioned start paths positive.

The loss is driven by sequencing and several large losing legs despite a
positive median leg.

Therefore the strongest current classification is:

`REGIME_DEPENDENT_ROTATION_EDGE`

rather than all-weather monotonic growth.

## Concrete bear-regime examples

### PEPE — 2025-03-03 -> 2025-05-13

- entry price: approximately 0.000009 USDT
- exit price: approximately 0.000014 USDT
- gross held return: +54.2%
- net cycle return after next transition cost: +54.0%
- contemporaneous median U10 token return: -6.3%
- excess versus U10 median: +60.5 pp
- held-token rank: 1 / 10
- entry regime: BEAR
- majority holding regime: BEAR

This is a direct example of the strategy gaining while the U10 cross-section
was negative.

### PEPE — 2025-11-09 -> 2026-01-06

- entry price: approximately 0.000006 USDT
- exit price: approximately 0.000007 USDT
- net cycle return: +15.0%
- contemporaneous U10 median: -16.5%
- excess: +31.6 pp
- held-token rank: 1 / 10
- entry / majority regime: BEAR

### XRP — 2026-06-29 -> 2026-07-04

- entry: 1.0485 USDT
- exit: 1.1348 USDT
- net cycle return: +8.1%
- U10 median same interval: +3.7%
- rank: 3 / 10
- entry / majority regime: BEAR

### PEPE — 2026-07-04 -> 2026-08-26

- entry: approximately 0.000003 USDT
- exit: approximately 0.000004 USDT
- net cycle return: +37.8%
- U10 median same interval: +13.5%
- excess: +24.4 pp
- held rank: 2 / 10
- entry regime: BEAR
- majority regime: MIXED

### Bear losses also exist

ALGO — 2024-06-20 -> 2024-07-07:
- net: -0.17%
- U10 median: -6.9%
- still outperformed the cross-section despite a small absolute loss.

TWT — 2026-02-13 -> 2026-05-19:
- net: -8.0%
- U10 median: +2.9%
- rank: 9 / 10
- clear failed rotation.

This is why the word "always" must be rejected.

## Concrete bull examples

HBAR — 2024-11-06 -> 2024-11-20:
- 0.0461 -> 0.1330 USDT
- net cycle return: +188.2%
- U10 median same interval: +36.6%
- excess: +151.9 pp
- rank: 1 / 10

TWT — 2025-07-16 -> 2025-09-22:
- 0.7765 -> 1.2225 USDT
- net: +57.3%
- U10 median: +1.1%
- excess: +56.4 pp
- rank: 1 / 10

AVAX — 2026-09-15 -> 2026-09-22:
- 7.563 -> 11.231 USDT
- net: +48.4%
- U10 median: +12.4%
- excess: +36.1 pp
- rank: 1 / 10

## Majority-regime robustness diagnostic

Using the majority regime over each holding leg rather than entry regime:

- BULL: 19 unique legs, 78.9% positive, median net +25.3%, median excess +15.1 pp
- MIXED: 7 unique legs, 57.1% positive, median net +2.4%, median excess +8.9 pp
- BEAR: 5 unique legs, 60.0% positive, median net +8.1%, median excess +4.5 pp

The sign conclusion remains:
- rotation edge strongest in bull;
- still positive in bear;
- weakest in mixed.

## Why the strategy can earn in bear

The direct price snapshots show the mechanism.

The strategy does not need all U10 tokens to rise. It needs the selected held
token to outperform alternatives during enough holding legs.

In bear-entry legs:

- selected token median rank was 2.5 / 10;
- median excess versus the U10 median token was +15.6 pp.

Therefore positive bear-regime capital can arise from **relative selection
edge**, not from broad market appreciation.

This is stronger evidence than comparing U10 only with total market
capitalization.

## Important reconciliation with prior bear-regime attribution

Earlier daily regime attribution showed that U10 returns isolated only on
bear-labelled calendar days were negative.

This rotation-leg audit asks a different question:

- prior study: what happens if we compound only returns occurring on bear days?
- current study: what happens to complete rotations that **start in a bear
  regime**, even if the holding period later crosses into another regime?

Both can be true simultaneously.

The strategy can lose on the subset of bear-labelled daily returns while a
rotation initiated during bear conditions later completes profitably.

Therefore do not replace the earlier bull-dependence result with a claim that
the strategy is universally profitable on every bear-labelled day.

Frozen synthesis:

`BULL_DEPENDENT_DAILY_ABSOLUTE_RETURNS + POSITIVE_BEAR_INITIATED_ROTATION_EDGE`

## Files in artifact

- `rotation_price_snapshots.csv`
  - all 10 token open prices at every transition execution for every start path
- `rotation_legs_all_starts.csv`
  - complete per-start leg ledger
- `rotation_legs_unique.csv`
  - de-duplicated rotation legs with entry/exit prices and all-token returns
- `start_path_summary.csv`
- `regime_summary.csv`
- `canonical_equivalence.csv`
- `summary.json`
- `report.md`

## No-repeat / production boundary

Do not rerun unchanged on the same historical window.

A repeat requires:
- unseen dates;
- changed U10 semantics/universe/costs;
- a documented trace bug;
- or a separate forward-validation hypothesis.

No live/paper, Telegram, exchange, allocation, or execution behavior changed.
