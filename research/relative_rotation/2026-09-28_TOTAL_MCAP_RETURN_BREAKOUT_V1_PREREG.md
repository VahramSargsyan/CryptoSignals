# TOTAL MARKET CAP RETURN vs BREAKOUT V1 — PREREG

Date: 2026-09-28  
Workflow mode: STRESS_TEST_ONLY  
Production/live changes: NONE

## Question

For TradingView `CRYPTOCAP:TOTAL` on daily closes, when TOTAL becomes stretched away from SMA25 / SMA50 / SMA100:

1. how often does the gap begin to mean-revert before extending materially farther;
2. how far does the gap typically extend after the first threshold crossing;
3. how long does normalization take;
4. when the gap shrinks, is convergence driven more by price returning toward the SMA or by the SMA catching up to price;
5. how do these outcomes differ between upside and downside excursions.

This is descriptive research only. It does not create a trading rule.

## Frozen source

Repository cache only:

`data/market/cryptocap_total_d1.csv`

Canonical metadata:
- symbol: `CRYPTOCAP:TOTAL`
- provider: TradingView
- timeframe: 1D
- rows: 1,398
- range: 2022-11-29 through 2026-09-26
- SHA256: `d0c9b3bac915e484069c6302ddce1a0b050abbb8de155e4106e39cf6e519d3ee`

No external market data may be fetched for this test.

## Moving averages

Recompute causally from cached daily close:
- SMA25
- SMA50
- SMA100

Also calculate 5-day SMA slope:
`SMA_t / SMA_(t-5) - 1`.

## Distance

For each SMA:

`distance = TOTAL_close / SMA - 1`

Positive distance means TOTAL is above the SMA.
Negative distance means TOTAL is below the SMA.

## Thresholds

Test these absolute distance thresholds without tuning:

- 10%
- 15%
- 20%
- 25%
- 30%
- 40%

Upside and downside are evaluated separately.

## Episode de-duplication

One prolonged excursion must not be counted as many independent signals.

For each SMA / threshold / direction:

1. an episode starts on the first daily close crossing the threshold from inside;
2. while the episode is active, no new episode at the same SMA / threshold / direction can start;
3. the episode is re-armed only after the directional distance has compressed to <= 50% of the threshold.

Higher thresholds crossed during the same broad market move are still separate threshold-level observations.

## Competing outcome barriers

At trigger, let `G` be the absolute directional distance from the SMA on the trigger close.

Track up to 90 calendar days.

### RETURN50

The directional gap shrinks to <= 50% of trigger gap:

`gap <= 0.50 * G`

### BREAKOUT25

The directional gap expands to >= 125% of trigger gap:

`gap >= 1.25 * G`

### First-outcome label

- `RETURN_FIRST`: RETURN50 occurs before BREAKOUT25
- `BREAKOUT_FIRST`: BREAKOUT25 occurs before RETURN50
- `SAME_DAY`: both first occur on the same daily close
- `UNRESOLVED_90D`: neither occurs within 90 calendar days
- `CENSORED`: dataset ends before 90 days and neither barrier was reached

This label is a research convention, not a market truth.

## Additional outcomes

For each episode record:

- trigger date
- trigger close
- SMA value
- trigger distance %
- 5-day SMA slope %
- SMA25/50/100 fan spread
- fan order: BULL_STACK / BEAR_STACK / MIXED
- maximum further directional distance within 90d
- maximum additional extension in percentage points
- days to maximum extension
- days to RETURN50
- days to first SMA touch/cross
- days to BREAKOUT25
- close return at 7/14/30/60/90 calendar-day horizons
- gap at the same horizons

Use last available daily observation on or before each horizon target date.

## Convergence attribution

When RETURN50 occurs, decompose log-gap convergence from trigger to RETURN50.

Let direction = +1 for upside excursion and -1 for downside excursion.

- price convergence contribution:
  `-direction * log(P_return / P_trigger)`
- SMA catch-up contribution:
  ` direction * log(SMA_return / SMA_trigger)`

Their sum equals the reduction of the signed log price/SMA gap.

Label:
- `PRICE_RETURN_DOMINANT` if price contribution > SMA contribution
- `SMA_CATCHUP_DOMINANT` otherwise

This is accounting attribution, not causal inference.

## Summary

For each SMA / threshold / direction report:

- episode count
- RETURN_FIRST %
- BREAKOUT_FIRST %
- unresolved/censored %
- median trigger distance
- median maximum additional extension
- median days to max extension
- median days to RETURN50
- median days to SMA touch
- median SMA slope at trigger
- PRICE_RETURN_DOMINANT share among RETURN50 episodes
- SMA_CATCHUP_DOMINANT share among RETURN50 episodes

Also produce slope-sign split:
- SMA slope aligned with excursion direction
- SMA slope opposed / flat

No parameter is changed after seeing results in this V1.

## Interpretation boundary

The test may identify historical transition zones, but it must not call them a guaranteed reversal point, breakout point, support/resistance, or production signal.

Small episode counts must be shown explicitly.

## Outputs

- `episodes.csv`
- `summary.csv`
- `slope_split.csv`
- `report.md`

Target test level:

`TEST_LEVEL: GITHUB_ACTIONS_REPOSITORY_CACHE_STRESS_TEST`
