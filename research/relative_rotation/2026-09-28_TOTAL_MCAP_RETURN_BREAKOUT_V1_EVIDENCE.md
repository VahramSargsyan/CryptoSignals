# TOTAL MARKET CAP RETURN vs BREAKOUT V1 — EVIDENCE

Date: 2026-09-28  
Workflow mode: STRESS_TEST_ONLY  
Production/live changes: NONE

## Runtime / source

Canonical repository cache only:

- file: `data/market/cryptocap_total_d1.csv`
- symbol: `CRYPTOCAP:TOTAL`
- provider: TradingView
- timeframe: 1D
- rows: 1,398
- range: 2022-11-29 through 2026-09-26
- canonical SHA256 from repository metadata: `d0c9b3bac915e484069c6302ddce1a0b050abbb8de155e4106e39cf6e519d3ee`

No external market-cap source was used in the executed analysis.

Preregistered rules are in:

`research/relative_rotation/2026-09-28_TOTAL_MCAP_RETURN_BREAKOUT_V1_PREREG.md`

## Event semantics

For SMA25 / SMA50 / SMA100, distance is:

`TOTAL_close / SMA - 1`

Thresholds:

`10%, 15%, 20%, 25%, 30%, 40%`

Upside and downside are separate.

One prolonged threshold excursion is de-duplicated until the gap compresses to <= half of that threshold.

At the trigger close, with directional gap `G`:

- RETURN50: gap shrinks to <= `0.50 * G`
- BREAKOUT25: gap expands to >= `1.25 * G`

The first barrier reached within 90 calendar days defines RETURN_FIRST or BREAKOUT_FIRST.

This is a research convention, not a production signal.

Total threshold-level observations across all SMA / thresholds / directions: **144**.

These 144 observations are not 144 fully independent market cycles because higher thresholds can occur inside the same broad move.

## Upside excursions

| SMA | Trigger | N | Return first | Breakout first | Unresolved | Median extra extension | Median days to half-gap | Median days to SMA touch |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 25 | +10% | 13 | 69.2% | 30.8% | 0.0% | +4.77 pp | 11 | 28 |
| 25 | +15% | 5 | 80.0% | 20.0% | 0.0% | +2.74 pp | 16 | 26 |
| 25 | +20% | 2 | 100.0% | 0.0% | 0.0% | +1.37 pp | 16 | 28.5 |
| 50 | +10% | 13 | 61.5% | 38.5% | 0.0% | +6.75 pp | 21 | 45 |
| 50 | +15% | 8 | 37.5% | 50.0% | 12.5% | +5.36 pp | 22 | 45.5 |
| 50 | +20% | 4 | 75.0% | 25.0% | 0.0% | +2.37 pp | 20.5 | 45.5 |
| 50 | +25% | 2 | 50.0% | 50.0% | 0.0% | +6.52 pp | 28.5 | 45 |
| 50 | +30% | 2 | 50.0% | 50.0% | 0.0% | +4.44 pp | 21.5 | 38 |
| 100 | +10% | 13 | 46.2% | 53.8% | 0.0% | +9.15 pp | 12 | 26 |
| 100 | +15% | 8 | **0.0%** | **100.0%** | 0.0% | +7.75 pp | 38 | 77 |
| 100 | +20% | 8 | 50.0% | 50.0% | 0.0% | +3.11 pp | 34 | 70 |
| 100 | +25% | 4 | **0.0%** | **75.0%** | 25.0% | +14.55 pp | 60 | 75 |
| 100 | +30% | 3 | 33.3% | 66.7% | 0.0% | +12.21 pp | 44 | 74 |
| 100 | +40% | 2 | 100.0% | 0.0% | 0.0% | +4.64 pp | 30 | 66.5 |

There were no +25% or larger SMA25 upside triggers in the cached period and no +40% SMA50 upside trigger.

## Downside excursions

| SMA | Trigger | N | Return first | Breakout first | Median extra extension | Median days to half-gap | Median days to SMA touch |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 25 | -10% | 10 | 60.0% | 40.0% | +4.27 pp | 6.5 | 13.5 |
| 25 | -15% | 3 | 66.7% | 33.3% | 0.00 pp | 8 | 26 |
| 25 | -20% | 1 | 100.0% | 0.0% | 0.00 pp | 8 | 25 |
| 25 | -25% | 1 | 100.0% | 0.0% | 0.00 pp | 8 | 25 |
| 50 | -10% | 11 | 54.5% | 45.5% | +5.76 pp | 6 | 30 |
| 50 | -15% | 7 | 71.4% | 28.6% | +2.40 pp | 10 | 39 |
| 50 | -20% | 2 | 100.0% | 0.0% | +3.59 pp | 15.5 | 40 |
| 50 | -25% | 1 | 100.0% | 0.0% | 0.00 pp | 20 | 38 |
| 100 | -10% | 9 | 33.3% | 66.7% | +5.39 pp | 19 | 43 |
| 100 | -15% | 7 | 57.1% | 42.9% | +5.83 pp | 39 | 58.5 |
| 100 | -20% | 3 | 66.7% | 33.3% | +8.39 pp | 39 | 60 |
| 100 | -25% | 1 | 100.0% | 0.0% | 0.00 pp | 27 | 70 |
| 100 | -30% | 1 | 100.0% | 0.0% | 0.00 pp | 27 | 70 |

High-threshold downside samples are too small for strong inference.

## The main finding: a large upside gap does NOT imply immediate price reversal

The clearest result is SMA100.

At the first +15% distance from SMA100:

- 8 episodes
- **8 / 8 hit the continuation barrier before the half-gap return barrier**
- median additional extension after trigger: **+7.75 percentage points**
- median time until the gap finally halved: **38 days**
- median time until TOTAL touched SMA100: **77 days**

At +25% from SMA100:

- 4 episodes
- 0 RETURN_FIRST
- 3 BREAKOUT_FIRST
- 1 unresolved/censored within the 90d competition window
- median additional extension: **+14.55 percentage points**

At +30%:

- 3 episodes
- 2 BREAKOUT_FIRST
- 1 RETURN_FIRST
- median additional extension: **+12.21 percentage points**

Only at +40% did both observed episodes return first, but N=2 is far too small to call +40% a stable reversal threshold.

Therefore the cache does not support a simple rule such as:

`far from SMA100 -> imminent reversal`

In the strongest historical upside regimes, 15-30% above SMA100 often represented continuation before normalization.

## "Does price return, or does the SMA catch price?"

For upside RETURN50 episodes, convergence attribution frequently favored the SMA catching up.

### SMA25 upside

- +10%: SMA catch-up dominant in **84.6%** of RETURN50 episodes
- +15%: **100%**
- +20%: **100%**

### SMA50 upside

- +10%: SMA catch-up dominant in **61.5%**
- +15%: **85.7%**
- +20% and above in the observed return episodes: **100%**

### SMA100 upside

- +15%: SMA catch-up dominant in **71.4%** of episodes that eventually achieved RETURN50
- +20%: **71.4%**
- +25%, +30%, +40%: **100%** among observed RETURN50 episodes

This supports the user's original intuition: during strong upside trends, "normalization" is often not a crash back to the moving average. The moving average can rise toward the market while TOTAL remains elevated.

This is accounting attribution of log-gap closure, not proof of causality.

## Large historical continuation examples

The largest additional extension observed after a threshold trigger included:

- 2024-02-09, SMA100 +10% trigger: trigger gap +13.25%, later max +50.41%, additional extension **+37.15 pp**
- 2024-10-29, SMA100 +10% trigger: trigger gap +12.09%, later max +46.66%, additional extension **+34.57 pp**
- 2024-02-12, SMA100 +15% trigger: trigger gap +18.00%, later max +50.41%, additional extension **+32.41 pp**
- 2024-11-06, SMA100 +15% trigger: trigger gap +15.82%, later max +46.66%, additional extension **+30.85 pp**

This is why distance alone is insufficient.

## Current cached point — 2026-09-26

Latest cached close:

- TOTAL: **$2.868T**
- SMA25: **$2.704T**
- distance from SMA25: **+6.07%**
- SMA25 5d slope: **+1.98%**

- SMA50: **$2.553T**
- distance from SMA50: **+12.34%**
- SMA50 5d slope: **+2.79%**

- SMA100: **$2.353T**
- distance from SMA100: **+21.90%**
- SMA100 5d slope: **+1.42%**

SMA stack:

`SMA25 > SMA50 > SMA100`

Fan spread across SMA25/50/100 relative to SMA100:

**14.92%**

Descriptive state:

`BULL_STACK / TOTAL materially extended above SMA100 while all three tested SMAs are rising`

This is not a sell signal.

## First interpretation

There is no single reliable "return point" visible from distance alone in this short cache.

The strongest provisional distinction is:

- fast SMA25 stretch tends to normalize relatively quickly, often because SMA25 catches up;
- SMA100 +15% to +30% is historically compatible with continued acceleration and should not be treated as automatic overheat;
- very large +40% SMA100 stretch may be closer to a return zone, but only two observations exist;
- the next useful discriminator is the **shape of the SMA fan / slope state at trigger**, not another distance threshold.

## Boundary

Do not promote any threshold from this file into paper/live, Telegram, allocation, or execution logic.

TEST_LEVEL: EXECUTED_ANALYSIS / REPOSITORY_CACHE
