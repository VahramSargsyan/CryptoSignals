# RR U10 BTC/ETH RATIO SMA CROSS V1 — EVIDENCE

Date: 2026-09-28  
Workflow mode: STRESS_TEST_ONLY  
Production/live changes: NONE

## Runtime identity

- Frozen universe: `RR_TARGET_U10_CANDIDATE_HBAR_V1`
- GitHub Actions run: `36444297651`
- Source commit: `3ba7a9165e62f5c1117a31da5e0e38d273076741`
- Artifact ID: `10980120912`
- Artifact: `rr-u10-btceth-ratio-sma-cross-v1-1`
- Artifact SHA256: `5208a49e1deaf4f0be20c8515e41b75b9942c3780b0cb82bd9f4167547012bb4`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Ratio

`BTC_ETH_RATIO = BTCUSDT / ETHUSDT`

Interpretation:

- ratio falling = ETH outperforming BTC;
- ratio rising = BTC outperforming ETH.

This is the inverse of ETH/BTC.

## Coverage

- evaluation: 2023-10-31 through 2026-09-26
- SMA25 / SMA50 / SMA100 / SMA200
- 232 raw crossover events
- 130 crosses persisted on the crossed side for the next 3 daily bars
- U10 normalized to 100 USDT at every event
- horizons: 7 / 14 / 30 / 60 / 90 days
- all 10 frozen U10 start states

## Main conclusion

Frozen descriptive classification:

`MIXED_BUT_USEFUL_AS_LAYERED_ROTATION_SIGNAL`

The simple theory:

`BTC/ETH falls => altseason => U10 rises`

is too strong.

The data supports a narrower interpretation:

1. ETH strength versus BTC on **fast/medium** ratio averages can be a useful alt-rotation attention signal.
2. ETH strength versus BTC on very long averages is not automatically bullish for U10.
3. BTC strength versus ETH above the **SMA200** is a materially more interesting U10 risk warning than fast BTC-strength crosses.
4. No single BTC/ETH crossover should be treated as an automatic U10 entry or exit rule.

## ETH-strength events — BTC/ETH crosses below SMA

### SMA25

Raw events:

| Horizon | Events | Median U10 | 100 USDT -> | Positive |
|---|---:|---:|---:|---:|
| 7d | 53 | +1.1% | 101.06 | 52.8% |
| 14d | 53 | +3.3% | 103.29 | 58.5% |
| 30d | 52 | +6.2% | 106.20 | 57.7% |
| 60d | 50 | +16.1% | 116.06 | 62.0% |
| 90d | 50 | +26.7% | 126.66 | 60.0% |

3-day-persistent events:

| Horizon | Events | Median U10 | 100 USDT -> | Positive |
|---|---:|---:|---:|---:|
| 7d | 26 | +1.8% | 101.80 | 61.5% |
| 14d | 26 | +3.2% | 103.19 | 57.7% |
| 30d | 25 | **+14.6%** | **114.58** | **64.0%** |
| 60d | 23 | +7.0% | 107.04 | 52.2% |
| 90d | 23 | +14.9% | 114.89 | 60.9% |

Interpretation:

`BTC/ETH below SMA25 and staying there for 3 days`

is a plausible **early alt-rotation attention signal**, especially on the 30-day horizon, but still far from deterministic.

### SMA50

Raw events:

- 7d: -0.1%
- 14d: +2.3%
- 30d: **+14.9%**, positive 75.9%
- 60d: +11.4%
- 90d: +5.3%

This is the strongest raw 30-day ETH-strength result.

3-day-persistent SMA50 events were weaker:
- 30d +4.2%
- 60d +2.9%
- 90d -2.9%

So persistence did not monotonically improve the signal.

### SMA100

ETH-strength crosses below SMA100 were not useful long-horizon U10 signals:

- 30d: +1.8%
- 60d: -10.9%
- 90d: -7.7%

3-day persistent:
- 30d +1.2%
- 60d -10.3%
- 90d -8.6%

### SMA200

ETH-strength crosses below SMA200 were mixed:

- 7d +3.9%
- 14d +2.2%
- 30d -4.2%
- 60d +2.6%
- 90d -6.7%

Therefore:

`BTC/ETH falling below a long-term average != automatic altseason`.

## BTC-strength events — BTC/ETH crosses above SMA

Fast BTC-strength crosses were not reliable U10 exit signals.

### SMA25

Raw:
- 30d +9.6%
- 60d +20.9%
- 90d +31.6%

Persistent:
- 30d +6.1%
- 60d +14.6%
- 90d +31.6%

So a simple rise of BTC/ETH above SMA25 does not imply that U10 should be exited.

### SMA50

Mostly mixed/positive:
- raw 30d +11.4%
- raw 60d +8.2%
- raw 90d +3.4%

Persistent 90d moved to -0.9%, but not strongly enough to be an exit rule.

### SMA100

More fragile:
- raw 60d -12.0%
- raw 90d -8.6%

Persistent:
- 60d +3.6%
- 90d approximately flat

Still inconsistent.

### SMA200 — strongest U10 risk warning

Raw BTC-strength cross above SMA200:

| Horizon | Events | Median U10 | 100 USDT -> | Positive |
|---|---:|---:|---:|---:|
| 7d | 9 | +1.6% | 101.57 | 66.7% |
| 14d | 9 | -2.6% | 97.43 | 44.4% |
| 30d | 9 | -3.7% | 96.32 | 22.2% |
| 60d | 9 | **-11.3%** | **88.75** | 22.2% |
| 90d | 8 | **-11.6%** | **88.39** | 12.5% |

3-day-persistent BTC-strength above SMA200:

| Horizon | Events | Median U10 | 100 USDT -> | Positive |
|---|---:|---:|---:|---:|
| 7d | 7 | +0.4% | 100.41 | 57.1% |
| 14d | 7 | -3.3% | 96.67 | 28.6% |
| 30d | 7 | -3.7% | 96.32 | 28.6% |
| 60d | 7 | **-11.3%** | **88.75** | 28.6% |
| 90d | 6 | **-18.4%** | **81.56** | **16.7%** |

This is the cleanest mirror signal in this study.

Frozen descriptive interpretation:

`BTC/ETH > SMA200 after upward cross = U10 RISK ATTENTION`

Not automatic exit, because:
- event count is only 7 persistent cases;
- this is historical/post-selection evidence;
- a production rule has not been tested.

## Relationship to the altseason theory

BTC/ETH falling is equivalent to ETH/BTC rising.

That is consistent with the common market-rotation narrative in which ETH begins outperforming BTC before or during broader alt strength.

But this test shows:

- the relationship is not monotonic across all moving averages;
- fast/medium ETH-strength crosses are more useful for U10 than long-term ETH-strength crosses;
- the strongest downside warning for U10 came from the **opposite direction** at the long SMA200: BTC regaining relative strength over ETH.

Therefore BTC/ETH should be treated as a **rotation/regime layer**, not the formal definition of altseason.

## Candidate attention hierarchy

### Alt-rotation attention

1. BTC/ETH crosses below SMA25
2. 3-day persistence below SMA25 strengthens the 30d observation
3. BTC/ETH crossing below SMA50 is another useful 30d attention event

### U10 risk attention

1. BTC/ETH crosses above SMA200
2. 3-day persistence above SMA200 strengthens concern
3. do not use fast upward crosses alone as U10 exits

## Next distinct hypothesis

A stronger combined indicator could test:

- TOTAL market-cap regime from SMA25/50/100/200/300
- BTC/ETH ratio regime
- U10 state

Example research question:

`Does U10 perform best when TOTAL is BULL_BUILDING/FULL_BULL while BTC/ETH is below SMA25/SMA50, and deteriorate when TOTAL is BEAR_BUILDING/FULL_BEAR while BTC/ETH is above SMA200?`

This requires separate preregistration and must not be inferred from the current test.

## No-repeat / production boundary

Do not rerun this exact crossover study unchanged on the same history.

A repeat requires:
- unseen dates;
- changed ratio/crossover semantics;
- changed U10 strategy/universe;
- or a separately preregistered combined-regime / forward-validation test.

No paper/live, Telegram, exchange, allocation, or execution behavior changed.
