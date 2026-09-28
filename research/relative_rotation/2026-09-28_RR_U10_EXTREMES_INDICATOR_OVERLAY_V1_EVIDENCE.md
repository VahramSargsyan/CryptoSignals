# RR U10 EXTREMES + INDICATOR OVERLAY V1 — EVIDENCE

Date: 2026-09-28
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Runtime identity

- Frozen universe: `RR_TARGET_U10_CANDIDATE_HBAR_V1`
- Run: `36462593685`
- Source commit: `07cc0d9228bc71f96b5658439f1f5a846b6f27f4`
- Artifact ID: `10987558717`
- Artifact SHA256: `41031266646e8d321ae2a9fd21e784b93f6cc3bc28347d73d27e20998f731096`
- TEST_LEVEL: `GITHUB_ACTIONS_REPOSITORY_CACHE_PLUS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Frozen thresholds

- EXPLOSIVE_MONTH: U10 monthly median return >= +40%
- SEVERE_DIP_MONTH: U10 monthly median return <= -15%

## Extreme months

| Month | Class | U10 | TOTAL MoM | TOTAL state | ETH/BTC MoM | ETH/BTC vs 25 | vs 50 | vs 100 | vs 200 |
|---|---|---:|---:|---|---:|---:|---:|---:|---:|
| 2023-12 | EXPLOSIVE | +40.3% | +15.4% | FULL_BULL | -0.8% | +1.6% | +0.5% | -1.9% | -8.4% |
| 2024-04 | SEVERE_DIP | -20.4% | -16.8% | MIXED | -2.9% | +1.0% | -1.5% | -6.0% | -7.2% |
| 2024-08 | SEVERE_DIP | -15.2% | -11.0% | BEAR_BUILDING | -14.8% | -1.9% | -9.0% | -15.6% | -16.6% |
| 2024-11 | EXPLOSIVE | +252.7% | +44.5% | FULL_BULL | +7.2% | +6.3% | +4.2% | -1.1% | -14.6% |
| 2025-02 | SEVERE_DIP | -15.2% | -20.5% | BEAR_BUILDING | -17.7% | -4.6% | -11.3% | -20.3% | -27.2% |
| 2025-07 | EXPLOSIVE | +49.4% | +14.2% | FULL_BULL | +37.7% | +10.2% | +22.1% | +31.0% | +27.9% |
| 2025-09 | EXPLOSIVE | +51.4% | +3.8% | MIXED | -10.4% | -4.8% | -6.1% | +8.6% | +30.7% |
| 2025-10 | SEVERE_DIP | -30.8% | -5.2% | MIXED | -3.4% | -1.8% | -4.5% | -4.3% | +17.0% |
| 2025-11 | EXPLOSIVE | +45.4% | -16.8% | BEAR_BUILDING | -5.7% | -0.0% | -3.5% | -8.8% | +3.5% |
| 2026-02 | SEVERE_DIP | -16.7% | -13.4% | FULL_BEAR | -5.8% | -0.1% | -6.2% | -9.9% | -15.4% |
| 2026-03 | SEVERE_DIP | -26.9% | +1.9% | FULL_BEAR | +5.1% | +2.5% | +3.9% | -1.7% | -7.3% |
| 2026-08 | EXPLOSIVE | +55.4% | +22.5% | BULL_BUILDING | +6.0% | +2.5% | +4.8% | +10.0% | +7.8% |
| 2026-09 | EXPLOSIVE | +73.6% | +9.4% | BULL_BUILDING | +1.7% | +0.4% | +2.1% | +7.2% | +8.4% |

## Pattern summary

### Explosive U10 months — 7

- TOTAL positive MoM: 85.7% (6/7)
- TOTAL bullish state: 71.4% (5/7)
- ETH/BTC positive MoM: 57.1% (4/7)
- ETH/BTC above SMA25: 71.4% (5/7)
- ETH/BTC above SMA50: 71.4% (5/7)
- ETH/BTC above SMA100: 57.1% (4/7)
- ETH/BTC above SMA200: 71.4% (5/7)

Interpretation:
Broad market expansion is more consistent with explosive U10 months than
ETH/BTC monthly direction alone. ETH/BTC fast-MA strength is common but not required.

Important exceptions:
- 2025-09: U10 +51.4% while ETH/BTC fell -10.4% and TOTAL was MIXED.
- 2025-11: U10 +45.4% while TOTAL fell -16.8% and state was BEAR_BUILDING.

Therefore no single market or ratio state explains all U10 upside.

### Severe U10 dip months — 6

- TOTAL positive MoM: 16.7% (1/6)
- TOTAL bullish state: 0.0% (0/6)
- ETH/BTC positive MoM: 16.7% (1/6)
- ETH/BTC above SMA25: 33.3% (2/6)
- ETH/BTC above SMA50: 16.7% (1/6)
- ETH/BTC above SMA100: 0.0% (0/6)
- ETH/BTC above SMA200: 16.7% (1/6)

Strong common pattern:

`SEVERE_DIP_MONTH -> TOTAL not bullish AND ETH/BTC below SMA100 in 6/6 historical cases`

This is descriptive, not a validated production exit rule.

## Major exit / return milestones

### 2024 loss and return

2024-04-12 — loss of bullish structure:
- TOTAL MIXED, $2.38T
- TOTAL vs SMA50: -1.7%
- vs SMA100: +16.0%
- vs SMA200: +40.2%
- vs SMA300: +58.9%
- ETH/BTC below SMA25/50/100/200

Interpretation:
The first warning appeared while TOTAL was still far above its long SMAs.
Fast structure deteriorated much earlier than SMA200/300.

2024-10-26 — BULL_BUILDING return:
- TOTAL above SMA50/100/200/300
- ETH/BTC still below all SMA25/50/100/200

2024-11-24 — FULL_BULL confirmation:
- TOTAL +31% to +45% above all long SMAs
- ETH/BTC still below SMA25/50/100/200

Interpretation:
The broad market recovered before ETH/BTC had fully turned bullish.
This is why waiting for ETH/BTC confirmation can be late.

### 2025/2026 major deterioration

2025-10-14 — confirmed exit from FULL_BULL:
- TOTAL already below SMA50 (-2.3%) and SMA100 (-0.8%)
- still above SMA200 (+10.4%) and SMA300 (+14.1%)
- ETH/BTC below SMA25/SMA50, but still above SMA100/SMA200

This is a classic early-deterioration configuration:
fast market averages break first while long averages still look healthy.

2025-11-03 — TOTAL below SMA200:
- TOTAL below SMA50/100/200
- still +4.2% above SMA300
- ETH/BTC below SMA25/50/100
- still +11.8% above SMA200

2026-02-19 — FULL_BEAR:
- TOTAL below SMA50/100/200/300 by 18%-32%
- ETH/BTC below SMA25/50/100/200
- full bearish confirmation is therefore very late versus the October/November warnings.

### False recovery then true recovery

2026-05-02 — temporary BULL_BUILDING:
- TOTAL above SMA50/100
- but still below SMA200 (-8.3%) and SMA300 (-18.2%)
- ETH/BTC below every tested SMA25/50/100/200

2026-05-22 — back to MIXED:
- temporary recovery failed.

This is a useful false-recovery example:
fast TOTAL structure alone was insufficient.

2026-08-17 — confirmed exit from FULL_BEAR:
- TOTAL around SMA50, still below SMA100/200/300
- ETH/BTC already above SMA25/50/100/200

2026-08-19 — TOTAL regains SMA200:
- ETH/BTC strongly above all tested SMAs

2026-08-21 — TOTAL regains SMA300:
- TOTAL now above SMA50/100/200/300
- ETH/BTC also above all tested SMAs

2026-08-27 — BULL_BUILDING:
- TOTAL above SMA50 +19.1%, SMA100 +19.5%, SMA200 +14.7%, SMA300 +4.4%
- ETH/BTC above SMA25 +3.2%, SMA50 +5.3%, SMA100 +10.2%, SMA200 +7.6%

2026-09-26:
- BULL_BUILDING remains active
- TOTAL above all four long averages
- ETH/BTC above SMA25/50/100/200

## Frozen interpretation

The strongest historical **risk** signature is clearer than the strongest
upside signature.

### Upside
Explosive U10 months often coincide with:
- rising TOTAL;
- bullish TOTAL state;
- ETH/BTC above fast MAs.

But there are meaningful exceptions.

### Downside
Severe U10 months consistently coincided with:
- TOTAL not in bullish state;
- ETH/BTC below SMA100.

Frozen research label:

`TOTAL_NOT_BULL + ETHBTC_BELOW_SMA100 = HIGH_ATTENTION_RISK_PATTERN`

This is not production-approved and is based on only 6 severe monthly events.

## No-repeat / production boundary

Do not rerun unchanged on the same historical window.

No live/paper, Telegram, exchange, allocation, or execution behavior changed.
