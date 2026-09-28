# RR U10 PURE BTC SMA200 REGIME V1 — EVIDENCE

Date: 2026-09-28
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Runtime identity
- Frozen universe: `RR_TARGET_U10_CANDIDATE_HBAR_V1`
- GitHub Actions run: `36432404601`
- Source commit: `bd131dcdc045467ffb4ddf2b6603365a6cd35951`
- Artifact ID: `10973937792`
- Artifact: `rr-u10-pure-btc-sma200-regime-v1-1`
- Artifact SHA256: `9e908e788863408f76c1505ff17ddee739e2b22660dd6055f709f3d7f9c39507`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Question
As a simple alternative to the H8 SMA50/SMA200 trend definition, what happens to the frozen U10 when BTC is simply above or below its trailing 200-day SMA?

## Frozen strategy
`TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR`

MATURE: 2023-10-31 -> 2026-09-26.

Core semantics unchanged:
- Binance Spot D1
- 180d rolling median
- ARM 15%
- reversal 3%
- confirmed T -> next-open
- strongest max-dislocation routing
- 0.1% physical transition cost
- all 10 starting assets

## Regime
- `BTC_ABOVE_SMA200`: BTC close >= trailing SMA200
- `BTC_BELOW_SMA200`: BTC close < trailing SMA200

No SMA50 confirmation and no tuned thresholds.

## Result

| Regime | Days | Episodes | U10 median conditioned return | Worst start | Positive starts | Conditioned DD | Median episode | Positive episodes | BTC same days |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| BTC_ABOVE_SMA200 | 661 | 15 | **+4454.9%** | +3471.9% | 100% | -47.1% | +6.7% | 93.3% | +456.0% |
| BTC_BELOW_SMA200 | 401 | 14 | **-28.3%** | -28.3% | 0% | -62.4% | -3.3% | 42.9% | -55.9% |

## Calendar check

### BTC_ABOVE_SMA200
- 2023 MATURE partial: +80.1%
- 2024: +291.5%
- 2025: +124.6%
- 2026 through cutoff: +187.7%

### BTC_BELOW_SMA200
- 2024: -17.1%
- 2025: -14.6%
- 2026 through cutoff: +1.3%

## Interpretation

The simple SMA200 split independently confirms the earlier regime result.

The U10 is historically very strong when BTC is above SMA200 and loses money when BTC is below SMA200.

Below SMA200 it historically loses substantially less than BTC on the same days:
- U10: -28.3%
- BTC: -55.9%

That is relative resilience, not bear-market profitability.

Frozen conclusion:

`PURE_SMA200_CONFIRMS_BULL_DEPENDENCE`

## Relation to H8 50/200 result

Earlier H8-style BTC regime:
- BTC_BULL: U10 +1257.2%
- BTC_BEAR: U10 -15.4%

Pure SMA200:
- above: +4454.9%
- below: -28.3%

The magnitudes differ because day classification differs, but the sign conclusion is unchanged:
- positive bull regime;
- negative bear regime.

## No-repeat rule
Do not rerun unchanged on the same history.

A rerun requires new unseen data, changed regime semantics, documented engine correction, changed universe/cost assumptions, or separate forward/defensive-overlay validation.

## Production boundary
No live/paper/Telegram/exchange behavior changed.
