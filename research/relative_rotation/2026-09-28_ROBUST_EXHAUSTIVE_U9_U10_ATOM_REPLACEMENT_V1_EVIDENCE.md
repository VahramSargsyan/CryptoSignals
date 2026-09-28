# ROBUST EXHAUSTIVE U9/U10 ATOM REPLACEMENT V1 — EVIDENCE

Date: 2026-09-28  
Workflow mode: STRESS_TEST_ONLY  
Production/live changes: NONE

## Runtime identity

- GitHub Actions run: 36401115596
- Source commit: `09f9e353a47fc65b9e8e8af0b71149fd047d7bf9`
- Artifact ID: `10960970166`
- Artifact: `robust-exhaustive-u9-u10-atom-replacement-v1-1`
- Artifact SHA256: `fb3cdb86e74aebf7d26378dcd9c54f139e79d2f7f89b29393b1444e729818e9a`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Data / fixed mechanics

- Source: Binance Spot daily OHLCV through the repository's existing historical adapter
- Common panel: 1,241 daily rows
- Common panel start: 2023-05-05
- Common panel end: 2026-09-26
- Pool: ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, AVAX, FIL, ETH, ALGO, ADA, XRP, HBAR
- Lookback: 180 days
- ARM: 15%
- Reversal: 3%
- Modeled transition cost: 0.1%
- Execution: confirmed signal at T -> next-day open
- Conflict routing: strongest max_dislocation, deterministic tie-break

Primary exhaustive windows:
- LAST_1Y: 2025-09-27 -> 2026-09-26
- LAST_2Y: 2024-09-27 -> 2026-09-26
- MATURE: 2023-10-31 -> 2026-09-26

## Exhaustive coverage

- U9: 5,005 / 5,005 universes
- U10: 3,003 / 3,003 universes
- Total: 8,008 universes
- Every universe evaluated from every possible start asset inside that universe.
- Finalist diagnostics: Top 30 U9 + Top 30 U10 received rolling 12m, rolling 24m, shifted endpoint and broad-bull-neutralized checks.

## Robust Top U9

| Rank | Universe | 1Y | 2Y | Mature | Mature median DD |
|---:|---|---:|---:|---:|---:|
| 1 | TWT, PEPE, SOL, AAVE, LINK, AVAX, FIL, ALGO, HBAR | +184.4% | +1555.1% | +4881.4% | -62.4% |
| 2 | TWT, PEPE, BNB, SOL, AAVE, AVAX, FIL, ALGO, HBAR | +174.9% | +2002.1% | +4396.1% | -62.4% |
| 3 | TWT, PEPE, SOL, TRX, AAVE, AVAX, FIL, ALGO, HBAR | +169.8% | +1492.5% | +4631.2% | -62.4% |
| 4 | TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, HBAR | +166.1% | +1935.1% | +6904.0% | -62.4% |
| 5 | TWT, PEPE, TRX, AAVE, LINK, AVAX, FIL, ALGO, HBAR | +168.6% | +1433.2% | +4605.5% | -62.4% |

## Robust Top U10

| Rank | Universe | 1Y | 2Y | Mature | Mature median DD |
|---:|---|---:|---:|---:|---:|
| 1 | TWT, PEPE, BNB, SOL, TRX, AAVE, AVAX, FIL, ALGO, HBAR | +166.7% | +1954.0% | +6837.0% | -62.4% |
| 2 | TWT, PEPE, SOL, TRX, AAVE, LINK, AVAX, FIL, ALGO, HBAR | +169.2% | +1458.3% | +4618.4% | -62.4% |
| 3 | TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR | +155.7% | +1850.9% | +3167.3% | -62.4% |
| 4 | TWT, PEPE, BNB, SOL, AAVE, AVAX, FIL, ALGO, XRP, HBAR | +166.2% | +1915.1% | +2661.1% | -62.4% |
| 5 | TWT, PEPE, BNB, SOL, TRX, AAVE, FIL, ALGO, XRP, HBAR | +128.5% | +1659.9% | +2519.8% | -59.7% |

## Matched token marginal contribution

Each token was evaluated in 2,002 exact matched contexts:
U10 containing token X versus the same U9 with X removed.

| Token | 1Y median delta | 1Y positive contexts | 2Y median delta | 2Y positive contexts | Mature median delta | Mature positive contexts |
|---|---:|---:|---:|---:|---:|---:|
| PEPE | +31.3 pp | 92.8% | +161.1 pp | 88.8% | +301.5 pp | 93.1% |
| AAVE | +36.1 pp | 93.1% | +102.8 pp | 94.3% | +163.7 pp | 95.9% |
| AVAX | +3.0 pp | 82.0% | +30.8 pp | 82.9% | +100.0 pp | 84.5% |
| FIL | +68.5 pp | 100.0% | +51.3 pp | 64.0% | +89.7 pp | 64.0% |
| HBAR | +0.1 pp | 51.6% | +64.4 pp | 89.4% | +89.6 pp | 78.3% |
| ALGO | +9.4 pp | 97.5% | -0.0 pp | 41.4% | +27.2 pp | 71.4% |
| ATOM | -4.6 pp | 42.9% | -135.0 pp | 18.5% | -264.3 pp | 11.3% |
| XRP | +0.1 pp | 56.8% | -0.1 pp | 33.8% | -297.3 pp | 20.7% |

Important interpretation:
- ATOM is not merely absent from recent winners; in exact matched contexts it is strongly negative on the 2Y and MATURE horizons.
- PEPE and AAVE have the strongest broad positive matched contribution across all three horizons.
- AVAX is positive in >82% of matched contexts on every horizon.
- FIL is positive in 100% of matched 1Y contexts and remains positive in median on longer horizons.
- HBAR is close to neutral at 1Y but strongly positive on 2Y/MATURE.
- ALGO is extremely consistent on the latest 1Y but much less uniform at 2Y.
- XRP's recent neutrality does not survive the MATURE matched test.

## Reference composition ranks

### Previous ATOM-out U9 research baseline
TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

Robust rank: **1141 / 5005**

This shows that the prior "ATOM OUT -> NOTHING IN" test was too conditional on a fixed U9 base. Once the full U9 space is allowed to move, that base is not a robust optimum.

### Current main target U9
TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP

Robust rank: **27 / 5005**

Primary metrics:
- 1Y median: +156.3%
- 2Y median: +1180.5%
- MATURE median: +1678.6%
- median DD across these primary windows: -62.4%

Finalist diagnostics:
- rolling 12m median return: +81.8%
- rolling 12m worst window: -11.5%
- rolling 12m positive-window rate: 91.3%
- rolling 24m median return: +268.9%
- rolling 24m worst window: +105.1%
- rolling 24m positive-window rate: 100%
- shifted 1Y endpoint median: +37.7%
- shifted 1Y endpoint worst: +14.5%
- shifted endpoint positive rate: 100%
- bull-neutralized MATURE median: +8.3%
- bull-neutralized worst start: -2.8%
- bull-neutralized positive-start rate: 77.8%

### Current target U9 + HBAR
TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR

Robust U10 rank: **3 / 3003**

Primary metrics:
- 1Y median: +155.7% (essentially unchanged vs current U9)
- 2Y median: +1850.9%
- MATURE median: +3167.3%
- median DD: -62.4%

Finalist diagnostics:
- rolling 12m median return: +150.7%
- rolling 12m worst window: -11.4%
- rolling 12m positive-window rate: 95.7%
- rolling 24m median return: +563.0%
- rolling 24m worst window: +268.6%
- rolling 24m positive-window rate: 100%
- shifted 1Y endpoint median: +38.4%
- shifted 1Y endpoint worst: +16.3%
- shifted endpoint positive rate: 100%
- bull-neutralized MATURE median: +32.2%
- bull-neutralized worst start: +17.7%
- bull-neutralized positive-start rate: 100%

Historical matched change versus current target U9:
- 1Y median: -0.5 pp
- 2Y median: +670.4 pp
- MATURE median: +1488.7 pp
- MATURE median DD: unchanged

This is the strongest directly actionable discovery of this pass, but it is still a post-selection historical discovery, not production approval.

## Best-ranked U9 versus best-ranked U10

Best robust U9:
TWT, PEPE, SOL, AAVE, LINK, AVAX, FIL, ALGO, HBAR

- rolling 12m median: +104.6%
- rolling 12m worst: +17.7%
- rolling 24m median: +469.2%
- rolling 24m worst: +128.8%
- endpoint median: +82.8%
- bull-neutral MATURE median: +22.9%
- bull-neutral worst start: +8.4%

Best robust U10:
TWT, PEPE, BNB, SOL, TRX, AAVE, AVAX, FIL, ALGO, HBAR

- rolling 12m median: +150.8%
- rolling 12m worst: +12.0%
- rolling 24m median: +648.9%
- rolling 24m worst: +268.6%
- endpoint median: +46.4%
- bull-neutral MATURE median: +75.0%
- bull-neutral worst start: +54.4%

The U10 finalist is stronger on median rolling returns and bull-neutralized history; the U9 finalist has the stronger shifted-1Y endpoint median and slightly stronger worst rolling-12m window. There is no clean risk-free "U10 always wins" conclusion.

## Top-50 frequency

U9:
- FIL 50/50
- PEPE 49/50
- ALGO 48/50
- HBAR 46/50
- AVAX 42/50
- TWT 42/50
- AAVE 35/50
- ATOM 5/50

U10:
- FIL 50/50
- ALGO 49/50
- TWT 48/50
- AVAX 46/50
- HBAR 46/50
- AAVE 42/50
- PEPE 42/50
- ATOM 6/50

Frequency is secondary evidence only; matched marginal contribution is more informative because it compares the same network with and without each token.

## Research conclusion

1. **ATOM OUT is materially strengthened.**
   ATOM shows negative median marginal contribution at 1Y, 2Y and MATURE, with especially weak 2Y/MATURE positive-context rates.

2. **The previous fixed U9 baseline is not supported by the full-space test.**
   Its rank is 1141/5005.

3. **The current main target U9 is historically strong but not the exhaustive optimum.**
   Its robust rank is 27/5005.

4. **HBAR is the most important immediate re-check for the current target U9.**
   Adding HBAR produces U10 rank 3/3003 and strongly improves the 2Y/MATURE/rolling/bull-neutral diagnostics with almost unchanged latest-1Y median return.

5. **Do not promote the discovered U10 directly from this backtest.**
   The U10+HBAR hypothesis was identified from known historical data. It needs a separate frozen confirmatory/forward-validation stage before changing the production/paper-live target.

## Production boundary

No files under the live/paper-live configuration were changed by this experiment.

Current `main` already contains `RELATIVE_ROTATION_TARGET_U9_TRANSITION_V1` from the previously merged migration patch. This stress test does not approve, revert, or modify that production state.

## Residual risks

- All ranking and matched-marginal evidence is historical and subject to selection/data-snooping risk.
- The finalist diagnostics overlap the same available historical dataset; they are robustness checks, not unseen out-of-sample evidence.
- The common panel begins in 2023 because all 15 selected assets must have common history.
- Modeled cost is 0.1% per transition and does not separately model time-varying spread/slippage/liquidity.
- Very large compounded historical returns amplify sensitivity to path assumptions.
- A separate frozen forward/confirmatory test is required before any production target change.
