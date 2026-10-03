# Current U10 vs U11 = U10 + LINK V1 — EVIDENCE

Date: 2026-10-03
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Runtime

- GitHub Actions run: `37141904027`
- Source commit: `dc44869845cf57fc814d303b1e67968c3188a273`
- Artifact ID: `11280323480`
- Artifact SHA256: `e68113eadd479f9d571ddb516338214e66f65aef35e21651926b1ddf9c2535c1`

## Compared universes

Current U10:

`TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR`

Candidate U11:

`TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR, LINK`

Rules:
- 180D rolling median
- ARM 15%
- reversal 3%
- destination dominance 1.5x
- cost 0.1%
- close-T signal -> next daily open execution

Primary comparison uses the same ten start assets in both universes.

## Results

| Window | U10 median | U11 median, same 10 starts | Delta | U11 better starts | U10 median DD | U11 median DD |
|---|---:|---:|---:|---:|---:|---:|
| Last 1Y | +122.0% | +122.0% | 0.0 pp | 0/10 | -64.0% | -64.0% |
| Last 2Y | +2332.7% | +2332.7% | 0.0 pp | 0/10 | -62.4% | -62.4% |
| Mature | +3592.8% | +3592.8% | 0.0 pp | 0/10 | -62.4% | -62.4% |

Rolling windows:
- 12m median: U10 +143.6%, U11 +144.3%
- 12m worst: both +12.4%
- 24m median: both +622.8%
- 24m worst: both +268.6%

Starting specifically from LINK under U11:
- Last 1Y: +169.2%
- Last 2Y: +2339.7%
- Mature: +3563.9%

## Interpretation

Adding LINK as an eleventh eligible destination did not alter any of the ten main endpoint paths over the fixed 1Y, 2Y or Mature windows. Under the current topology and destination-dominance rule, LINK did not displace the selected destinations on those paths.

The slight rolling-12m median improvement shows LINK can matter in some shifted-start windows, but the effect is small and not broad enough to claim historical dominance.

This result does not support replacing the frozen production U10 with U11 on historical return evidence alone. It does support keeping U11+LINK as a clean shadow/forward candidate if desired.

## Boundary

This U11 test was proposed after LINK had already been singled out, so any improvement is post-selection historical evidence.
No production config, Telegram routing or execution behavior changed.
