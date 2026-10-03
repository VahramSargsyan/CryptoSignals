# RR U10 + Binance Flexible Earn V1 — EVIDENCE

Date: 2026-10-04
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Runtime identity

- GitHub Actions run: `37153890097`
- Source commit: `35e4dda272dcc76fbac6e79b237e75d418373187`
- Artifact ID: `11285102437`
- Artifact SHA256: `59a93d3fb74640398d9bff3ac3d4a7c10c8e20247a8ca1667ed3fac42aa4f4eb`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_PRICE_DATA + FROZEN_APR_SENSITIVITY`

## Frozen APR assumptions

| Asset | APR |
|---|---:|
| TRX | 3.1500% |
| ALGO | 1.3011% |
| TWT | 0.6337% |
| AVAX | 0.3691% |
| FIL | 0.3299% |
| AAVE | 0.1899% |
| PEPE | 0.1437% |
| HBAR | 0.0687% |
| BNB | 0.0457% |
| XRP | 0.0431% |

TRX uses the user's Binance screenshot from 2026-10-04.
The other nine assets use the machine-readable Binance Flexible Earn snapshot available to the research run, updated 2026-04-21T13:04:50Z.

These APRs are intentionally frozen and applied backward only as a sensitivity layer. This is not a reconstruction of historical Binance APRs.

## RR mechanics

- U10: TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR
- 180D rolling median
- ARM: 15%
- reversal: 3%
- destination dominance: 1.5x
- transition cost: 0.1%
- confirmed close T -> next daily open execution
- Earn does not alter RR signals or routing
- daily Earn approximation: quantity *= 1 + APR / 365 for each held day
- rewards remain in the same asset and compound
- subscription/redemption delay modeled as zero
- BNB Launchpool/HODLer/Megadrop extra rewards excluded

## Results

| Window | Pure RR median | RR + Earn median | Earn lift vs final equity | Annualized Earn-only lift | 30d equivalent |
|---|---:|---:|---:|---:|---:|
| 1Y | +122.0% | +123.0% | +0.43% | +0.426% | +0.035% |
| 2Y | +2332.7% | +2349.3% | +0.69% | +0.343% | +0.028% |
| Mature | +3592.8% | +3630.8% | +1.02% | +0.348% | +0.029% |

Mature period: 2023-10-31 -> 2026-10-02, 1068 days.

## Mature pooled holding allocation

| Asset | Holding share | APR | APR contribution |
|---|---:|---:|---:|
| FIL | 19.23% | 0.3299% | 0.0634% |
| TWT | 18.37% | 0.6337% | 0.1164% |
| PEPE | 18.06% | 0.1437% | 0.0260% |
| XRP | 14.78% | 0.0431% | 0.0064% |
| BNB | 9.04% | 0.0457% | 0.0041% |
| HBAR | 7.71% | 0.0687% | 0.0053% |
| AVAX | 4.66% | 0.3691% | 0.0172% |
| AAVE | 3.88% | 0.1899% | 0.0074% |
| TRX | 2.55% | 3.1500% | 0.0802% |
| ALGO | 1.72% | 1.3011% | 0.0224% |

Pooled Mature time-weighted APR: **0.349%**.
Median per-start time-weighted APR: **0.348%**.

## Key observations

1. The simple median of the ten APRs understates the RR-specific yield slightly. Actual RR time allocation raises the effective estimate to about 0.35%/year.
2. TWT is the largest APR contributor in the Mature sample because it combines meaningful yield with 18.37% holding time.
3. TRX is held only 2.55% of pooled Mature days but contributes about 0.0802 percentage points to annual APR because its frozen APR is 3.15%.
4. FIL is held most often (19.23%) and contributes about 0.0634 percentage points.
5. The staking layer is small versus RR trading alpha, but it is positive carry on capital that would otherwise be idle between rotations.
6. Over 1068 days the frozen Earn layer increases final equity by about 1.02% relative to the same RR path without Earn.
7. Because RR capital itself compounds strongly in the historical simulation, a 1.02% lift to final equity can correspond to a noticeable absolute amount even though the annual Earn-only rate is only about 0.35%.

## Boundary / residual risks

- Historical Flexible Earn APR series was not reconstructed.
- Nine APR inputs are older than the current date; only TRX was refreshed from the user's current screenshot.
- Binance APRs can change and promotional/tier rates may differ by balance and account.
- Subscription timing, redemption timing and temporary product unavailability are not modeled.
- BNB ancillary rewards are excluded.
- This evidence does not modify or approve any production staking automation.

Production RR, Telegram and manual execution remain unchanged.
