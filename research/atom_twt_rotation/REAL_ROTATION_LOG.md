# ATOM ↔ TWT Real Rotation Log

Status: ACTIVE_RESEARCH_LOG
Strategy candidate: ATOM_TWT_RELATIVE_ROTATION_V1
Workflow mode: PATCH_FIX
Purpose: preserve real manual rotations for later comparison with paper-live signals.

## Frozen candidate parameters

- Relative pair: TWT/ATOM
- Lookback: 180 days
- ARM threshold: 15%
- Reversal confirmation: 3% from the post-ARM extreme
- Rule: reaching 15% arms the signal; it does not immediately trigger a rotation.
- ATOM side: staking yield is treated separately in strategy evaluation.

## Real rotations

### ROT-20260926-001 — TWT → ATOM

Evidence source: user-provided Binance portfolio screenshots on 2026-09-26.

Observed before state:
- Screenshot time: 15:26 local time
- TWT quantity: 198.3612438519
- Displayed TWT value: approximately 119.08 USDT

Observed after state:
- Screenshot time: 16:49 local time
- ATOM quantity: 64.91320282
- Displayed ATOM value: approximately 118.40 USDT

Derived from observed quantities:
- Direction: TWT → ATOM
- Effective cross ratio: 1 ATOM ≈ 3.055791969 TWT
- Effective reciprocal ratio: 1 TWT ≈ 0.327247408 ATOM
- Displayed-value difference: approximately -0.68 USDT (-0.57%)

Unknown / not inferable from these screenshots:
- exact execution timestamp
- exact execution price in USDT
- exact trading fee
- exact slippage
- transaction/order ID

Important:
The displayed-value difference must not be treated as the exact fee because the before and after screenshots were captured at different times and market prices may have moved.

## Follow-up fields to add when available

- exact execution timestamp
- exchange order/trade ID
- exact fee
- exact TWT execution price
- exact ATOM execution price
- signal state at execution: PREWATCH / ARMED / CONFIRMED / MANUAL
- 180d median TWT/ATOM at execution
- deviation from median at execution
- post-ARM extreme
- retracement from extreme
