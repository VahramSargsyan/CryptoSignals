# U10 SOL-to-ETH substitution v1 — preregistration

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Status: POST-AUDIT HYPOTHESIS GENERATION

## Derivation

The preceding bull-run confound audit fixed an objective PRIMARY explosive rule before results.

It found:
- SOL is PRIMARY explosive;
- removing SOL from U10 improves mature median return and leaves ATOM-start unchanged;
- SOL's own strongest 90d bull interval contributes no measured positive held-route P&L to U10;
- ETH is not PRIMARY explosive;
- ETH preserves the SMART_CONTRACT_L1 niche.

Therefore the substitution SOL -> ETH is derived mechanically from the audit rather than chosen from future-return ranking.

## Fixed universes

BASE_U10:
ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL, HBAR

DROP_SOL_U9:
ATOM, TWT, PEPE, BNB, TRX, AAVE, LINK, FIL, HBAR

SOL_TO_ETH_U10:
ATOM, TWT, PEPE, BNB, ETH, TRX, AAVE, LINK, FIL, HBAR

For context only:
DROP_HBAR_U9:
ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, FIL

## Frozen strategy

- Binance Spot 1D closed candles
- 180d median
- ARM 15%
- reversal 3%
- strongest confirmed max-dislocation router
- next-open execution
- 0.1% transition cost
- cross-universe medians use the same original eight starters where available:
  ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK.
  For SOL_TO_ETH_U10, ETH replaces SOL as the eighth starter to preserve one starter per slot/niche.
  ATOM-start is always reported separately.

## Windows

1. mature: 2023-10-31 -> 2026-09-26
2. latest year: 2025-09-27 -> 2026-09-26
3. latest two years: 2024-09-27 -> 2026-09-26
4. rolling 12m and 24m monthly-start windows

## PEPE bull-run attribution

Also report performance after neutralizing positive daily equity gains while PEPE is held during its strongest frozen 90d bull interval:
2024-02-23 -> 2024-05-23.

This keeps signal/router paths unchanged and is attribution only.

## Decision guardrail

This is post-hoc hypothesis generation because the substitution was derived after viewing the bull-run audit.
No live membership change is authorized.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
