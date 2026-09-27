# Relative Rotation Universe Scale v1 — preregistration

Date: 2026-09-27
Mode: STRESS_TEST_ONLY

Purpose: test whether the 8-token relative-rotation graph benefits or degrades when many additional crypto nodes are admitted under the same frozen 180d / 15% ARM / 3% reversal logic.

The live 8-token monitor is not changed.

Fixed tiers before outcome review:

- U8: ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK
- U20: U8 + BTC, ETH, XRP, DOGE, ADA, AVAX, DOT, LTC, BCH, NEAR, UNI, FIL
- U30: U20 + ICP, XLM, ETC, RUNE, CRV, SAND, MANA, OP, ARB, APE
- U40: U30 + GALA, AXS, THETA, VET, ALGO, XTZ, CHZ, ENJ, COMP, LDO

Pair counts: U8=28, U20=190, U30=435, U40=780.

Rules are frozen:
- Binance Spot 1D closed candles;
- rolling median 180;
- ARM 15%;
- reversal confirmation 3%;
- strongest confirmed max-dislocation conflict router;
- next daily open execution;
- 0.1% transition cost.

Evaluation windows are the same five sequential 180-day windows used by the established 8-node research, plus 2025-03-29 -> 2026-03-28 and the already-opened 2026-03-29 -> 2026-09-26 diagnostic window.

This first pass is deliberately UNRESTRICTED topology. NEW_NODE_PROBATION is a separate second pass. No larger universe may be promoted to live use from this experiment alone.

If an asset lacks current/continuous Binance data, the affected tier is marked SKIPPED. It is not silently replaced after seeing performance.

Availability amendment before any U40 outcome existed: EOSUSDT was found stale at 2025-05-26, so U40 was SKIPPED. EOS is replaced with LDO solely for current-data availability; U8/U20/U30 composition is unchanged.
