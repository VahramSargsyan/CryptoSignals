# U20 Relative Rotation Deep Dive v1 — preregistration

Date: 2026-09-27
Mode: STRESS_TEST_ONLY

Purpose: dissect the fixed 20-token relative-rotation universe without changing the live 8-token monitor and without tuning after outcome review.

U20 is fixed before this run:
ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, BTC, ETH, XRP, DOGE, ADA, AVAX, DOT, LTC, BCH, NEAR, UNI, FIL.

Canonical signal remains 1D closed Binance Spot / rolling median 180d / ARM 15% / reversal 3% / next-open execution / strongest confirmed max-dislocation conflict router / 0.1% transition cost.

Primary horizon is long-cycle, not 180-day windows.

Tests fixed before outcomes:

1. Mature rolling 12-month and 24-month windows, stepped monthly.
2. Longest mature available window after 180-day warm-up. A 36-month mature window is reported only if sufficient history exists; otherwise it is explicitly unavailable rather than fabricated.
3. Compare U20 to U8 using the same eight starting assets, with ATOM-start shown separately.
4. Compare each strategy run to HODL of the same starting asset and to equal-weight U20 over the same window.
5. Node ablation: remove each of the 12 U20 additions one at a time.
6. Marginal admission: add each of the 12 additions one at a time to U8.
7. Full-cycle route/occupancy/transition centrality for U20.
8. Router sensitivity: strongest, weakest, first deterministic candidate, skip conflict.
9. Parameter sensitivity: 120/15/3, 240/15/3, 180/12.5/3, 180/20/3, 180/15/2, 180/15/5 versus canonical 180/15/3.
10. Cost sensitivity: 0.10%, 0.25%, 0.50%, 1.00% per transition.

Primary comparable starts for cross-universe medians are the original eight assets. This avoids mechanically changing the median merely because U20 has twelve additional starting assets.

No result from this run promotes U20 to live. No token is removed or admitted to production based on this single pass.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST