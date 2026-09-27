# U9 Meme-Node Substitution v1 — Evidence

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Branch: research/u9-meme-node-substitution-v1
GitHub Actions run: 36332216372
Result: PASS
TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST

## Research question

Can PEPE be replaced by another meme asset while keeping the cleaner non-meme core unchanged?

Fixed non-meme core:
ATOM, TWT, BNB, TRX, AAVE, LINK, FIL, HBAR

Tested:
- U8_NO_MEME
- U9_PEPE
- U9_DOGE
- U9_SHIB
- U9_BONK

Frozen engine:
- Binance Spot 1D closed candles
- 180d rolling median
- 15% ARM
- 3% reversal confirmation
- strongest confirmed max-dislocation router
- next-open execution
- 0.1% transition cost

Cross-universe medians use the same eight non-meme starters:
ATOM, TWT, BNB, TRX, AAVE, LINK, FIL, HBAR.

## Fair common window

BONK is the youngest tested meme asset on Binance.

All-candidate common history begins:
2023-12-15

After the required 180 common daily rows of signal warm-up:

FAIR MATURE START:
2024-06-11

End:
2026-09-26

This is methodologically important because PEPE's strongest historical 90d bull interval ended on 2024-05-23.

Therefore the primary fair comparison begins AFTER the PEPE bull-run interval that previously inflated long-history results.

## Historical strongest 90d meme drawups

| Meme | Strongest 90d low-to-high drawup | Interval |
|---|---:|---|
| DOGE | +5416.47% | 2021-01-27 -> 2021-04-19 |
| SHIB | +1206.78% | 2021-09-07 -> 2021-10-27 |
| PEPE | +1155.46% | 2024-02-23 -> 2024-05-23 |
| BONK | +279.94% | 2024-02-06 -> 2024-03-04 |

All four satisfy the preregistered PRIMARY-explosive price-path rule.

However, every listed strongest bull interval occurs BEFORE the fair mature start of 2024-06-11.

Thus own-bull-neutralization is identical to raw performance in the fair common-period test for all four meme candidates.

This is an unusually clean control for the user's bull-run-confound concern.

## Fair common-period result: 2024-06-11 -> 2026-09-26

| Universe | Median return | ATOM-start | Median DD | Positive common starts |
|---|---:|---:|---:|---:|
| U8_NO_MEME | -27.20% | -27.27% | -71.24% | 0/8 |
| U9_DOGE | -27.20% | -27.27% | -71.24% | 0/8 |
| U9_SHIB | -27.20% | -27.27% | -71.24% | 0/8 |
| U9_BONK | +77.48% | +77.33% | -70.23% | 8/8 |
| U9_PEPE | +411.92% | +411.49% | -59.67% | 8/8 |

PEPE remains dramatically stronger even though its February-May 2024 explosive rally is completely outside the evaluated mature window.

Therefore PEPE's superiority in this test cannot be explained by harvesting that historical bull run.

## Latest one-year window

2025-09-27 -> 2026-09-26

| Universe | Median return |
|---|---:|
| U8_NO_MEME | +18.91% |
| U9_DOGE | +18.91% |
| U9_SHIB | +18.91% |
| U9_BONK | -24.16% |
| U9_PEPE | +79.66% |

PEPE again leads on the exact latest year.

ATOM-start for U9_PEPE:
+100.43%

## Latest two-year window

2024-09-27 -> 2026-09-26

| Universe | Median return |
|---|---:|
| U8_NO_MEME | +6.95% |
| U9_DOGE | +6.95% |
| U9_SHIB | +6.95% |
| U9_BONK | +160.77% |
| U9_PEPE | +652.13% |

Again, this two-year interval begins months after PEPE's strongest identified bull-run interval.

## Rolling-window robustness from fair mature start

### Rolling 12m — 15 monthly starts

| Universe | Median 12m | Worst 12m | Positive windows |
|---|---:|---:|---:|
| U8_NO_MEME | -14.43% | -51.22% | 33.33% |
| U9_DOGE | -21.20% | -51.22% | 26.67% |
| U9_SHIB | -8.05% | -50.36% | 33.33% |
| U9_BONK | +70.62% | -40.62% | 66.67% |
| U9_PEPE | +149.21% | +12.32% | 100.00% |

U9_PEPE is the only candidate with all 15 available rolling 12m median windows positive.

### Rolling 24m — 3 available monthly starts

| Universe | Median 24m | Worst 24m | Positive windows |
|---|---:|---:|---:|
| U8_NO_MEME | -26.07% | -42.12% | 33.33% |
| U9_DOGE | -26.07% | -42.12% | 33.33% |
| U9_SHIB | -26.07% | -42.12% | 33.33% |
| U9_BONK | +94.21% | +77.71% | 100.00% |
| U9_PEPE | +404.95% | +242.32% | 100.00% |

The available 24m sample is small because the all-candidate fair start is 2024-06-11.

## Route/topology attribution

### DOGE

ATOM-start occupancy in DOGE:
0%

Entries:
0

Exits:
0

The common-starter results are effectively identical to U8_NO_MEME.

DOGE is an inert node under the canonical router in this fair period.

### SHIB

ATOM-start occupancy in SHIB:
0%

Entries:
0

Exits:
0

The common-starter results are again effectively identical to U8_NO_MEME.

SHIB is also inert under the canonical router in this fair period.

### BONK

ATOM-start occupancy:
24.82%

Entries:
3

Exits:
2

ATOM route:
ATOM -> BNB -> TWT -> FIL -> BONK -> AAVE -> TWT -> ATOM -> FIL -> ATOM -> BONK -> TWT -> AAVE -> HBAR -> BONK

BONK is a real routing node, but it can become a long-duration destination. The ATOM route ends in BONK in the mature window, consistent with stale-hold risk.

### PEPE

ATOM-start occupancy:
17.30%

Entries:
3

Exits:
3

ATOM route:
ATOM -> BNB -> TWT -> FIL -> PEPE -> HBAR -> TWT -> ATOM -> FIL -> ATOM -> PEPE -> AAVE -> TWT -> AAVE -> HBAR -> PEPE -> ATOM

Observed bridge corridors include:
- FIL -> PEPE -> HBAR
- ATOM -> PEPE -> AAVE
- HBAR -> PEPE -> ATOM

Unlike DOGE/SHIB, PEPE is actively used.
Unlike BONK, PEPE also exits cleanly in the observed route.

This is consistent with PEPE being a useful graph bridge/hub, not merely a historical bull-run beneficiary.

## Meme-start diagnostic

Starting directly from each meme over the fair mature window:

- PEPE: +428.95%
- BONK: +85.17%
- DOGE: -22.96%
- SHIB: -22.17%

Meme-start is not used in the fair cross-universe median, but it confirms the same qualitative ranking.

## Main conclusion

The hypothesis that PEPE's value is merely an artifact of its February-May 2024 bull run is rejected by this diagnostic.

The fair common-period test starts after that bull run and PEPE still:
- outperforms the no-meme graph;
- outperforms DOGE;
- outperforms SHIB;
- outperforms BONK;
- improves drawdown;
- creates useful bridge routes;
- maintains strong latest-1Y and latest-2Y results;
- has all available rolling 12m and 24m median windows positive.

DOGE and SHIB are not functional replacements under the current signal/router.
BONK is a functional but substantially weaker replacement with more stale-hold behavior.

## Research baseline implication

Keep PEPE in U9_CLEANER for research.

Current cleaner research universe remains:

ATOM, TWT, PEPE, BNB, TRX, AAVE, LINK, FIL, HBAR

PEPE is still a mandatory real holding for the user, but this test independently supports keeping it for structural reasons as well.

## Limitations

- PEPE was already known to be promising before this test; this remains hypothesis-generation.
- The all-candidate fair mature sample is only about 27.5 months because BONK joined Binance late.
- Only one canonical router and signal parameter set were used here.
- Meme markets can structurally change; historical bridge behavior is not guaranteed to persist.
- The test does not imply PEPE itself will repeat its past price appreciation.

No live files changed.
