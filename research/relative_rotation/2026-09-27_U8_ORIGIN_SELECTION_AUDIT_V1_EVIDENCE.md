# U8 Origin & Selection Audit v1 — Evidence

Date: 2026-09-27
Mode: STRESS_TEST_ONLY
Branch: research/u8-origin-selection-audit-v1
GitHub Actions run: 36319615870
Result: PASS
TEST_LEVEL: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST

## 1. Historical origin of U8

Repository chronology on 2026-09-26:

1. ATOM/TWT already existed as the seed relative-rotation strategy.
2. Commit 61c5752b2964cb0bffe03be351d2ed256fae6f66 defined the next task as a PEPE relative-rotation search.
3. Commit 16298262aa913d655aeca7541838c39e34fd0823 recorded the preliminary PEPE pair scan.
4. That scan's strongest five documented PEPE candidates were:
   BNB, SOL, TRX, AAVE, LINK.
5. Commit f11942b60859249f2cbfc50fa43774c9c962b8f4 then created the first full graph:
   ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK.

Therefore the historical U8 was not a random market basket. It was effectively:

ATOM/TWT seed + PEPE hub + five historically shortlisted PEPE neighbours.

## 2. Universe-selection leakage

The preliminary PEPE screen used common history through 2026-03-28.

The first full graph then called 2025-03-29 -> 2026-03-28 its primary OOS window.

Signal execution inside that period was causal, but the universe itself had been selected with data that included the same period.

Therefore:
- GRAPH_SIGNAL_OOS: partially valid as a routing/signal test;
- UNIVERSE_SELECTION_OOS: not valid for that window.

This does not invalidate U8. It means historical U8 performance must be interpreted as strategy + curated universe, not as a clean proof of the universe-selection rule.

## 3. Historical PEPE-screen reconstruction

Using current Binance public closed-daily data and the preregistered reconstruction method at the historical 2026-03-28 cutoff produced:

BNB, TRX, AAVE, SOL, AVAX

Overlap with the documented historical top five:
4/5.

LINK was replaced by AVAX in the reconstruction.

Interpretation:
- the broad PEPE-hub logic is reproducible;
- the exact fifth member is sensitive to dataset / implementation details;
- the historical shortlist cannot currently be reproduced bit-for-bit from repository evidence alone.

## 4. Exhaustive causal audit universe

Documented original candidate pool:

ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, AVAX, FIL, ETH, ALGO, ADA, XRP, HBAR.

ATOM and TWT were fixed seeds.

Every possible choice of six additional assets from the remaining thirteen was enumerated:

1,716 distinct U8 universes.

Frozen signal/router:
- 1D Binance closed candles
- 180d median
- 15% ARM
- 3% reversal
- next-open
- 0.1% transition cost
- strongest confirmed max-dislocation router

No signal parameter was tuned.

## 5. Primary causal experiment

Selection cutoff:
2024-10-31.

Training:
2023-10-31 -> 2024-10-31.

Future:
2024-11-01 -> 2026-09-26, approximately 23 months.

### Historical U8

Assets:
ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK.

Training median:
+139.29%.

Training rank:
46 / 1,716.

Future median:
+319.20%.

Future ATOM-start:
+333.58%.

Future rank:
515 / 1,716.

Sets with higher future median:
514.

Historical U8 was strong but not exceptional among all possible U8 compositions.

### Causal PEPE-star reconstruction

Training-only PEPE top five at 2024-10-31:

BNB, TRX, ETH, AAVE, SOL.

Causal U8:
ATOM, TWT, PEPE, BNB, TRX, ETH, AAVE, SOL.

Future median:
+238.91%.

Future rank:
770 / 1,716.

The causal PEPE-star rule did not reconstruct the later historical U8 and underperformed it.

### Best TRAINING U8

Training-only winner:

ATOM, TWT, PEPE, BNB, TRX, AAVE, FIL, ETH.

Training median:
+183.41%.

Future median:
+170.63%.

Future rank:
973 / 1,716.

Therefore simple historical-return maximization did not identify a superior future universe.

### Train/future relationship

Spearman-style rank correlation across all 1,716 universes:
+0.316.

Past one-year universe performance had only weak predictive relationship with the following approximately 23 months.

## 6. Secondary causal experiment

Selection cutoff:
2025-03-28.

Training:
2024-03-29 -> 2025-03-28.

Future:
2025-03-29 -> 2026-09-26, approximately 18 months.

### Historical U8

Training median:
-20.50%.

Training rank:
292 / 1,716.

Future median:
+153.38%.

Future ATOM-start:
+152.76%.

Future rank:
337 / 1,716.

Sets with higher future median:
336.

### Causal PEPE-star

Training-only PEPE top five:

BNB, TRX, AAVE, XRP, SOL.

Causal U8:
ATOM, TWT, PEPE, BNB, TRX, AAVE, XRP, SOL.

Future median:
+116.14%.

Future rank:
522 / 1,716.

### Best TRAINING U8

ATOM, TWT, BNB, SOL, LINK, ALGO, XRP, HBAR.

Training median:
+5.82%.

Future median:
+10.36%.

Future rank:
1,342 / 1,716.

### Train/future relationship

Rank correlation:
-0.026.

In this experiment, one-year past universe performance had essentially no predictive relation to future universe performance.

## 7. Historical U8 is not uniquely magical

Across the two causal future periods:

- 514 / 1,716 sets beat historical U8 in the primary future period.
- 336 / 1,716 sets beat it in the secondary future period.
- 308 sets beat historical U8 in BOTH future periods.

Therefore the evidence does not support a claim that the exact historical U8 composition is uniquely optimal.

It remains a good working universe with real forward evidence, but many other eight-node compositions were historically stronger.

## 8. Hub-style alternatives

Using the same structural template:

ATOM + TWT + HUB + five training-ranked neighbours

produced other useful future graphs.

Primary experiment:
- ETH hub was the strongest future hub-set:
  ATOM, TWT, ETH, HBAR, XRP, BNB, AAVE, ADA
- future median: +346.43%
- it slightly beat historical U8's +319.20%
- however its training hub rank was only 5/13.

Secondary experiment:
- TRX hub was the strongest future hub-set:
  ATOM, TWT, TRX, AAVE, LINK, PEPE, SOL, ADA
- future median: +226.38%
- it beat historical U8's +153.38%
- training hub rank was only 5/13.

The hub concept can generate other strong networks, but choosing the hub from prior return alone is not validated.

## 9. Strong post-hoc network-synergy discovery

This section is DISCOVERY_ONLY / HINDSIGHT and must not be promoted directly to live.

The strongest robust-by-future-rank family contained an unexpected structure.

Among the 50 sets with the best worst rank across both future periods:
- PEPE appeared in 50/50
- FIL appeared in 50/50
- HBAR appeared in 50/50
- AAVE appeared in 28/50
- ETH appeared in 0/50

This is notable because the original PEPE pair screen classified:
- FIL as secondary / weaker;
- HBAR as rejected;
- ALGO as rejected.

Yet the best future set in the primary experiment was:

ATOM, TWT, PEPE, BNB, AAVE, FIL, ALGO, HBAR.

Its results:
- primary future median: +1,921.91%
- primary future rank: 1/1,716
- secondary future median: +543.57%
- secondary future rank: 3/1,716
- median max DD in these future evaluations: about -53.73%
- training ranks: only 434/1,716 and 1,435/1,716.

This set is NOT a live candidate because it was discovered using future outcomes.

It is evidence that pairwise screening can miss network-level complementarity.

### Interaction evidence

There are 120 possible U8 sets in this constrained pool containing PEPE + FIL + HBAR.

Median future return of those 120 sets:
- primary future period: +973.76%
- secondary future period: +348.77%.

All 120 were positive in both future evaluations.

For the 36 sets also containing AAVE:
- primary median future return: +1,382.93%
- secondary median future return: +510.10%.

These periods overlap, so this is not independent validation. It is a strong discovery hypothesis only.

## 10. Main interpretation

The earlier list-construction logic was asymmetric:

- U8 was historically curated around a PEPE pair screen.
- U20 was primarily a scale-expansion experiment.

Therefore U8 vs U20 was not a clean comparison of graph size alone.

More importantly, the exhaustive audit shows:

1. Eight is not proven to be a magic node count.
2. Historical U8 is not a uniquely optimal set.
3. Maximizing prior one-year graph return is a poor universe selector.
4. Pairwise GOOD_PAIR / BAD_PAIR screening is also insufficient.
5. Network interactions matter materially.
6. Tokens weak alone can become valuable bridges in a graph.
7. The next selection research should focus on causal topology / complementarity / bridge value, not on adding more tokens indiscriminately or choosing the highest past-return set.

## 11. Production decision

Canonical live graph remains unchanged:

ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK.

No production files were changed.

Reason:
- U8 has live/forward continuity and is already operational;
- alternative sets found here are research discoveries;
- the strongest alternatives were identified using future data and therefore cannot be promoted cleanly.

## 12. Next research gate

Preregister a topology-selection rule BEFORE evaluating its future period.

Candidate research features:
- bridge utility / unique transition coverage;
- cycle coverage;
- stale-hold penalty;
- route diversity without naive diversification;
- node marginal contribution across multiple training subwindows;
- conflict-router dependence;
- pair complementarity rather than standalone pair return.

The discovery hypothesis PEPE + FIL + HBAR (+ possibly AAVE) may be used to design topology features, but must not be hard-coded as a preferred production set.
