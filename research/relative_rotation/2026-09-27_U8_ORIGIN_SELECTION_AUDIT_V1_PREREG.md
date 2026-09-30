# U8 Origin & Selection Audit v1 — preregistration

Date: 2026-09-27
Mode: STRESS_TEST_ONLY

## Question

Determine why the canonical 8-token relative-rotation universe
ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK
performed unusually well, and whether its construction logic can causally produce other useful 8-token universes.

No live configuration is changed.

## Historical reconstruction

Repository history shows:
1. ATOM/TWT existed first as the seed relative-rotation strategy.
2. A PEPE pair screen then tested a documented liquid candidate pool.
3. The strongest PEPE candidates were BNB, SOL, TRX, AAVE, LINK.
4. The first full 8-node graph was exactly ATOM + TWT + PEPE + those five PEPE shortlist assets.

The original PEPE screen used history through 2026-03-28. Therefore later calling 2025-03-29 -> 2026-03-28 a graph OOS window did not make universe selection itself out-of-sample. This audit explicitly separates selection history from future evaluation.

## Documented original candidate pool

ATOM, TWT, PEPE, BNB, SOL, TRX, AAVE, LINK, AVAX, FIL, ETH, ALGO, ADA, XRP, HBAR

No new token is added to this first audit.

## Frozen strategy

- Binance Spot 1D closed candles
- rolling median: 180 days
- ARM: 15%
- reversal confirmation: 3%
- strongest confirmed max-dislocation router
- next-open execution
- 0.1% transition cost

No parameter tuning is allowed in this audit.

## Primary causal selection date

2024-10-31

Training graph window:
2023-10-31 -> 2024-10-31

Future evaluation:
2024-11-01 -> 2026-09-26
approximately 23 months.

## Secondary causal selection date

2025-03-28

Training graph window:
2024-03-29 -> 2025-03-28

Future evaluation:
2025-03-29 -> 2026-09-26
approximately 18 months.

## Tests fixed before results

1. Reconstruct the PEPE-star rule using only data available at each selection date.
2. Build PEPE causal U8 as:
   ATOM + TWT + PEPE + top five PEPE neighbours by pair-screen score.
3. Repeat the same hub-and-five-neighbours construction for every non-seed hub in the documented 15-token pool.
4. Exhaustively enumerate all 1,716 U8 sets containing ATOM and TWT:
   choose six of the remaining thirteen tokens.
5. Rank every U8 only by its one-year TRAINING graph median across its own eight possible starting assets.
6. Evaluate every already-defined U8 on the future period without re-selection.
7. Measure train-vs-future rank correlation.
8. Record original U8 training rank, future rank and percentile.
9. Record the training-selected top U8's future rank.
10. Record how many causally selected hub sets outperform the historical U8 in the future period.
11. Reconstruct the PEPE shortlist at the historical 2026-03-28 cutoff as a consistency check only; it is not a causal selection experiment.

## Pair-screen ranking

For each hub/target pair:
- one-year evaluation windows;
- window starts shifted by approximately 60 days;
- both starting assets tested;
- primary score: median excess versus 50/50 HODL;
- tie-breakers: fraction beating 50/50, median excess versus best HODL, symbol.

## Interpretation guardrail

The best future-performing set is hindsight evidence only and must never be treated as a selection rule.
Only a set chosen from training data before its future evaluation can count as causal evidence.

TEST_LEVEL target: GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST
