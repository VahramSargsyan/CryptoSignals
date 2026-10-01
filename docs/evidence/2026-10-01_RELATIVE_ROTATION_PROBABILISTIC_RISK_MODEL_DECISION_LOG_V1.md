# Relative Rotation Probabilistic Risk Model — Decision Log V1

Date: 2026-10-01
Status: ACTIVE RESEARCH MEMORY
Workflow mode: STRESS_TEST_ONLY
Production change: NONE

Canonical evidence:

`research/relative_rotation/2026-10-01_RR_PROBABILISTIC_RISK_MODEL_V1_EVIDENCE.md`

Runtime:
- run: `36816834403`
- commit: `5b53854931df9b857683a4ac4eff7a621fb78c01`
- artifact: `11141124671`
- SHA256: `3c11d6e102faaa568f9e631856548a7dc7a44fa70d3e178cab13a384a8324ed8`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Frozen decisions

1. Bayesian probability modeling is viable as a **risk-attention layer**, not as a production veto.
2. Primary model remains `COUNT_PRIMARY`, frozen before runtime.
3. Approximate shrunk any-loss probabilities by warning count:
   - 0 warnings: 8.4%
   - 1: 11.6%
   - 2: 17.2%
   - 3: 26.7%
   - 4: 40.2%
4. These values have wide uncertainty and must not be treated as precise true probabilities.
5. Ordinary exploratory HYBRID (warning count + strength + reversal + breadth) had the best prequential any-loss metrics:
   - Brier 0.224 vs 0.269 baseline
   - AUC 0.626
6. STATE_HYBRID with recent loss-series information did not improve forecasting:
   - Brier 0.261
   - AUC 0.473
7. Two-plus prior negative exit batches were followed by 0/4 losses in this tiny sample.
   This does not justify a bullish streak rule; it only rejects the hypothesis that loss streak should currently be an automatic risk multiplier.
8. Material-loss prediction remains weak because there are only five <= -10% events.

## Classification

`PROBABILITY_LAYER = PROMISING_FOR_FORWARD_SHADOW_LOGGING`

`LOSS_STREAK_ESCALATION = NOT_SUPPORTED`

`PROBABILITY_BASED_SIZING = NOT_YET_APPROVED`

## Next admissible step

Freeze the probability formulas and record risk cards prospectively for new RR signals.

A future sizing study should use genuinely unseen observations or be explicitly labeled post-selection if performed now.

## Production boundary

No live/paper/Telegram/exchange/universe/position-size behavior changed.
