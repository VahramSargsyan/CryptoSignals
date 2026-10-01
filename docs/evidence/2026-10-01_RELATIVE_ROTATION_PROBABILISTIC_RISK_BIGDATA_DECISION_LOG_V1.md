# Relative Rotation Probabilistic Risk Big-Data — Decision Log V1

Date: 2026-10-01
Status: ACTIVE RESEARCH MEMORY
Workflow mode: STRESS_TEST_ONLY
Production change: NONE

Canonical evidence:

`research/relative_rotation/2026-10-01_RR_PROBABILISTIC_RISK_BIGDATA_V1_EVIDENCE.md`

Runtime:
- run `36819044674`
- commit `877167d19ad388f25634fd09755ba5cf38dd65a9`
- artifact `11141879084`
- SHA256 `01a67bc573db87c85e493398b4b4affaeaad7d4b82df83a03a15c2974749d130`

## Dataset

- 2527 current-U10 RR+DDG executable route episodes
- 899 unique signal dates
- 973 losses
- 443 material losses <= -10%

## Supersession / refinement

This large-data study **supersedes the small 30-trade warning-count interpretation for general route-risk inference**.

Do not use the old monotonic heuristic:
`0-2 warnings safe / 3-4 warnings dangerous`.

Large panel any-loss rates are:
- 0: 13.3%
- 1: 37.3%
- 2: 39.6%
- 3: 41.7%
- 4: 33.8%

Four warnings are not the highest-risk bucket.

## Loss-series state

Large panel:

- streak 0: 29.9% next-route loss
- streak 1: 41.6%
- streak 2+: 53.4%

Unlike the 30-trade sample, loss-series state contains information at scale.

But the effect varies materially by year and is not a universal multiplier.

Classification:

`LOSS_STREAK = KEEP_AS_DYNAMIC_STATE_FEATURE, NOT_STATIC_RULE`

## Validation

Final 1Y any-loss:
- STATE_HYBRID Brier 0.209 vs baseline 0.229
- AUC 0.649

Monthly full-history any-loss:
- STATE_HYBRID Brier 0.270 vs baseline 0.279
- AUC 0.448

2026 monthly walk-forward:
- Brier 0.206 vs baseline 0.237
- AUC 0.694

Therefore the state model is promising but non-stationary.

## Severe loss

For <= -10% loss:
- current-signal HYBRID final 1Y AUC 0.636
- STATE_HYBRID AUC 0.471

Do not use loss streak as a tail-loss predictor.

## Frozen decision

Preferred research architecture becomes two-headed:

1. state/regime model for `P(any negative/chop)`;
2. current-signal model for `P(material loss)`.

No sizing or veto mapping is approved.

## Next research

Dynamic/regime-conditioned Bayesian model is the next justified test.

Optional pre-PEPE history must remain a separate transport stratum.

## Production boundary

No production/live/paper/Telegram/exchange/universe/sizing changes.
