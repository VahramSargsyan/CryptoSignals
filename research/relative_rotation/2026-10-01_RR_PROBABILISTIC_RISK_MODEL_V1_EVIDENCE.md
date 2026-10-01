# RR PROBABILISTIC RISK MODEL V1 — EVIDENCE

Date: 2026-10-01
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Runtime identity

- Research branch: `research-run/probabilistic-risk-model-v1`
- GitHub Actions run: `36816834403`
- Source commit: `5b53854931df9b857683a4ac4eff7a621fb78c01`
- Artifact ID: `11141124671`
- Artifact SHA256: `3c11d6e102faaa568f9e631856548a7dc7a44fa70d3e178cab13a384a8324ed8`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Research question

Can the frozen negative-trade forensic features be converted into a conservative probabilistic risk estimate for each new Relative Rotation signal, and does recent losing-trade sequence state add predictive information?

This is a probability-model research pass. It does not authorize a veto, sizing rule, or live execution change.

## Dataset

Current U10+DDG:
`TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR`

MATURE baseline:
- median strategy return: +3588.4%

Unique completed forensic episodes:
- 30 trades
- 7 losses
- 5 material losses <= -10%

The warning-domain definitions were inherited from the already documented negative-trade forensic study.

## Models locked before results

### COUNT_PRIMARY

Single predictor:
- number of independent warning domains, centered around 2.

This model was frozen as the primary model before runtime because the sample is very small.

### DOMAINS_DIAGNOSTIC

Four domain indicators separately:
1. RR confirmation caution
2. network/destination caution
3. market caution
4. TOTAL/ratio caution

### HYBRID_EXPLORATORY

- warning-domain count
- effective RR strength
- primary reversal size
- U10 breadth50

### STATE_HYBRID_EXPLORATORY

Added before the first probabilistic runtime after the user proposed loss-series state:

- warning-domain count
- prior negative exit-batch streak
- loss rate among the latest five fully completed unique episodes

State features are causal:
only trades fully completed before the current signal are used.

Because the forensic table contains alternative-path episodes, the streak is defined on chronological exit-date batches rather than pretending all deduplicated episodes form one realized path.

## Bayesian specification

Bayesian logistic models were fit with explicit shrinkage priors.

- intercept prior centered around 25% event probability
- coefficient priors centered at zero
- smaller prior SD for more complex models
- posterior probability intervals reported

The purpose is to reduce overconfidence from a 30-trade sample, not to create information that does not exist.

## Primary probability curve — any loss

| Warning domains | Trades | Losses | Empirical | Beta-binomial mean (90%) | Shrunk logistic mean (90%) |
|---:|---:|---:|---:|---|---|
| 0 | 1 | 0 | 0.0% | 33.3% (2.5–77.6%) | 8.4% (0.9–27.7%) |
| 1 | 2 | 0 | 0.0% | 25.0% (1.7–63.1%) | 11.6% (2.7–28.5%) |
| 2 | 8 | 0 | 0.0% | 10.0% (0.6–28.4%) | 17.2% (7.5–31.1%) |
| 3 | 11 | 4 | 36.4% | 38.5% (18.0–61.0%) | 26.7% (15.1–40.3%) |
| 4 | 8 | 3 | 37.5% | 40.0% (16.9–65.6%) | 40.2% (20.5–62.8%) |

Important:
- the model does not assign 0% risk to the historically clean 0–2 buckets;
- uncertainty is large;
- risk increases with warning count after shrinkage.

## Primary probability curve — material loss <= -10%

| Warning domains | Trades | Material losses | Shrunk probability mean (90%) |
|---:|---:|---:|---|
| 0 | 1 | 0 | 10.1% (1.0–32.8%) |
| 1 | 2 | 0 | 11.8% (2.7–29.4%) |
| 2 | 8 | 0 | 15.0% (6.2–27.9%) |
| 3 | 11 | 3 | 20.5% (10.5–33.1%) |
| 4 | 8 | 2 | 28.9% (12.1–51.2%) |

The material-loss target is even more data-starved: only five events.

## LOO calibration — COUNT primary

Any loss:

| Predicted band | N | Losses | Mean predicted | Observed |
|---|---:|---:|---:|---:|
| <15% | 3 | 0 | 11.2% | 0.0% |
| 15–30% | 19 | 4 | 22.8% | 21.1% |
| 30–45% | 8 | 3 | 40.4% | 37.5% |

This is encouraging descriptive calibration, but it is not true OOS evidence because the warning-domain definitions themselves came from known history.

## Model validation

### Any-loss target

| Model | Status | LOO Brier | LOO baseline | LOO AUC | Prequential Brier | Preq baseline | Preq AUC |
|---|---|---:|---:|---:|---:|---:|---:|
| COUNT | PRIMARY | 0.176 | 0.191 | 0.609 | 0.253 | 0.269 | 0.440 |
| DOMAINS | DIAGNOSTIC | 0.165 | 0.191 | 0.752 | 0.250 | 0.269 | 0.418 |
| HYBRID | EXPLORATORY | 0.168 | 0.191 | 0.708 | **0.224** | 0.269 | **0.626** |
| STATE HYBRID | EXPLORATORY_STATEFUL | 0.165 | 0.191 | 0.689 | 0.261 | 0.269 | 0.473 |

Prequential evaluation:
- first 10 chronological trades are training only;
- each later trade is predicted using earlier observations.

The ordinary HYBRID is the strongest tested prequential model for any loss.

It improves Brier from 0.269 baseline to 0.224 and achieves AUC 0.626.

This is exploratory, not promotion evidence.

### Material-loss target

| Model | Prequential Brier | Baseline | Prequential AUC |
|---|---:|---:|---:|
| COUNT | 0.207 | 0.210 | 0.400 |
| DOMAINS | 0.207 | 0.210 | 0.280 |
| HYBRID | 0.207 | 0.210 | 0.507 |
| STATE HYBRID | **0.227** | 0.210 | 0.413 |

There is no convincing material-loss model yet.

## Loss-series / streak finding

The user proposed that a sequence of losing trades might itself increase risk.

Historical causal state diagnostics:

| Prior negative exit-batch streak | Trades | Next losses | Loss rate | Median next trade |
|---|---:|---:|---:|---:|
| 0 | 23 | 6 | 26.1% | +19.8% |
| 1 | 3 | 1 | 33.3% | +2.3% |
| 2+ | 4 | **0** | **0.0%** | **+61.1%** |

Latest-five completed trade loss rate:
- >=40%: 13 trades, 2 losses, 15.4% loss rate
- >=60%: 5 trades, 2 losses, 40.0% loss rate

High warning count (>=3) plus prior negative streak >=1:
- 6 trades
- 1 loss
- 16.7% loss rate
- median trade +30.7%

Therefore there is currently **no evidence that a losing streak should mechanically increase the next-trade risk estimate**.

The fitted STATE_HYBRID posterior actually assigns negative median coefficients to:
- prior negative streak
- recent five-trade loss rate

but both 90% posterior intervals cross zero / odds-ratio 1.

This must not be interpreted as evidence that losing streaks are bullish. The sample is too small.

Frozen interpretation:

`LOSS_STREAK_PERSISTENCE_NOT_SUPPORTED_IN_CURRENT_SAMPLE`

## Key implication

The strongest current probability architecture is not:

`current risk + losing streak multiplier`

It is closer to:

`current signal quality + network/market warning context + RR strength/reversal/breadth`

The stateful layer should remain logged, but it should not receive positive risk weight unless future observations support it.

## Production use

Not approved:
- no automatic HOLD threshold
- no position-size mapping from probability
- no live probability-based execution
- no loss-streak escalation
- no probability threshold promotion

Potential next research:
1. forward shadow probability cards for every new RR signal;
2. freeze COUNT primary and HYBRID exploratory formulas;
3. collect genuinely unseen outcomes;
4. assess calibration and Brier/log-loss on forward data;
5. only then test a preregistered probability-to-position-size mapping.

## Fundamental layer

Point-in-time fundamental data remains unavailable and excluded.

## No-repeat rule

Do not rerun unchanged on the same 30-trade history merely to reproduce these values.

A new run requires:
- new completed forward trades;
- corrected strategy semantics;
- new point-in-time fundamental data;
- or a separately preregistered probability/sizing hypothesis.

## Production boundary

No production/live/Telegram/exchange/universe/sizing behavior changed.
