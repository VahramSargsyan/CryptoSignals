# RR PROBABILISTIC RISK BIG-DATA V1 — EVIDENCE

Date: 2026-10-01
Workflow mode: STRESS_TEST_ONLY
Production/live changes: NONE

## Runtime identity

- Research branch: `research-run/probabilistic-risk-bigdata-v1`
- GitHub Actions run: `36819044674`
- Source commit: `877167d19ad388f25634fd09755ba5cf38dd65a9`
- Artifact ID: `11141879084`
- Artifact SHA256: `01a67bc573db87c85e493398b4b4affaeaad7d4b82df83a03a15c2974749d130`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Research question

Does the probabilistic RR risk concept survive when expanded from the 30 physically visited completed trades to a much larger causal panel of all executable current-U10 source/date RR+DDG route opportunities?

## Dataset semantics

Current U10 only:

`TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR`

For every date and every source asset where current RR+DDG produced an executable route:

1. signal at close T;
2. execute destination at next daily open;
3. hold destination until its next executable RR+DDG route;
4. exit at next-open;
5. compute 0.1% entry + 0.1% exit cost;
6. attach only signal-time warning/context features.

This is a counterfactual route-opportunity panel, not a claim that all routes were simultaneously held.

Anti-leak rule:
a training label is available only after that route has fully exited.

## Size

- route episodes: **2527**
- unique signal dates: **899**
- losses: **973 / 2527 = 38.5%**
- material losses <= -10%: **443 / 2527 = 17.5%**
- all 10 sources and all 10 destinations represented

This is over 80x larger than the 30-trade forensic path sample.

## Warning-count finding: small-sample monotonicity did not survive

Large-panel any-loss rates:

| Warning domains | N | Losses | Loss rate | Median route return |
|---:|---:|---:|---:|---:|
| 0 | 15 | 2 | 13.3% | +16.6% |
| 1 | 169 | 63 | 37.3% | +7.5% |
| 2 | 603 | 239 | 39.6% | +7.0% |
| 3 | 1027 | 428 | 41.7% | +5.5% |
| 4 | 713 | 241 | **33.8%** | **+12.7%** |

Large-panel material-loss rates:

| Warning domains | N | Material losses | Rate |
|---:|---:|---:|---:|
| 0 | 15 | 0 | 0.0% |
| 1 | 169 | 31 | 18.3% |
| 2 | 603 | 139 | **23.1%** |
| 3 | 1027 | 200 | 19.5% |
| 4 | 713 | 73 | **10.2%** |

Therefore:

`WARNING_COUNT_MONOTONIC_RISK = NOT_GENERALIZED`

The 30-trade observation that all losses clustered at 3-4 warnings was useful hypothesis generation but is not a general route-level law.

## Final 1Y holdout

Discovery training uses 1656 routes whose outcomes were fully known before 2025-09-27.

Validation:
- 755 completed routes
- 233 losses
- 89 material losses

### Any-loss target

| Model | Brier | Constant baseline | AUC |
|---|---:|---:|---:|
| COUNT | 0.228 | 0.229 | 0.434 |
| HYBRID current-signal | 0.222 | 0.229 | 0.481 |
| **STATE_HYBRID** | **0.209** | 0.229 | **0.649** |

STATE_HYBRID:
- warning count
- prior negative exit-batch streak
- recent five-completed-route loss rate

was materially stronger on the final 1Y any-loss holdout.

Validation calibration for STATE_HYBRID:

| Predicted band | N | Losses | Observed loss rate |
|---|---:|---:|---:|
| 30-45% | 446 | 99 | 22.2% |
| 45-60% | 252 | 80 | 31.7% |
| >=60% | 57 | 54 | **94.7%** |

Important caveat:
the >=60% group had many small losses, not severe losses. Only about 3.5% of that high-any-loss group were <= -10% in a supplemental diagnostic.

Thus the state model appears to detect **chop / probability of finishing below entry**, not necessarily tail-loss magnitude.

### Material-loss target <= -10%

| Model | Brier | Baseline | AUC |
|---|---:|---:|---:|
| COUNT | 0.108 | 0.111 | 0.542 |
| **HYBRID current-signal** | **0.106** | 0.111 | **0.636** |
| STATE_HYBRID | 0.116 | 0.111 | 0.471 |

For severe losses, recent loss-series state does not help.

Current-signal strength/reversal/breadth is the more useful exploratory signal.

## Monthly causal walk-forward

Across 2303 later route episodes, each month is predicted using only routes fully completed before that month.

### Any-loss target

| Model | Brier | Baseline | AUC |
|---|---:|---:|---:|
| COUNT | 0.282 | 0.279 | 0.368 |
| HYBRID | 0.290 | 0.279 | 0.362 |
| **STATE_HYBRID** | **0.270** | 0.279 | 0.448 |

STATE_HYBRID improves probability calibration modestly but does not rank losses well over the entire history.

Year-level STATE_HYBRID walk-forward diagnostics:

| Year | N | Loss rate | Brier model | Brier baseline | AUC |
|---:|---:|---:|---:|---:|---:|
| 2024 | 894 | 56.7% | 0.322 | 0.339 | 0.460 |
| 2025 | 892 | 22.6% | 0.254 | **0.243** | 0.364 |
| 2026 | 517 | 36.8% | **0.206** | 0.237 | **0.694** |

This is strong evidence of non-stationarity.

The state layer is useful in some regimes, especially 2026, but not as a universal fixed relationship.

## Losing-streak state on the large panel

Across all 2527 route episodes:

| Prior negative exit-batch streak | Routes | Next losses | Loss rate | Median next route |
|---|---:|---:|---:|---:|
| 0 | 1280 | 383 | 29.9% | +15.4% |
| 1 | 642 | 267 | 41.6% | +5.2% |
| 2+ | 605 | 323 | **53.4%** | **-1.3%** |

This directly reverses the misleading small 30-trade sample, where 2+ prior negative batches happened to be followed by 0/4 losses.

Thus:

`LOSS_STREAK_SIGNAL_EXISTS_ON_LARGE_ROUTE_PANEL`

but it is not temporally stationary.

By year:

| Year | streak 0 loss rate | streak 1 | streak 2+ |
|---:|---:|---:|---:|
| 2023 | 10.1% | 44.0% | 20.0% |
| 2024 | 52.0% | 59.4% | 61.4% |
| 2025 | 19.3% | 33.1% | 23.8% |
| 2026 | 26.0% | 22.8% | **82.6%** |

Therefore the streak behaves more like a market/strategy regime-state variable than a timeless monotonic risk multiplier.

## Leave-source-out transport

Hold out every source asset completely, fit on the other nine, and predict the held-out source.

Any-loss:

| Model | Brier | Baseline | AUC |
|---|---:|---:|---:|
| COUNT | 0.238 | 0.237 | 0.469 |
| HYBRID | 0.236 | 0.237 | 0.542 |
| **STATE_HYBRID** | **0.225** | 0.237 | **0.622** |

This supports some cross-source transportability of the state layer, but source holdout is not a time holdout.

## Interpretation

The large-data pass changes the research conclusion.

### Rejected / weakened

- simple warning-count monotonic probability curve;
- probability based only on 0-4 warning count;
- small-sample inference that loss streak had no value.

### Survives / becomes more interesting

1. A probabilistic layer remains viable.
2. Recent strategy/route loss state contains substantial information on the large panel.
3. That information is **regime-dependent and time-varying**.
4. Current-signal features appear more useful for predicting severe loss.
5. State features appear more useful for predicting ordinary negative/choppy outcomes.

This suggests a future architecture with two probability targets:

- `P(any negative / chop)` — state/regime-aware;
- `P(material downside)` — current signal/network/market-quality aware.

Do not collapse them into one risk number yet.

## Research classification

`COUNT_PROBABILITY_MODEL = NOT_GENERALIZED`

`STATEFUL_ANY_LOSS_MODEL = PROMISING_BUT_NONSTATIONARY`

`HYBRID_MATERIAL_LOSS_MODEL = PROMISING_BUT_EXPLORATORY`

`PROBABILITY_BASED_POSITION_SIZING = NOT_APPROVED`

## Next admissible research

1. regime-conditioned / dynamic Bayesian model;
2. explicit time-varying coefficient or regime interaction for streak state;
3. genuinely forward shadow probability cards;
4. optional older-history transport test using compatible pre-PEPE universes, kept as a separate stratum rather than mixed silently with U10.

## Production boundary

No production/live/paper/Telegram/exchange/universe/sizing behavior changed.

## Residual risks

- 2527 route episodes are not 2527 independent market experiments;
- many routes share dates, assets, and macro regimes;
- counterfactual routes were not all physically held;
- warning-domain definitions originated from known U10 history;
- state relationship is strongly non-stationary;
- large N reduces variance but does not remove structural dependence or selection bias.
