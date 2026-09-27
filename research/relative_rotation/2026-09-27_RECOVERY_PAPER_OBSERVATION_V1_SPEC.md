# Recovery Paper Observation V1 — Build Specification

Date: 2026-09-27
Workflow mode: BUILD_NEW_APP
Status: IMPLEMENTATION SPEC / PAPER ONLY

## 1. Objective

Create a forward-only paper-observation module for the current relative-rotation defensive universe.

The module must record what the frozen recovery models know on each newly closed D1 candle, without changing trading behavior.

It is an evidence collector, not a trading strategy.

## 2. Frozen universe

- ATOM
- TWT
- PEPE
- BNB
- SOL
- TRX
- AAVE
- LINK

Source: Binance Spot canonical D1 candles through the existing repository data adapter.

## 3. Frozen stress semantics

- stress entry: third consecutive close with breadth <= 3;
- recovery: third consecutive close with breadth >= 5;
- breadth: count of assets above causal SMA200;
- episode start/end remain the confirming close, matching V1/V2 research semantics.

## 4. Frozen recovery evidence

### State model

Features:

- breadth_sma200;
- median_sma200_gap.

Model:

- scikit-learn LogisticRegression;
- L2;
- C=1.0;
- solver=lbfgs;
- max_iter=1000;
- StandardScaler fit on training rows only;
- episode-balanced sample weights;
- only prior completed episodes are training data;
- confirmed recovery close excluded from training rows.

Recorded horizons:

- P(recovery <= 7d)
- P(recovery <= 14d)

These probabilities are evidence only and MUST NOT create BUY/SELL/exit commands.

### Duration / uncertainty layer

Reuse frozen V2 survival implementation:

- Kaplan-Meier;
- Weibull;
- duration-support status;
- prior completed episode durations plus current right-censored age.

Recorded horizons:

- 7d / 14d / 30d / 45d / 60d

The survival channel is context only.
It MUST NOT override state evidence or place trades.

## 5. Forward-only rule

Each scheduled run observes only the latest fully closed UTC D1 candle.

The row is keyed by that candle timestamp.

If the same candle already exists in the journal:

- do not append a duplicate;
- report ALREADY_OBSERVED;
- do not rewrite the historical prediction.

Future facts are never written back into the raw prediction row.

A separate derived resolved-history file may later attach realized recovery outcomes to historical predictions.

## 6. Paper journal fields

At minimum:

- observation_candle;
- generated_at;
- model_id;
- source_commit_sha;
- dataset identity summary;
- market_status;
- stress_episode_id;
- stress_start;
- episode_age_days;
- stress_entry_streak;
- recovery_confirmation_streak;
- completed_training_episodes;
- current breadth;
- current median SMA200 gap;
- current breadth delta 7d;
- state P7;
- state P14;
- KM / Weibull probabilities 7/14/30/45/60;
- KM / Weibull median remaining;
- duration support status;
- max prior completed duration;
- latest completed stress end/duration;
- note that all probabilities are PAPER_ONLY.

## 7. Persistent evidence

GitHub code branch owns implementation.

A separate branch:

`paper/recovery-observations-v1`

owns the small append-only observation journal and derived resolved-history CSV.

The scheduled workflow may write only:

- `paper_observations/recovery_v1/observations.csv`
- `paper_observations/recovery_v1/resolved_history.csv`

This keeps daily evidence commits out of strategy/code history.

Raw market history is re-downloadable and is not persisted in GitHub.

## 8. Historical reproduction gate

Before any forward row may be appended, the module must reproduce the frozen current-universe Breadth+Gap development benchmark at the 2026-03-28 cutoff.

Expected episode-balanced Brier:

- 7d ~= 0.2104
- 14d ~= 0.2066
- mean ~= 0.2085

A material mismatch fails closed and prevents journal mutation.

## 9. External dependencies

Existing:

- pandas 2.2.3
- lifelines 0.30.3
- choix 0.4.1
- evalica 0.4.2

New runtime dependency:

- scikit-learn 1.8.0

scikit-learn 1.8.0 is BSD-licensed. Only its public API is used; no upstream source code is copied.

## 10. Outputs per run

Artifact directory:

`paper_artifacts/recovery_paper_observation_v1/<run_id>/`

Files:

- observation.json
- observation.csv
- episode_catalog.csv
- resolved_history.csv
- run_manifest.json
- report.md

## 11. Acceptance evidence

Before merge/promotion:

1. py_compile passes;
2. focused unit tests pass;
3. historical reproduction gate passes;
4. manual GitHub Actions run completes;
5. current observation is produced from a fully closed D1 candle;
6. rerun on the same candle proves idempotency;
7. no production/paper-live strategy files are modified;
8. no BUY/SELL/order execution code exists in the module.

## 12. Rollback

Disable/remove the workflow.

The observation branch can be retained as historical evidence or deleted independently.

No production rollback and no data migration are required.

## 13. Promotion path

BUILD -> PAPER_OBSERVATION -> accumulate future unseen episodes -> score resolved predictions -> only then consider a separately preregistered capital experiment.

No automatic path to LIVE_SIGNALS exists.
