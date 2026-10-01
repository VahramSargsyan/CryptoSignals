# Relative Rotation Dynamic Regime Risk — Decision Log V1

Date: 2026-10-01
Status: ACTIVE RESEARCH MEMORY
Workflow mode: STRESS_TEST_ONLY
Production change: NONE

Canonical evidence:
`research/relative_rotation/2026-10-01_RR_DYNAMIC_REGIME_RISK_V1_EVIDENCE.md`

Runtime:
- GitHub Actions run: `36820703622`
- source commit: `fda7d3321badeeaf66554568a833f0c52d21264b`
- artifact: `11143022507`
- SHA256: `a78ab43099a5fe09d86ee8268e9af11a823f649bb4544d6f6728802524d1cacb`
- TEST_LEVEL: `GITHUB_ACTIONS_LIVE_PUBLIC_DATA_STRESS_TEST`

## Frozen result

All pre-registered dynamic models FAIL the research robustness gate.

### Any-loss full monthly walk-forward

- STATIC_STATE: Brier 0.270 vs baseline 0.279
- REGIME_COMPACT: 0.290
- REGIME_DETAIL: 0.297
- DECAY_365: 0.295
- DECAY_180: 0.301

No dynamic model beats STATIC_STATE.

### Final 1Y any-loss

STATIC_STATE remains strongest:
- Brier 0.209
- AUC 0.649

REGIME_COMPACT:
- Brier 0.255
- AUC 0.373

## Key lesson

`LOSS_STREAK_STATE != SIMPLE_MACRO_REGIME_MULTIPLIER`

Coarse BTC/BREADTH/TOTAL conditioning and fixed 180/365-day recency decay are rejected.

The descriptive interaction surface is non-monotonic:
some apparently more adverse regimes have lower next-loss rates than milder regimes.

## Classification

`DYNAMIC_REGIME_CONDITIONING_V1 = REJECTED_FOR_PREDICTION`

`RECENCY_DECAY_180_365 = REJECTED`

`STATIC_STATE = RESEARCH_CONTROL_ONLY`

## No-repeat

Do not repeat unchanged on the same history.

## Production boundary

No live/paper/Telegram/exchange/universe/veto/sizing changes authorized.
