# MIGRATION_PLAN — GRID OSS Forward Candidate v1

Date: 2026-09-26  
Mode: BUILD_NEW_APP  
Scope: additive paper-live profile only  
Production money execution: NO

## Why a migration plan exists

The change adds a new module relationship:

`scripts/run_grid_paper_live.py` → `strategies/crypto/link_level_grid/oss_forward_candidate.py`

Project rules require a migration plan when module relationships change.

## Current state

Paper-live profiles:

- CONTROL_BASE
- CANDIDATE_WIDE
- MICRO_ONLY_WIDE
- MID_ONLY_WIDE

All current profiles are stateless deterministic replays from the frozen paper start.

## Target state

Add one new profile:

- `MID_OSS_ATR50_TRAIL7`

The existing four profiles remain unchanged.

Candidate rules:

- normalized capital: 2000 per symbol;
- MID-only;
- linear depth allocation p=1;
- 1095-candle causal H/L;
- ATR14;
- H/L regrid gate: ATR shift >50%;
- regrid cooldown: 60 daily candles;
- price escape may also force a range refresh after cooldown;
- standard MID recovery: +18 sublevels;
- standard MID percentage targets;
- after target is reached, arm trailing exit;
- actual sell occurs only on a later candle after 7% retracement from the observed post-target peak;
- fee: 10 bps;
- slippage: 5 bps;
- 100% realized proceeds return to the same MID slot;
- no real orders.

## Data compatibility

No database, Sheet, schema, identifier, or persisted account state changes.

Artifacts remain backward-compatible CSV/JSON collections with one additional `profile` value and optional candidate-specific event type `ARM_EXIT`.

Consumers that filter only BUY/SELL remain compatible.

Telegram policy explicitly counts only BUY and SELL. `ARM_EXIT` is diagnostic evidence only.

## Install sequence

1. Add `oss_forward_candidate.py`.
2. Add unit/regression tests.
3. Register `MID_OSS_ATR50_TRAIL7` in paper-live.
4. Add the profile to the existing scheduled workflow.
5. Run full repository tests.
6. Run paper-live runtime smoke.
7. Merge only after green CI.

## Rollback

Rollback is additive and stateless:

1. remove `MID_OSS_ATR50_TRAIL7` from the workflow profile list;
2. remove its PROFILES entry;
3. optionally remove `oss_forward_candidate.py` and its tests.

No market position, broker state, database migration, or durable user data must be unwound.

## Compatibility gates

- Existing four profile outputs must remain unchanged for the same candles/source SHA/config.
- New candidate must reproduce the research harness candidate semantics on a shared deterministic dataset.
- No candidate result may mutate canonical `strategy.py`.
- No candidate result changes the strategy registry status.
- No paper result authorizes live-money execution.

## Residual risk

The candidate parameters were selected on the existing five-asset historical development sample. Forward observation is required to evaluate overfit.
