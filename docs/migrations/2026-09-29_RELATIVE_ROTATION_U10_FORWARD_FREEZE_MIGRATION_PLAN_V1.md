# Relative Rotation U10 Forward Freeze — Migration Plan V1

Date: 2026-09-29
Workflow mode: PATCH_FIX + ECOSYSTEM_PLANNING

## Objective

Freeze the final forward-observation TARGET U10 and move the existing
paper-live monitor from transitional TARGET U9 semantics to frozen TARGET U10
semantics without enabling automatic trading.

Frozen TARGET U10:

`TWT, PEPE, BNB, TRX, AAVE, AVAX, FIL, ALGO, XRP, HBAR`

Universe version:

`RR_TARGET_U10_FROZEN_V1`

Forward validation start:

`2026-09-29T00:00:00Z`

No candle before this timestamp may count as forward evidence for the frozen U10.

## Current state before migration

Paper/live config:
- held asset: ATOM
- target: U9
- sunset/watch: ATOM, SOL, LINK, HBAR
- manual execution only: true
- migration mode: true
- defensive overlay: disabled

## Changes

1. HBAR moves from sunset/watch into TARGET.
2. TARGET becomes the frozen 10-asset set above.
3. Sunset/watch becomes:
   - ATOM
   - SOL
   - LINK
4. Current real held asset remains ATOM.
5. Migration mode remains enabled until the real held asset exits the sunset set.
6. Defensive overlay remains disabled.
7. Forward validation metadata is written to config/documentation.
8. Telegram sender is corrected to obey `should_notify`.
9. Sunset watch alerts are relabeled clearly so ARMED/PREWATCH cannot be mistaken
   for a held-asset confirmed rotation.
10. No historical backfill is written into forward evidence.

## Data/schema impact

The JSON config gains additive forward-observation metadata:
- `universe_version`
- `forward_validation_enabled`
- `forward_validation_start`

No database or spreadsheet schema exists here.

## Rollback

Rollback is repository-only:

1. restore TARGET to the previous U9;
2. return HBAR to sunset/watch;
3. restore previous strategy identifier;
4. keep `held_asset=ATOM` unless Vahram has manually executed a later real swap;
5. disable forward validation metadata;
6. do not delete forward evidence already created — mark it invalidated by the rollback commit.

Never roll back a real/manual exchange transaction by changing repository state.

## Acceptance

- unit tests pass;
- live public-data dry run passes;
- TARGET count = 10 and includes HBAR;
- sunset/watch count = 3 and excludes HBAR;
- held asset remains ATOM;
- LINK watch ARM notification explicitly says PREWATCH / not current held-asset swap;
- Telegram sender skips when `should_notify=false`;
- no exchange-order code or API trading key introduced;
- no historical forward evidence backfilled.

## Promotion boundary

This migration freezes the U10 universe for forward/paper observation.
It does not authorize automatic execution or claim clean forward validation yet.
