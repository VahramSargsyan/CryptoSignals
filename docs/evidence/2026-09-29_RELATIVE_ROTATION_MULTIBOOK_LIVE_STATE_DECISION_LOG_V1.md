# Relative Rotation Multi-Book Live State Decision Log V1

Date: 2026-09-29
Workflow mode: PATCH_FIX
Status: ACTIVE_FORWARD_MULTIBOOK / MANUAL_EXECUTION_ONLY

## Canonical live books

The frozen-U10 forward monitor now tracks two real manual capital branches.

### BOOK_1

- held asset: ATOM
- role: primary branch
- quantity: not recorded in this config update
- provenance: existing real TWT -> ATOM branch

### BOOK_2

- held asset: LINK
- starting tracked quantity: 100 LINK
- role: secondary branch
- provenance: user-confirmed live position at forward start

The 100 LINK value is a user-confirmed starting snapshot. It is not reconstructed
from exchange history and is not an inferred fill.

## Compatibility

Top-level `held_asset=ATOM` remains as a backward-compatible alias for BOOK_1.

Canonical live state is now `position_books`.

Config schema version: 4.

## Runtime semantics

Every configured book:
- uses the same frozen U10;
- uses the same 180d median / 15% ARM / 3% reversal engine;
- uses the same strongest-confirmed max-dislocation router;
- is evaluated independently from its own currently held asset;
- may only rotate into TARGET;
- remains manual execution only.

Assets held by a real book are excluded from independent sunset-watch evaluation
for that run, preventing duplicate/misleading alerts.

## Current expected signal presentation

Under the latest verified pre-forward closed candle, LINK has a confirmed
LINK -> HBAR route under the frozen U10 and a separate LINK -> FIL ARM/prewatch.

With the multi-book state:
- LINK -> HBAR must be displayed under BOOK_2;
- LINK -> FIL must be displayed as another BOOK_2 prewatch;
- neither may be labeled as an unrelated LINK watch;
- BOOK_1/ATOM remains independent.

A confirmed signal is still not evidence that a manual exchange was executed.
After any real swap, that book's held asset and actual received quantity must be
updated explicitly.

## Migration

See:

`docs/migrations/2026-09-29_RELATIVE_ROTATION_MULTIBOOK_LIVE_STATE_MIGRATION_PLAN_V1.md`

## Production boundary

- no automatic exchange execution;
- no private trading keys;
- no automatic config mutation after signals;
- no inferred quantity updates.

## Residual risks

- BOOK_1 quantity is intentionally unknown in this patch;
- BOOK_2 100 LINK is user-confirmed but not exchange-verified by the repository;
- manual execution delay/spread/slippage may differ from the model;
- future convergence of two books into the same TARGET token is allowed unless a
  separately approved multibook diversification rule is introduced.
