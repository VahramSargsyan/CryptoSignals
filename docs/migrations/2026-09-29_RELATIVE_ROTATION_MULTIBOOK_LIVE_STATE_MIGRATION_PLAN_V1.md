# Relative Rotation Multi-Book Live State — Migration Plan V1

Date: 2026-09-29
Workflow mode: PATCH_FIX

## Objective

Represent the user's two real manual Relative Rotation capital branches without
changing the frozen U10 signal engine:

- BOOK_1: currently held in ATOM
- BOOK_2: currently held in LINK, starting tracked quantity 100 LINK

This is state/monitoring migration only. It does not authorize an exchange order.

## Compatibility strategy

The existing top-level `held_asset` remains as a legacy alias for BOOK_1 so
older tooling does not break.

New canonical live-position state is `position_books`.

Schema version changes from 3 to 4.

## New canonical structure

```json
"position_books": [
  {
    "book_id": "BOOK_1",
    "held_asset": "ATOM",
    "tracking_start": "2026-09-29T00:00:00Z"
  },
  {
    "book_id": "BOOK_2",
    "held_asset": "LINK",
    "quantity": 100.0,
    "initial_quantity": 100.0,
    "tracking_start": "2026-09-29T00:00:00Z"
  }
]
```

The 100 LINK quantity is user-confirmed starting state for forward tracking,
not an inferred exchange fill.

## Runtime changes

1. Evaluate the same frozen U10 routing engine independently for every book.
2. A signal from BOOK_2/LINK is shown as a real BOOK_2 signal, not as an
   independent watch-only message.
3. Assets currently held by any book are removed from the independent
   sunset-watch evaluation for that run.
4. SOL remains an independent sunset/watch asset.
5. Telegram identifies the book and current held asset for every actionable
   signal.
6. The top-level legacy `held_events` and `held_pair_states` continue to
   represent BOOK_1 for backward compatibility.
7. No automatic quantity conversion occurs after a signal. After a real manual
   swap, held asset and received quantity must be updated explicitly.

## Data impact

Additive config/report schema change:
- `position_books`
- `book_events`
- `book_pair_states`

No database/Sheet migration exists.

## Rollback

1. Remove `position_books`.
2. Restore schema_version 3.
3. Keep top-level `held_asset=ATOM`.
4. LINK returns to independent sunset-watch behavior.
5. Preserve any real manual trades in the real rotation log; repository rollback
   must never pretend a real exchange trade did not occur.

## Acceptance

- legacy single-book config still parses;
- BOOK_1 resolves to ATOM;
- BOOK_2 resolves to LINK / 100 LINK;
- both books use TARGET-only destinations;
- LINK is not duplicated in watch events while BOOK_2 holds it;
- current frozen-U10 LINK signal appears under BOOK_2;
- Telegram sender policy remains unchanged;
- unit regressions and live Binance D1 dry run pass;
- no order-placement code is added.

## Residual risks

- BOOK_1 quantity is intentionally not invented because the user did not provide
  it in this task;
- BOOK_2 quantity is a starting snapshot, not independently exchange-verified;
- future manual swaps require explicit quantity/state updates;
- execution delay, spread and slippage remain outside the signal monitor.
