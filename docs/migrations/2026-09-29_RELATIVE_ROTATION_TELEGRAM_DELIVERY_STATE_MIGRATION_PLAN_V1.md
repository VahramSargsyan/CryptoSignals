# Relative Rotation Telegram Delivery State — Migration Plan V1

Date: 2026-09-29
Workflow mode: PATCH_FIX

## Objective

Prevent Telegram loss for the two configured live Relative Rotation books:

- BOOK_1: ATOM
- BOOK_2: 100 LINK

The patch must recover recent unsent ARMED/CONFIRMED events and suppress duplicate delivery across push, schedule, and manual workflow runs.

## Scope

Changed runtime behavior is limited to notification delivery.

Unchanged:
- Relative Rotation pair math;
- 180d median;
- 15% ARM;
- 3% reversal confirmation;
- strongest-confirmed router;
- TARGET U10 membership;
- manual execution only;
- exchange-order behavior (none).

## State schema

A small runtime state artifact is introduced:

`relative-rotation-notification-state`

File:

`relative_rotation_notification_state.json`

Schema:

```json
{
  "schema_version": 1,
  "sent_event_ids": []
}
```

Stable event ID:

`BOOK_ID|EVENT|DATE|FROM|TO|PAIR`

The state is operational notification metadata only. It is not strategy evidence and does not change a book holding.

## Replay window

Each run exposes current-book ARMED/CONFIRMED events from a bounded 7-day replay window, never earlier than the configured monitor start.

Reason:
- recover a signal if a scheduled run is delayed or skipped;
- allow the existing LINK -> HBAR CONFIRMED event from 2026-09-27 to be delivered after the notification patch;
- replay prior CONFIRMED events but keep ARMED/PREWATCH delivery limited to the latest closed candle;
- prevent unbounded historical replay if the dedupe artifact expires.

Forward-validation semantics remain anchored at 2026-09-29T00:00:00Z and are not backfilled.

## Tracked positions

Operational alert tracking is restricted to the two live books:
- ATOM
- LINK

Independent sunset-watch alerts are disabled by setting `watch_assets=[]`.

Sunset routing classification remains unchanged:
- ATOM
- SOL
- LINK

## Workflow behavior

1. Pull-request runs test/verify only and never send Telegram.
2. Push, schedule, and workflow_dispatch runs restore latest notification state artifact.
3. Monitor creates stable replay candidates for current books.
4. Sender removes already-sent event IDs.
5. Unsent candidates are delivered.
6. Event IDs are persisted only after Telegram send succeeds.
7. If Telegram secrets are missing or send fails, event IDs remain unsent so a later run can retry.
8. Workflow concurrency serializes Relative Rotation runs to reduce duplicate-delivery races.

## Rollback

Rollback is repository-only:

1. restore prior workflow;
2. restore prior sender;
3. restore prior monitor candidate logic;
4. restore prior `watch_assets` value if independent sunset watches are desired;
5. ignore/delete the notification-state artifacts.

Do not alter any real/manual crypto position as part of rollback.

## Acceptance

- unit tests cover replay of LINK -> HBAR for BOOK_2;
- unit tests prove duplicate suppression;
- unit tests prove missing Telegram secrets do not mark an event sent;
- SOL is not treated as an active live-book notification candidate;
- PR workflow passes;
- live-data dry run still identifies BOOK_1=ATOM and BOOK_2=LINK correctly;
- after merge, main push run may deliver the recovered unsent LINK -> HBAR event exactly once;
- no automatic trading code is introduced.

## Residual risks

- GitHub Actions artifacts are operational state, not a transactional database;
- an outage longer than the 7-day replay window can still lose a notification;
- concurrent execution risk is reduced by workflow concurrency but GitHub itself remains an external dependency;
- Telegram delivery acknowledgment only confirms the Telegram API accepted the send, not that the user read it.
