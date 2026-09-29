# Relative Rotation Pending Execution Reminder — Decision Log V2

Date: 2026-09-30
Workflow mode: PATCH_FIX
Status: IMPLEMENTED_ON_PATCH_BRANCH / PENDING_PROMOTION

## Incident

The 22:30 Armenia reminder did not arrive for the prior morning Relative Rotation signal.

Repository Actions history confirms:
- the evening reminder workflow code existed on main;
- the scheduled expression `30 18 * * *` was configured;
- GitHub Actions did not create the expected 18:30 UTC scheduled run.

Therefore the failure was not Telegram deduplication alone: the scheduled job itself was absent.

## Failure mode

The previous design had one evening cron. If GitHub skipped that run, a signal that had already been delivered once in the morning could receive no second reminder.

The existing sender also limited the evening fallback to same-candle CONFIRMED events.

## Decision

Use redundant scheduled attempts plus unresolved-execution semantics.

A CONFIRMED route remains reminder-eligible while:
- the corresponding book still holds its `from_asset`;
- the event remains inside the bounded replay window.

The latest strongest CONFIRMED route per book is used.

Evening attempts are scheduled for 22:30, 23:00 and 23:30 Armenia. They share one per-candle reminder key so successful delivery is not duplicated.

The next morning also repeats a still-unresolved confirmed signal before falling back to the ordinary rotation snapshot.

## Execution acknowledgement

No explicit button or external execution API is introduced.

The canonical acknowledgement remains the existing manual workflow:
1. user executes the trade;
2. trade is logged;
3. book `held_asset` and quantity are updated.

Once the book no longer holds the original `from_asset`, the old confirmed route disappears from notification candidates.

## Safety boundary

No strategy rule, routing formula, U10 membership, or real-money execution behavior changes.

TEST_LEVEL target:
`UNIT_REGRESSION + GITHUB_ACTIONS_LIVE_PUBLIC_DATA_DRY_RUN`
