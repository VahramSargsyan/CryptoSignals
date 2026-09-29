# Relative Rotation Pending Execution Reminder — Migration Plan V2

Date: 2026-09-30
Workflow mode: PATCH_FIX

## Objective

Prevent a confirmed manual Relative Rotation signal from disappearing after its first Telegram delivery when the user has not yet executed the trade.

The live book itself is the execution acknowledgement:
- while the book still holds the signal's `from_asset`, the signal is treated as unresolved;
- after the real trade is recorded and the book `held_asset` changes, that old signal is no longer eligible for reminder delivery.

## Scope

Changed:
- notification reminder semantics;
- evening schedule redundancy;
- reminder-ID namespace stored in the existing notification-state artifact.

Unchanged:
- Relative Rotation pair mathematics;
- 180d median;
- 15% ARM;
- 3% reversal confirmation;
- strongest-confirmed router;
- U10 universe;
- manual execution only;
- exchange-order behavior (none).

## Reminder schedule

Primary morning run:
- 00:20 UTC (~04:20 Armenia)

Evening reminder attempts:
- 18:30 UTC (~22:30 Armenia)
- 19:00 UTC (~23:00 Armenia)
- 19:30 UTC (~23:30 Armenia)

All three evening runs share the same closed-candle reminder ID, so only the first successful one sends Telegram.

If all evening scheduled runs are missed by GitHub, the next morning run checks the unresolved confirmed signal again.

## Reminder selection

For each live book:
1. consider only CONFIRMED candidates whose `from_asset` equals the currently configured `held_asset`;
2. use the most recent confirmed date in the replay window;
3. if several confirms exist on that date, use the same strongest-dislocation ordering as the canonical router;
4. ignore ARMED/PREWATCH for execution reminders.

This means a completed trade automatically stops reminders after the live book is updated.

## State IDs

Existing artifact and JSON schema remain structurally unchanged:

`relative-rotation-notification-state`

```json
{
  "schema_version": 1,
  "sent_event_ids": []
}
```

New operational IDs are additive:

- `MORNING_PENDING|<latest_closed_candle>|<event_id>`
- `EVENING_PENDING|<latest_closed_candle>|<event_id>`

The `latest_closed_candle` component allows one morning and one evening reminder on a new daily candle while preventing duplicate retries for the same candle.

## Backward compatibility

Existing base event IDs and earlier `EVENING_REMINDER|...` IDs remain readable and harmless. No state rewrite is required.

## Rollback

Revert:
- `.github/workflows/relative-rotation-paper-live-v1.yml`
- `scripts/send_relative_rotation_report.py`
- `tests/test_send_relative_rotation_report.py`

Existing notification-state artifacts may remain; unused reminder IDs are inert.

## Acceptance

- initial CONFIRMED event can be sent once;
- evening reminder can repeat the same unresolved confirmed event even after the initial event ID is marked sent;
- 23:00/23:30 retry runs deduplicate after a successful 22:30 send;
- a stale-but-still-unresolved confirmed event can be reminded;
- the next morning reminds again if the held asset was not changed;
- after held_asset changes, the old route is absent from monitor candidates and cannot remind;
- ARMED/PREWATCH is not repeated as an execution reminder;
- PR tests pass;
- live-data dry run passes;
- no real orders are introduced.

## Residual risks

- GitHub Actions can theoretically skip all scheduled runs;
- fallback therefore relies on the next successfully executed scheduled run;
- if a real trade is executed but the book config/log is not updated, reminders will continue;
- notification state is stored in GitHub Actions artifacts, not a transactional database.
