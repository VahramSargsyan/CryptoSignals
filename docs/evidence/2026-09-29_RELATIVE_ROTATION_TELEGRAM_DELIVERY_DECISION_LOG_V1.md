# Relative Rotation Telegram Delivery Patch Decision Log V1

Date: 2026-09-29
Workflow mode: PATCH_FIX
Status: IMPLEMENTED_ON_PATCH_BRANCH / PENDING_PROMOTION

## Incident

A valid BOOK_2 signal existed:

`LINK -> HBAR / CONFIRMED`

The monitor reported:
- BOOK_2 held = 100 LINK
- confirmed = 1
- armed = 1
- should_notify = true

But the workflow run was triggered by `push`, and the Telegram step was explicitly skipped for push events. The subsequent scheduled run did not occur, so the signal was not delivered.

## Root cause

Two independent conditions combined:

1. Telegram delivery was disabled on push runs.
2. Notification selection only used the latest closed candle and had no persistent delivered/pending state.

Therefore a skipped schedule could permanently strand a valid signal.

## Patch decision

Use a bounded replay + dedupe state architecture.

- live books: ATOM and LINK only;
- replay window: 7 days from current latest closed candle, bounded by monitor_start;
- stable event IDs;
- sender-level deduplication;
- state persisted as a GitHub Actions artifact;
- event marked sent only after successful Telegram API call;
- Telegram allowed on main push, schedule and workflow_dispatch;
- Telegram blocked on pull_request;
- workflow serialized with concurrency.

## Safety boundary

The patch changes notification transport only.

It does not:
- execute swaps;
- change RR formulas;
- change U10 routing;
- infer fills;
- mutate live positions.

## Expected recovery

The missed 2026-09-27 BOOK_2 LINK -> HBAR CONFIRMED event falls inside the replay window and should be delivered once as a clearly labelled recovered historical event. Stale ARMED/PREWATCH events are not replayed. Current-candle BOOK_2 events remain eligible normally. If no matching sent-state artifact exists, the first promoted main run should persist the delivered event IDs after successful Telegram acceptance.

## Promotion gate

Promote only after:
- unit tests PASS;
- workflow PR run PASS;
- live public-data monitor run PASS;
- no Telegram send occurs from PR context.

TEST_LEVEL before promotion must be stated exactly.
